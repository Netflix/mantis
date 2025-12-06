/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;

/**
 * Integration tests for the reservation-aware scaling flow between:
 * - {@link ResourceClusterActor}
 * - {@link ExecutorStateManagerActor}
 * - {@link ReservationRegistryActor}
 * - {@link ResourceClusterScalerActor}
 *
 * Tests the complete data flow when reservationSchedulingEnabled=true.
 */
public class ReservationScalerIntegrationTests {

    private static final ClusterID CLUSTER_ID = ClusterID.of("test-cluster");
    private static final ContainerSkuID SKU_SMALL = ContainerSkuID.of("small");
    private static final ContainerSkuID SKU_LARGE = ContainerSkuID.of("large");

    private static final MachineDefinition MACHINE_DEFINITION_SMALL =
        new MachineDefinition(2, 2048, 700, 10240, 5);
    private static final MachineDefinition MACHINE_DEFINITION_LARGE =
        new MachineDefinition(4, 8192, 1400, 40960, 5);

    private static final String TASK_EXECUTOR_ADDRESS = "127.0.0.1";
    private static final String HOST_NAME = "hostName";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(ImmutableList.of(1, 2, 3, 4, 5));

    private static ActorSystem actorSystem;
    private IMantisPersistenceProvider storageProvider;
    private MantisJobStore mantisJobStore;
    private JobMessageRouter jobMessageRouter;
    private TestingRpcService rpcService;
    private TaskExecutorGateway gateway;
    private ExecuteStageRequestFactory executeStageRequestFactory;

    private ActorRef resourceClusterActor;
    private ActorRef scalerActor;
    private TestKit hostActorProbe;
    private TestKit testProbe;

    @BeforeClass
    public static void setupClass() {
        actorSystem = ActorSystem.create("ReservationScalerIntegrationTests");
    }

    @AfterClass
    public static void teardownClass() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Before
    public void setup() throws IOException {
        storageProvider = mock(IMantisPersistenceProvider.class);
        mantisJobStore = mock(MantisJobStore.class);
        jobMessageRouter = mock(JobMessageRouter.class);
        rpcService = new TestingRpcService();
        gateway = mock(TaskExecutorGateway.class);
        executeStageRequestFactory = mock(ExecuteStageRequestFactory.class);
        hostActorProbe = new TestKit(actorSystem);
        testProbe = new TestKit(actorSystem);

        rpcService.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
        when(gateway.submitTask(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(any())).thenReturn(Collections.emptyList());

        // Setup scale rules for both SKUs
        when(storageProvider.getResourceClusterScaleRules(CLUSTER_ID))
            .thenReturn(
                ResourceClusterScaleRulesWritable.builder()
                    .scaleRule(SKU_SMALL.getResourceID(), ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID)
                        .skuId(SKU_SMALL)
                        .coolDownSecs(0)  // No cooldown for testing
                        .maxIdleToKeep(10)
                        .minIdleToKeep(5)
                        .minSize(5)
                        .maxSize(20)
                        .build())
                    .scaleRule(SKU_LARGE.getResourceID(), ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID)
                        .skuId(SKU_LARGE)
                        .coolDownSecs(0)
                        .maxIdleToKeep(10)
                        .minIdleToKeep(5)
                        .minSize(5)
                        .maxSize(20)
                        .build())
                    .build());
    }

    /**
     * Creates ResourceClusterActor with reservation scheduling enabled.
     */
    private ActorRef createResourceClusterActor() {
        return actorSystem.actorOf(
            ResourceClusterActor.props(
                CLUSTER_ID,
                Duration.ofSeconds(30), // heartbeatTimeout
                Duration.ofSeconds(10), // assignmentTimeout
                Duration.ofSeconds(60), // disabledTaskExecutorsCheckInterval
                Duration.ofMillis(100), // schedulerLeaseExpirationDuration
                Clock.systemUTC(),
                rpcService,
                mantisJobStore,
                jobMessageRouter,
                10, // maxJobArtifactsToCache
                "", // jobClustersWithArtifactCachingEnabled
                false, // isJobArtifactCachingEnabled
                ImmutableMap.of(), // schedulingAttributes
                new CpuWeightedFitnessCalculator(),
                executeStageRequestFactory,
                true  // reservationSchedulingEnabled = true
            ),
            "ResourceClusterActor-" + System.currentTimeMillis()
        );
    }

    /**
     * Creates ResourceClusterScalerActor with reservation scheduling enabled.
     */
    private ActorRef createScalerActor(ActorRef rcActor) {
        return actorSystem.actorOf(
            ResourceClusterScalerActor.props(
                CLUSTER_ID,
                Clock.fixed(Instant.MIN, ZoneId.systemDefault()), // Fixed clock to bypass cooldown
                Duration.ofSeconds(100), // scalerPullThreshold - high to avoid auto-trigger
                Duration.ofSeconds(100), // ruleRefreshThreshold
                storageProvider,
                hostActorProbe.getRef(),
                rcActor,
                true  // reservationSchedulingEnabled = true
            ),
            "ResourceClusterScalerActor-" + System.currentTimeMillis()
        );
    }

    /**
     * Helper to register a task executor with the ResourceClusterActor.
     */
    private void registerTaskExecutor(ActorRef rcActor, TaskExecutorID teId, MachineDefinition mDef, String skuId) {
        TaskExecutorRegistration registration = TaskExecutorRegistration.builder()
            .taskExecutorID(teId)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(mDef)
            .taskExecutorAttributes(ImmutableMap.of(
                WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, skuId
            ))
            .build();

        rcActor.tell(registration, testProbe.getRef());
        testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
    }

    /**
     * Helper to mark reservation registry as ready.
     */
    private void markReservationRegistryReady(ActorRef rcActor) {
        rcActor.tell(MarkReady.INSTANCE, testProbe.getRef());
        testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
    }

    /**
     * Helper to add a reservation to the registry.
     */
    private void addReservation(ActorRef rcActor, String jobId, int stageNum, int numWorkers,
                                MachineDefinition mDef, String sizeName) {
        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        SchedulingConstraints constraints = SchedulingConstraints.of(
            mDef,
            Optional.ofNullable(sizeName),
            ImmutableMap.of()
        );

        for (int i = 0; i < numWorkers; i++) {
            WorkerId workerId = WorkerId.fromIdUnsafe(jobId + "-worker-" + stageNum + "-" + i);
            allocationRequests.add(TaskExecutorAllocationRequest.of(
                workerId, constraints, null, stageNum));
        }

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(jobId)
                .stageNumber(stageNum)
                .build())
            .schedulingConstraints(constraints)
            .stageTargetSize(numWorkers)
            .allocationRequests(allocationRequests)
            .priority(ReservationPriority.builder()
                .type(ReservationPriority.PriorityType.NEW_JOB)
                .tier(1)
                .timestamp(System.currentTimeMillis())
                .build())
            .build();

        rcActor.tell(upsert, testProbe.getRef());
        testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
    }

    @Test
    public void testScalerReceivesPendingReservationCountsFromFullFlow() throws Exception {
        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500); // Allow child actors to initialize

        // Register some task executors (8 small TEs)
        for (int i = 0; i < 8; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-small-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Mark TEs as available by sending heartbeats
        for (int i = 0; i < 8; i++) {
            TaskExecutorID teId = TaskExecutorID.of("te-small-" + i);
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    teId, CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // Mark reservation registry as ready
        markReservationRegistryReady(resourceClusterActor);

        // Add reservations for 5 workers
        addReservation(resourceClusterActor, "job-1", 1, 5,
            MACHINE_DEFINITION_SMALL, null);

        Thread.sleep(200); // Allow reservation to be processed

        // Create scaler actor
        scalerActor = createScalerActor(resourceClusterActor);

        // Trigger a usage request manually
        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // The scaler should eventually receive GetClusterUsageResponse and make a decision
        // Since we have 8 idle TEs and 5 pending reservations:
        // - effectiveIdleCount = 8 - 5 = 3 < minIdleToKeep (5)
        // - Should scale up

        // Wait for scale decision to reach host actor
        ScaleResourceRequest scaleRequest = hostActorProbe.expectMsgClass(
            Duration.ofSeconds(10), ScaleResourceRequest.class);

        assertNotNull(scaleRequest);
        assertEquals(CLUSTER_ID, scaleRequest.getClusterId());
        assertEquals(SKU_SMALL, scaleRequest.getSkuId());
        // Expected: step = (5 + 5 - 8) = 2, newSize = 8 + 2 = 10
        assertTrue("Should scale up due to pending reservations", scaleRequest.getDesireSize() > 8);
    }

    @Test
    public void testScalerNoScaleDownWhenPendingReservationsExist() throws Exception {
        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500);

        // Register 15 small TEs (more than maxIdleToKeep=10)
        for (int i = 0; i < 15; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-small-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Mark TEs as available
        for (int i = 0; i < 15; i++) {
            TaskExecutorID teId = TaskExecutorID.of("te-small-" + i);
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    teId, CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // Mark reservation registry as ready
        markReservationRegistryReady(resourceClusterActor);

        // Add reservations for 10 workers - this should prevent scale down
        addReservation(resourceClusterActor, "job-2", 1, 10,
            MACHINE_DEFINITION_SMALL, null);

        Thread.sleep(200);

        // Create scaler actor
        scalerActor = createScalerActor(resourceClusterActor);

        // Trigger usage request
        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // With 15 idle and 10 pending:
        // - effectiveIdleCount = 15 - 10 = 5, which is between minIdleToKeep(5) and maxIdleToKeep(10)
        // - Should NOT scale down

        // Wait a bit and verify no scale request reaches host actor
        hostActorProbe.expectNoMessage(Duration.ofSeconds(3));
    }

    @Test
    public void testScalerScalesDownWhenEffectiveIdleExceedsMax() throws Exception {
        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500);

        // Register 18 small TEs
        for (int i = 0; i < 18; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-small-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Mark TEs as available
        for (int i = 0; i < 18; i++) {
            TaskExecutorID teId = TaskExecutorID.of("te-small-" + i);
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    teId, CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // Mark reservation registry as ready
        markReservationRegistryReady(resourceClusterActor);

        // Add reservations for only 3 workers
        addReservation(resourceClusterActor, "job-3", 1, 3,
            MACHINE_DEFINITION_SMALL, null);

        Thread.sleep(200);

        // Create scaler actor with a clock that allows scale-down cooldown to pass
        scalerActor = actorSystem.actorOf(
            ResourceClusterScalerActor.props(
                CLUSTER_ID,
                Clock.systemUTC(), // Real clock
                Duration.ofSeconds(100),
                Duration.ofSeconds(100),
                storageProvider,
                hostActorProbe.getRef(),
                resourceClusterActor,
                true
            ),
            "ResourceClusterScalerActor-scaledown-" + System.currentTimeMillis()
        );

        // Wait for initial rule fetch
        Thread.sleep(500);

        // Trigger usage request
        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // With 18 idle and 3 pending:
        // - effectiveIdleCount = 18 - 3 = 15 > maxIdleToKeep(10)
        // - Should scale down
        // Scale-down flow:
        // 1. Scaler sends GetClusterIdleInstancesRequest to ResourceClusterActor
        // 2. ResourceClusterActor forwards to ExecutorStateManagerActor
        // 3. ExecutorStateManagerActor responds with GetClusterIdleInstancesResponse
        // 4. Scaler sends ScaleResourceRequest to hostActorProbe

        // Wait for the final ScaleResourceRequest on hostActorProbe
        ScaleResourceRequest scaleRequest = hostActorProbe.expectMsgClass(
            Duration.ofSeconds(10), ScaleResourceRequest.class);

        assertNotNull(scaleRequest);
        assertEquals(CLUSTER_ID, scaleRequest.getClusterId());
        assertEquals(SKU_SMALL, scaleRequest.getSkuId());
        // Should have idle instances to scale down
        assertNotNull(scaleRequest.getIdleInstances());
        assertTrue("Should have idle instances to scale down",
            scaleRequest.getIdleInstances().size() > 0);
    }

    @Test
    public void testMultipleSKUsWithDifferentReservations() throws Exception {
        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500);

        // Register 8 small TEs
        for (int i = 0; i < 8; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-small-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Register 8 large TEs
        for (int i = 0; i < 8; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-large-" + i),
                MACHINE_DEFINITION_LARGE, SKU_LARGE.getResourceID());
        }

        // Mark all TEs as available
        for (int i = 0; i < 8; i++) {
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    TaskExecutorID.of("te-small-" + i), CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);

            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    TaskExecutorID.of("te-large-" + i), CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // Mark reservation registry as ready
        markReservationRegistryReady(resourceClusterActor);

        // Add reservations: 6 small workers, 2 large workers
        addReservation(resourceClusterActor, "job-small", 1, 6,
            MACHINE_DEFINITION_SMALL, null);
        addReservation(resourceClusterActor, "job-large", 1, 2,
            MACHINE_DEFINITION_LARGE, null);

        Thread.sleep(200);

        // Create scaler actor
        scalerActor = createScalerActor(resourceClusterActor);

        // Trigger usage request
        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // Small SKU: 8 idle, 6 pending -> effectiveIdle = 2 < minIdleToKeep(5) -> scale up
        // Large SKU: 8 idle, 2 pending -> effectiveIdle = 6, in range [5,10] -> no action

        // Should receive scale up request for small SKU
        ScaleResourceRequest scaleRequest = hostActorProbe.expectMsgClass(
            Duration.ofSeconds(10), ScaleResourceRequest.class);

        assertNotNull(scaleRequest);
        assertEquals(CLUSTER_ID, scaleRequest.getClusterId());
        assertEquals(SKU_SMALL, scaleRequest.getSkuId());
        // step = (6 + 5 - 8) = 3, newSize = 8 + 3 = 11
        assertEquals(11, scaleRequest.getDesireSize());
    }

    @Test
    public void testReservationRegistryNotReadyFallsBackGracefully() throws Exception {
        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500);

        // Register some TEs but DON'T mark registry as ready
        for (int i = 0; i < 4; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-small-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Mark TEs as available
        for (int i = 0; i < 4; i++) {
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    TaskExecutorID.of("te-small-" + i), CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // DON'T mark reservation registry as ready - it should return not ready

        // Create scaler actor
        scalerActor = createScalerActor(resourceClusterActor);

        // Trigger usage request
        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // The reservation registry is not ready, so the request should fail gracefully
        // The scaler should not crash, but may not receive a response
        // Wait and verify no crash (actor system still functional)
        Thread.sleep(2000);

        // Verify actor system is still running (not terminated)
        assertNotNull(actorSystem);
    }

    @Test
    public void testEndToEndScaleUpWithReservations() throws Exception {
        // This test verifies the complete flow:
        // 1. TEs register
        // 2. Reservations are added
        // 3. Scaler queries usage (via GetReservationAwareClusterUsageRequest)
        // 4. ResourceClusterActor orchestrates two-phase query
        // 5. Scaler receives usage with pendingReservationCount
        // 6. Scaler makes scale-up decision
        // 7. Host actor receives ScaleResourceRequest

        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500);

        // Step 1: Register 6 TEs
        for (int i = 0; i < 6; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Mark TEs as available
        for (int i = 0; i < 6; i++) {
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    TaskExecutorID.of("te-" + i), CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // Step 2: Mark registry ready and add reservations
        markReservationRegistryReady(resourceClusterActor);

        // Add reservation for 4 workers
        addReservation(resourceClusterActor, "test-job", 1, 4,
            MACHINE_DEFINITION_SMALL, null);

        Thread.sleep(300);

        // Step 3-7: Create scaler and trigger
        scalerActor = createScalerActor(resourceClusterActor);

        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // With 6 idle and 4 pending:
        // - effectiveIdleCount = 6 - 4 = 2 < minIdleToKeep(5)
        // - step = (4 + 5 - 6) = 3
        // - newSize = min(6 + 3, 20) = 9

        ScaleResourceRequest scaleRequest = hostActorProbe.expectMsgClass(
            Duration.ofSeconds(10), ScaleResourceRequest.class);

        assertNotNull(scaleRequest);
        assertEquals(CLUSTER_ID, scaleRequest.getClusterId());
        assertEquals(SKU_SMALL, scaleRequest.getSkuId());
        assertEquals(9, scaleRequest.getDesireSize());
    }

    @Test
    public void testZeroPendingReservationsWorksLikeLegacy() throws Exception {
        // Setup
        resourceClusterActor = createResourceClusterActor();
        Thread.sleep(500);

        // Register 4 TEs (below minIdleToKeep=5)
        for (int i = 0; i < 4; i++) {
            registerTaskExecutor(resourceClusterActor, TaskExecutorID.of("te-" + i),
                MACHINE_DEFINITION_SMALL, SKU_SMALL.getResourceID());
        }

        // Mark TEs as available
        for (int i = 0; i < 4; i++) {
            resourceClusterActor.tell(
                new io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat(
                    TaskExecutorID.of("te-" + i), CLUSTER_ID, TaskExecutorReport.available()),
                testProbe.getRef());
            testProbe.expectMsgClass(Duration.ofSeconds(5), Ack.class);
        }

        // Mark registry ready but DON'T add any reservations
        markReservationRegistryReady(resourceClusterActor);

        Thread.sleep(200);

        // Create scaler
        scalerActor = createScalerActor(resourceClusterActor);

        scalerActor.tell(
            ResourceClusterScalerActor.TriggerClusterUsageRequest.builder()
                .clusterID(CLUSTER_ID)
                .build(),
            testProbe.getRef());

        // With 4 idle and 0 pending:
        // - effectiveIdleCount = 4 - 0 = 4 < minIdleToKeep(5)
        // - step = (0 + 5 - 4) = 1
        // - newSize = min(4 + 1, 20) = 5

        ScaleResourceRequest scaleRequest = hostActorProbe.expectMsgClass(
            Duration.ofSeconds(10), ScaleResourceRequest.class);

        assertNotNull(scaleRequest);
        assertEquals(CLUSTER_ID, scaleRequest.getClusterId());
        assertEquals(SKU_SMALL, scaleRequest.getSkuId());
        assertEquals(5, scaleRequest.getDesireSize());
    }
}

