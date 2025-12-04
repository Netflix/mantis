/*
 * Copyright 2025 Netflix, Inc.
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
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.ResourceClusterReservationAwareScheduler;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.rpc.RpcService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;

/**
 * Integration tests for the reservation scheduling flow between:
 * - {@link ResourceClusterReservationAwareScheduler}
 * - {@link ResourceClusterAkkaImpl}
 * - {@link ResourceClustersManagerActor}
 * - {@link ResourceClusterActor}
 *
 * Tests the complete message routing and processing flow when reservationSchedulingEnabled=true.
 */
public class ResourceClusterReservationSchedulerIntegrationTests {

    private static final ClusterID CLUSTER_ID = ClusterID.of("test-cluster-integration");
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(2, 2048, 700, 10240, 5);
    private static final String JOB_ID = "test-job-1";
    private static final int STAGE_NUMBER = 1;

    private static ActorSystem actorSystem;
    private IMantisPersistenceProvider storageProvider;
    private MantisJobStore mantisJobStore;
    private JobMessageRouter jobMessageRouter;
    private TestingRpcService rpcService;
    private MasterConfiguration masterConfiguration;
    private ExecuteStageRequestFactory executeStageRequestFactory;
    private MantisPropertiesLoader propertiesLoader;

    private ActorRef resourceClustersManagerActor;
    private ResourceCluster resourceCluster;
    private ResourceClusterReservationAwareScheduler scheduler;

    @BeforeClass
    public static void setupClass() {
        actorSystem = ActorSystem.create("ResourceClusterReservationSchedulerIntegrationTests");
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
        masterConfiguration = mock(MasterConfiguration.class);
        executeStageRequestFactory = mock(ExecuteStageRequestFactory.class);
        propertiesLoader = new DefaultMantisPropertiesLoader(System.getProperties());

        // Setup MasterConfiguration mocks
        when(masterConfiguration.getHeartbeatIntervalInMs()).thenReturn(30000);
        when(masterConfiguration.getAssignmentIntervalInMs()).thenReturn(10000);
        when(masterConfiguration.getSchedulerLeaseExpirationDurationInMs()).thenReturn(100);
        when(masterConfiguration.getMaxJobArtifactsToCache()).thenReturn(10);
        when(masterConfiguration.getJobClustersWithArtifactCachingEnabled()).thenReturn("");
        when(masterConfiguration.isJobArtifactCachingEnabled()).thenReturn(false);
        when(masterConfiguration.getSchedulingConstraints()).thenReturn(ImmutableMap.of());
        when(masterConfiguration.getFitnessCalculator()).thenReturn(new CpuWeightedFitnessCalculator());
        when(masterConfiguration.getAvailableTaskExecutorMutatorHook()).thenReturn(null);
        when(masterConfiguration.isReservationSchedulingEnabled()).thenReturn(true);

        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(any())).thenReturn(Collections.emptyList());
        when(mantisJobStore.getJobArtifactsToCache(any())).thenReturn(Collections.emptyList());

        // Setup scale rules mock - ResourceClusterScalerActor needs this during preStart()
        when(storageProvider.getResourceClusterScaleRules(any(ClusterID.class)))
            .thenReturn(ResourceClusterScaleRulesWritable.builder().build());

        // Create ResourceClustersManagerActor
        ActorRef hostActorProbe = new TestKit(actorSystem).getRef();
        resourceClustersManagerActor = actorSystem.actorOf(
            ResourceClustersManagerActor.props(
                masterConfiguration,
                Clock.systemUTC(),
                rpcService,
                mantisJobStore,
                hostActorProbe,
                storageProvider,
                jobMessageRouter
            ),
            "ResourceClustersManagerActor-" + System.currentTimeMillis()
        );

        // Create ResourceClusterAkkaImpl that talks to ResourceClustersManagerActor
        resourceCluster = new ResourceClusterAkkaImpl(
            resourceClustersManagerActor,
            Duration.ofSeconds(15),
            CLUSTER_ID,
            new LongDynamicProperty(propertiesLoader, "rate.limit.perSec", 10000L)
        );

        // Create ResourceClusterReservationAwareScheduler that uses ResourceClusterAkkaImpl
        scheduler = new ResourceClusterReservationAwareScheduler(resourceCluster);

        // Allow actors to initialize
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Test that upsertReservation flows through the entire stack:
     * Scheduler -> ResourceClusterAkkaImpl -> ResourceClustersManagerActor -> ResourceClusterActor
     */
    @Test
    public void testUpsertReservationFlow() throws ExecutionException, InterruptedException {
        // Create a reservation request
        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE_DEFINITION,
            Optional.empty(),
            ImmutableMap.of()
        );

        for (int i = 0; i < 3; i++) {
            WorkerId workerId = WorkerId.fromIdUnsafe(JOB_ID + "-worker-" + STAGE_NUMBER + "-" + i);
            allocationRequests.add(TaskExecutorAllocationRequest.of(
                workerId, constraints, null, STAGE_NUMBER));
        }

        UpsertReservation upsertRequest = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(JOB_ID)
                .stageNumber(STAGE_NUMBER)
                .build())
            .schedulingConstraints(constraints)
            .stageTargetSize(3)
            .allocationRequests(allocationRequests)
            .priority(ReservationPriority.builder()
                .type(ReservationPriority.PriorityType.NEW_JOB)
                .tier(1)
                .timestamp(System.currentTimeMillis())
                .build())
            .build();

        // Call through scheduler -> ResourceClusterAkkaImpl -> ResourceClustersManagerActor -> ResourceClusterActor
        CompletableFuture<Ack> future = scheduler.upsertReservation(upsertRequest);
        Ack ack = future.get();

        assertNotNull("Ack should not be null", ack);
        assertEquals("Should receive Ack", Ack.getInstance(), ack);
    }

    /**
     * Test that cancelReservation flows through the entire stack.
     */
    @Test
    public void testCancelReservationFlow() throws ExecutionException, InterruptedException {
        // First, create a reservation
        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE_DEFINITION,
            Optional.empty(),
            ImmutableMap.of()
        );

        WorkerId workerId = WorkerId.fromIdUnsafe(JOB_ID + "-worker-" + STAGE_NUMBER + "-0");
        allocationRequests.add(TaskExecutorAllocationRequest.of(
            workerId, constraints, null, STAGE_NUMBER));

        UpsertReservation upsertRequest = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(JOB_ID)
                .stageNumber(STAGE_NUMBER)
                .build())
            .schedulingConstraints(constraints)
            .stageTargetSize(1)
            .allocationRequests(allocationRequests)
            .priority(ReservationPriority.builder()
                .type(ReservationPriority.PriorityType.NEW_JOB)
                .tier(1)
                .timestamp(System.currentTimeMillis())
                .build())
            .build();

        // Create reservation first
        scheduler.upsertReservation(upsertRequest).get();

        // Now cancel it
        CancelReservation cancelRequest = CancelReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(JOB_ID)
                .stageNumber(STAGE_NUMBER)
                .build())
            .build();

        // Call through scheduler -> ResourceClusterAkkaImpl -> ResourceClustersManagerActor -> ResourceClusterActor
        CompletableFuture<Ack> future = scheduler.cancelReservation(cancelRequest);
        Ack ack = future.get();

        assertNotNull("Ack should not be null", ack);
        assertEquals("Should receive Ack", Ack.getInstance(), ack);
    }

    /**
     * Test that markRegistryReady flows through the entire stack.
     */
    @Test
    public void testMarkRegistryReadyFlow() throws ExecutionException, InterruptedException {
        // Call through scheduler -> ResourceClusterAkkaImpl -> ResourceClustersManagerActor -> ResourceClusterActor
        CompletableFuture<Ack> future = resourceCluster.markRegistryReady();
        Ack ack = future.get();

        assertNotNull("Ack should not be null", ack);
        assertEquals("Should receive Ack", Ack.getInstance(), ack);
    }

    /**
     * Test multiple reservations for different jobs/stages flow correctly.
     */
    @Test
    public void testMultipleReservationsFlow() throws ExecutionException, InterruptedException {
        // Create reservation for job-1, stage-1
        Set<TaskExecutorAllocationRequest> allocationRequests1 = new HashSet<>();
        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE_DEFINITION,
            Optional.empty(),
            ImmutableMap.of()
        );

        WorkerId workerId1 = WorkerId.fromIdUnsafe("job-1-worker-1-0");
        allocationRequests1.add(TaskExecutorAllocationRequest.of(
            workerId1, constraints, null, 1));

        UpsertReservation request1 = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId("job-1")
                .stageNumber(1)
                .build())
            .schedulingConstraints(constraints)
            .stageTargetSize(1)
            .allocationRequests(allocationRequests1)
            .priority(ReservationPriority.builder()
                .type(ReservationPriority.PriorityType.NEW_JOB)
                .tier(1)
                .timestamp(System.currentTimeMillis())
                .build())
            .build();

        // Create reservation for job-2, stage-2
        Set<TaskExecutorAllocationRequest> allocationRequests2 = new HashSet<>();
        WorkerId workerId2 = WorkerId.fromIdUnsafe("job-2-worker-2-0");
        allocationRequests2.add(TaskExecutorAllocationRequest.of(
            workerId2, constraints, null, 2));

        UpsertReservation request2 = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId("job-2")
                .stageNumber(2)
                .build())
            .schedulingConstraints(constraints)
            .stageTargetSize(1)
            .allocationRequests(allocationRequests2)
            .priority(ReservationPriority.builder()
                .type(ReservationPriority.PriorityType.NEW_JOB)
                .tier(1)
                .timestamp(System.currentTimeMillis())
                .build())
            .build();

        // Both should succeed
        Ack ack1 = scheduler.upsertReservation(request1).get();
        Ack ack2 = scheduler.upsertReservation(request2).get();

        assertNotNull("Ack1 should not be null", ack1);
        assertNotNull("Ack2 should not be null", ack2);
        assertEquals("Should receive Ack", Ack.getInstance(), ack1);
        assertEquals("Should receive Ack", Ack.getInstance(), ack2);
    }

    /**
     * Test that the scheduler correctly reports that it handles allocation retries.
     */
    @Test
    public void testSchedulerHandlesAllocationRetries() {
        assertTrue("Scheduler should handle allocation retries",
            scheduler.schedulerHandlesAllocationRetries());
    }

    /**
     * Test that unscheduleJob logs a warning but doesn't throw.
     */
    @Test
    public void testUnscheduleJobLogsWarning() {
        // Should not throw, just log warning
        scheduler.unscheduleJob(JOB_ID);
        // If we get here, the test passes
        assertTrue(true);
    }

    /**
     * Test that unscheduleWorker throws UnsupportedOperationException.
     */
//    @Test(expected = UnsupportedOperationException.class)
//    public void testUnscheduleWorkerThrowsException() {
//        WorkerId workerId = WorkerId.fromIdUnsafe(JOB_ID + "-worker-1-0");
//        scheduler.unscheduleWorker(workerId, Optional.empty());
//    }

    /**
     * Test that updateWorkerSchedulingReadyTime throws UnsupportedOperationException.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testUpdateWorkerSchedulingReadyTimeThrowsException() {
        WorkerId workerId = WorkerId.fromIdUnsafe(JOB_ID + "-worker-1-0");
        scheduler.updateWorkerSchedulingReadyTime(workerId, System.currentTimeMillis());
    }

    /**
     * Test end-to-end flow: create reservation, mark ready, cancel reservation.
     */
    @Test
    public void testEndToEndReservationFlow() throws ExecutionException, InterruptedException {
        // Step 1: Create reservation
        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        SchedulingConstraints constraints = SchedulingConstraints.of(
            MACHINE_DEFINITION,
            Optional.empty(),
            ImmutableMap.of()
        );

        for (int i = 0; i < 2; i++) {
            WorkerId workerId = WorkerId.fromIdUnsafe(JOB_ID + "-worker-" + STAGE_NUMBER + "-" + i);
            allocationRequests.add(TaskExecutorAllocationRequest.of(
                workerId, constraints, null, STAGE_NUMBER));
        }

        UpsertReservation upsertRequest = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(JOB_ID)
                .stageNumber(STAGE_NUMBER)
                .build())
            .schedulingConstraints(constraints)
            .stageTargetSize(2)
            .allocationRequests(allocationRequests)
            .priority(ReservationPriority.builder()
                .type(ReservationPriority.PriorityType.NEW_JOB)
                .tier(1)
                .timestamp(System.currentTimeMillis())
                .build())
            .build();

        Ack upsertAck = scheduler.upsertReservation(upsertRequest).get();
        assertNotNull("Upsert ack should not be null", upsertAck);

        // Step 2: Mark registry ready
        Ack readyAck = resourceCluster.markRegistryReady().get();
        assertNotNull("Ready ack should not be null", readyAck);

        // Step 3: Cancel reservation
        CancelReservation cancelRequest = CancelReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(JOB_ID)
                .stageNumber(STAGE_NUMBER)
                .build())
            .build();

        Ack cancelAck = scheduler.cancelReservation(cancelRequest).get();
        assertNotNull("Cancel ack should not be null", cancelAck);
    }
}

