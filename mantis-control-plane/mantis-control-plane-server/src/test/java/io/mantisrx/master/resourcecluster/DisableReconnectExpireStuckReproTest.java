/*
 * Copyright 2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.master.resourcecluster.ResourceClusterAkkaImpl;
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Repro for the symptom: after a targeted DisableTaskExecutorsRequest + disconnect + reconnect-while-disabled
 * + disable-expiry, the TE shows up in getAvailableTaskExecutors / ResourceOverview but getTaskExecutorsFor
 * throws NoResourceAvailableException because the TE is missing from executorsByGroup.
 *
 * Expected on unfixed master: this test FAILS with NoResourceAvailableException at the final allocation.
 * Expected after the fix (tryMarkAvailable on onNodeEnabled): test passes.
 */
@Slf4j
public class DisableReconnectExpireStuckReproTest {

    private static final TaskExecutorID TE_ID = TaskExecutorID.of("reproTE");
    private static final String TE_ADDRESS = "127.0.0.1";
    private static final ClusterID CLUSTER_ID = ClusterID.of("reproCluster");
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration DISABLED_CHECK_INTERVAL = Duration.ofSeconds(10);
    private static final Duration SCHEDULER_LEASE_EXPIRATION = Duration.ofMillis(100);
    private static final String HOST_NAME = "reproHost";

    private static final ContainerSkuID SKU_ID = ContainerSkuID.of("reproSku");
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(2f, 2014, 128.0, 1024, 1);

    private static ActorSystem actorSystem;

    private final TestingRpcService rpcService = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);
    private MantisJobStore mantisJobStore;
    private JobMessageRouter jobMessageRouter;
    private ActorRef resourceClusterActor;
    private ResourceCluster resourceCluster;
    private final MantisPropertiesLoader propertiesLoader =
        new DefaultMantisPropertiesLoader(System.getProperties());

    @BeforeClass
    public static void bootActor() {
        actorSystem = ActorSystem.create();
    }

    @AfterClass
    public static void shutdownActor() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Before
    public void setupTest() throws Exception {
        rpcService.registerGateway(TE_ADDRESS, gateway);

        mantisJobStore = mock(MantisJobStore.class);
        jobMessageRouter = mock(JobMessageRouter.class);

        when(gateway.submitTask(any()))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        doReturn(ImmutableList.of())
            .when(mantisJobStore).loadAllDisableTaskExecutorsRequests(CLUSTER_ID);
        doReturn(ImmutableList.of())
            .when(mantisJobStore).getJobArtifactsToCache(CLUSTER_ID);

        MasterConfiguration masterConfig = mock(MasterConfiguration.class);
        when(masterConfig.getTimeoutSecondsToReportStart()).thenReturn(1);
        ExecuteStageRequestFactory executeStageRequestFactory = new ExecuteStageRequestFactory(masterConfig);

        final Props props = ResourceClusterActor.props(
            CLUSTER_ID,
            HEARTBEAT_TIMEOUT,
            ASSIGNMENT_TIMEOUT,
            DISABLED_CHECK_INTERVAL,
            SCHEDULER_LEASE_EXPIRATION,
            Clock.systemDefaultZone(),
            rpcService,
            mantisJobStore,
            jobMessageRouter,
            0,
            "",
            false,
            ImmutableMap.of(),
            new CpuWeightedFitnessCalculator(),
            executeStageRequestFactory,
            false);

        resourceClusterActor = actorSystem.actorOf(props);
        resourceCluster = new ResourceClusterAkkaImpl(
            resourceClusterActor,
            Duration.ofSeconds(15),
            CLUSTER_ID,
            new LongDynamicProperty(propertiesLoader, "resourcecluster.gateway.maxConcurrentRequests.repro", 100000L));
    }

    @Test
    public void reconnectDuringTargetedDisablePlusExpirePreservesSchedulability() throws Exception {
        runReproSequence(
            /* disableAttributes */ ImmutableMap.of(),
            /* targetedTaskExecutorId */ Optional.of(TE_ID));
    }

    /**
     * Attribute-based variant: the same drift happens because
     * {@code onDisableTaskExecutorsRequestExpiry}'s attribute branch
     * (ExecutorStateManagerActor.java:1249-1262) calls {@code onNodeEnabled()}
     * without calling {@code delegate.tryMarkAvailable(id)}. The attribute disable
     * is used by NF's UpgradeClusterChildWorkflowImpl drain when the caller
     * targets a scale group / SKU instead of a single TE id.
     */
    @Test
    public void reconnectDuringAttributeBasedDisablePlusExpirePreservesSchedulability() throws Exception {
        runReproSequence(
            /* disableAttributes */ ImmutableMap.of("repro", "attr"),
            /* targetedTaskExecutorId */ Optional.empty());
    }

    private void runReproSequence(
        Map<String, String> disableAttributes,
        Optional<TaskExecutorID> targetedTaskExecutorId) throws Exception {
        new TestKit(actorSystem) {{
            TaskExecutorRegistration registration = buildRegistration(TE_ID);
            doReturn(registration).when(mantisJobStore).getTaskExecutor(TE_ID);

            // 1) Register + Occupied heartbeat: TE becomes Running (simulates the realistic case where
            //    the TE is actively running a worker at the time the ASG-upgrade workflow disables it).
            //    The Running state causes tryMarkUnavailable -> the TE is NOT in executorsByGroup anymore.
            //    This is important: the bug only manifests when the TE is NOT present in executorsByGroup
            //    at the moment of the disconnect/reconnect sequence.
            WorkerId occupyingWorker = WorkerId.fromIdUnsafe("repro-job-0-worker-0-1");
            assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(registration).get());
            assertEquals(Ack.getInstance(),
                resourceCluster.heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(TE_ID, CLUSTER_ID, TaskExecutorReport.occupied(occupyingWorker))).get());

            assertTrue("Baseline: TE should be registered before disable",
                resourceCluster.getRegisteredTaskExecutors().get().contains(TE_ID));

            // 2) DisableTaskExecutorsRequest with short expiry (wall-clock via akka timers).
            Duration expiryIn = Duration.ofSeconds(2);
            Instant expiry = Instant.now().plus(expiryIn);
            Instant disableAt = Instant.now();
            assertEquals(Ack.getInstance(),
                resourceCluster.disableTaskExecutorsFor(
                    disableAttributes,
                    expiry,
                    targetedTaskExecutorId).get());

            // Give CheckDisabledTaskExecutors a moment to flip the disabled flag on the state.
            Thread.sleep(200);

            // 3) Explicitly disconnect the TE (simulates heartbeat-timeout / crash).
            assertEquals(Ack.getInstance(),
                resourceCluster.disconnectTaskExecutor(
                    new TaskExecutorDisconnection(TE_ID, CLUSTER_ID)).get());

            // 4) Reconnect: re-register + available heartbeat BEFORE the disable expires.
            assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(registration).get());
            assertEquals(Ack.getInstance(),
                resourceCluster.heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(TE_ID, CLUSTER_ID, TaskExecutorReport.available())).get());

            // 5) Wait past expiry so ExpireDisableTaskExecutorsRequest fires.
            long elapsedMs = Instant.now().toEpochMilli() - disableAt.toEpochMilli();
            long remainingMs = Math.max(0, expiryIn.toMillis() - elapsedMs);
            Thread.sleep(remainingMs + 1500);

            // Post-expiry: the TE is reported as available by the API & metric (matches the operator's symptom).
            List<TaskExecutorID> availableList = resourceCluster.getAvailableTaskExecutors().get();
            log.info("Post-expiry getAvailableTaskExecutors: {}", availableList);
            assertTrue("Post-expiry: getAvailableTaskExecutors must report the TE (matches operator's symptom)",
                availableList.contains(TE_ID));

            ResourceCluster.ResourceOverview overview = resourceCluster.resourceOverview().get();
            log.info("Post-expiry resourceOverview: {}", overview);
            assertTrue("Post-expiry: numAvailable >= 1 (matches operator's symptom)",
                overview.getNumAvailableTaskExecutors() >= 1);

            // 6) The key assertion: scheduler must also be able to pick this TE.
            //    Pre-fix: throws NoResourceAvailableException. Post-fix: returns the TE.
            WorkerId scheduleWorkerId = WorkerId.fromIdUnsafe("repro-job-2-worker-0-1");
            try {
                Map<TaskExecutorAllocationRequest, TaskExecutorID> after =
                    resourceCluster.getTaskExecutorsFor(
                        Collections.singleton(makeAllocReq(scheduleWorkerId))).get();
                assertNotNull(after);
                assertEquals("Post-expiry: scheduler should find exactly the reconnected TE",
                    1, after.size());
                assertTrue("Post-expiry: scheduler should return the formerly-disabled TE",
                    after.containsValue(TE_ID));
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause instanceof NoResourceAvailableException) {
                    fail("REPRODUCED THE BUG: NoResourceAvailableException despite TE being reported "
                        + "as available by getAvailableTaskExecutors and resourceOverview. Cause: " + cause.getMessage());
                } else {
                    throw ee;
                }
            }
        }};
    }

    private TaskExecutorAllocationRequest makeAllocReq(WorkerId workerId) {
        return TaskExecutorAllocationRequest.of(
            workerId,
            SchedulingConstraints.of(MACHINE_DEFINITION),
            null,
            0,
            MantisJobDurationType.Perpetual);
    }

    private TaskExecutorRegistration buildRegistration(TaskExecutorID id) {
        return TaskExecutorRegistration.builder()
            .taskExecutorID(id)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TE_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, SKU_ID.getResourceID(),
                    "repro", "attr"))
            .build();
    }
}
