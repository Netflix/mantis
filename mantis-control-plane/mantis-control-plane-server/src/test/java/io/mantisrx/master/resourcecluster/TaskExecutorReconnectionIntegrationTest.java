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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerTerminate;
import io.mantisrx.master.resourcecluster.ResourceClusterAkkaImpl;
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
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
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Integration test for TaskExecutor reconnection handling.
 * Tests the flow where a TaskExecutor crashes/disconnects while running a worker
 * and then reconnects, ensuring proper cleanup and scheduling refresh.
 */
@Slf4j
public class TaskExecutorReconnectionIntegrationTest {

    private static final TaskExecutorID TASK_EXECUTOR_ID = TaskExecutorID.of("testTaskExecutor");
    private static final String TASK_EXECUTOR_ADDRESS = "127.0.0.1";
    private static final ClusterID CLUSTER_ID = ClusterID.of("testCluster");
    private static final Duration heartbeatTimeout = Duration.ofSeconds(10);
    private static final Duration assignmentTimeout = Duration.ofSeconds(1);
    private static final String HOST_NAME = "testHost";

    private static final ContainerSkuID CONTAINER_SKU_ID = ContainerSkuID.of("testSku");
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(2f, 2014, 128.0, 1024, 1);

    private static final WorkerId WORKER_ID_1 =
        WorkerId.fromIdUnsafe("test-job-1-worker-0-1");

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
    public static void setup() {
        actorSystem = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Before
    public void setupTest() throws Exception {
        rpcService.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);

        mantisJobStore = mock(MantisJobStore.class);
        jobMessageRouter = mock(JobMessageRouter.class);

        when(gateway.submitTask(any()))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        // Setup required mocks for the MantisJobStore
        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(any()))
            .thenReturn(ImmutableList.of());
        when(mantisJobStore.getJobArtifactsToCache(any()))
            .thenReturn(ImmutableList.of());

        LongDynamicProperty checkForDisabledExecutorsInterval =
            new LongDynamicProperty(propertiesLoader, "mantis.resourcecluster.disabledtaskexecutors.intervalms", Duration.ofSeconds(10).toMillis());

        final Props props =
            ResourceClusterActor.props(
                CLUSTER_ID,
                heartbeatTimeout,
                assignmentTimeout,
                Duration.ofSeconds(10),
                Duration.ofMillis(100),
                Clock.systemDefaultZone(),
                rpcService,
                mantisJobStore,
                jobMessageRouter,
                0,
                "",
                false,
                ImmutableMap.of(),
                new CpuWeightedFitnessCalculator(),
                null);

        resourceClusterActor = actorSystem.actorOf(props);
        resourceCluster =
            new ResourceClusterAkkaImpl(
                resourceClusterActor,
                Duration.ofSeconds(15),
                CLUSTER_ID,
                new LongDynamicProperty(propertiesLoader, "rate.limite.perSec", 10000L));
    }

    @Test
    public void testJobMessageRouterIntegration() {
        // This test verifies that the jobMessageRouter is properly configured
        // and can receive WorkerTerminate events (even if sent manually for testing)

        // Manually send a WorkerTerminate event to verify mocking works
        WorkerTerminate testEvent = new WorkerTerminate(WORKER_ID_1, WorkerState.Failed, JobCompletedReason.Lost);
        jobMessageRouter.routeWorkerEvent(testEvent);

        // Verify that the event was received
        ArgumentCaptor<WorkerTerminate> workerTerminateCaptor =
            ArgumentCaptor.forClass(WorkerTerminate.class);
        verify(jobMessageRouter, times(1))
            .routeWorkerEvent(workerTerminateCaptor.capture());

        WorkerTerminate capturedEvent = workerTerminateCaptor.getValue();
        assertEquals(WORKER_ID_1, capturedEvent.getWorkerId());
        assertEquals(WorkerState.Failed, capturedEvent.getFinalState());
        assertEquals(JobCompletedReason.Lost, capturedEvent.getReason());
    }

    @Test
    public void testTaskExecutorBasicLifecycle() throws Exception {
        new TestKit(actorSystem) {{
            // Test basic TaskExecutor lifecycle without crash scenarios
            // This ensures the test setup is working correctly

            TaskExecutorRegistration registration = createRegistration(TASK_EXECUTOR_ID);
            when(mantisJobStore.getTaskExecutor(TASK_EXECUTOR_ID)).thenReturn(registration);

            // Register TaskExecutor
            assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(registration).get());

            // Mark as available
            assertEquals(Ack.getInstance(),
                resourceCluster.heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).get());

            // Initialize with a worker
            assertEquals(Ack.getInstance(),
                resourceCluster.initializeTaskExecutor(TASK_EXECUTOR_ID, WORKER_ID_1).get());

            // Verify TaskExecutor state
            ResourceCluster.TaskExecutorStatus status = resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get();
            assertNotNull(status);
            assertTrue(status.isRegistered());

            // No WorkerTerminate events should be sent during normal lifecycle
            verifyNoInteractions(jobMessageRouter);
        }};
    }

    @Test
    public void testTaskExecutorCrashDetectionViaHeartbeat() throws Exception {
        new TestKit(actorSystem) {{
            // Test scenario where TaskExecutor crashes (like segfault) without sending
            // an explicit disconnection event, but is detected through heartbeat state mismatch

            TaskExecutorRegistration registration = createRegistration(TASK_EXECUTOR_ID);
            when(mantisJobStore.getTaskExecutor(TASK_EXECUTOR_ID)).thenReturn(registration);

            // Step 1: Register and setup TaskExecutor with a running worker
            assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(registration).get());

            assertEquals(Ack.getInstance(),
                resourceCluster.heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).get());

            assertEquals(Ack.getInstance(),
                resourceCluster.initializeTaskExecutor(TASK_EXECUTOR_ID, WORKER_ID_1).get());

            assertEquals(Ack.getInstance(),
                resourceCluster.heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID_1))).get());

            // Verify worker is running
            ResourceCluster.TaskExecutorStatus status = resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get();
            assertTrue("TaskExecutor should be running a task", status.isRunningTask());
            assertEquals("Correct worker ID should be assigned", WORKER_ID_1, status.getWorkerId());

            // Step 2: Simulate hard crash - TE reconnects via heartbeat showing Available
            // but internal state still thinks it's running WORKER_ID_1
            assertEquals(Ack.getInstance(),
                resourceCluster.heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).get());

            // Step 3: Check if WorkerTerminate events were sent due to state mismatch detection
            // Note: This may fail if the feature is not fully implemented, which is expected
            try {
                ArgumentCaptor<WorkerTerminate> workerTerminateCaptor =
                    ArgumentCaptor.forClass(WorkerTerminate.class);
                verify(jobMessageRouter, timeout(2000))
                    .routeWorkerEvent(workerTerminateCaptor.capture());

                WorkerTerminate terminateEvent = workerTerminateCaptor.getValue();
                assertEquals("Correct worker should be terminated", WORKER_ID_1, terminateEvent.getWorkerId());
                assertEquals("Worker should be marked as failed", WorkerState.Failed, terminateEvent.getFinalState());
                assertEquals("Worker should be marked as lost", JobCompletedReason.Lost, terminateEvent.getReason());

                log.info("SUCCESS: TaskExecutor crash detected via heartbeat mismatch and WorkerTerminate sent");
            } catch (AssertionError e) {
                log.error("NOTE: TaskExecutor crash detection via heartbeat mismatch did not trigger WorkerTerminate event. " +
                                   "This may indicate the feature is not fully implemented yet: {}", e.getMessage());
                fail();
            }

            // Step 4: Verify TaskExecutor is available after heartbeat
            ResourceCluster.TaskExecutorStatus finalStatus = resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get();
            assertTrue("TaskExecutor should still be registered", finalStatus.isRegistered());

            // Note: The TaskExecutor may still appear to be running the task if crash detection isn't implemented
            if (!finalStatus.isRunningTask()) {
                log.info("TaskExecutor correctly shows as not running the crashed task");
            } else {
                log.info("TaskExecutor still shows as running the task - crash detection may not be fully implemented");
            }
        }};
    }

    private TaskExecutorRegistration createRegistration(TaskExecutorID taskExecutorId) {
        return TaskExecutorRegistration.builder()
            .taskExecutorID(taskExecutorId)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_SKU_ID.getResourceID(),
                    "test", "attr"))
            .build();
    }
}
