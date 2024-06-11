/*
 * Copyright 2022 Netflix, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.resourcecluster.TaskExecutorTaskCancelledException;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

public class TaskExecutorStateTest {
    private final AtomicReference<Clock> actual =
        new AtomicReference<>(Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));

    private final Clock clock = new DelegateClock(actual);

    private final TestingRpcService rpc = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);
    private final JobMessageRouter router = mock(JobMessageRouter.class);

    private final TaskExecutorState state = TaskExecutorState.of(clock, rpc, router);

    private static final TaskExecutorID TASK_EXECUTOR_ID = TaskExecutorID.of("taskExecutorId");
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final String TASK_EXECUTOR_ADDRESS = "127.0.0.1";
    private static final String HOST_NAME = "hostName";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(ImmutableList.of(1, 2, 3, 4, 5));
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(1.0, 2.0, 3.0, 4.0, 5);
    private static final Map<String, String> ATTRIBUTES =
        ImmutableMap.of("attr1", "attr2");
    private static final WorkerId WORKER_ID = WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1");

    @Before
    public void setup() {
        rpc.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
    }

    @Test
    public void testRegularLifecycle() throws TaskExecutorTaskCancelledException {
        Instant currentTime;
        // Registration
        assertTrue(state.onRegistration(
            TaskExecutorRegistration.builder()
                .taskExecutorID(TASK_EXECUTOR_ID)
                .clusterID(CLUSTER_ID)
                .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
                .hostname(HOST_NAME)
                .workerPorts(WORKER_PORTS)
                .machineDefinition(MACHINE_DEFINITION)
                .taskExecutorAttributes(ATTRIBUTES)
                .build()));
        assertTrue(state.isRegistered());
        assertFalse(state.isDisconnected());

        // heartbeat after registration
        currentTime = tick();
        assertTrue(state.onHeartbeat(new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())));
        assertTrue(state.isRegistered());
        assertFalse(state.isAssigned());
        assertFalse(state.isRunningTask());
        assertEquals(currentTime, state.getLastActivity());

        // assignment
        currentTime = tick();
        assertTrue(state.onAssignment(WORKER_ID));
        assertTrue(state.isRegistered());
        assertTrue(state.isAssigned());
        assertFalse(state.isRunningTask());

        // status change to running
        currentTime = tick();
        assertTrue(state.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID))));
        assertTrue(state.isRegistered());
        assertFalse(state.isAssigned());
        assertTrue(state.isRunningTask());
        assertEquals(currentTime, state.getLastActivity());

        // heartbeat when running
        currentTime = tick();
        assertFalse(state.onHeartbeat(new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID))));
        assertTrue(state.isRegistered());
        assertFalse(state.isAssigned());
        assertTrue(state.isRunningTask());
        assertEquals(currentTime, state.getLastActivity());

        // stopping the task
        currentTime = tick();
        assertTrue(state.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())));
        assertTrue(state.isRegistered());
        assertFalse(state.isAssigned());
        assertFalse(state.isRunningTask());
        assertEquals(currentTime, state.getLastActivity());
    }

    @Test
    public void testInitializationLifecycle() throws TaskExecutorTaskCancelledException {
        Instant currentTime;
        // Registration
        assertTrue(state.onRegistration(TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION)
            .taskExecutorAttributes(ATTRIBUTES)
            .build()));
        assertTrue(state.isRegistered());
        assertFalse(state.isDisconnected());

        // heartbeat after registration
        currentTime = tick();
        assertTrue(state.onHeartbeat(new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID))));
        assertTrue(state.isRegistered());
        assertFalse(state.isAssigned());
        assertTrue(state.isRunningTask());
        assertFalse(state.isAvailable());
        assertEquals(currentTime, state.getLastActivity());
    }

    private Instant tick() {
        return actual.updateAndGet(currentTime -> Clock.offset(currentTime, Duration.ofSeconds(1))).instant();
    }
}
