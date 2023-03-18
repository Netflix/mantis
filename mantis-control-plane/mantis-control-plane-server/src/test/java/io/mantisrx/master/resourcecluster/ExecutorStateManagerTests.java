/*
 * Copyright 2023 Netflix, Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorAssignmentRequest;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExecutorStateManagerTests {
    private final AtomicReference<Clock> actual =
        new AtomicReference<>(Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));
    private final Clock clock = new DelegateClock(actual);

    private final TestingRpcService rpc = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);
    private final JobMessageRouter router = mock(JobMessageRouter.class);
    private final TaskExecutorState state1 = TaskExecutorState.of(clock, rpc, router);
    private final TaskExecutorState state2 = TaskExecutorState.of(clock, rpc, router);

    private final TaskExecutorState state3 = TaskExecutorState.of(clock, rpc, router);


    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final TaskExecutorID TASK_EXECUTOR_ID_1 = TaskExecutorID.of("taskExecutorId1");

    private static final TaskExecutorID TASK_EXECUTOR_ID_2 = TaskExecutorID.of("taskExecutorId2");
    private static final TaskExecutorID TASK_EXECUTOR_ID_3 = TaskExecutorID.of("taskExecutorId3");


    private static final String TASK_EXECUTOR_ADDRESS = "127.0.0.1";
    private static final String HOST_NAME = "hostName";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(ImmutableList.of(1, 2, 3, 4, 5));
    private static final MachineDefinition MACHINE_DEFINITION_1 =
        new MachineDefinition(1.0, 2.0, 3.0, 4.0, 5);

    private static final MachineDefinition MACHINE_DEFINITION_2 =
        new MachineDefinition(4.0, 2.0, 3.0, 4.0, 5);
    private static final Map<String, String> ATTRIBUTES =
        ImmutableMap.of("attr1", "attr2");
    private static final WorkerId WORKER_ID = WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1");

    private final TaskExecutorRegistration registration1 = TaskExecutorRegistration.builder()
        .taskExecutorID(TASK_EXECUTOR_ID_1)
                .clusterID(CLUSTER_ID)
                .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
                .hostname(HOST_NAME)
                .workerPorts(WORKER_PORTS)
                .machineDefinition(MACHINE_DEFINITION_1)
                .taskExecutorAttributes(ATTRIBUTES)
                .build();

    private final TaskExecutorRegistration registration2 = TaskExecutorRegistration.builder()
        .taskExecutorID(TASK_EXECUTOR_ID_2)
        .clusterID(CLUSTER_ID)
        .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
        .hostname(HOST_NAME)
        .workerPorts(WORKER_PORTS)
        .machineDefinition(MACHINE_DEFINITION_2)
        .taskExecutorAttributes(ATTRIBUTES)
        .build();

    private final TaskExecutorRegistration registration3 = TaskExecutorRegistration.builder()
        .taskExecutorID(TASK_EXECUTOR_ID_3)
        .clusterID(CLUSTER_ID)
        .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
        .hostname(HOST_NAME)
        .workerPorts(WORKER_PORTS)
        .machineDefinition(MACHINE_DEFINITION_2)
        .taskExecutorAttributes(ATTRIBUTES)
        .build();

    private final ExecutorStateManager stateManager = new ExecutorStateManagerImpl();

    @BeforeEach
    public void setup() {
        rpc.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
    }

    @Test
    public void testGetBestFit() {
        Optional<Pair<TaskExecutorID, TaskExecutorState>> bestFitO =
            stateManager.findBestFit(new TaskExecutorAssignmentRequest(MACHINE_DEFINITION_2, WORKER_ID, CLUSTER_ID));

        assertFalse(bestFitO.isPresent());

        stateManager.putIfAbsent(TASK_EXECUTOR_ID_1, state1);
        state1.onRegistration(registration1);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_1, CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.markAvailable(TASK_EXECUTOR_ID_1);

        stateManager.putIfAbsent(TASK_EXECUTOR_ID_2, state2);
        state2.onRegistration(registration2);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_2, CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.markAvailable(TASK_EXECUTOR_ID_2);

        stateManager.putIfAbsent(TASK_EXECUTOR_ID_3, state3);
        state3.onRegistration(registration3);
        stateManager.markAvailable(TASK_EXECUTOR_ID_3);

        // test machine def 1
        bestFitO =
            stateManager.findBestFit(new TaskExecutorAssignmentRequest(MACHINE_DEFINITION_1, WORKER_ID, CLUSTER_ID));
        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getLeft());
        assertEquals(state1, bestFitO.get().getRight());

        bestFitO =
            stateManager.findBestFit(new TaskExecutorAssignmentRequest(MACHINE_DEFINITION_2, WORKER_ID, CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_2, bestFitO.get().getLeft());
        assertEquals(state2, bestFitO.get().getRight());

        // enable e3 and disable e2
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_3, CLUSTER_ID,
            TaskExecutorReport.available()));
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_2, CLUSTER_ID,
            TaskExecutorReport.occupied(WORKER_ID)));

        bestFitO =
            stateManager.findBestFit(new TaskExecutorAssignmentRequest(MACHINE_DEFINITION_2, WORKER_ID, CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_3, bestFitO.get().getLeft());
        assertEquals(state3, bestFitO.get().getRight());

        // test mark as unavailable
        stateManager.markUnavailable(TASK_EXECUTOR_ID_3);
        bestFitO =
            stateManager.findBestFit(new TaskExecutorAssignmentRequest(MACHINE_DEFINITION_2, WORKER_ID, CLUSTER_ID));

        assertFalse(bestFitO.isPresent());
    }
}
