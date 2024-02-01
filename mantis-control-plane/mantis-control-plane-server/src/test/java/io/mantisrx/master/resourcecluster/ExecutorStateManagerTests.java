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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.master.resourcecluster.ExecutorStateManagerImpl.TaskExecutorHolder;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.BestFit;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

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

    private static final String SCALE_GROUP_1 = "io-mantisrx-v001";
    private static final String SCALE_GROUP_2 = "io-mantisrx-v002";
    private static final String SCALE_GROUP_3 = "io-mantisrx-v003";

    private static final Map<String, String> ATTRIBUTES_WITH_SCALE_GROUP_1 =
        ImmutableMap.of(WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_1);

    private static final Map<String, String> ATTRIBUTES_WITH_SCALE_GROUP_2 =
        ImmutableMap.of(WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_2);

    private static final Map<String, String> ATTRIBUTES_WITH_SCALE_GROUP_3 =
        ImmutableMap.of(WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_3);

    private static final WorkerId WORKER_ID = WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1");

    private final TaskExecutorRegistration registration1 =
        getRegistrationBuilder(TASK_EXECUTOR_ID_1, MACHINE_DEFINITION_1, ATTRIBUTES).build();

    private final TaskExecutorRegistration registration2 =
        getRegistrationBuilder(TASK_EXECUTOR_ID_2, MACHINE_DEFINITION_2, ATTRIBUTES).build();

    private final TaskExecutorRegistration registration3 =
        getRegistrationBuilder(TASK_EXECUTOR_ID_3, MACHINE_DEFINITION_2, ATTRIBUTES).build();

    private static TaskExecutorRegistration.TaskExecutorRegistrationBuilder getRegistrationBuilder(
        TaskExecutorID id,
        MachineDefinition mDef,
        Map<String,
        String> attributes) {
        return TaskExecutorRegistration.builder()
            .taskExecutorID(id)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(mDef)
            .taskExecutorAttributes(attributes);
    }

    private ExecutorStateManager stateManager = new ExecutorStateManagerImpl("");;

    @Before
    public void setup() {
        rpc.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
    }

    @Test
    public void testGetBestFit() {
        Optional<BestFit> bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0)),
                CLUSTER_ID));

        assertFalse(bestFitO.isPresent());

        stateManager.trackIfAbsent(TASK_EXECUTOR_ID_1, state1);
        state1.onRegistration(registration1);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_1, CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TASK_EXECUTOR_ID_1);

        stateManager.trackIfAbsent(TASK_EXECUTOR_ID_2, state2);
        state2.onRegistration(registration2);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_2, CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TASK_EXECUTOR_ID_2);

        stateManager.trackIfAbsent(TASK_EXECUTOR_ID_3, state3);
        state3.onRegistration(registration3);

        // test machine def 1
        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)), CLUSTER_ID));
        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(state1, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0)), CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_2, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(state2, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // disable e1 and should get nothing
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_1, CLUSTER_ID,
            TaskExecutorReport.occupied(WORKER_ID)));
        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)), CLUSTER_ID));
        assertFalse(bestFitO.isPresent());

        // enable e3 and disable e2
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_3, CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TASK_EXECUTOR_ID_3);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_2, CLUSTER_ID,
            TaskExecutorReport.occupied(WORKER_ID)));

        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0)), CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_3, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(state3, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // test mark as unavailable
        stateManager.tryMarkUnavailable(TASK_EXECUTOR_ID_3);
        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0)), CLUSTER_ID));

        assertFalse(bestFitO.isPresent());
    }

    @Test
    public void testTaskExecutorHolderCreation() {
        TaskExecutorHolder taskExecutorHolder = TaskExecutorHolder.of(
            TASK_EXECUTOR_ID_1,
            getRegistrationBuilder(TASK_EXECUTOR_ID_1, MACHINE_DEFINITION_1, ATTRIBUTES).build());
        assertEquals("empty-generation", taskExecutorHolder.getGeneration());
        assertEquals(TASK_EXECUTOR_ID_1, taskExecutorHolder.getId());

        taskExecutorHolder = TaskExecutorHolder.of(
            TASK_EXECUTOR_ID_2,
            getRegistrationBuilder(TASK_EXECUTOR_ID_2, MACHINE_DEFINITION_1, ATTRIBUTES_WITH_SCALE_GROUP_1).build());
        assertEquals(SCALE_GROUP_1, taskExecutorHolder.getGeneration());
        assertEquals(TASK_EXECUTOR_ID_2, taskExecutorHolder.getId());

        taskExecutorHolder = TaskExecutorHolder.of(
            TASK_EXECUTOR_ID_2,
            getRegistrationBuilder(TASK_EXECUTOR_ID_2, MACHINE_DEFINITION_2, ATTRIBUTES_WITH_SCALE_GROUP_2).build());
        assertEquals(SCALE_GROUP_2, taskExecutorHolder.getGeneration());
        assertEquals(TASK_EXECUTOR_ID_2, taskExecutorHolder.getId());

        ImmutableMap<String, String> attributeWithGeneration = ImmutableMap.of(
            WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_1,
            WorkerConstants.MANTIS_WORKER_CONTAINER_GENERATION, SCALE_GROUP_2);

        taskExecutorHolder = TaskExecutorHolder.of(
            TASK_EXECUTOR_ID_2,
            getRegistrationBuilder(TASK_EXECUTOR_ID_2, MACHINE_DEFINITION_2, attributeWithGeneration).build());
        assertEquals(SCALE_GROUP_2, taskExecutorHolder.getGeneration());
        assertEquals(TASK_EXECUTOR_ID_2, taskExecutorHolder.getId());
    }

    @Test
    public void testGetBestFit_WithGenerationFromScaleGroup() {
        Optional<BestFit> bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0)),
                    CLUSTER_ID));
        assertFalse(bestFitO.isPresent());

        /*
        Setup 3 TE where te1 is in group 2 while te2/3 in group 1. The best fit should be te1.
         */

        // add te0 to another mDef, should not be chosen.
        TaskExecutorState teState0 = registerNewTaskExecutor(TaskExecutorID.of("te0"),
            MACHINE_DEFINITION_2,
            ATTRIBUTES_WITH_SCALE_GROUP_2,
            stateManager);

        TaskExecutorState teState1 = registerNewTaskExecutor(TASK_EXECUTOR_ID_1,
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_2,
            stateManager);

        TaskExecutorState teState2 = registerNewTaskExecutor(TASK_EXECUTOR_ID_2,
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        TaskExecutorState teState3 = registerNewTaskExecutor(TASK_EXECUTOR_ID_3,
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        // should get te1 with group2
        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(teState1, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // add new TE in group1 doesn't affect result.
        TaskExecutorState teState4 = registerNewTaskExecutor(TaskExecutorID.of("te4"),
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(teState1, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // remove te1 and add new te in both groups
        teState1.onTaskExecutorStatusChange(
            new TaskExecutorStatusChange(TASK_EXECUTOR_ID_1, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID)));

        TaskExecutorID te5Id = TaskExecutorID.of("te5");
        TaskExecutorState teState5 = registerNewTaskExecutor(te5Id,
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_2,
            stateManager);

        TaskExecutorState teState6 = registerNewTaskExecutor(TaskExecutorID.of("te6"),
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(te5Id, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(teState5, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // disable all group2 TEs and allow bestFit from group1
        teState5.onTaskExecutorStatusChange(
            new TaskExecutorStatusChange(te5Id, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID)));
        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertNotEquals(te5Id, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertNotEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(SCALE_GROUP_1,
            Objects.requireNonNull(bestFitO.get().getBestFit().values().stream().findFirst().get().getRight().getRegistration())
                .getAttributeByKey(WorkerConstants.AUTO_SCALE_GROUP_KEY).orElse("invalid"));

        assertNotNull(stateManager.get(TASK_EXECUTOR_ID_1));
        assertNull(stateManager.get(TaskExecutorID.of("invalid")));
    }

    private TaskExecutorState registerNewTaskExecutor(TaskExecutorID id, MachineDefinition mdef,
        Map<String, String> attributes,
        ExecutorStateManager stateManager) {
        TaskExecutorState state = TaskExecutorState.of(clock, rpc, router);
        TaskExecutorRegistration reg = getRegistrationBuilder(id, mdef, attributes).build();
        stateManager.trackIfAbsent(id, state);
        state.onRegistration(reg);
        state.onTaskExecutorStatusChange(
            new TaskExecutorStatusChange(
                id,
                CLUSTER_ID,
                TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(id);

        return state;
    }

    @Test
    public void testGetBestFit_WithDifferentResourcesSameSku() {
        registerNewTaskExecutor(TASK_EXECUTOR_ID_1,
            MACHINE_DEFINITION_2,
            ATTRIBUTES_WITH_SCALE_GROUP_2,
            stateManager);

        // should get te1 with group2
        Optional<BestFit> bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    new HashSet<>(Arrays.asList(
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0),
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 1))),
                    CLUSTER_ID));

        assertFalse(bestFitO.isPresent());

        registerNewTaskExecutor(TASK_EXECUTOR_ID_2,
            MACHINE_DEFINITION_2,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    new HashSet<>(Arrays.asList(
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of()), null, 0),
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 1))),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(new HashSet<>(Arrays.asList(TASK_EXECUTOR_ID_1, TASK_EXECUTOR_ID_2)), bestFitO.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFitWithAllocationAttributes() {
        stateManager = new ExecutorStateManagerImpl("jdk:8,sbn:3");

        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8ID"), MACHINE_DEFINITION_1, ImmutableMap.of()).build();

        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17ID"), MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8ID"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8ID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8ID"));

        // no matching found for jdk:17
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17ID"), state2);
        state2.onRegistration(jdk17Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17ID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17ID"));

        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk17ID"))), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFitWithAllocationAttributesDefaults() {
        stateManager = new ExecutorStateManagerImpl("jdk:8,sbn:3");

        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8ID"), MACHINE_DEFINITION_1, ImmutableMap.of()).build();

        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17ID"), MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17ID"), state2);
        state2.onRegistration(jdk17Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17ID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17ID"));

        // no matching found for jdk:8
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "8")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8ID"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8ID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8ID"));

        // best fit found using default allocation attribute
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "8")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk8ID"))), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFitWithMultipleAllocationAttributes() {
        stateManager = new ExecutorStateManagerImpl("jdk:17,sbn:2");

        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8ID"), MACHINE_DEFINITION_1, ImmutableMap.of()).build();

        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17ID"), MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8ID"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8ID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8ID"));

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17ID"), state2);
        state2.onRegistration(jdk17Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17ID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17ID"));

        // no matching found for jdk17/sbn3
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17", "sbn", "3")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        TaskExecutorRegistration jdk17Sbn3Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17SbnID"), MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17", "sbn", "3")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17SbnID"), state3);
        state3.onRegistration(jdk17Sbn3Te);
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17SbnID"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17SbnID"));

        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17", "sbn", "3")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk17SbnID"))), bestFit.get().getTaskExecutorIDSet());
    }
}
