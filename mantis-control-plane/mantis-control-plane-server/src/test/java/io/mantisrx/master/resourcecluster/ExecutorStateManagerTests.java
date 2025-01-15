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
import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
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

    private static final MachineDefinition MACHINE_DEFINITION_3 =
        new MachineDefinition(2.0, 2.0, 3.0, 4.0, 5);

    private static final Map<String, String> ATTRIBUTES =
        ImmutableMap.of("attr1", "attr2");

    private static final String SCALE_GROUP_1 = "io-mantisrx-v001";
    private static final String SCALE_GROUP_2 = "io-mantisrx-v002";
    private static final String SCALE_GROUP_3 = "io-mantisrx-v003";

    private static final Map<String, String> ATTRIBUTES_WITH_SCALE_GROUP_1 =
        ImmutableMap.of(WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_1);

    private static final Map<String, String> ATTRIBUTES_WITH_SCALE_GROUP_2 =
        ImmutableMap.of(WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_2);

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

    private ExecutorStateManager stateManager = new ExecutorStateManagerImpl(ImmutableMap.of());

    @Before
    public void setup() {
        rpc.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
    }

    @Test
    public void testGetBestFit() {
        Optional<BestFit> bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0)),
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
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 0)), CLUSTER_ID));
        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(state1, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0)), CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_2, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(state2, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // disable e1 and should get nothing
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_1, CLUSTER_ID,
            TaskExecutorReport.occupied(WORKER_ID)));
        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 0)), CLUSTER_ID));
        assertFalse(bestFitO.isPresent());

        // enable e3 and disable e2
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_3, CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TASK_EXECUTOR_ID_3);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TASK_EXECUTOR_ID_2, CLUSTER_ID,
            TaskExecutorReport.occupied(WORKER_ID)));

        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0)), CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_3, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(state3, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // test mark as unavailable
        stateManager.tryMarkUnavailable(TASK_EXECUTOR_ID_3);
        bestFitO =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0)), CLUSTER_ID));

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
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0)),
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
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 0)),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(TASK_EXECUTOR_ID_1, bestFitO.get().getBestFit().values().stream().findFirst().get().getLeft());
        assertEquals(teState1, bestFitO.get().getBestFit().values().stream().findFirst().get().getRight());

        // add new TE in group1 doesn't affect result.
        TaskExecutorState teState4 = registerNewTaskExecutor(TaskExecutorID.of("te4"),
            MACHINE_DEFINITION_1,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        this.actual.set(Clock.fixed(Instant.ofEpochSecond(2), ZoneId.systemDefault()));
        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 0)),
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
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 0)),
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
                    Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 0)),
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
    public void testGetBestFit_WithDifferentResourcesSameSku() throws InterruptedException {
        registerNewTaskExecutor(TASK_EXECUTOR_ID_1,
            MACHINE_DEFINITION_2,
            ATTRIBUTES_WITH_SCALE_GROUP_2,
            stateManager);

        this.actual.set(Clock.fixed(Instant.ofEpochSecond(2), ZoneId.systemDefault()));

        // should get te1 with group2
        Optional<BestFit> bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    new HashSet<>(Arrays.asList(
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0),
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 1))),
                    CLUSTER_ID));

        assertFalse(bestFitO.isPresent());

        registerNewTaskExecutor(TASK_EXECUTOR_ID_2,
            MACHINE_DEFINITION_2,
            ATTRIBUTES_WITH_SCALE_GROUP_1,
            stateManager);

        // do not move the clock, should still get nothing as scheduler lease is still valid
        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    new HashSet<>(Arrays.asList(
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0),
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 1))),
                    CLUSTER_ID));

        assertFalse(bestFitO.isPresent());

        this.actual.set(Clock.fixed(Instant.ofEpochSecond(3), ZoneId.systemDefault()));

        bestFitO =
            stateManager.findBestFit(
                new TaskExecutorBatchAssignmentRequest(
                    new HashSet<>(Arrays.asList(
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0),
                        TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1), null, 1))),
                    CLUSTER_ID));

        assertTrue(bestFitO.isPresent());
        assertEquals(new HashSet<>(Arrays.asList(TASK_EXECUTOR_ID_1, TASK_EXECUTOR_ID_2)), bestFitO.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFit_WithSchedulingAttributes() {
        stateManager = new ExecutorStateManagerImpl(ImmutableMap.of("jdk", "8", "constraint0", "whatever"));

        // register TE with jdk:8
        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8"), MACHINE_DEFINITION_1, ImmutableMap.of()).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8"));

        // no matching found for jdk:17
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        // register TE with jdk:17
        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17"), state2);
        state2.onRegistration(jdk17Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17"));

        // matching found for jdk:17
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk17"))), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFit_WithSchedulingAttributesDefaults() {
        stateManager = new ExecutorStateManagerImpl(ImmutableMap.of("jdk", "8", "constraint0", "whatever"));

        // register TE with jdk:17
        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17"), state1);
        state1.onRegistration(jdk17Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17"));

        // no matching found for jdk:8
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "8")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        // register TE with jdk:8
        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8"), MACHINE_DEFINITION_1, ImmutableMap.of()).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8"), state2);
        state2.onRegistration(jdk8Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8"));

        // best fit found using default allocation attribute
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of()), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk8"))), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFit_WithMultipleSchedulingAttributes() {
        stateManager = new ExecutorStateManagerImpl(ImmutableMap.of("jdk", "17", "constraint0", "whatever"));

        // register TE with jdk:8
        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "8")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8"));

        // register TE with constraint0:another
        TaskExecutorRegistration jdk8Constraint0Te =
            getRegistrationBuilder(TaskExecutorID.of("constraint0"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_CONSTRAINT0", "another")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("constraint0"), state2);
        state2.onRegistration(jdk8Constraint0Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("constraint0"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("constraint0"));

        // register TE with jdk:17/constraint0:different
        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17"), state3);
        state3.onRegistration(jdk17Te);
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17"));

        // no matching found for jdk17/constraint0
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17", "constraint0", "different")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        // register TE with jdk:17/constraint0:whatever
        TaskExecutorRegistration jdk17Constraint0Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17Constraint0"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "17", "MANTIS_SCHEDULING_ATTRIBUTE_CONSTRAINT0", "whatever")).build();

        TaskExecutorState state4 = TaskExecutorState.of(clock, rpc, router);
        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17Constraint0"), state4);
        state4.onRegistration(jdk17Constraint0Te);
        state4.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17Constraint0"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17Constraint0"));

        // matching found for jdk17/constraint0
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, ImmutableMap.of("jdk", "17", "constraint0", "whatever")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk17Constraint0"))), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFit_WithSameCoresDifferentMemory() {
        // register TE with 2cores, 14GB mem
        TaskExecutorRegistration te0 =
            getRegistrationBuilder(TaskExecutorID.of("te0"), new MachineDefinition(2, 14000, 1024, 1), ImmutableMap.of()).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("te0"), state1);
        state1.onRegistration(te0);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("te0"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("te0"));

        // register TE with 2cores, 54GB mem
        TaskExecutorRegistration te1 =
            getRegistrationBuilder(TaskExecutorID.of("te1"), new MachineDefinition(2, 54000, 1024, 1), ImmutableMap.of()).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("te1"), state2);
        state2.onRegistration(te1);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("te1"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("te1"));

        // no matching found for 2cores, 10GB since the fit TE shape is 2cores, 14GB and there is only 1 TE available
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                new HashSet<>(Arrays.asList(
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1"), SchedulingConstraints.of(new MachineDefinition(2, 10000, 1024, 1)), null, 0),
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-1-2"), SchedulingConstraints.of(new MachineDefinition(2, 10000, 1024, 1)), null, 1))),
                CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        // register TE with 2cores, 54GB mem
        TaskExecutorRegistration te2 =
            getRegistrationBuilder(TaskExecutorID.of("te2"), new MachineDefinition(2, 54000, 1024, 1), ImmutableMap.of()).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("te2"), state3);
        state3.onRegistration(te2);
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("te2"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("te2"));

        // no matching found for 2cores, 10GB since the fit TE shape is still 2cores, 14GB and there is only 1 TE available
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                new HashSet<>(Arrays.asList(
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1"), SchedulingConstraints.of(new MachineDefinition(2, 10000, 1024, 1)), null, 0),
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-1-2"), SchedulingConstraints.of(new MachineDefinition(2, 10000, 1024, 1)), null, 1))),
                CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        // matching found for 2cores, 34GB since the fit TE shape is now 2cores, 54GB and there are 2 TE available
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                new HashSet<>(Arrays.asList(
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1"), SchedulingConstraints.of(new MachineDefinition(2, 30000, 1024, 1)), null, 0),
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-1-2"), SchedulingConstraints.of(new MachineDefinition(2, 30000, 1024, 1)), null, 1))),
                CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(ImmutableSet.of(TaskExecutorID.of("te1"), TaskExecutorID.of("te2")), bestFit.get().getTaskExecutorIDSet());

        TaskExecutorState state4 = TaskExecutorState.of(clock, rpc, router);
        TaskExecutorRegistration te3 =
            getRegistrationBuilder(TaskExecutorID.of("te3"), new MachineDefinition(2, 14000, 1024, 1), ImmutableMap.of()).build();

        // register TE with 2cores, 14GB mem
        stateManager.trackIfAbsent(TaskExecutorID.of("te3"), state4);
        state4.onRegistration(te3);
        state4.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("te3"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("te3"));

        // matching found for 2cores, 14GB since the fit TE shape is now 2cores, 14GB and there are 2 TE available
        this.actual.set(Clock.fixed(Instant.ofEpochSecond(2), ZoneId.systemDefault()));
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                new HashSet<>(Arrays.asList(
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1"), SchedulingConstraints.of(new MachineDefinition(2, 10000, 1024, 1)), null, 0),
                    TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-1-2"), SchedulingConstraints.of(new MachineDefinition(2, 10000, 1024, 1)), null, 1))),
                CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(ImmutableSet.of(TaskExecutorID.of("te0"), TaskExecutorID.of("te3")), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFit_WithSizeName() {
        stateManager = new ExecutorStateManagerImpl(ImmutableMap.of("jdk", "8", "constraint0", "whatever"));

        // register small TE with (default) jdk:8
        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8"), MACHINE_DEFINITION_1, ImmutableMap.of(WorkerConstants.MANTIS_CONTAINER_SIZE_NAME_KEY, "small")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8"));

        // register small TE with jdk:17
        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "17", WorkerConstants.MANTIS_CONTAINER_SIZE_NAME_KEY, "small")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17"), state2);
        state2.onRegistration(jdk17Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17"));

        // no matching found for large + jdk17
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, Optional.of("large"), ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertFalse(bestFit.isPresent());

        // matching the only small + jdk17 --> machine definition is not taken into account
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, Optional.of("small"), ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk17"))), bestFit.get().getTaskExecutorIDSet());

        // matching the only small + jdk8 --> machine definition is not taken into account
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2, Optional.of("small"), ImmutableMap.of("jdk", "8")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk8"))), bestFit.get().getTaskExecutorIDSet());
    }


    @Test
    public void testGetBestFit_WithSizeNameOnRequestButNotTaskExecutor() {
        // backward compatibility test where only the scheduling request asks for small containers + machine definition
        stateManager = new ExecutorStateManagerImpl(ImmutableMap.of("jdk", "8", "constraint0", "whatever"));

        // register TE with (default) jdk:8 with no size
        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8"), MACHINE_DEFINITION_1, ImmutableMap.of()).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8"));

        // register TE with jdk:17
        TaskExecutorRegistration jdk17Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk17"), MACHINE_DEFINITION_1, ImmutableMap.of("MANTIS_SCHEDULING_ATTRIBUTE_JDK", "17")).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk17"), state2);
        state2.onRegistration(jdk17Te);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk17"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk17"));

        // matching found for small using machine definition
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, Optional.of("small"), ImmutableMap.of("jdk", "17")), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk17"))), bestFit.get().getTaskExecutorIDSet());
    }

    @Test
    public void testGetBestFit_WithSameSizeSelectYoungerGeneration() {
        // register TE with no size - gen 1
        TaskExecutorRegistration jdk8Te =
            getRegistrationBuilder(TaskExecutorID.of("jdk8"), MACHINE_DEFINITION_1, ImmutableMap.of(WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_1)).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8"), state1);
        state1.onRegistration(jdk8Te);
        state1.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8"));

        // register TE with size - gen 2
        TaskExecutorRegistration jdk8TeWithSize =
            getRegistrationBuilder(TaskExecutorID.of("jdk8TeWithSize"), MACHINE_DEFINITION_1, ImmutableMap.of(WorkerConstants.MANTIS_CONTAINER_SIZE_NAME_KEY, "small", WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_2)).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8TeWithSize"), state2);
        state2.onRegistration(jdk8TeWithSize);
        state2.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8TeWithSize"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8TeWithSize"));

        // matching found for gen 2
        Optional<BestFit> bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, Optional.empty(), ImmutableMap.of()), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk8TeWithSize"))), bestFit.get().getTaskExecutorIDSet());

        // register TE with size - gen 2
        TaskExecutorRegistration jdk8TeWithSizeGen3 =
            getRegistrationBuilder(TaskExecutorID.of("jdk8TeWithSizeGen3"), MACHINE_DEFINITION_1, ImmutableMap.of(WorkerConstants.MANTIS_CONTAINER_SIZE_NAME_KEY, "small", WorkerConstants.AUTO_SCALE_GROUP_KEY, SCALE_GROUP_3)).build();

        stateManager.trackIfAbsent(TaskExecutorID.of("jdk8TeWithSizeGen3"), state3);
        state3.onRegistration(jdk8TeWithSizeGen3);
        state3.onTaskExecutorStatusChange(new TaskExecutorStatusChange(TaskExecutorID.of("jdk8TeWithSizeGen3"), CLUSTER_ID,
            TaskExecutorReport.available()));
        stateManager.tryMarkAvailable(TaskExecutorID.of("jdk8TeWithSizeGen3"));

        // matching not found for gen 3
        bestFit =
            stateManager.findBestFit(new TaskExecutorBatchAssignmentRequest(
                Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_1, Optional.empty(), ImmutableMap.of()), null, 0)), CLUSTER_ID));
        assertTrue(bestFit.isPresent());
        assertEquals(new HashSet<>(Collections.singletonList(TaskExecutorID.of("jdk8TeWithSizeGen3"))), bestFit.get().getTaskExecutorIDSet());
    }
}
