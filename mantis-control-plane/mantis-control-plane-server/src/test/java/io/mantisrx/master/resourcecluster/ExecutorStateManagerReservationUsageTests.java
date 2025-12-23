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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationInfo;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link ExecutorStateManagerImpl#getClusterUsageWithReservations} and related helper methods.
 */
public class ExecutorStateManagerReservationUsageTests {

    private final AtomicReference<Clock> actual =
        new AtomicReference<>(Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));
    private final Clock clock = new DelegateClock(actual);

    private final TestingRpcService rpc = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);
    private final JobMessageRouter router = mock(JobMessageRouter.class);

    private static final ClusterID CLUSTER_ID = ClusterID.of("testClusterId");
    private static final String TASK_EXECUTOR_ADDRESS = "127.0.0.1";
    private static final String HOST_NAME = "hostName";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(ImmutableList.of(1, 2, 3, 4, 5));

    private static final MachineDefinition MACHINE_DEFINITION_SMALL =
        new MachineDefinition(1.0, 2048.0, 10240.0, 128.0, 5);
    private static final MachineDefinition MACHINE_DEFINITION_MEDIUM =
        new MachineDefinition(2.0, 4096.0, 20480.0, 256.0, 5);
    private static final MachineDefinition MACHINE_DEFINITION_LARGE =
        new MachineDefinition(4.0, 8192.0, 40960.0, 512.0, 5);

    private static final String SKU_SMALL = "sku-small";
    private static final String SKU_MEDIUM = "sku-medium";
    private static final String SKU_LARGE = "sku-large";

    private ExecutorStateManagerImpl stateManager;

    @Before
    public void setup() {
        rpc.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
        stateManager = new ExecutorStateManagerImpl(ImmutableMap.of());
    }

    // Helper method to create registrations with SKU
    private static TaskExecutorRegistration.TaskExecutorRegistrationBuilder getRegistrationBuilder(
            TaskExecutorID id,
            MachineDefinition mDef,
            String skuId,
            Map<String, String> additionalAttributes) {
        ImmutableMap.Builder<String, String> attrs = ImmutableMap.<String, String>builder()
            .put(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, skuId);
        if (additionalAttributes != null) {
            attrs.putAll(additionalAttributes);
        }
        return TaskExecutorRegistration.builder()
            .taskExecutorID(id)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(mDef)
            .taskExecutorAttributes(attrs.build());
    }

    private TaskExecutorState registerTaskExecutor(
            TaskExecutorID id,
            MachineDefinition mDef,
            String skuId,
            Map<String, String> additionalAttributes,
            boolean available) {
        TaskExecutorState state = TaskExecutorState.of(clock, rpc, router);
        TaskExecutorRegistration reg = getRegistrationBuilder(id, mDef, skuId, additionalAttributes).build();
        stateManager.trackIfAbsent(id, state);
        state.onRegistration(reg);
        if (available) {
            state.onTaskExecutorStatusChange(
                new TaskExecutorStatusChange(id, CLUSTER_ID, TaskExecutorReport.available()));
            stateManager.tryMarkAvailable(id);
        }
        return state;
    }

    // Standard groupKeyFunc that extracts SKU ID
    private static Optional<String> groupKeyFunc(TaskExecutorRegistration registration) {
        return registration.getTaskExecutorContainerDefinitionId()
            .map(ContainerSkuID::getResourceID);
    }

    @Test
    public void testGetClusterUsageWithReservations_EmptyCluster() {
        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            Collections.emptyList());

        assertEquals(CLUSTER_ID, response.getClusterID());
        assertTrue(response.getUsages().isEmpty());
    }

    @Test
    public void testGetClusterUsageWithReservations_NoReservations() {
        // Register 3 small TEs (2 available, 1 busy)
        registerTaskExecutor(TaskExecutorID.of("te1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("te2"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        TaskExecutorState busyState = registerTaskExecutor(
            TaskExecutorID.of("te3"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        busyState.onTaskExecutorStatusChange(
            new TaskExecutorStatusChange(TaskExecutorID.of("te3"), CLUSTER_ID,
                TaskExecutorReport.occupied(null)));

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            Collections.emptyList());

        assertEquals(1, response.getUsages().size());
        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(SKU_SMALL, usage.getUsageGroupKey());
        assertEquals(2, usage.getIdleCount());  // 2 available
        assertEquals(3, usage.getTotalCount());  // 3 total
        assertEquals(0, usage.getPendingReservationCount());  // No reservations
        assertEquals(2, usage.getEffectiveIdleCount());  // effectiveIdle = idle - pending = 2 - 0 = 2
    }

    @Test
    public void testGetClusterUsageWithReservations_WithPendingReservations() {
        // Register 5 small TEs (all available)
        for (int i = 1; i <= 5; i++) {
            registerTaskExecutor(TaskExecutorID.of("te" + i), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        }

        // Create pending reservation for 3 workers
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("constraint-key-1")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(3)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        assertEquals(1, response.getUsages().size());
        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(SKU_SMALL, usage.getUsageGroupKey());
        assertEquals(5, usage.getIdleCount());  // 5 available
        assertEquals(5, usage.getTotalCount());  // 5 total
        assertEquals(3, usage.getPendingReservationCount());  // 3 pending from reservation
        assertEquals(2, usage.getEffectiveIdleCount());  // effectiveIdle = 5 - 3 = 2
    }

    @Test
    public void testGetClusterUsageWithReservations_MultipleSkus() {
        // Register TEs for multiple SKUs
        registerTaskExecutor(TaskExecutorID.of("small1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("small2"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("medium1"), MACHINE_DEFINITION_MEDIUM, SKU_MEDIUM, null, true);
        registerTaskExecutor(TaskExecutorID.of("medium2"), MACHINE_DEFINITION_MEDIUM, SKU_MEDIUM, null, true);
        registerTaskExecutor(TaskExecutorID.of("medium3"), MACHINE_DEFINITION_MEDIUM, SKU_MEDIUM, null, true);

        // Create pending reservations for different SKUs
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("small-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(1)
                .reservationCount(1)
                .build(),
            PendingReservationInfo.builder()
                .canonicalConstraintKey("medium-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_MEDIUM))
                .totalRequestedWorkers(2)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        assertEquals(2, response.getUsages().size());

        // Find and verify small SKU usage
        UsageByGroupKey smallUsage = response.getUsages().stream()
            .filter(u -> SKU_SMALL.equals(u.getUsageGroupKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(2, smallUsage.getIdleCount());
        assertEquals(2, smallUsage.getTotalCount());
        assertEquals(1, smallUsage.getPendingReservationCount());
        assertEquals(1, smallUsage.getEffectiveIdleCount());

        // Find and verify medium SKU usage
        UsageByGroupKey mediumUsage = response.getUsages().stream()
            .filter(u -> SKU_MEDIUM.equals(u.getUsageGroupKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(3, mediumUsage.getIdleCount());
        assertEquals(3, mediumUsage.getTotalCount());
        assertEquals(2, mediumUsage.getPendingReservationCount());
        assertEquals(1, mediumUsage.getEffectiveIdleCount());
    }

    @Test
    public void testGetClusterUsageWithReservations_ReservationForNonExistentSku() {
        // Register only small TEs
        registerTaskExecutor(TaskExecutorID.of("small1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("small2"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        // Create pending reservation for large SKU (which doesn't exist)
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("large-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_LARGE))
                .totalRequestedWorkers(5)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        // Should only have small SKU in response (large reservation couldn't be mapped)
        assertEquals(1, response.getUsages().size());
        UsageByGroupKey smallUsage = response.getUsages().get(0);
        assertEquals(SKU_SMALL, smallUsage.getUsageGroupKey());
        assertEquals(2, smallUsage.getIdleCount());
        assertEquals(2, smallUsage.getTotalCount());
        assertEquals(0, smallUsage.getPendingReservationCount());  // Large reservation not mapped
    }

    @Test
    public void testGetClusterUsageWithReservations_WithSizeNameMatching() {
        // Register TEs with size name
        Map<String, String> smallSizeAttrs = ImmutableMap.of(WorkerConstants.MANTIS_CONTAINER_SIZE_NAME_KEY, "small");
        Map<String, String> mediumSizeAttrs = ImmutableMap.of(WorkerConstants.MANTIS_CONTAINER_SIZE_NAME_KEY, "medium");

        registerTaskExecutor(TaskExecutorID.of("small1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, smallSizeAttrs, true);
        registerTaskExecutor(TaskExecutorID.of("small2"), MACHINE_DEFINITION_SMALL, SKU_SMALL, smallSizeAttrs, true);
        registerTaskExecutor(TaskExecutorID.of("medium1"), MACHINE_DEFINITION_MEDIUM, SKU_MEDIUM, mediumSizeAttrs, true);

        // Create reservation with size name matching
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("small-size-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL, Optional.of("small"), ImmutableMap.of()))
                .totalRequestedWorkers(2)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        assertEquals(2, response.getUsages().size());

        // Small SKU should have the pending reservations
        UsageByGroupKey smallUsage = response.getUsages().stream()
            .filter(u -> SKU_SMALL.equals(u.getUsageGroupKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(2, smallUsage.getPendingReservationCount());

        // Medium SKU should have no pending reservations
        UsageByGroupKey mediumUsage = response.getUsages().stream()
            .filter(u -> SKU_MEDIUM.equals(u.getUsageGroupKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(0, mediumUsage.getPendingReservationCount());
    }

    @Test
    public void testGetClusterUsageWithReservations_AggregatesMultipleReservationsToSameSku() {
        // Register small TEs
        for (int i = 1; i <= 10; i++) {
            registerTaskExecutor(TaskExecutorID.of("te" + i), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        }

        // Create multiple pending reservations all mapping to small SKU
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("job1-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(3)
                .reservationCount(1)
                .build(),
            PendingReservationInfo.builder()
                .canonicalConstraintKey("job2-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(4)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        assertEquals(1, response.getUsages().size());
        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(SKU_SMALL, usage.getUsageGroupKey());
        assertEquals(10, usage.getIdleCount());
        assertEquals(10, usage.getTotalCount());
        assertEquals(7, usage.getPendingReservationCount());  // 3 + 4 = 7
        assertEquals(3, usage.getEffectiveIdleCount());  // 10 - 7 = 3
    }

    @Test
    public void testGetClusterUsageWithReservations_SkipsZeroWorkerReservations() {
        registerTaskExecutor(TaskExecutorID.of("te1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        // Create reservation with 0 workers
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("empty-constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(0)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        assertEquals(1, response.getUsages().size());
        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(0, usage.getPendingReservationCount());  // 0 workers not counted
    }

    @Test
    public void testGetClusterUsageWithReservations_NullReservationsList() {
        registerTaskExecutor(TaskExecutorID.of("te1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            null);

        assertEquals(1, response.getUsages().size());
        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(0, usage.getPendingReservationCount());
    }

    @Test
    public void testMapGroupKeyToSku_FindsFirstValidHolder() {
        // Register multiple TEs for the same SKU, but first one has null state
        TaskExecutorID te1 = TaskExecutorID.of("te1");
        TaskExecutorID te2 = TaskExecutorID.of("te2");
        TaskExecutorID te3 = TaskExecutorID.of("te3");

        // Register te1 - track but don't register (no registration = invalid)
        TaskExecutorState state1 = TaskExecutorState.of(clock, rpc, router);
        stateManager.trackIfAbsent(te1, state1);
        // Note: state1 has no registration

        // Register te2 with valid registration
        registerTaskExecutor(te2, MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        // Register te3 with valid registration
        registerTaskExecutor(te3, MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        // Create a reservation and verify it gets mapped correctly via te2 or te3
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(1)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        // Should successfully map the reservation even though te1 has no registration
        assertEquals(1, response.getUsages().size());
        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(SKU_SMALL, usage.getUsageGroupKey());
        assertEquals(1, usage.getPendingReservationCount());
    }

    @Test
    public void testGetClusterUsageWithReservations_EffectiveIdleCountNeverNegative() {
        // Register only 2 TEs
        registerTaskExecutor(TaskExecutorID.of("te1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("te2"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        // Create reservation for 5 workers (more than available)
        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(5)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(2, usage.getIdleCount());
        assertEquals(5, usage.getPendingReservationCount());
        // Effective idle should be max(0, 2-5) = 0, not negative
        assertEquals(0, usage.getEffectiveIdleCount());
    }

    @Test
    public void testGetClusterUsageWithReservations_MixedAvailableAndBusy() {
        // Register 5 TEs: 3 available, 2 busy
        registerTaskExecutor(TaskExecutorID.of("te1"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("te2"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        registerTaskExecutor(TaskExecutorID.of("te3"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);

        TaskExecutorState busyState1 = registerTaskExecutor(
            TaskExecutorID.of("te4"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        busyState1.onTaskExecutorStatusChange(
            new TaskExecutorStatusChange(TaskExecutorID.of("te4"), CLUSTER_ID, TaskExecutorReport.occupied(null)));

        TaskExecutorState busyState2 = registerTaskExecutor(
            TaskExecutorID.of("te5"), MACHINE_DEFINITION_SMALL, SKU_SMALL, null, true);
        busyState2.onTaskExecutorStatusChange(
            new TaskExecutorStatusChange(TaskExecutorID.of("te5"), CLUSTER_ID, TaskExecutorReport.occupied(null)));

        List<PendingReservationInfo> pendingReservations = ImmutableList.of(
            PendingReservationInfo.builder()
                .canonicalConstraintKey("constraint")
                .schedulingConstraints(SchedulingConstraints.of(MACHINE_DEFINITION_SMALL))
                .totalRequestedWorkers(2)
                .reservationCount(1)
                .build()
        );

        GetClusterUsageResponse response = stateManager.getClusterUsageWithReservations(
            CLUSTER_ID,
            ExecutorStateManagerReservationUsageTests::groupKeyFunc,
            pendingReservations);

        UsageByGroupKey usage = response.getUsages().get(0);
        assertEquals(3, usage.getIdleCount());  // Only 3 are available
        assertEquals(5, usage.getTotalCount());  // All 5 are registered
        assertEquals(2, usage.getPendingReservationCount());
        assertEquals(1, usage.getEffectiveIdleCount());  // 3 - 2 = 1
    }
}

