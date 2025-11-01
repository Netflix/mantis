package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CancelReservation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CancelReservationAck;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.MarkReady;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationGroupView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ReservationKey;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.UpsertReservation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.Reservation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorsAllocation;
import io.mantisrx.master.resourcecluster.ReservationRegistryActor.ReservationAllocationResponse;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import java.util.HashSet;
import java.util.Set;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReservationRegistryActorTest {

    private static final MachineDefinition MACHINE =
        new MachineDefinition(2.0, 4096, 128.0, 10240, 1);
    private static final Duration PROCESS_INTERVAL = Duration.ofMillis(5);
    private static final Instant BASE_INSTANT = Instant.parse("2024-01-01T00:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(BASE_INSTANT, ZoneOffset.UTC);
    private static final ClusterID TEST_CLUSTER_ID = ClusterID.of("test-cluster");
    private static ActorSystem system;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("ReservationRegistryActorTest");
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void shouldGroupReservationsByCanonicalConstraint() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        SchedulingConstraints skuAConstraints = constraints("sku-a", ImmutableMap.of("attr", "1"));
        SchedulingConstraints skuBConstraints = constraints("sku-b", Collections.emptyMap());
        SchedulingConstraints skuCConstraints = constraints(null, ImmutableMap.of("j2", "v2"));

        upsert(registry, probe, "jobA", 1, skuAConstraints, 3, 6);
        upsert(registry, probe, "jobB", 2, skuAConstraints, 2, 4);
        upsert(registry, probe, "jobC", 1, skuBConstraints, 4, 5);
        upsert(registry, probe, "jobC", 2, skuCConstraints, 5, 6);
        upsert(registry, probe, "jobD", 2, skuCConstraints, 6, 7);

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        assertNotNull(probe.expectMsgClass(Ack.class));

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());

        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(view.isReady());
        assertEquals(3, view.getGroups().size());

        PendingReservationGroupView groupA = view.getGroups().get(canonicalKeyFor(skuAConstraints));
        assertNotNull(groupA);
        assertEquals(2, groupA.getReservationCount());
        assertEquals(5, groupA.getTotalRequestedWorkers());
        assertEquals(2, groupA.getReservations().size());

        PendingReservationGroupView groupB = view.getGroups().get(canonicalKeyFor(skuBConstraints));
        assertNotNull(groupB);
        assertEquals(1, groupB.getReservationCount());
        assertEquals(4, groupB.getTotalRequestedWorkers());

        PendingReservationGroupView groupC = view.getGroups().get(canonicalKeyFor(skuCConstraints));
        assertNotNull(groupC);
        assertEquals(2, groupC.getReservationCount());
        assertEquals(11, groupC.getTotalRequestedWorkers());

        stopActor(registry);
    }

    @Test
    public void shouldMarkReadyAndHandleCancellation() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        ReservationKey key = ReservationKey.builder().jobId("job-ready").stageNumber(1).build();
        SchedulingConstraints constraints = constraints("sku-ready", Collections.emptyMap());

        upsert(registry, probe, key, constraints, 3, 3);

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView notReadyView = probe.expectMsgClass(PendingReservationsView.class);
        assertFalse(notReadyView.isReady());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView readyView = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(readyView.isReady());

        registry.tell(
            CancelReservation.builder().reservationKey(key).build(),
            probe.getRef());
        CancelReservationAck cancelled = probe.expectMsgClass(CancelReservationAck.class);
        assertTrue(cancelled.isCancelled());

        registry.tell(
            CancelReservation.builder().reservationKey(key).build(),
            probe.getRef());
        CancelReservationAck cancelAgain = probe.expectMsgClass(CancelReservationAck.class);
        assertFalse(cancelAgain.isCancelled());

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView emptyView = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(emptyView.getGroups().isEmpty());

        stopActor(registry);
    }

    @Test
    public void shouldDeduplicateOnIdenticalReservationShape() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        ReservationKey key = ReservationKey.builder().jobId("job-dedupe").stageNumber(0).build();
        SchedulingConstraints constraints = constraints("sku-dedupe", ImmutableMap.of("zone", "us-west"));

        upsert(registry, probe, key, constraints, 5, 5);
        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());
        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView initialView = probe.expectMsgClass(PendingReservationsView.class);
        PendingReservationGroupView initialGroup =
            initialView.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(initialGroup);
        assertEquals(1, initialGroup.getReservationCount());
        assertEquals(5, initialGroup.getTotalRequestedWorkers());

        upsert(registry, probe, key, constraints, 5, 5);
        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView dedupedView = probe.expectMsgClass(PendingReservationsView.class);
        PendingReservationGroupView dedupedGroup =
            dedupedView.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(dedupedGroup);
        assertEquals(1, dedupedGroup.getReservationCount());
        assertEquals(5, dedupedGroup.getTotalRequestedWorkers());

        stopActor(registry);
    }

    @Test
    public void shouldRetainExistingReservationsWhenTargetSizeIncreases() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        ReservationKey key = ReservationKey.builder().jobId("job-scale-up").stageNumber(1).build();
        SchedulingConstraints constraints = constraints("sku-scale-up", Collections.emptyMap());

        upsert(registry, probe, key, constraints, 2, 2, BASE_INSTANT.toEpochMilli());
        upsert(registry, probe, key, constraints, 3, 4, BASE_INSTANT.plusSeconds(5).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());
        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());

        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);
        PendingReservationGroupView group = view.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(group);
        assertEquals(2, group.getReservationCount());
        assertEquals(
            List.of(2, 4),
            group.getReservations().stream()
                .map(ReservationRegistryActor.ReservationSnapshot::getStageTargetSize)
                .collect(Collectors.toList())
        );

        stopActor(registry);
    }

    @Test
    public void shouldDropOlderReservationsWhenTargetSizeDecreases() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        ReservationKey key = ReservationKey.builder().jobId("job-scale-down").stageNumber(2).build();
        SchedulingConstraints constraints = constraints("sku-scale-down", Collections.emptyMap());

        upsert(registry, probe, key, constraints, 2, 4, BASE_INSTANT.toEpochMilli());
        upsert(registry, probe, key, constraints, 2, 5, BASE_INSTANT.plusSeconds(1).toEpochMilli());
        upsert(registry, probe, key, constraints, 1, 3, BASE_INSTANT.plusSeconds(2).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());
        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());

        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);
        PendingReservationGroupView group = view.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(group);
        assertEquals(1, group.getReservationCount());
        assertEquals(
            List.of(3),
            group.getReservations().stream()
                .map(ReservationRegistryActor.ReservationSnapshot::getStageTargetSize)
                .collect(Collectors.toList())
        );

        stopActor(registry);
    }

    private static void upsert(
        ActorRef registry,
        TestKit probe,
        String jobId,
        int stage,
        SchedulingConstraints constraints,
        int requestedWorkers,
        int stageTargetSize
    ) {
        ReservationKey key = ReservationKey.builder().jobId(jobId).stageNumber(stage).build();
        upsert(registry, probe, key, constraints, requestedWorkers, stageTargetSize);
    }

    private static void upsert(
        ActorRef registry,
        TestKit probe,
        ReservationKey key,
        SchedulingConstraints constraints,
        int requestedWorkers,
        int stageTargetSize
    ) {
        upsert(
            registry,
            probe,
            key,
            constraints,
            requestedWorkers,
            stageTargetSize,
            BASE_INSTANT.toEpochMilli());
    }

    private static void upsert(
        ActorRef registry,
        TestKit probe,
        ReservationKey key,
        SchedulingConstraints constraints,
        int requestedWorkers,
        int stageTargetSize,
        long priorityTimestamp
    ) {
        ResourceClusterActor.ReservationPriority priority = ResourceClusterActor.ReservationPriority.builder()
            .type(ResourceClusterActor.ReservationPriority.PriorityType.NEW_JOB)
            .tier(0)
            .timestamp(priorityTimestamp)
            .build();

        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        for (int i = 0; i < requestedWorkers; i++) {
            WorkerId workerId = WorkerId.fromId(
                String.format("%s-%d-worker-%d-%d", key.getJobId(), key.getStageNumber(), i, 0)
            ).get();
            allocationRequests.add(TaskExecutorAllocationRequest.of(
                workerId, constraints, null, key.getStageNumber()
            ));
        }

        UpsertReservation upsert =
            UpsertReservation.builder()
                .reservationKey(key)
                .schedulingConstraints(constraints)
                .allocationRequests(allocationRequests)
                .stageTargetSize(stageTargetSize)
                .priority(priority)
                .build();
        registry.tell(upsert, probe.getRef());
        probe.expectMsg(Ack.getInstance());
    }

    private static SchedulingConstraints constraints(String sizeName, Map<String, String> attributes) {
        Map<String, String> attrMap = attributes == null ? Collections.emptyMap() : attributes;
        return SchedulingConstraints.of(MACHINE, Optional.ofNullable(sizeName), attrMap);
    }

    private static String canonicalKeyFor(SchedulingConstraints constraints) {
        StringBuilder builder = new StringBuilder();
        MachineDefinition machine = constraints.getMachineDefinition();
        if (machine != null) {
            builder.append("md:")
                .append(machine.getCpuCores()).append('/')
                .append(machine.getMemoryMB()).append('/')
                .append(machine.getDiskMB()).append('/')
                .append(machine.getNetworkMbps()).append('/')
                .append(machine.getNumPorts());
        } else {
            builder.append("md:none");
        }

        builder.append(";size=").append(constraints.getSizeName().orElse("~"));

        Map<String, String> attrs = constraints.getSchedulingAttributes();
        if (attrs != null && !attrs.isEmpty()) {
            builder.append(";attr=");
            attrs.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry ->
                    builder.append(entry.getKey())
                        .append('=')
                        .append(entry.getValue())
                        .append(','));
        } else {
            builder.append(";attr=-");
        }
        return builder.toString();
    }

    @Test
    public void shouldSendBatchAssignmentRequestWhenProcessingReservations() {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        ReservationKey key = ReservationKey.builder().jobId("job-batch").stageNumber(1).build();
        SchedulingConstraints constraints = constraints("sku-batch", Collections.emptyMap());

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-batch-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-batch-1-worker-0-2");

        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 1);

        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(
            worker2, constraints, null, 1);

        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        allocationRequests.add(req1);
        allocationRequests.add(req2);

        ResourceClusterActor.ReservationPriority priority = ResourceClusterActor.ReservationPriority.builder()
            .type(ResourceClusterActor.ReservationPriority.PriorityType.NEW_JOB)
            .tier(0)
            .timestamp(BASE_INSTANT.toEpochMilli())
            .build();

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(allocationRequests)
            .stageTargetSize(2)
            .priority(priority)
            .build();

        registry.tell(upsert, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        TaskExecutorBatchAssignmentRequest batchRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);

        assertNotNull(batchRequest);
        assertEquals(2, batchRequest.getAllocationRequests().size());
        assertTrue(batchRequest.getAllocationRequests().contains(req1));
        assertTrue(batchRequest.getAllocationRequests().contains(req2));

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView viewWhileInflight = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(viewWhileInflight.isReady());
        assertEquals(1, viewWhileInflight.getGroups().size());

        PendingReservationGroupView groupWhileInflight = viewWhileInflight.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(groupWhileInflight);
        assertEquals(1, groupWhileInflight.getReservationCount());
        assertEquals(2, groupWhileInflight.getTotalRequestedWorkers());

        stopActor(registry);
    }

    @Test
    public void shouldNotSendMultipleBatchRequestsForSameConstraintGroup() {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, Duration.ofMillis(100)));

        SchedulingConstraints constraints = constraints("sku-inflight", Collections.emptyMap());

        ReservationKey key1 = ReservationKey.builder().jobId("job-inflight-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-inflight-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-inflight-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-inflight-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 1);

        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(
            worker2, constraints, null, 1);

        upsertWithAllocations(registry, probe, key1, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());
        upsertWithAllocations(registry, probe, key2, constraints, Set.of(req2), 1, BASE_INSTANT.plusSeconds(1).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        TaskExecutorBatchAssignmentRequest firstRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(firstRequest);

        registry.tell(ResourceClusterActor.ForceProcessReservationsTick.INSTANCE, probe.getRef());
        parent.expectNoMessage(Duration.ofMillis(500));

        stopActor(registry);
    }

    @Test
    public void shouldClearInFlightAndSendNextRequestOnSuccess() {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        SchedulingConstraints constraints = constraints("sku-success", Collections.emptyMap());

        ReservationKey key1 = ReservationKey.builder().jobId("job-success-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-success-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-success-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-success-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 1);

        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(
            worker2, constraints, null, 1);

        upsertWithAllocations(registry, probe, key1, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());
        upsertWithAllocations(registry, probe, key2, constraints, Set.of(req2), 1, BASE_INSTANT.plusSeconds(1).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        TaskExecutorBatchAssignmentRequest firstRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(firstRequest);
        Reservation completedReservation = firstRequest.getReservation();

        TaskExecutorsAllocation allocation = new TaskExecutorsAllocation(
            Collections.emptyMap(),
            completedReservation);

        registry.tell(new ReservationAllocationResponse(completedReservation, allocation, null), probe.getRef());

        TaskExecutorBatchAssignmentRequest secondRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(secondRequest);
        assertEquals(1, secondRequest.getAllocationRequests().size());
        assertTrue(secondRequest.getAllocationRequests().contains(req2));

        stopActor(registry);
    }

    @Test
    public void shouldClearInFlightOnCancellation() {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        SchedulingConstraints constraints = constraints("sku-cancel", Collections.emptyMap());

        ReservationKey key1 = ReservationKey.builder().jobId("job-cancel-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-cancel-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-cancel-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-cancel-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(
            worker1, constraints, null, 1);

        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(
            worker2, constraints, null, 1);

        upsertWithAllocations(registry, probe, key1, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());
        upsertWithAllocations(registry, probe, key2, constraints, Set.of(req2), 1, BASE_INSTANT.plusSeconds(1).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        parent.expectMsgClass(Duration.ofSeconds(2), TaskExecutorBatchAssignmentRequest.class);

        registry.tell(CancelReservation.builder().reservationKey(key1).build(), probe.getRef());
        CancelReservationAck ack = probe.expectMsgClass(CancelReservationAck.class);
        assertTrue(ack.isCancelled());

        TaskExecutorBatchAssignmentRequest nextRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(nextRequest);
        assertEquals(1, nextRequest.getAllocationRequests().size());
        assertTrue(nextRequest.getAllocationRequests().contains(req2));

        stopActor(registry);
    }

    @Test
    public void shouldHandleMultipleConstraintGroupsIndependently() {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL));

        SchedulingConstraints constraintsA = constraints("sku-a", ImmutableMap.of("zone", "us-west"));
        SchedulingConstraints constraintsB = constraints("sku-b", ImmutableMap.of("zone", "us-east"));

        ReservationKey keyA = ReservationKey.builder().jobId("job-a").stageNumber(1).build();
        ReservationKey keyB = ReservationKey.builder().jobId("job-b").stageNumber(1).build();

        WorkerId workerA = WorkerId.fromIdUnsafe("job-a-worker-0-1");
        WorkerId workerB = WorkerId.fromIdUnsafe("job-b-worker-0-1");

        TaskExecutorAllocationRequest reqA = TaskExecutorAllocationRequest.of(
            workerA, constraintsA, null, 1);

        TaskExecutorAllocationRequest reqB = TaskExecutorAllocationRequest.of(
            workerB, constraintsB, null, 1);

        upsertWithAllocations(registry, probe, keyA, constraintsA, Set.of(reqA), 1, BASE_INSTANT.toEpochMilli());
        upsertWithAllocations(registry, probe, keyB, constraintsB, Set.of(reqB), 1, BASE_INSTANT.toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        Set<TaskExecutorAllocationRequest> receivedRequests = new HashSet<>();
        receivedRequests.addAll(parent.expectMsgClass(Duration.ofSeconds(2), TaskExecutorBatchAssignmentRequest.class).getAllocationRequests());
        receivedRequests.addAll(parent.expectMsgClass(Duration.ofSeconds(2), TaskExecutorBatchAssignmentRequest.class).getAllocationRequests());

        assertEquals(2, receivedRequests.size());
        assertTrue(receivedRequests.contains(reqA));
        assertTrue(receivedRequests.contains(reqB));

        stopActor(registry);
    }

    private static void upsertWithAllocations(
        ActorRef registry,
        TestKit probe,
        ReservationKey key,
        SchedulingConstraints constraints,
        Set<TaskExecutorAllocationRequest> allocationRequests,
        int stageTargetSize,
        long priorityTimestamp
    ) {
        ResourceClusterActor.ReservationPriority priority = ResourceClusterActor.ReservationPriority.builder()
            .type(ResourceClusterActor.ReservationPriority.PriorityType.NEW_JOB)
            .tier(0)
            .timestamp(priorityTimestamp)
            .build();

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(allocationRequests)
            .stageTargetSize(stageTargetSize)
            .priority(priority)
            .build();

        registry.tell(upsert, probe.getRef());
        probe.expectMsg(Ack.getInstance());
    }

    private static void stopActor(ActorRef actor) {
        TestKit watcher = new TestKit(system);
        watcher.watch(actor);
        system.stop(actor);
        watcher.expectTerminated(actor);
    }
}
