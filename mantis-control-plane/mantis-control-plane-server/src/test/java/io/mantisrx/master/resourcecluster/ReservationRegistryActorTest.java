package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Status;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationGroupView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorsAllocation;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;
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
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

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
        probe.expectMsgClass(Ack.class);

        registry.tell(
            CancelReservation.builder().reservationKey(key).build(),
            probe.getRef());
        probe.expectMsgClass(Ack.class);

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView emptyView = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(emptyView.getGroups().isEmpty());

        stopActor(registry);
    }

    @Test
    public void shouldDeduplicateOnIdenticalReservationShape() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

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
        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
            .tier(0)
            .timestamp(priorityTimestamp)
            .build();

        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        for (int i = 0; i < requestedWorkers; i++) {
            WorkerId workerId = WorkerId.fromId(
                String.format("%s-%d-worker-%d-%d", key.getJobId(), key.getStageNumber(), i, 0)
            ).get();
            allocationRequests.add(createAllocationRequest(
                workerId, constraints, key.getStageNumber()
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
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        ReservationKey key = ReservationKey.builder().jobId("job-batch").stageNumber(1).build();
        SchedulingConstraints constraints = constraints("sku-batch", Collections.emptyMap());

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-batch-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-batch-1-worker-0-2");

        TaskExecutorAllocationRequest req1 = createAllocationRequest(
            worker1, constraints, 1);

        TaskExecutorAllocationRequest req2 = createAllocationRequest(
            worker2, constraints, 1);

        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        allocationRequests.add(req1);
        allocationRequests.add(req2);

        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
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
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, Duration.ofMillis(100), null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-inflight", Collections.emptyMap());

        ReservationKey key1 = ReservationKey.builder().jobId("job-inflight-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-inflight-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-inflight-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-inflight-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = createAllocationRequest(
            worker1, constraints, 1);

        TaskExecutorAllocationRequest req2 = createAllocationRequest(
            worker2, constraints, 1);

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
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-success", Collections.emptyMap());

        ReservationKey key1 = ReservationKey.builder().jobId("job-success-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-success-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-success-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-success-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = createAllocationRequest(
            worker1, constraints, 1);

        TaskExecutorAllocationRequest req2 = createAllocationRequest(
            worker2, constraints, 1);

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

        registry.tell(allocation, probe.getRef());

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
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-cancel", Collections.emptyMap());

        ReservationKey key1 = ReservationKey.builder().jobId("job-cancel-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-cancel-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-cancel-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-cancel-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = createAllocationRequest(
            worker1, constraints, 1);

        TaskExecutorAllocationRequest req2 = createAllocationRequest(
            worker2, constraints, 1);

        upsertWithAllocations(registry, probe, key1, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());
        upsertWithAllocations(registry, probe, key2, constraints, Set.of(req2), 1, BASE_INSTANT.plusSeconds(1).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        parent.expectMsgClass(Duration.ofSeconds(2), TaskExecutorBatchAssignmentRequest.class);

        registry.tell(CancelReservation.builder().reservationKey(key1).build(), probe.getRef());
        probe.expectMsgClass(Ack.class);

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
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraintsA = constraints("sku-a", ImmutableMap.of("zone", "us-west"));
        SchedulingConstraints constraintsB = constraints("sku-b", ImmutableMap.of("zone", "us-east"));

        ReservationKey keyA = ReservationKey.builder().jobId("job-a").stageNumber(1).build();
        ReservationKey keyB = ReservationKey.builder().jobId("job-b").stageNumber(1).build();

        WorkerId workerA = WorkerId.fromIdUnsafe("job-a-worker-0-1");
        WorkerId workerB = WorkerId.fromIdUnsafe("job-b-worker-0-1");

        TaskExecutorAllocationRequest reqA = createAllocationRequest(
            workerA, constraintsA, 1);

        TaskExecutorAllocationRequest reqB = createAllocationRequest(
            workerB, constraintsB, 1);

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
        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
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

    @Test
    public void shouldRetryInFlightReservationAfterTimeout() throws InterruptedException {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);

        // Use a short timeout for testing - we'll use actual time passing
        Duration timeout = Duration.ofMillis(150);
        Clock systemClock = Clock.systemUTC();

        ActorRef registry = parent.childActorOf(
            ReservationRegistryActor.props(TEST_CLUSTER_ID, systemClock, PROCESS_INTERVAL, timeout, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-timeout", Collections.emptyMap());
        ReservationKey key = ReservationKey.builder().jobId("job-timeout").stageNumber(1).build();
        WorkerId worker1 = WorkerId.fromIdUnsafe("job-timeout-1-worker-0-1");
        TaskExecutorAllocationRequest req1 = createAllocationRequest(
            worker1, constraints, 1);

        upsertWithAllocations(registry, probe, key, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Wait for the batch request to be sent (reservation becomes in-flight with timestamp set to now)
        TaskExecutorBatchAssignmentRequest firstRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(firstRequest);
        assertEquals(1, firstRequest.getAllocationRequests().size());
        assertTrue(firstRequest.getAllocationRequests().contains(req1));

        // Wait for the timeout period to pass
        Thread.sleep(timeout.toMillis() + 50);

        // Force process - the reservation should now timeout and be retried
        registry.tell(ResourceClusterActor.ForceProcessReservationsTick.INSTANCE, probe.getRef());

        // After timeout, the reservation should be retried (cleared and re-sent)
        TaskExecutorBatchAssignmentRequest retryRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(retryRequest);
        assertEquals(1, retryRequest.getAllocationRequests().size());
        assertTrue(retryRequest.getAllocationRequests().contains(req1));

        stopActor(registry);
    }

    @Test
    public void shouldUpdateTimestampOnStatusFailure() throws InterruptedException {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);

        Duration timeout = Duration.ofMillis(200);
        Clock systemClock = Clock.systemUTC();

        ActorRef registry = parent.childActorOf(
            ReservationRegistryActor.props(TEST_CLUSTER_ID, systemClock, Duration.ofMillis(1000), timeout, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-failure", Collections.emptyMap());
        ReservationKey key = ReservationKey.builder().jobId("job-failure").stageNumber(1).build();
        WorkerId worker1 = WorkerId.fromIdUnsafe("job-failure-1-worker-0-1");
        TaskExecutorAllocationRequest req1 = createAllocationRequest(
            worker1, constraints, 1);

        upsertWithAllocations(registry, probe, key, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Wait for the batch request to be sent
        TaskExecutorBatchAssignmentRequest firstRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(firstRequest);
        Reservation reservation = firstRequest.getReservation();
        assertNotNull(reservation);
        String constraintKey = reservation.getCanonicalConstraintKey();

        // Wait a bit to let some time pass
        Thread.sleep(100);

        // Send Status.Failure with NoResourceAvailableException containing the constraint key
        // This should update the timestamp for the matching reservation
        NoResourceAvailableException exception = new NoResourceAvailableException(
            "No resource available for test", constraintKey);
        Status.Failure failure = new Status.Failure(exception);

        registry.tell(failure, parent.getRef());

        // Now wait for most of the timeout period (but not all, since timestamp was just updated)
        Thread.sleep(110);

        // Force process - since timestamp was updated 100ms ago, it should NOT timeout yet
        // (timeout is 200ms, so we're still within the timeout window)
        registry.tell(ResourceClusterActor.ForceProcessReservationsTick.INSTANCE, probe.getRef());

        // Should NOT receive another request immediately since timestamp was updated
        parent.expectNoMessage(Duration.ofMillis(100));

        // Now wait for the full timeout period from the timestamp update
        //Thread.sleep(150);

        // Force process again - now it should timeout and retry
        registry.tell(ResourceClusterActor.ForceProcessReservationsTick.INSTANCE, probe.getRef());

        // Should receive retry request after timeout
        TaskExecutorBatchAssignmentRequest retryRequest = parent.expectMsgClass(
            Duration.ofSeconds(2),
            TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(retryRequest);
        assertEquals(1, retryRequest.getAllocationRequests().size());
        assertTrue(retryRequest.getAllocationRequests().contains(req1));

        stopActor(registry);
    }


    @Test
    public void shouldOrderReservationsByPriority() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-priority", Collections.emptyMap());

        // 1. NEW_JOB (Lowest priority)
        ReservationKey keyLow = ReservationKey.builder().jobId("job-low").stageNumber(1).build();
        upsert(registry, probe, keyLow, constraints, 1, 1,
            ReservationPriority.PriorityType.NEW_JOB, 0, BASE_INSTANT.toEpochMilli());

        // 2. REPLACE (Highest priority)
        ReservationKey keyHigh = ReservationKey.builder().jobId("job-high").stageNumber(1).build();
        upsert(registry, probe, keyHigh, constraints, 1, 1,
            ReservationPriority.PriorityType.REPLACE, 0, BASE_INSTANT.toEpochMilli());

        // 3. SCALE (Medium priority)
        ReservationKey keyMedium = ReservationKey.builder().jobId("job-medium").stageNumber(1).build();
        upsert(registry, probe, keyMedium, constraints, 1, 1,
            ReservationPriority.PriorityType.SCALE, 0, BASE_INSTANT.toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);

        PendingReservationGroupView group = view.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(group);
        assertEquals(3, group.getReservations().size());

        // Expect order: REPLACE, SCALE, NEW_JOB
        assertEquals(keyHigh, group.getReservations().get(0).getReservationKey());
        assertEquals(keyMedium, group.getReservations().get(1).getReservationKey());
        assertEquals(keyLow, group.getReservations().get(2).getReservationKey());

        stopActor(registry);
    }

    @Test
    public void shouldOrderReservationsByTimestampWithinSamePriority() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-time", Collections.emptyMap());

        // 1. Middle timestamp
        ReservationKey keyMid = ReservationKey.builder().jobId("job-mid").stageNumber(1).build();
        upsert(registry, probe, keyMid, constraints, 1, 1,
            ReservationPriority.PriorityType.NEW_JOB, 0, BASE_INSTANT.plusSeconds(10).toEpochMilli());

        // 2. Oldest timestamp (First)
        ReservationKey keyOld = ReservationKey.builder().jobId("job-old").stageNumber(1).build();
        upsert(registry, probe, keyOld, constraints, 1, 1,
            ReservationPriority.PriorityType.NEW_JOB, 0, BASE_INSTANT.toEpochMilli());

        // 3. Newest timestamp (Last)
        ReservationKey keyNew = ReservationKey.builder().jobId("job-new").stageNumber(1).build();
        upsert(registry, probe, keyNew, constraints, 1, 1,
            ReservationPriority.PriorityType.NEW_JOB, 0, BASE_INSTANT.plusSeconds(20).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);

        PendingReservationGroupView group = view.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(group);
        assertEquals(3, group.getReservations().size());

        // Expect order: Old, Mid, New
        assertEquals(keyOld, group.getReservations().get(0).getReservationKey());
        assertEquals(keyMid, group.getReservations().get(1).getReservationKey());
        assertEquals(keyNew, group.getReservations().get(2).getReservationKey());

        stopActor(registry);
    }

    private static void upsert(
        ActorRef registry,
        TestKit probe,
        ReservationKey key,
        SchedulingConstraints constraints,
        int requestedWorkers,
        int stageTargetSize,
        ReservationPriority.PriorityType type,
        int tier,
        long priorityTimestamp
    ) {
        ReservationPriority priority = ReservationPriority.builder()
            .type(type)
            .tier(tier)
            .timestamp(priorityTimestamp)
            .build();

        Set<TaskExecutorAllocationRequest> allocationRequests = new HashSet<>();
        for (int i = 0; i < requestedWorkers; i++) {
            WorkerId workerId = WorkerId.fromId(
                String.format("%s-%d-worker-%d-%d", key.getJobId(), key.getStageNumber(), i, 0)
            ).get();
            allocationRequests.add(createAllocationRequest(
                workerId, constraints, key.getStageNumber()
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

    private static TaskExecutorAllocationRequest createAllocationRequest(
        WorkerId workerId,
        SchedulingConstraints constraints,
        int stageNum
    ) {
        return TaskExecutorAllocationRequest.of(workerId, constraints, null, stageNum);
    }

    @Test
    public void shouldReturnNotReadyWhenRegistryNotReady() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-test", Collections.emptyMap());
        upsert(registry, probe, "job-test", 1, constraints, 2, 2);

        // Request before marking ready
        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);
        assertFalse(response.isReady());
        assertTrue(response.getReservations().isEmpty());

        stopActor(registry);
    }

    @Test
    public void shouldReturnEmptyListWhenReadyButNoReservations() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);
        assertTrue(response.isReady());
        assertTrue(response.getReservations().isEmpty());

        stopActor(registry);
    }

    @Test
    public void shouldReturnReservationsWithActualSchedulingConstraints() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints skuAConstraints = constraints("sku-a", ImmutableMap.of("zone", "us-west"));
        SchedulingConstraints skuBConstraints = constraints("sku-b", ImmutableMap.of("zone", "us-east"));

        upsert(registry, probe, "jobA", 1, skuAConstraints, 3, 3);
        upsert(registry, probe, "jobB", 2, skuAConstraints, 2, 2);
        upsert(registry, probe, "jobC", 1, skuBConstraints, 4, 4);

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);

        assertTrue(response.isReady());
        assertEquals(2, response.getReservations().size());

        // Find the two constraint groups
        ReservationRegistryActor.PendingReservationInfoSnapshot groupA = response.getReservations().stream()
            .filter(r -> r.getCanonicalConstraintKey().equals(canonicalKeyFor(skuAConstraints)))
            .findFirst()
            .orElse(null);
        assertNotNull(groupA);
        assertEquals(2, groupA.getReservationCount());
        assertEquals(5, groupA.getTotalRequestedWorkers()); // 3 + 2
        assertNotNull(groupA.getSchedulingConstraints());
        assertEquals(skuAConstraints.getSizeName(), groupA.getSchedulingConstraints().getSizeName());
        assertEquals(skuAConstraints.getSchedulingAttributes(), groupA.getSchedulingConstraints().getSchedulingAttributes());

        ReservationRegistryActor.PendingReservationInfoSnapshot groupB = response.getReservations().stream()
            .filter(r -> r.getCanonicalConstraintKey().equals(canonicalKeyFor(skuBConstraints)))
            .findFirst()
            .orElse(null);
        assertNotNull(groupB);
        assertEquals(1, groupB.getReservationCount());
        assertEquals(4, groupB.getTotalRequestedWorkers());
        assertNotNull(groupB.getSchedulingConstraints());
        assertEquals(skuBConstraints.getSizeName(), groupB.getSchedulingConstraints().getSizeName());
        assertEquals(skuBConstraints.getSchedulingAttributes(), groupB.getSchedulingConstraints().getSchedulingAttributes());

        stopActor(registry);
    }

    @Test
    public void shouldReturnCorrectReservationCountsPerConstraintGroup() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-count", Collections.emptyMap());

        // Add multiple reservations to same constraint group
        upsert(registry, probe, "job1", 1, constraints, 2, 2);
        upsert(registry, probe, "job2", 1, constraints, 3, 3);
        upsert(registry, probe, "job3", 1, constraints, 1, 1);

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);

        assertTrue(response.isReady());
        assertEquals(1, response.getReservations().size());

        ReservationRegistryActor.PendingReservationInfoSnapshot snapshot = response.getReservations().get(0);
        assertEquals(3, snapshot.getReservationCount()); // 3 reservations
        assertEquals(6, snapshot.getTotalRequestedWorkers()); // 2 + 3 + 1
        assertEquals(canonicalKeyFor(constraints), snapshot.getCanonicalConstraintKey());
        assertNotNull(snapshot.getSchedulingConstraints());

        stopActor(registry);
    }

    @Test
    public void shouldHandleReservationsWithDifferentMachineDefinitions() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        MachineDefinition machine1 = new MachineDefinition(2.0, 4096, 128.0, 10240, 1);
        MachineDefinition machine2 = new MachineDefinition(4.0, 8192, 256.0, 20480, 2);

        SchedulingConstraints constraints1 = SchedulingConstraints.of(machine1, Optional.of("size-1"), Collections.emptyMap());
        SchedulingConstraints constraints2 = SchedulingConstraints.of(machine2, Optional.of("size-2"), Collections.emptyMap());

        upsert(registry, probe, "job1", 1, constraints1, 2, 2);
        upsert(registry, probe, "job2", 1, constraints2, 3, 3);

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);

        assertTrue(response.isReady());
        assertEquals(2, response.getReservations().size());

        // Verify machine definitions are preserved in SchedulingConstraints
        for (ReservationRegistryActor.PendingReservationInfoSnapshot snapshot : response.getReservations()) {
            assertNotNull(snapshot.getSchedulingConstraints());
            assertNotNull(snapshot.getSchedulingConstraints().getMachineDefinition());
        }

        stopActor(registry);
    }

    @Test
    public void shouldSkipEmptyConstraintGroups() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-test", Collections.emptyMap());

        // Add a reservation and then cancel it
        ReservationKey key = ReservationKey.builder().jobId("job-test").stageNumber(1).build();
        upsert(registry, probe, key, constraints, 2, 2);
        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Cancel the reservation
        registry.tell(CancelReservation.builder().reservationKey(key).build(), probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Request scaler reservations - should return empty list since group was removed
        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);

        assertTrue(response.isReady());
        assertTrue(response.getReservations().isEmpty());

        stopActor(registry);
    }

    @Test
    public void shouldReturnActualSchedulingConstraintsNotParsed() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        // Use complex constraints with attributes
        SchedulingConstraints originalConstraints = constraints("sku-complex", ImmutableMap.of("zone", "us-west", "env", "prod"));

        upsert(registry, probe, "job-complex", 1, originalConstraints, 2, 2);

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE, probe.getRef());
        ReservationRegistryActor.PendingReservationsForScalerResponse response = probe.expectMsgClass(
            ReservationRegistryActor.PendingReservationsForScalerResponse.class);

        assertTrue(response.isReady());
        assertEquals(1, response.getReservations().size());

        ReservationRegistryActor.PendingReservationInfoSnapshot snapshot = response.getReservations().get(0);
        SchedulingConstraints returnedConstraints = snapshot.getSchedulingConstraints();

        // Verify it's the actual object (same machine definition, size name, attributes)
        assertNotNull(returnedConstraints);
        assertEquals(originalConstraints.getSizeName(), returnedConstraints.getSizeName());
        assertEquals(originalConstraints.getSchedulingAttributes(), returnedConstraints.getSchedulingAttributes());
        assertEquals(originalConstraints.getMachineDefinition().getCpuCores(),
            returnedConstraints.getMachineDefinition().getCpuCores(), 0.01);
        assertEquals(originalConstraints.getMachineDefinition().getMemoryMB(),
            returnedConstraints.getMachineDefinition().getMemoryMB(), 0.01);

        stopActor(registry);
    }

    @Test
    public void shouldOrderReservationsDeterministicallyByJobId() {
        TestKit probe = new TestKit(system);
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, PROCESS_INTERVAL, null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-ordering", Collections.emptyMap());

        // Upsert 3 reservations with identical priority timestamps but different jobIds
        ReservationKey keyC = ReservationKey.builder().jobId("job-c").stageNumber(1).build();
        upsert(registry, probe, keyC, constraints, 1, 1, BASE_INSTANT.toEpochMilli());

        ReservationKey keyA = ReservationKey.builder().jobId("job-a").stageNumber(1).build();
        upsert(registry, probe, keyA, constraints, 1, 1, BASE_INSTANT.toEpochMilli());

        ReservationKey keyB = ReservationKey.builder().jobId("job-b").stageNumber(1).build();
        upsert(registry, probe, keyB, constraints, 1, 1, BASE_INSTANT.toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        registry.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);

        PendingReservationGroupView group = view.getGroups().get(canonicalKeyFor(constraints));
        assertNotNull(group);
        assertEquals(3, group.getReservations().size());

        // Since all have the same priority, thenComparing by jobId should sort them alphabetically
        assertEquals("job-a", group.getReservations().get(0).getReservationKey().getJobId());
        assertEquals("job-b", group.getReservations().get(1).getReservationKey().getJobId());
        assertEquals("job-c", group.getReservations().get(2).getReservationKey().getJobId());

        stopActor(registry);
    }

    /**
     * D4 fix: When an in-flight reservation completes, the in-flight entry should be cleared
     * unconditionally by constraint key, allowing other reservations in the same constraint
     * group to proceed. This verifies that clearInFlightIfMatches matches by constraint key
     * alone (not full equals), so a second reservation in the same group can go in-flight
     * after the first completes.
     */
    @Test
    public void shouldClearInFlightAfterReservationUpdate() {
        TestKit probe = new TestKit(system);
        TestKit parent = new TestKit(system);
        ActorRef registry = parent.childActorOf(ReservationRegistryActor.props(TEST_CLUSTER_ID, FIXED_CLOCK, Duration.ofMillis(100), null, null, new ResourceClusterActorMetrics()));

        SchedulingConstraints constraints = constraints("sku-inflight-update", Collections.emptyMap());

        // Two reservations with same constraint key but different reservation keys
        ReservationKey key1 = ReservationKey.builder().jobId("job-update-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-update-2").stageNumber(1).build();

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-update-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-update-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = createAllocationRequest(worker1, constraints, 1);
        TaskExecutorAllocationRequest req2 = createAllocationRequest(worker2, constraints, 1);

        upsertWithAllocations(registry, probe, key1, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());
        upsertWithAllocations(registry, probe, key2, constraints, Set.of(req2), 1, BASE_INSTANT.plusSeconds(1).toEpochMilli());

        registry.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // First reservation goes in-flight (batch request sent to parent)
        TaskExecutorBatchAssignmentRequest firstRequest = parent.expectMsgClass(
            Duration.ofSeconds(2), TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(firstRequest);
        Reservation completedReservation = firstRequest.getReservation();

        // While first is in-flight, upsert an UPDATE for key1 with different worker count
        // This changes the registry entry but not the in-flight entry
        WorkerId worker1b = WorkerId.fromIdUnsafe("job-update-1-worker-0-2");
        TaskExecutorAllocationRequest req1b = createAllocationRequest(worker1b, constraints, 1);
        upsertWithAllocations(registry, probe, key1, constraints, Set.of(req1, req1b), 2, BASE_INSTANT.toEpochMilli());

        // Simulate allocation result for the ORIGINAL reservation (stale version)
        TaskExecutorsAllocation allocation = new TaskExecutorsAllocation(Collections.emptyMap(), completedReservation);
        registry.tell(allocation, probe.getRef());

        // Force a processing tick - the in-flight should be cleared, allowing key2 to proceed
        registry.tell(ResourceClusterActor.ForceProcessReservationsTick.INSTANCE, probe.getRef());

        // Verify the second reservation can now go in-flight
        TaskExecutorBatchAssignmentRequest secondRequest = parent.expectMsgClass(
            Duration.ofSeconds(2), TaskExecutorBatchAssignmentRequest.class);
        assertNotNull(secondRequest);

        stopActor(registry);
    }

    private static void stopActor(ActorRef actor) {
        TestKit watcher = new TestKit(system);
        watcher.watch(actor);
        system.stop(actor);
        watcher.expectTerminated(actor);
    }
}
