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
import io.mantisrx.master.resourcecluster.ResourceClusterActor.UpsertReservation;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(FIXED_CLOCK, PROCESS_INTERVAL));

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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(FIXED_CLOCK, PROCESS_INTERVAL));

        ReservationKey key = ReservationKey.builder().jobId("job-ready").stageNumber(1).build();
        SchedulingConstraints constraints = constraints("sku-ready", Collections.emptyMap());

        upsert(registry, probe, key, constraints, 3, 3);

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
        ActorRef registry = system.actorOf(ReservationRegistryActor.props(FIXED_CLOCK, PROCESS_INTERVAL));

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
        UpsertReservation upsert =
            UpsertReservation.builder()
                .reservationKey(key)
                .schedulingConstraints(constraints)
                .requestedWorkers(requestedWorkers)
                .stageTargetSize(stageTargetSize)
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

    private static void stopActor(ActorRef actor) {
        TestKit watcher = new TestKit(system);
        watcher.watch(actor);
        system.stop(actor);
        watcher.expectTerminated(actor);
    }
}
