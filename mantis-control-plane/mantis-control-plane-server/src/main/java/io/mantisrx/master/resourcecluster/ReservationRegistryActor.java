package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CancelReservation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CancelReservationAck;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.MarkReady;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationGroupView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ProcessReservationsTick;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.Reservation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ReservationKey;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.UpsertReservation;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Actor responsible for tracking and prioritizing reservations per scheduling constraint. The actor keeps all
 * reservation state in-memory under the resource cluster and notifies the parent when batches are ready to assign.
 * Lifecycle integration with executor state manager will be added in a follow-up change.
 */
@Slf4j
public class ReservationRegistryActor extends AbstractActorWithTimers {
    private static final String TIMER_KEY_PROCESS = "reservation-registry-process";
    private static final Duration DEFAULT_PROCESS_INTERVAL = Duration.ofMillis(1000);

    private final Clock clock;
    private final Duration processingInterval;
    private final Duration processingCooldown;
    private final Comparator<ReservationEntry> reservationComparator;

    private final Map<ReservationKey, ReservationEntry> reservationsByKey;
    private final Map<String, ConstraintGroup> reservationsByConstraint;

    private long sequenceGenerator;
    private boolean ready;
    private Instant lastProcessAt;

    public ReservationRegistryActor(Clock clock, Duration processingInterval) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.processingInterval = processingInterval == null ? DEFAULT_PROCESS_INTERVAL : processingInterval;
        this.processingCooldown = this.processingInterval.dividedBy(2);
        this.reservationComparator =
            Comparator
                .comparingLong(ReservationEntry::getPriorityEpoch)
                .thenComparingLong(ReservationEntry::getSequence)
                .thenComparing(entry -> entry.getReservation().getKey().getJobId())
                .thenComparingInt(entry -> entry.getReservation().getKey().getStageNumber());
        this.reservationsByKey = new HashMap<>();
        this.reservationsByConstraint = new HashMap<>();
        this.lastProcessAt = Instant.EPOCH;
    }

    public static Props props(Clock clock, Duration processingInterval) {
        return Props.create(ReservationRegistryActor.class, clock, processingInterval);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getTimers().startTimerWithFixedDelay(TIMER_KEY_PROCESS, ProcessReservationsTick.INSTANCE, processingInterval);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(UpsertReservation.class, this::onUpsertReservation)
            .match(CancelReservation.class, this::onCancelReservation)
            .match(GetPendingReservationsView.class, this::onGetPendingReservationsView)
            .match(MarkReady.class, message -> onMarkReady())
            .match(ProcessReservationsTick.class, message -> onProcessReservationsTick())
            .build();
    }

    private void onMarkReady() {
        if (!ready) {
            ready = true;
            log.info("Reservation registry marked ready; pending reservations={}", reservationsByKey.size());
        }
        sender().tell(Ack.getInstance(), self());
        triggerProcessingLoop();
    }

    private void onUpsertReservation(UpsertReservation message) {
        ReservationKey key = message.getReservationKey();
        ReservationEntry existing = reservationsByKey.get(key);

        Instant now = clock.instant();
        String canonicalConstraintKey = canonicalize(message.getSchedulingConstraints());
        long priorityEpoch = message.getPriorityOverride()
            .orElseGet(() -> existing != null ? existing.getPriorityEpoch() : now.toEpochMilli());
        long sequence = existing != null ? existing.getSequence() : sequenceGenerator++;

        Reservation reservation = Reservation.builder()
            .key(key)
            .schedulingConstraints(message.getSchedulingConstraints())
            .canonicalConstraintKey(canonicalConstraintKey)
            .requestedWorkers(message.getRequestedWorkers())
            .stageTargetSize(message.getStageTargetSize())
            .createdAt(existing != null ? existing.getReservation().getCreatedAt() : now)
            .lastUpdatedAt(now)
            .build();

        if (existing != null && existing.getReservation().hasSameShape(reservation)) {
            Reservation refreshed = existing.getReservation().toBuilder()
                .lastUpdatedAt(now)
                .build();
            ReservationEntry refreshedEntry = new ReservationEntry(refreshed, existing.getPriorityEpoch(), existing.getSequence());
            replaceEntry(existing, refreshedEntry);
            sender().tell(Ack.getInstance(), self());
            triggerProcessingLoop();
            return;
        }

        ReservationEntry newEntry = new ReservationEntry(reservation, priorityEpoch, sequence);
        if (existing != null) {
            removeEntry(existing);
        }

        addEntry(newEntry);
        log.debug("Upserted reservation {} (priorityEpoch={}, requestedWorkers={})",
            key, priorityEpoch, reservation.getRequestedWorkers());
        sender().tell(Ack.getInstance(), self());
        triggerProcessingLoop();
    }

    private void onCancelReservation(CancelReservation cancel) {
        ReservationEntry existing = reservationsByKey.get(cancel.getReservationKey());
        if (existing != null) {
            removeEntry(existing);
            log.debug("Cancelled reservation {}", cancel.getReservationKey());
            sender().tell(new CancelReservationAck(cancel.getReservationKey(), true), self());
            triggerProcessingLoop();
        } else {
            sender().tell(new CancelReservationAck(cancel.getReservationKey(), false), self());
        }
    }

    private void onGetPendingReservationsView(GetPendingReservationsView request) {
        if (!ready) {
            sender().tell(
                PendingReservationsView.builder()
                    .ready(ready)
                    .build(),
                self());
            return;
        }

        Map<String, PendingReservationGroupView> groups = new LinkedHashMap<>();
        reservationsByConstraint.forEach((key, group) -> groups.put(key, group.snapshot()));
        sender().tell(
            PendingReservationsView.builder()
                .ready(ready)
                .groups(groups)
                .build(),
            self());
    }

    private void onProcessReservationsTick() {
        Instant now = clock.instant();
        int totalReservations = reservationsByConstraint.values()
            .stream()
            .mapToInt(ConstraintGroup::size)
            .sum();
        if (totalReservations == 0) {
            return;
        }

        Duration sinceLastProcess = Duration.between(lastProcessAt, now);
        if (sinceLastProcess.compareTo(processingCooldown) < 0) {
            log.trace("Skipping reservation processing tick due to cooldown (sinceLast={}ms, cooldown={}ms)",
                sinceLastProcess.toMillis(),
                processingCooldown.toMillis());
            return;
        }

        lastProcessAt = now;
        // TODO (reservation-registry): integrate with ExecutorStateManagerActor for allocation signals.
        log.trace("Reservation registry tick - queued reservations: {}", totalReservations);
    }

    private void addEntry(ReservationEntry entry) {
        reservationsByKey.put(entry.getReservation().getKey(), entry);
        ConstraintGroup group = reservationsByConstraint.computeIfAbsent(
            entry.getReservation().getCanonicalConstraintKey(),
            key -> new ConstraintGroup(key, reservationComparator));
        group.add(entry);
    }

    private void replaceEntry(ReservationEntry existing, ReservationEntry replacement) {
        removeEntry(existing);
        addEntry(replacement);
    }

    private void removeEntry(ReservationEntry entry) {
        ReservationEntry mapped = reservationsByKey.get(entry.getReservation().getKey());
        if (mapped == entry) {
            reservationsByKey.remove(entry.getReservation().getKey());
        }
        ConstraintGroup group = reservationsByConstraint.get(entry.getReservation().getCanonicalConstraintKey());
        if (group != null) {
            group.remove(entry);
            if (group.isEmpty()) {
                reservationsByConstraint.remove(group.getCanonicalConstraintKey());
            }
        }
    }

    private void triggerProcessingLoop() {
        self().tell(ProcessReservationsTick.INSTANCE, self());
    }

    private static String canonicalize(@Nullable SchedulingConstraints constraints) {
        if (constraints == null) {
            return "constraints:none";
        }

        StringBuilder builder = new StringBuilder();
        MachineDefinition machineDefinition = constraints.getMachineDefinition();
        if (machineDefinition != null) {
            builder.append("md:")
                .append(machineDefinition.getCpuCores()).append('/')
                .append(machineDefinition.getMemoryMB()).append('/')
                .append(machineDefinition.getDiskMB()).append('/')
                .append(machineDefinition.getNetworkMbps()).append('/')
                .append(machineDefinition.getNumPorts());
        } else {
            builder.append("md:none");
        }

        builder.append(";size=").append(constraints.getSizeName().orElse("~"));

        Map<String, String> attributes = constraints.getSchedulingAttributes();
        if (attributes != null && !attributes.isEmpty()) {
            builder.append(";attr=");
            attributes.entrySet()
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

    private static final class ConstraintGroup {
        private final String canonicalConstraintKey;
        private final NavigableSet<ReservationEntry> queue;
        private int totalRequestedWorkers;

        private ConstraintGroup(String canonicalConstraintKey, Comparator<ReservationEntry> comparator) {
            this.canonicalConstraintKey = canonicalConstraintKey;
            this.queue = new TreeSet<>(comparator);
            this.totalRequestedWorkers = 0;
        }

        void add(ReservationEntry entry) {
            if (queue.add(entry)) {
                totalRequestedWorkers += entry.getReservation().getRequestedWorkers();
            }
        }

        void remove(ReservationEntry entry) {
            if (queue.remove(entry)) {
                totalRequestedWorkers -= entry.getReservation().getRequestedWorkers();
            }
        }

        boolean isEmpty() {
            return queue.isEmpty();
        }

        String getCanonicalConstraintKey() {
            return canonicalConstraintKey;
        }

        PendingReservationGroupView snapshot() {
            List<ReservationSnapshot> reservations = queue
                .stream()
                .map(ReservationSnapshot::fromEntry)
                .collect(Collectors.toList());
            return PendingReservationGroupView.builder()
                .canonicalConstraintKey(canonicalConstraintKey)
                .reservationCount(queue.size())
                .totalRequestedWorkers(totalRequestedWorkers)
                .reservations(reservations)
                .build();
        }

        int size() {
            return queue.size();
        }
    }

    @Value
    static class ReservationEntry {
        Reservation reservation;
        long priorityEpoch;
        long sequence;
    }

    @Value
    @Builder
    public static class ReservationSnapshot {
        ReservationKey reservationKey;
        int requestedWorkers;
        int stageTargetSize;
        Instant createdAt;
        Instant lastUpdatedAt;
        long priorityEpoch;

        static ReservationSnapshot fromEntry(ReservationEntry entry) {
            Reservation reservation = entry.getReservation();
            return ReservationSnapshot.builder()
                .reservationKey(reservation.getKey())
                .requestedWorkers(reservation.getRequestedWorkers())
                .stageTargetSize(reservation.getStageTargetSize())
                .createdAt(reservation.getCreatedAt())
                .lastUpdatedAt(reservation.getLastUpdatedAt())
                .priorityEpoch(entry.getPriorityEpoch())
                .build();
        }
    }
}
