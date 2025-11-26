package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status;
import akka.pattern.Patterns;
import io.mantisrx.common.Ack;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationGroupView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ProcessReservationsTick;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import io.mantisrx.server.worker.TaskExecutorGateway;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import scala.compat.java8.FutureConverters;
import com.netflix.spectator.api.TagList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;

import static akka.pattern.Patterns.pipe;

/**
 * Actor responsible for tracking and prioritizing reservations per scheduling constraint. The actor keeps all
 * reservation state in-memory under the resource cluster and notifies the parent when batches are ready to assign.
 * Lifecycle integration with executor state manager will be added in a follow-up change.
 */
@Slf4j
public class ReservationRegistryActor extends AbstractActorWithTimers {
    private static final String TIMER_KEY_PROCESS = "reservation-registry-process";
    private static final String TIMER_KEY_AUTO_MARK_READY = "reservation-registry-auto-mark-ready";
    private static final Duration DEFAULT_PROCESS_INTERVAL = Duration.ofMillis(1000);
    private static final Duration DEFAULT_AUTO_MARK_READY_TIMEOUT = Duration.ofMinutes(5);

    private final ClusterID clusterID;
    private final Clock clock;
    private final Duration processingInterval;
    private final Duration inFlightReservationTimeout;
    private final Duration autoMarkReadyTimeout;
    private final ResourceClusterActorMetrics metrics;

    private final Duration processingCooldown;
    private final Comparator<Reservation> reservationComparator;

    private final Map<ReservationKey, LinkedList<Reservation>> reservationsByKey;
    private final Map<String, ConstraintGroup> reservationsByConstraint;
    private final Map<String, Reservation> inFlightReservations;
    private final Map<String, Instant> inFlightReservationRequestTimestamps;

    private boolean ready;
    private Instant lastProcessAt;

    public ReservationRegistryActor(ClusterID clusterID, Clock clock, Duration processingInterval, Duration inFlightReservationTimeout, Duration autoMarkReadyTimeout, ResourceClusterActorMetrics metrics) {
        this.clusterID = clusterID;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.processingInterval = processingInterval == null ? DEFAULT_PROCESS_INTERVAL : processingInterval;
        this.processingCooldown = this.processingInterval.dividedBy(2);
        this.inFlightReservationTimeout = inFlightReservationTimeout == null ? this.processingInterval.multipliedBy(5) : inFlightReservationTimeout;
        this.autoMarkReadyTimeout = autoMarkReadyTimeout == null ? DEFAULT_AUTO_MARK_READY_TIMEOUT : autoMarkReadyTimeout;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.reservationComparator = Comparator
            .comparing(Reservation::getPriority)
            .thenComparingInt(System::identityHashCode);
        this.reservationsByKey = new HashMap<>();
        this.reservationsByConstraint = new HashMap<>();
        this.inFlightReservations = new HashMap<>();
        this.inFlightReservationRequestTimestamps = new HashMap<>();
        this.lastProcessAt = Instant.EPOCH;
    }

    public static Props props(ClusterID clusterID, Clock clock, Duration processingInterval, Duration inFlightReservationTimeout, Duration autoMarkReadyTimeout, ResourceClusterActorMetrics metrics) {
        return Props.create(ReservationRegistryActor.class, clusterID, clock, processingInterval, inFlightReservationTimeout, autoMarkReadyTimeout, metrics);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getTimers().startTimerWithFixedDelay(TIMER_KEY_PROCESS, ProcessReservationsTick.INSTANCE, processingInterval);
        // Start a one-time timer to auto-mark ready after the configured timeout if not already ready
        getTimers().startSingleTimer(TIMER_KEY_AUTO_MARK_READY, AutoMarkReadyTick.INSTANCE, autoMarkReadyTimeout);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(UpsertReservation.class, this::onUpsertReservation)
            .match(CancelReservation.class, this::onCancelReservation)
            .match(GetPendingReservationsView.class, this::onGetPendingReservationsView)
            .match(MarkReady.class, message -> onMarkReady())
            .match(ProcessReservationsTick.class, message -> onProcessReservationsTick(false))
            .match(ResourceClusterActor.ForceProcessReservationsTick.class, message -> onProcessReservationsTick(true))
            .match(AutoMarkReadyTick.class, message -> onAutoMarkReadyTick())
            // .match(ReservationAllocationResponse.class, this::onTaskExecutorBatchAssignmentResult)
            .match(ResourceClusterActor.TaskExecutorsAllocation.class, this::onTaskExecutorBatchAssignmentResult)
            .match(Status.Failure.class, this::onStatusFailure)
            .build();
    }

    private void onMarkReady() {
        if (!ready) {
            ready = true;
            log.info("Reservation registry marked ready; pending reservations={}", reservationsByKey.size());
            // Cancel the auto-mark-ready timer since we're now ready
            getTimers().cancel(TIMER_KEY_AUTO_MARK_READY);
        }
        sender().tell(Ack.getInstance(), self());
        triggerForcedProcessingLoop();
    }

    private void onAutoMarkReadyTick() {
        if (!ready) {
            log.info("Auto-mark-ready timer expired after {}ms; marking reservation registry as ready", autoMarkReadyTimeout.toMillis());
            ready = true;
            log.info("Reservation registry auto-marked ready; pending reservations={}", reservationsByKey.size());
            triggerForcedProcessingLoop();
        } else {
            log.debug("Auto-mark-ready timer fired but registry is already ready; ignoring");
        }
    }

    private void onUpsertReservation(UpsertReservation message) {
        ReservationKey key = message.getReservationKey();
        LinkedList<Reservation> existingReservations = reservationsByKey.get(key);
        String canonicalConstraintKey = canonicalize(message.getSchedulingConstraints());

        Reservation reservation = Reservation.fromUpsertReservation(
            message,
            canonicalConstraintKey
        );

        Reservation sameShapeReservation = findReservationWithSameShape(existingReservations, reservation);
        if (sameShapeReservation != null) {
            log.warn("Replacing existing reservation {} with new reservation {}", sameShapeReservation, reservation);
            replaceEntry(sameShapeReservation, reservation);

            // Metric for reservation update
            metrics.incrementCounter(
                ResourceClusterActorMetrics.RESERVATION_UPSERTED,
                TagList.create(ImmutableMap.of(
                    "resourceCluster",
                    clusterID.getResourceID(),
                    "jobId",
                    key.getJobId(),
                    "operation",
                    "update")));

            sender().tell(Ack.getInstance(), self());
            triggerProcessingLoop();
            return;
        }

        // todo: this implies that if a job with pending scaleup got scaled down, it needs to reset worker list and
        // re-submit a reservation with all to-be-created workers if any.
        if (existingReservations != null && !existingReservations.isEmpty()) {
            int latestTargetSize = existingReservations.getLast().getStageTargetSize();
            if (reservation.getStageTargetSize() < latestTargetSize) {
                new ArrayList<>(existingReservations).forEach(this::removeEntryAndClearInFlight);
            }
        }

        addEntry(reservation);

        // Metric for reservation insert
        metrics.incrementCounter(
            ResourceClusterActorMetrics.RESERVATION_UPSERTED,
            TagList.create(ImmutableMap.of(
                "resourceCluster",
                clusterID.getResourceID(),
                "jobId",
                key.getJobId(),
                "operation",
                "insert")));

        log.info("Upserted reservation {} (priority={}, requestedWorkers={})",
            key, reservation.getPriority(), reservation.getRequestedWorkersCount());
        sender().tell(Ack.getInstance(), self());
        triggerProcessingLoop();
    }

    private void onCancelReservation(CancelReservation cancel) {
        LinkedList<Reservation> existingReservations = reservationsByKey.get(cancel.getReservationKey());
        if (existingReservations != null && !existingReservations.isEmpty()) {
            new ArrayList<>(existingReservations).forEach(this::removeEntryAndClearInFlight);
            log.info("Cancelled reservation {}", cancel.getReservationKey());
            sender().tell(Ack.getInstance(), self());
            triggerForcedProcessingLoop();
        } else {
            sender().tell(Ack.getInstance(), self());
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

    private void onProcessReservationsTick(boolean forced) {
        Instant now = clock.instant();
        Duration sinceLastProcess = Duration.between(lastProcessAt, now);
        if (!forced && sinceLastProcess.compareTo(processingCooldown) < 0) {
            log.trace("Skipping reservation processing tick due to cooldown (sinceLast={}ms, cooldown={}ms)",
                sinceLastProcess.toMillis(),
                processingCooldown.toMillis());
            return;
        }

        lastProcessAt = now;
        log.trace("Reservation registry tick - queued reservations");
        processReservation();
    }

    /*
    Main process logic for each sku (constraintKey) is:
    - take the first item in queue and track it as inflight if inflight is empty.
    - send batch allocation request for the inflight reservation (only trigger once when it's insert as inflight).
    - process loop ends on the group. Rely on callback from allocation (ReservationAllocationResponse) to update inflight.
    - once inflight gets allocated the reservation is considered done. Any worker task submit error or worker init
        error is going to be tracked by JobActor worker level heartbeat timeout. This also requires the executorManager
        to send JobActor worker state update when the TEs are leased for allocation. This allows the reservation
        registry to be free from tracking those failure.
     */
    private void processReservation() {
        if (!ready) {
            log.trace("Reservation registry not ready; skipping processing");
            return;
        }

        for (ConstraintGroup group : reservationsByConstraint.values()) {
            String constraintKey = group.getCanonicalConstraintKey();
            Reservation inFlight = inFlightReservations.get(constraintKey);

            if (inFlight != null) {
                // Check if the in-flight reservation has timed out and needs to be retried
                Instant requestTimestamp = inFlightReservationRequestTimestamps.get(constraintKey);
                if (requestTimestamp != null) {
                    Duration timeSinceRequest = Duration.between(requestTimestamp, clock.instant());
                    if (timeSinceRequest.compareTo(inFlightReservationTimeout) >= 0) {
                        log.info("In-flight reservation {} for constraint group {} has timed out ({}ms), retrying",
                            inFlight.getKey(), constraintKey, timeSinceRequest.toMillis());

                        // Metric for inflight timeout
                        metrics.incrementCounter(
                            ResourceClusterActorMetrics.RESERVATION_INFLIGHT_TIMEOUT,
                            TagList.create(ImmutableMap.of(
                                "resourceCluster",
                                clusterID.getResourceID(),
                                "jobId",
                                inFlight.getKey().getJobId())));

                        // Clear the in-flight state to allow retry
                        inFlightReservations.remove(constraintKey);
                        inFlightReservationRequestTimestamps.remove(constraintKey);
                        // Continue processing to retry the reservation
                    } else {
                        log.info("Skipping constraint group {} - already has in-flight reservation {} (waiting for {}ms)",
                            constraintKey, inFlight.getKey(), inFlightReservationTimeout.minus(timeSinceRequest).toMillis());
                        continue;
                    }
                } else {
                    log.info("Skipping constraint group {} - already has in-flight reservation {}",
                        constraintKey, inFlight.getKey());
                    continue;
                }
            }

            // the top reservation is only removed from group from upsert change and success batch assignment reply.
            Optional<Reservation> topReservationO = group.peekTop();
            if (topReservationO.isEmpty()) {
                log.debug("Skipping constraint group {} - no reservation to process", constraintKey);
                continue;
            }

            log.info("Sending batch assignment request for reservation {} in constraint group {}",
                topReservationO.get().getKey(), constraintKey);

            Reservation reservation = topReservationO.get();
            metrics.incrementCounter(
                ResourceClusterActorMetrics.RESERVATION_PROCESSED,
                TagList.create(ImmutableMap.of(
                    "resourceCluster",
                    clusterID.getResourceID(),
                    "jobId",
                    reservation.getKey().getJobId(),
                    "constraintKey",
                    constraintKey)));

            inFlightReservations.put(constraintKey, reservation);
            inFlightReservationRequestTimestamps.put(constraintKey, clock.instant());

            ResourceClusterActor.TaskExecutorBatchAssignmentRequest request =
                new ResourceClusterActor.TaskExecutorBatchAssignmentRequest(
                    topReservationO.get().getAllocationRequests(),
                    this.clusterID,
                    topReservationO.get());

            // todo: make ESM track allocation results. Dedupe on repeated requests and only clear after registry ack.
            // todo: make batch request ack as reply. retry if allocation failed or inflight timeout.

//            CompletionStage<ReservationAllocationResponse> askFut = FutureConverters.toJava(
//                Patterns.ask(
//                    getContext().parent(),
//                    request,
//                    this.inFlightReservationTimeout.toMillis()))
//                .thenApply( res -> new ReservationAllocationResponse(
//                    ((ResourceClusterActor.TaskExecutorsAllocation) res).getReservation(),
//                    ((ResourceClusterActor.TaskExecutorsAllocation) res),
//                    null))
//                .exceptionally(failure -> new ReservationAllocationResponse(
//                    topReservationO.get(), null, failure))
//                ;
//
//            pipe(
//                askFut,
//                getContext().dispatcher())
//                .to(self());

            getContext().parent().tell(request, self());
        }
    }

    private void onTaskExecutorBatchAssignmentResult(ResourceClusterActor.TaskExecutorsAllocation result) {
        if (result == null || result.getReservation() == null) {
            log.warn("Received null reservation allocation result, ignoring");
            return;
        }

        Reservation reservation = result.getReservation();
        String constraintKey = reservation.getCanonicalConstraintKey();

        log.info("Received batch assignment result for reservation {} (constraintKey={})",
            reservation.getKey(), constraintKey);

        clearInFlightIfMatches(reservation);

        // todo: validate reservation match the result.
        removeEntry(reservation);
        triggerForcedProcessingLoop();

//        if (result.isSuccess()) {
//            removeEntry(reservation);
//            triggerForcedProcessingLoop();
//        } else {
//            log.warn("Reservation allocation failed for {}: {}",
//                result.getReservation().getKey(),
//                result.error != null ? result.error.getMessage() : "unknown error");
//        }
    }

    private void clearInFlightIfMatches(Reservation reservation) {
        String constraintKey = reservation.getCanonicalConstraintKey();
        Reservation inFlight = inFlightReservations.get(constraintKey);

        if (inFlight != null && inFlight.equals(reservation)) {
            inFlightReservations.remove(constraintKey);
            inFlightReservationRequestTimestamps.remove(constraintKey);
            log.debug("Cleared in-flight reservation for constraint group {}", constraintKey);
        }
    }

    private void onStatusFailure(Status.Failure failure) {
        Throwable cause = failure.cause();
        if (cause instanceof NoResourceAvailableException) {
            NoResourceAvailableException exception = (NoResourceAvailableException) cause;
            String exceptionConstraintKey = exception.getConstraintKey();

            log.info("Received NoResourceAvailableException: {} (constraintKey={})",
                exception.getMessage(), exceptionConstraintKey);

            if (exceptionConstraintKey != null) {
                // Match the exact reservation by constraint key
                Reservation matchingReservation = inFlightReservations.get(exceptionConstraintKey);
                if (matchingReservation != null) {
                    // Update the timestamp for the matching in-flight reservation
                    // This tracks when the reservation last received a NoResourceAvailableException
                    // The reservation will be retried if this timestamp is older than the timeout
                    inFlightReservationRequestTimestamps.put(exceptionConstraintKey, clock.instant());
                    log.info("Updated request timestamp for in-flight reservation {} (constraintKey={}) due to NoResourceAvailableException",
                        matchingReservation.getKey(), exceptionConstraintKey);
                } else {
                    log.warn("Received NoResourceAvailableException for constraintKey {} but no matching in-flight reservation found",
                        exceptionConstraintKey);
                }
            }
        } else {
            log.warn("Received Status.Failure with non-NoResourceAvailableException: {}", cause != null ? cause.getClass().getName() : "null");
        }
    }

    private void addEntry(Reservation reservation) {
        reservationsByKey
            .computeIfAbsent(reservation.getKey(), key -> new LinkedList<>())
            .addLast(reservation);
        ConstraintGroup group = reservationsByConstraint.computeIfAbsent(
            reservation.getCanonicalConstraintKey(),
            key -> new ConstraintGroup(key, reservationComparator));
        group.add(reservation);
    }

    private void replaceEntry(Reservation existing, Reservation replacement) {
        removeEntryAndClearInFlight(existing);
        addEntry(replacement);
    }

    private void removeEntry(Reservation reservation) {
        LinkedList<Reservation> reservations = reservationsByKey.get(reservation.getKey());
        if (reservations != null) {
            reservations.remove(reservation);
            if (reservations.isEmpty()) {
                reservationsByKey.remove(reservation.getKey());
            }
        }
        ConstraintGroup group = reservationsByConstraint.get(reservation.getCanonicalConstraintKey());
        if (group != null) {
            group.remove(reservation);
            if (group.isEmpty()) {
                reservationsByConstraint.remove(group.getCanonicalConstraintKey());
            }
        }
    }

    private void removeEntryAndClearInFlight(Reservation reservation) {
        removeEntry(reservation);
        clearInFlightIfMatches(reservation);
    }

    @Nullable
    private Reservation findReservationWithSameShape(
        @Nullable LinkedList<Reservation> reservations,
        Reservation candidate
    ) {
        if (reservations == null || reservations.isEmpty()) {
            return null;
        }

        for (Iterator<Reservation> iterator = reservations.descendingIterator(); iterator.hasNext(); ) {
            Reservation existing = iterator.next();
            if (existing.hasSameShape(candidate)) {
                return existing;
            }
        }
        return null;
    }

    private void triggerProcessingLoop() {
        self().tell(ProcessReservationsTick.INSTANCE, self());
    }

    private void triggerForcedProcessingLoop() {
        self().tell(ResourceClusterActor.ForceProcessReservationsTick.INSTANCE, self());
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

    /**
     * Timer message to trigger auto-mark-ready after timeout.
     */
    enum AutoMarkReadyTick {
        INSTANCE
    }

    private static final class ConstraintGroup {
        private final String canonicalConstraintKey;
        private final NavigableSet<Reservation> queue;
        private int totalRequestedWorkers;

        private ConstraintGroup(String canonicalConstraintKey, Comparator<Reservation> comparator) {
            this.canonicalConstraintKey = canonicalConstraintKey;
            this.queue = new TreeSet<>(comparator);
            this.totalRequestedWorkers = 0;
        }

        void add(Reservation reservation) {
            if (queue.add(reservation)) {
                totalRequestedWorkers += reservation.getRequestedWorkersCount();
            }
        }

        void remove(Reservation reservation) {
            if (queue.remove(reservation)) {
                totalRequestedWorkers -= reservation.getRequestedWorkersCount();
            }
        }

        boolean isEmpty() {
            return queue.isEmpty();
        }

        Optional<Reservation> peekTop() {
            return queue.isEmpty() ? Optional.empty() : Optional.of(queue.first());
        }

        String getCanonicalConstraintKey() {
            return canonicalConstraintKey;
        }

        PendingReservationGroupView snapshot() {
            List<ReservationSnapshot> reservations = queue
                .stream()
                .map(ReservationSnapshot::fromReservation)
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
    @Builder
    public static class ReservationSnapshot {
        ReservationKey reservationKey;
        int requestedWorkers;
        int stageTargetSize;
        Instant createdAt;
        Instant lastUpdatedAt;
        long priorityEpoch;

        static ReservationSnapshot fromReservation(Reservation reservation) {
            return ReservationSnapshot.builder()
                .reservationKey(reservation.getKey())
                .requestedWorkers(reservation.getRequestedWorkersCount())
                .stageTargetSize(reservation.getStageTargetSize())
                .priorityEpoch(reservation.getPriority().getTimestamp())
                .build();
        }
    }

    @Value
    @Builder
    public static class ReservationAllocationResponse {
        Reservation reservation;
        ResourceClusterActor.TaskExecutorsAllocation taskExecutorsAllocation;
        Throwable error;

        public boolean isSuccess() {
            return error == null;
        }
    }
}
