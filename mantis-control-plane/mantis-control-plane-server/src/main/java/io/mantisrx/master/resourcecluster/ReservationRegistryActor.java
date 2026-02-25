package io.mantisrx.master.resourcecluster;

import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.MarkReady;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.Reservation;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationKey;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.actor.Status;
import com.netflix.spectator.api.TagList;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationGroupView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ProcessReservationsTick;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
 * in current lifecycle resource cluster actor (the parent) won't start until all job clusters finish initialization,
 * thus it's safe to do the auto-mark-ready in this class. Be careful with this limitation when making changes to
 * lifecycle between job cluster actors and resource cluster/registry actors.
 */
@Slf4j
public class ReservationRegistryActor extends AbstractActorWithTimers {
    private static final String TIMER_KEY_PROCESS = "reservation-registry-process";
    private static final String TIMER_KEY_AUTO_MARK_READY = "reservation-registry-auto-mark-ready";
    private static final Duration DEFAULT_PROCESS_INTERVAL = Duration.ofMillis(1000);
    private static final Duration DEFAULT_AUTO_MARK_READY_TIMEOUT = Duration.ofSeconds(5);

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
            .thenComparing(r -> r.getKey().getJobId())
            .thenComparingInt(r -> r.getKey().getStageNumber());
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
        getTimers().startSingleTimer(TIMER_KEY_AUTO_MARK_READY, AutoMarkReadyTick.INSTANCE, autoMarkReadyTimeout);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(UpsertReservation.class, this::onUpsertReservation)
            .match(CancelReservation.class, this::onCancelReservation)
            .match(GetPendingReservationsView.class, this::onGetPendingReservationsView)
            .match(GetPendingReservationsForScaler.class, this::onGetPendingReservationsForScaler)
            .match(MarkReady.class, message -> onMarkReady())
            .match(ProcessReservationsTick.class, message -> onProcessReservationsTick(false))
            .match(ResourceClusterActor.ForceProcessReservationsTick.class, message -> onProcessReservationsTick(true))
            .match(AutoMarkReadyTick.class, message -> onAutoMarkReadyTick())
            .match(ResourceClusterActor.TaskExecutorsAllocation.class, this::onTaskExecutorBatchAssignmentResult)
            .match(Status.Failure.class, this::onStatusFailure)
            .build();
    }

    private void onMarkReady() {
        log.info("Mark ready for registry: {}", this.clusterID);
        if (!ready) {
            ready = true;
            log.info("{}: Reservation registry marked ready; pending reservations={}", this.clusterID, reservationsByKey.size());
            // Cancel the auto-mark-ready timer since we're now ready
            getTimers().cancel(TIMER_KEY_AUTO_MARK_READY);
            publishPendingReservationCountGauge();
        }
        sender().tell(Ack.getInstance(), self());
        triggerForcedProcessingLoop();
    }

    private void onAutoMarkReadyTick() {
        if (!ready) {
            ready = true;
            log.info("{}: Reservation registry auto-marked ready; pending reservations={}", this.clusterID, reservationsByKey.size());
            publishPendingReservationCountGauge();
            triggerForcedProcessingLoop();
        } else {
            log.debug("{}: Auto-mark-ready timer fired but registry is already ready; ignoring", this.clusterID);
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
            log.warn("{}: Replacing existing reservation {} with new reservation {}", this.clusterID, sameShapeReservation, reservation);
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

        log.info("{}: Upserted reservation {} (priority={}, requestedWorkers={})",
            this.clusterID, key, reservation.getPriority(), reservation.getRequestedWorkersCount());
        publishPendingReservationCountGauge();
        sender().tell(Ack.getInstance(), self());
        triggerProcessingLoop();
    }

    private void onCancelReservation(CancelReservation cancel) {
        LinkedList<Reservation> existingReservations = reservationsByKey.get(cancel.getReservationKey());
        if (existingReservations != null && !existingReservations.isEmpty()) {
            new ArrayList<>(existingReservations).forEach(this::removeEntryAndClearInFlight);
            log.info("{}: Cancelled reservation {}", this.clusterID, cancel.getReservationKey());
            publishPendingReservationCountGauge();
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

    /**
     * Handler for scaler integration - returns pending reservations with actual SchedulingConstraints.
     */
    private void onGetPendingReservationsForScaler(GetPendingReservationsForScaler request) {
        if (!ready) {
            sender().tell(
                PendingReservationsForScalerResponse.builder()
                    .ready(false)
                    .build(),
                self());
            return;
        }

        List<PendingReservationInfoSnapshot> reservationInfos = new ArrayList<>();

        for (ConstraintGroup group : reservationsByConstraint.values()) {
            // Get a sample reservation from this group to extract SchedulingConstraints
            Optional<Reservation> sampleReservation = group.peekTop();
            if (sampleReservation.isEmpty()) {
                continue;
            }

            reservationInfos.add(PendingReservationInfoSnapshot.builder()
                .canonicalConstraintKey(group.getCanonicalConstraintKey())
                .schedulingConstraints(sampleReservation.get().getSchedulingConstraints())  // Actual constraints!
                .totalRequestedWorkers(group.getTotalRequestedWorkers())
                .reservationCount(group.size())
                .build());
        }

        sender().tell(
            PendingReservationsForScalerResponse.builder()
                .ready(true)
                .reservations(reservationInfos)
                .build(),
            self());
    }

    private void onProcessReservationsTick(boolean forced) {
        Instant now = clock.instant();
        Duration sinceLastProcess = Duration.between(lastProcessAt, now);
        if (!forced && sinceLastProcess.compareTo(processingCooldown) < 0) {
            log.trace("{}: Skipping reservation processing tick due to cooldown (sinceLast={}ms, cooldown={}ms)",
                this.clusterID,
                sinceLastProcess.toMillis(),
                processingCooldown.toMillis());
            return;
        }

        lastProcessAt = now;
        log.trace("{}: Reservation registry tick - queued reservations", this.clusterID);
        processReservation();
    }

    /**
     * Main process logic for each SKU (constraintKey):
     * 1. Take the first item in queue and track it as in-flight if in-flight is empty.
     * 2. Send batch allocation request for the in-flight reservation (only trigger once when inserted as in-flight).
     * 3. Process loop ends on the group. Rely on callback from allocation to update in-flight.
     * 4. Once in-flight gets allocated the reservation is considered done. Worker task submit errors
     *    are tracked by JobActor worker level heartbeat timeout.
     */
    private void processReservation() {
        if (!ready) {
            log.warn("Reservation registry {} not ready; skipping processing", this.clusterID);
            return;
        }

        for (ConstraintGroup group : reservationsByConstraint.values()) {
            String constraintKey = group.getCanonicalConstraintKey();

            if (shouldSkipConstraintGroup(constraintKey)) {
                continue;
            }

            Optional<Reservation> topReservation = group.peekTop();
            if (topReservation.isEmpty()) {
                log.debug("{}: Skipping constraint group {} - no reservation to process", this.clusterID, constraintKey);
                continue;
            }

            sendBatchAssignmentRequest(constraintKey, topReservation.get());
        }
    }

    /**
     * Checks if a constraint group should be skipped due to an existing in-flight reservation.
     * Returns true if we should skip processing this group, false if we can proceed.
     * Side effect: clears timed-out in-flight reservations.
     */
    private boolean shouldSkipConstraintGroup(String constraintKey) {
        Reservation inFlight = inFlightReservations.get(constraintKey);
        if (inFlight == null) {
            return false;
        }

        Instant requestTimestamp = inFlightReservationRequestTimestamps.get(constraintKey);
        if (requestTimestamp == null) {
            log.debug("{}: Skipping constraint group {} - already has in-flight reservation {}",
                this.clusterID, constraintKey, inFlight.getKey());
            metrics.incrementCounter(
                ResourceClusterActorMetrics.RESERVATION_PROCESSING_SKIPPED,
                TagList.create(ImmutableMap.of(
                    "resourceCluster", clusterID.getResourceID(),
                    "reason", "inFlightNoTimestamp")));
            return true;
        }

        Duration timeSinceRequest = Duration.between(requestTimestamp, clock.instant());
        if (timeSinceRequest.compareTo(inFlightReservationTimeout) < 0) {
            log.debug("{}: Skipping constraint group {} - already has in-flight reservation {} (waiting for {}ms)",
                this.clusterID,
                constraintKey,
                inFlight.getKey(),
                inFlightReservationTimeout.minus(timeSinceRequest).toMillis());
            metrics.incrementCounter(
                ResourceClusterActorMetrics.RESERVATION_PROCESSING_SKIPPED,
                TagList.create(ImmutableMap.of(
                    "resourceCluster", clusterID.getResourceID(),
                    "reason", "inFlightWaiting")));
            return true;
        }

        // In-flight reservation has timed out - clear it and allow retry
        log.debug("{}: In-flight reservation {} for constraint group {} has timed out ({}ms), retrying",
            this.clusterID, inFlight.getKey(), constraintKey, timeSinceRequest.toMillis());

        metrics.incrementCounter(
            ResourceClusterActorMetrics.RESERVATION_INFLIGHT_TIMEOUT,
            TagList.create(ImmutableMap.of(
                "resourceCluster", clusterID.getResourceID(),
                "jobId", inFlight.getKey().getJobId())));

        inFlightReservations.remove(constraintKey);
        inFlightReservationRequestTimestamps.remove(constraintKey);
        return false;
    }

    private void sendBatchAssignmentRequest(String constraintKey, Reservation reservation) {
        log.info("{}: Sending batch assignment request for reservation {} in constraint group {}",
            this.clusterID, reservation.getKey(), constraintKey);

        metrics.incrementCounter(
            ResourceClusterActorMetrics.RESERVATION_PROCESSED,
            TagList.create(ImmutableMap.of(
                "resourceCluster", clusterID.getResourceID(),
                "jobId", reservation.getKey().getJobId())));

        inFlightReservations.put(constraintKey, reservation);
        inFlightReservationRequestTimestamps.put(constraintKey, clock.instant());

        ResourceClusterActor.TaskExecutorBatchAssignmentRequest request =
            new ResourceClusterActor.TaskExecutorBatchAssignmentRequest(
                reservation.getAllocationRequests(),
                this.clusterID,
                reservation);

        getContext().parent().tell(request, self());
    }

    private void onTaskExecutorBatchAssignmentResult(ResourceClusterActor.TaskExecutorsAllocation result) {
        if (result == null || result.getReservation() == null) {
            log.warn("{}: Received null reservation allocation result, ignoring", this.clusterID);
            return;
        }

        Reservation reservation = result.getReservation();
        String constraintKey = reservation.getCanonicalConstraintKey();

        log.info("{}: Received batch assignment result for reservation {} (constraintKey={})",
            this.clusterID, reservation.getKey(), constraintKey);

        // Record reservation fulfillment latency
        long createdAtMillis = reservation.getCreatedAt();
        long fulfillmentDurationNanos = Duration.between(
            Instant.ofEpochMilli(createdAtMillis),
            clock.instant()
        ).toNanos();

        metrics.recordTimer(
            ResourceClusterActorMetrics.RESERVATION_FULFILLMENT_LATENCY,
            fulfillmentDurationNanos,
            TagList.create(ImmutableMap.of(
                "resourceCluster",
                clusterID.getResourceID(),
                "jobId",
                reservation.getKey().getJobId(),
                "priorityType",
                reservation.getPriority().getType().name())));

        clearInFlightIfMatches(reservation);
        removeEntry(reservation);
        publishPendingReservationCountGauge();
        triggerForcedProcessingLoop();
    }

    private void clearInFlightIfMatches(Reservation reservation) {
        String constraintKey = reservation.getCanonicalConstraintKey();
        Reservation inFlight = inFlightReservations.get(constraintKey);

        if (inFlight != null) {
            inFlightReservations.remove(constraintKey);
            inFlightReservationRequestTimestamps.remove(constraintKey);
            log.debug("{}: Cleared in-flight reservation for constraint group {} (matched={})",
                this.clusterID, constraintKey, inFlight.equals(reservation));
        }
    }

    private void onStatusFailure(Status.Failure failure) {
        Throwable cause = failure.cause();
        if (cause instanceof NoResourceAvailableException) {
            NoResourceAvailableException exception = (NoResourceAvailableException) cause;
            String exceptionConstraintKey = exception.getConstraintKey();

            log.info("{}: Received NoResourceAvailableException: {} (constraintKey={})",
                this.clusterID, exception.getMessage(), exceptionConstraintKey);

            if (exceptionConstraintKey != null) {
                // Match the exact reservation by constraint key
                Reservation matchingReservation = inFlightReservations.get(exceptionConstraintKey);
                if (matchingReservation != null) {
                    // Update the timestamp for the matching in-flight reservation
                    // This tracks when the reservation last received a NoResourceAvailableException
                    // The reservation will be retried if this timestamp is older than the timeout
                    inFlightReservationRequestTimestamps.put(exceptionConstraintKey, clock.instant());
                    log.info("{}: Updated request timestamp for in-flight reservation {} (constraintKey={}) due to NoResourceAvailableException",
                        this.clusterID, matchingReservation.getKey(), exceptionConstraintKey);
                } else {
                    log.warn("{}: Received NoResourceAvailableException for constraintKey {} but no matching in-flight reservation found",
                        this.clusterID, exceptionConstraintKey);
                }
            }
        } else {
            log.warn("{}: Received Status.Failure with non-NoResourceAvailableException: {}", this.clusterID, cause != null ? cause.getClass().getName() : "null");
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
            boolean removed = reservations.remove(reservation);
            if (!removed) {
                log.warn("Reservation not found in reservationsByKey for removal: {}", reservation);
            }
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

    /**
     * Publishes the current count of pending reservations as a gauge metric.
     * Tracks both total pending reservations and breakdown by constraint group.
     */
    private void publishPendingReservationCountGauge() {
        // Total pending reservations across all constraint-groups
        int totalPendingCount = reservationsByKey.values().stream()
            .mapToInt(List::size)
            .sum();

        metrics.setGauge(
            ResourceClusterActorMetrics.NUM_PENDING_RESERVATIONS,
            totalPendingCount,
            TagList.create(ImmutableMap.of(
                "resourceCluster",
                clusterID.getResourceID())));

        // Per-constraint-group pending count for more granular tracking
        reservationsByConstraint.forEach((constraintKey, group) -> {
            // Truncate constraintKey to 12 chars max to avoid high cardinality issues
            String truncatedKey = constraintKey.length() > 12
                ? constraintKey.substring(0, 12)
                : constraintKey;

            metrics.setGauge(
                ResourceClusterActorMetrics.NUM_PENDING_RESERVATIONS,
                group.size(),
                TagList.create(ImmutableMap.of(
                    "resourceCluster",
                    clusterID.getResourceID(),
                    "constraintKey",
                    truncatedKey)));
        });
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

    /**
     * Request for pending reservations with full scheduling constraints (for scaler integration).
     */
    enum GetPendingReservationsForScaler {
        INSTANCE
    }

    /**
     * Response containing pending reservations with actual SchedulingConstraints.
     */
    @Value
    @Builder
    public static class PendingReservationsForScalerResponse {
        boolean ready;
        @Builder.Default
        List<PendingReservationInfoSnapshot> reservations = Collections.emptyList();
    }

    /**
     * Snapshot of pending reservation info including actual SchedulingConstraints.
     */
    @Value
    @Builder
    public static class PendingReservationInfoSnapshot {
        String canonicalConstraintKey;
        SchedulingConstraints schedulingConstraints;
        int totalRequestedWorkers;
        int reservationCount;
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

        int getTotalRequestedWorkers() {
            return totalRequestedWorkers;
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
