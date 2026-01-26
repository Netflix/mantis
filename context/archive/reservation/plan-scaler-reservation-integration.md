# Scaler-Reservation Integration Plan

## Overview

This document outlines the integration plan for the reservation-based scheduling system with the `ResourceClusterScalerActor`. The goal is to make scaling decisions aware of pending reservations so the cluster can proactively scale up to meet demand and avoid premature scale-down.

## Problem Statement

The `ResourceClusterScalerActor` currently makes scaling decisions based solely on **current cluster usage** (`idleCount`, `totalCount`). With the new reservation-based scheduling system, there's a critical gap:

1. **Pending reservations are not factored into scaling decisions** - If 10 workers are in the reservation queue, scaling down would be premature
2. **Scale-up should be more aggressive** when reservations are pending - The system should proactively scale up to fulfill pending demand
3. **Key mismatch** - Scaler uses `ContainerSkuID` (SKU), while reservations are grouped by `canonicalConstraintKey` (SchedulingConstraints)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        ResourceClusterScalerActor                                │
│   (+ reservationSchedulingEnabled flag from MasterConfiguration)                │
│                                                                                  │
│   GetReservationAwareClusterUsageRequest ─────────────────────────────────┐     │
└───────────────────────────────────────────────────────────────────────────│─────┘
                                                                            │
                                                                            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            ResourceClusterActor                                  │
│   onGetReservationAwareClusterUsage():                                          │
│     1. Ask ReservationRegistryActor for GetPendingReservationsForScaler         │
│     2. Forward to ExecutorStateManagerActor with pending reservations           │
└─────────────────────────────────────────────────────────────────────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────────────┐              ┌────────────────────────────────────────┐
│ ReservationRegistryActor│              │     ExecutorStateManagerActor          │
│                         │              │  (+ reservationSchedulingEnabled flag) │
│ GetPendingReservations- │              │                                        │
│ ForScaler               │              │  GetClusterUsageWithReservationsRequest│
│ → List<PendingReserva-  │              │  → Compute usage + map reservations    │
│   tionInfoSnapshot>     │              │    to SKUs via findBestGroupForUsage() │
│   (with actual          │              │                                        │
│    SchedulingConstraints)              │                                        │
└─────────────────────────┘              └────────────────────────────────────────┘
                                                        │
                                                        ▼
                                         ┌────────────────────────────────────────┐
                                         │       ExecutorStateManagerImpl         │
                                         │  (+ reservationSchedulingEnabled flag) │
                                         │                                        │
                                         │  - findBestGroupForUsage(): Like       │
                                         │    findBestGroup but relaxed (no need  │
                                         │    for available TEs)                  │
                                         │  - mapGroupKeyToSkuViaGroupKeyFunc():  │
                                         │    Sample TE from executorsByGroup     │
                                         │    → SKU via groupKeyFunc              │
                                         │  - getClusterUsageWithReservations():  │
                                         │    Merge current usage + pending       │
                                         │    reservation counts                  │
                                         └────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Configuration Flag Propagation

#### 1.1 MasterConfiguration

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/server/master/config/MasterConfiguration.java`

```java
@Config("mantis.scheduling.reservation.enabled")
@Default("false")
boolean isReservationSchedulingEnabled();
```

#### 1.2 Update ResourceClusterScalerActor Constructor

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ResourceClusterScalerActor.java`

```java
public class ResourceClusterScalerActor extends AbstractActorWithTimers {
    // ... existing fields ...

    // NEW: Flag to enable reservation-aware scaling
    private final boolean reservationSchedulingEnabled;

    public static Props props(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        Duration ruleRefreshThreshold,
        IMantisPersistenceProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor,
        boolean reservationSchedulingEnabled) {  // NEW parameter
        return Props.create(
            ResourceClusterScalerActor.class,
            clusterId,
            clock,
            scalerPullThreshold,
            ruleRefreshThreshold,
            storageProvider,
            resourceClusterHostActor,
            resourceClusterActor,
            reservationSchedulingEnabled);
    }

    public ResourceClusterScalerActor(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        Duration ruleRefreshThreshold,
        IMantisPersistenceProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor,
        boolean reservationSchedulingEnabled) {  // NEW parameter
        // ... existing initialization ...
        this.reservationSchedulingEnabled = reservationSchedulingEnabled;
    }
}
```

#### 1.3 Update ExecutorStateManagerActor Constructor

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ExecutorStateManagerActor.java`

```java
public class ExecutorStateManagerActor extends AbstractActorWithTimers {
    // ... existing fields ...

    // NEW: Flag to enable reservation-aware usage computation
    private final boolean reservationSchedulingEnabled;

    public static Props props(
        ExecutorStateManagerImpl delegate,
        Clock clock,
        RpcService rpcService,
        JobMessageRouter jobMessageRouter,
        MantisJobStore mantisJobStore,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        Duration disabledTaskExecutorsCheckInterval,
        ClusterID clusterID,
        boolean isJobArtifactCachingEnabled,
        String jobClustersWithArtifactCachingEnabled,
        ResourceClusterActorMetrics metrics,
        ExecuteStageRequestFactory executeStageRequestFactory,
        boolean reservationSchedulingEnabled) {  // NEW parameter
        // ... update Props.create call ...
    }

    ExecutorStateManagerActor(
        ExecutorStateManagerImpl delegate,
        // ... existing params ...
        boolean reservationSchedulingEnabled) {  // NEW parameter
        // ... existing initialization ...
        this.reservationSchedulingEnabled = reservationSchedulingEnabled;
    }
}
```

#### 1.4 Update ExecutorStateManagerImpl Constructor

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ExecutorStateManagerImpl.java`

```java
public class ExecutorStateManagerImpl implements ExecutorStateManager {
    // ... existing fields ...

    // NEW: Flag to enable reservation-aware usage computation
    private final boolean reservationSchedulingEnabled;

    ExecutorStateManagerImpl(
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator,
        Duration schedulerLeaseExpirationDuration,
        AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook,
        boolean reservationSchedulingEnabled) {  // NEW parameter
        this.schedulingAttributes = schedulingAttributes;
        this.fitnessCalculator = fitnessCalculator;
        this.schedulerLeaseExpirationDuration = schedulerLeaseExpirationDuration;
        this.availableTaskExecutorMutatorHook = availableTaskExecutorMutatorHook;
        this.reservationSchedulingEnabled = reservationSchedulingEnabled;
    }
}
```

---

### Phase 2: New Message Types

#### 2.1 Add New Messages in ResourceClusterActor

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ResourceClusterActor.java`

```java
/**
 * Request from ScalerActor to get cluster usage with pending reservation counts.
 * This triggers a two-phase query: first to ReservationRegistryActor, then to ExecutorStateManagerActor.
 */
@Value
static class GetReservationAwareClusterUsageRequest {
    ClusterID clusterID;
    Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc;
}

/**
 * Pending reservation info with actual SchedulingConstraints (not just constraint key string).
 * This avoids the need to parse constraint keys back into machine definitions.
 */
@Value
@Builder
static class PendingReservationInfo {
    String canonicalConstraintKey;
    SchedulingConstraints schedulingConstraints;  // Actual constraints - no parsing needed!
    int totalRequestedWorkers;
    int reservationCount;
}

/**
 * Internal message to pass pending reservations from ReservationRegistryActor
 * to ExecutorStateManagerActor for final usage computation.
 * Contains actual SchedulingConstraints for direct matching.
 */
@Value
static class GetClusterUsageWithReservationsRequest {
    ClusterID clusterID;
    Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc;
    List<PendingReservationInfo> pendingReservations;  // Contains actual constraints
}
```

#### 2.2 Extend GetClusterUsageResponse with Pending Reservation Count

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/proto/GetClusterUsageResponse.java`

```java
@Value
@Builder
public class GetClusterUsageResponse {
    ClusterID clusterID;

    @Singular
    List<UsageByGroupKey> usages;

    @Value
    @Builder
    public static class UsageByGroupKey {
        String usageGroupKey;
        int idleCount;
        int totalCount;

        // NEW: Count of workers pending in reservation queue for this SKU
        @Builder.Default
        int pendingReservationCount = 0;

        /**
         * Effective idle count accounting for pending reservations.
         * If pending reservations exist, those "idle" TEs will soon be consumed.
         */
        public int getEffectiveIdleCount() {
            return Math.max(0, idleCount - pendingReservationCount);
        }
    }
}
```

---

### Phase 3: ReservationRegistryActor Changes

#### 3.1 Add New Message Types and Handler

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ReservationRegistryActor.java`

```java
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
```

#### 3.2 Add Handler in createReceive()

```java
@Override
public Receive createReceive() {
    return receiveBuilder()
        .match(UpsertReservation.class, this::onUpsertReservation)
        .match(CancelReservation.class, this::onCancelReservation)
        .match(GetPendingReservationsView.class, this::onGetPendingReservationsView)
        .match(GetPendingReservationsForScaler.class, this::onGetPendingReservationsForScaler)  // NEW
        .match(MarkReady.class, message -> onMarkReady())
        .match(ProcessReservationsTick.class, message -> onProcessReservationsTick(false))
        .match(ResourceClusterActor.ForceProcessReservationsTick.class, message -> onProcessReservationsTick(true))
        .match(AutoMarkReadyTick.class, message -> onAutoMarkReadyTick())
        .match(ResourceClusterActor.TaskExecutorsAllocation.class, this::onTaskExecutorBatchAssignmentResult)
        .match(Status.Failure.class, this::onStatusFailure)
        .build();
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
```

#### 3.3 Expose ConstraintGroup Methods

Add getter methods to the `ConstraintGroup` inner class:

```java
private static final class ConstraintGroup {
    // ... existing fields and methods ...

    int getTotalRequestedWorkers() {
        return totalRequestedWorkers;
    }

    int size() {
        return queue.size();
    }
}
```

---

### Phase 4: ResourceClusterActor Handler Implementation

#### 4.1 Add Handler for GetReservationAwareClusterUsageRequest

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ResourceClusterActor.java`

```java
@Override
public Receive createReceive() {
    return
        ReceiveBuilder
            .create()
            // ... existing handlers ...
            .match(GetReservationAwareClusterUsageRequest.class,
                metrics.withTracking(this::onGetReservationAwareClusterUsage))
            .build();
}

/**
 * Handler for reservation-aware cluster usage request.
 * Two-phase approach:
 * 1. Ask ReservationRegistryActor for pending reservations with actual SchedulingConstraints
 * 2. Forward to ExecutorStateManagerActor with the enriched reservation info
 */
private void onGetReservationAwareClusterUsage(GetReservationAwareClusterUsageRequest request) {
    final ActorRef originalSender = sender();
    final ActorRef self = self();

    // Phase 1: Get pending reservations with actual constraints from ReservationRegistryActor
    CompletionStage<ReservationRegistryActor.PendingReservationsForScalerResponse> reservationsFuture =
        FutureConverters.toJava(Patterns.ask(
            reservationRegistryActor,
            ReservationRegistryActor.GetPendingReservationsForScaler.INSTANCE,
            Duration.ofSeconds(5).toMillis()))
        .thenApply(ReservationRegistryActor.PendingReservationsForScalerResponse.class::cast);

    reservationsFuture.whenComplete((reservationsResponse, error) -> {
        if (error != null) {
            log.warn("Failed to get pending reservations for usage request", error);
            // Fall back to regular usage request without reservations
            executorStateManagerActor.tell(
                new GetClusterUsageRequest(request.getClusterID(), request.getGroupKeyFunc()),
                originalSender);
        } else {
            // Phase 2: Convert to PendingReservationInfo and forward to ExecutorStateManagerActor
            List<PendingReservationInfo> pendingReservations;
            if (reservationsResponse.isReady()) {
                pendingReservations = reservationsResponse.getReservations().stream()
                    .map(snapshot -> PendingReservationInfo.builder()
                        .canonicalConstraintKey(snapshot.getCanonicalConstraintKey())
                        .schedulingConstraints(snapshot.getSchedulingConstraints())
                        .totalRequestedWorkers(snapshot.getTotalRequestedWorkers())
                        .reservationCount(snapshot.getReservationCount())
                        .build())
                    .collect(Collectors.toList());
            } else {
                pendingReservations = Collections.emptyList();
            }

            log.debug("Forwarding usage request with {} pending reservation groups", pendingReservations.size());

            executorStateManagerActor.tell(
                new GetClusterUsageWithReservationsRequest(
                    request.getClusterID(),
                    request.getGroupKeyFunc(),
                    pendingReservations),
                originalSender);
        }
    });
}
```

---

### Phase 5: ExecutorStateManagerActor Handler

#### 5.1 Add Handler for GetClusterUsageWithReservationsRequest

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ExecutorStateManagerActor.java`

```java
@Override
public Receive createReceive() {
    return receiveBuilder()
        // ... existing handlers ...
        .match(GetClusterUsageRequest.class, this::onGetClusterUsage)
        .match(GetClusterUsageWithReservationsRequest.class, this::onGetClusterUsageWithReservations)  // NEW
        .build();
}

private void onGetClusterUsage(GetClusterUsageRequest req) {
    sender().tell(this.delegate.getClusterUsage(req), self());
}

// NEW: Handler for reservation-aware usage request
private void onGetClusterUsageWithReservations(GetClusterUsageWithReservationsRequest req) {
    sender().tell(
        this.delegate.getClusterUsageWithReservations(
            req.getClusterID(),
            req.getGroupKeyFunc(),
            req.getPendingReservations()),
        self());
}
```

---

### Phase 6: ExecutorStateManagerImpl Implementation

#### 6.1 Add Reservation-Aware Usage Computation

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ExecutorStateManagerImpl.java`

```java
/**
 * Compute cluster usage with pending reservation counts.
 * Uses actual SchedulingConstraints from reservations - no parsing needed!
 *
 * @param clusterID The cluster ID
 * @param groupKeyFunc Function to extract SKU key from TaskExecutorRegistration
 * @param pendingReservations Pending reservations with actual SchedulingConstraints
 * @return Usage response with pending reservation counts per SKU
 */
public GetClusterUsageResponse getClusterUsageWithReservations(
        ClusterID clusterID,
        Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc,
        List<PendingReservationInfo> pendingReservations) {

    // Step 1: Get base usage (same as existing getClusterUsage logic)
    Map<String, Pair<Integer, Integer>> usageByGroupKey = computeBaseUsage(groupKeyFunc);
    Map<String, Integer> pendingCountByGroupKey = computePendingCountByGroupKey(groupKeyFunc);

    // Step 2: Map pending reservations to SKU keys using actual SchedulingConstraints
    Map<String, Integer> reservationCountBySku =
        mapReservationsToSku(pendingReservations, groupKeyFunc);

    // Step 3: Build response with merged counts
    GetClusterUsageResponseBuilder resBuilder = GetClusterUsageResponse.builder().clusterID(clusterID);

    // Collect all SKU keys (from both current usage and reservations)
    Set<String> allSkuKeys = new HashSet<>();
    allSkuKeys.addAll(usageByGroupKey.keySet());
    allSkuKeys.addAll(reservationCountBySku.keySet());

    for (String skuKey : allSkuKeys) {
        Pair<Integer, Integer> usage = usageByGroupKey.getOrDefault(skuKey, Pair.of(0, 0));
        int pendingFromScheduler = pendingCountByGroupKey.getOrDefault(skuKey, 0);
        int pendingFromReservations = reservationCountBySku.getOrDefault(skuKey, 0);

        resBuilder.usage(UsageByGroupKey.builder()
            .usageGroupKey(skuKey)
            .idleCount(usage.getLeft() - pendingFromScheduler)
            .totalCount(usage.getRight())
            .pendingReservationCount(pendingFromReservations)
            .build());
    }

    GetClusterUsageResponse res = resBuilder.build();
    log.info("Usage result with reservations: {}", res);
    return res;
}

/**
 * Compute base usage by SKU.
 */
private Map<String, Pair<Integer, Integer>> computeBaseUsage(
        Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc) {
    Map<String, Pair<Integer, Integer>> usageByGroupKey = new HashMap<>();

    taskExecutorStateMap.forEach((key, value) -> {
        if (value == null || value.getRegistration() == null) {
            return;
        }

        Optional<String> groupKeyO = groupKeyFunc.apply(value.getRegistration());
        if (!groupKeyO.isPresent()) {
            return;
        }

        String groupKey = groupKeyO.get();
        Pair<Integer, Integer> kvState = Pair.of(
            value.isAvailable() && !value.isDisabled() ? 1 : 0,
            value.isRegistered() ? 1 : 0);

        usageByGroupKey.merge(groupKey, kvState,
            (prev, curr) -> Pair.of(prev.getLeft() + curr.getLeft(), prev.getRight() + curr.getRight()));
    });

    return usageByGroupKey;
}

/**
 * Compute pending scheduler request counts by SKU.
 */
private Map<String, Integer> computePendingCountByGroupKey(
        Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc) {
    Map<String, Integer> result = new HashMap<>();

    // Get all unique TaskExecutorGroupKeys from registered executors
    taskExecutorStateMap.values().stream()
        .filter(state -> state.getRegistration() != null)
        .map(state -> state.getRegistration().getGroup())
        .distinct()
        .forEach(group -> {
            int pendingCount = getPendingCountByTaskExecutorGroup(group);
            if (pendingCount > 0) {
                // Map TaskExecutorGroupKey to SKU using sample TE
                Optional<String> skuKey = mapGroupKeyToSkuViaGroupKeyFunc(group, groupKeyFunc);
                skuKey.ifPresent(sku -> result.merge(sku, pendingCount, Integer::sum));
            }
        });

    return result;
}

/**
 * Map pending reservations to SKU keys using actual SchedulingConstraints.
 * Uses findBestGroupForUsage with real constraints - NO PARSING NEEDED!
 */
private Map<String, Integer> mapReservationsToSku(
        List<PendingReservationInfo> pendingReservations,
        Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc) {

    Map<String, Integer> reservationCountBySku = new HashMap<>();

    for (PendingReservationInfo reservation : pendingReservations) {
        int totalRequestedWorkers = reservation.getTotalRequestedWorkers();
        if (totalRequestedWorkers <= 0) {
            continue;
        }

        // Use actual SchedulingConstraints directly - no parsing needed!
        SchedulingConstraints constraints = reservation.getSchedulingConstraints();
        Optional<TaskExecutorGroupKey> bestGroup = findBestGroupForUsage(constraints);

        if (bestGroup.isPresent()) {
            // Map TaskExecutorGroupKey to SKU using sample TE
            Optional<String> skuKey = mapGroupKeyToSkuViaGroupKeyFunc(bestGroup.get(), groupKeyFunc);
            skuKey.ifPresent(sku ->
                reservationCountBySku.merge(sku, totalRequestedWorkers, Integer::sum));

            log.debug("Mapped reservation {} ({} workers) to SKU {} via group {}",
                reservation.getCanonicalConstraintKey(),
                totalRequestedWorkers,
                skuKey.orElse("unknown"),
                bestGroup.get());
        } else {
            log.warn("Cannot map reservation constraints {} to any TaskExecutorGroup; " +
                     "{} pending workers will not be counted in scaling",
                     constraints, totalRequestedWorkers);
        }
    }

    return reservationCountBySku;
}

/**
 * Find the best matching TaskExecutorGroupKey for given SchedulingConstraints.
 * This is a relaxed version of findBestGroup - it doesn't require available TEs,
 * just finds which group would be used for scheduling.
 *
 * Reuses existing matching logic from findBestGroupBySizeNameMatch and
 * findBestGroupByFitnessCalculator but without availability requirements.
 *
 * @param constraints The actual SchedulingConstraints from the reservation
 * @return Optional TaskExecutorGroupKey that would match these constraints
 */
private Optional<TaskExecutorGroupKey> findBestGroupForUsage(SchedulingConstraints constraints) {
    if (constraints == null) {
        return executorsByGroup.keySet().stream().findFirst();
    }

    // Try size name match first (same logic as findBestGroupBySizeNameMatch)
    Optional<TaskExecutorGroupKey> bySizeName = executorsByGroup.keySet().stream()
        .filter(group -> group.getSizeName().isPresent())
        .filter(group -> constraints.getSizeName().isPresent())
        .filter(group -> group.getSizeName().get().equalsIgnoreCase(constraints.getSizeName().get()))
        .filter(group -> areSchedulingAttributeConstraintsSatisfied(constraints, group.getSchedulingAttributes()))
        .findFirst();

    if (bySizeName.isPresent()) {
        return bySizeName;
    }

    // Fall back to fitness calculator (same logic as findBestGroupByFitnessCalculator)
    if (constraints.getMachineDefinition() != null) {
        return executorsByGroup.keySet().stream()
            // Filter out if both sizeName exist and are different
            .filter(group -> {
                Optional<String> teGroupSizeName = group.getSizeName();
                Optional<String> requestSizeName = constraints.getSizeName();
                return !(teGroupSizeName.isPresent() && requestSizeName.isPresent()
                    && !teGroupSizeName.get().equalsIgnoreCase(requestSizeName.get()));
            })
            // Verify scheduling attribute constraints
            .filter(group -> areSchedulingAttributeConstraintsSatisfied(constraints, group.getSchedulingAttributes()))
            // Calculate fitness and filter positive scores
            .map(group -> Pair.of(group,
                fitnessCalculator.calculate(constraints.getMachineDefinition(), group.getMachineDefinition())))
            .filter(pair -> pair.getRight() > 0)
            // Get highest fitness score
            .max(Comparator.comparingDouble(Pair::getRight))
            .map(Pair::getLeft);
    }

    // Last resort: return any group that satisfies attribute constraints
    return executorsByGroup.keySet().stream()
        .filter(group -> areSchedulingAttributeConstraintsSatisfied(constraints, group.getSchedulingAttributes()))
        .findFirst();
}

/**
 * Map a TaskExecutorGroupKey to SKU key using the provided groupKeyFunc.
 * Uses a sample TE from the group to get the registration for mapping.
 */
private Optional<String> mapGroupKeyToSkuViaGroupKeyFunc(
        TaskExecutorGroupKey groupKey,
        Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc) {

    NavigableSet<TaskExecutorHolder> holders = executorsByGroup.get(groupKey);
    if (holders == null || holders.isEmpty()) {
        log.debug("No TaskExecutors found for group {}", groupKey);
        return Optional.empty();
    }

    // Get sample TE registration
    TaskExecutorHolder sampleHolder = holders.first();
    TaskExecutorState state = taskExecutorStateMap.get(sampleHolder.getId());
    if (state == null || state.getRegistration() == null) {
        log.debug("Sample TE {} has no registration", sampleHolder.getId());
        return Optional.empty();
    }

    // Apply the same groupKeyFunc that scaler uses
    return groupKeyFunc.apply(state.getRegistration());
}
```

---

### Phase 7: Update ResourceClusterScalerActor Usage Logic

#### 7.1 Update Usage Request to Use Reservation-Aware Query

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ResourceClusterScalerActor.java`

```java
private void onTriggerClusterUsageRequest(TriggerClusterUsageRequest req) {
    log.trace("Requesting cluster usage: {}", this.clusterId);
    if (this.skuToRuleMap.isEmpty()) {
        log.info("{} scaler is disabled due to no rules", this.clusterId);
        return;
    }

    if (reservationSchedulingEnabled) {
        // Use reservation-aware usage request
        this.resourceClusterActor.tell(
            new GetReservationAwareClusterUsageRequest(
                this.clusterId,
                ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            self());
    } else {
        // Legacy behavior
        this.resourceClusterActor.tell(
            new GetClusterUsageRequest(
                this.clusterId,
                ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            self());
    }
}
```

#### 7.2 Update ClusterAvailabilityRule to Use Effective Idle Count

```java
public Optional<ScaleDecision> apply(UsageByGroupKey usage) {
    Optional<ScaleDecision> decision = Optional.empty();

    // Use effective idle count that accounts for pending reservations
    int effectiveIdleCount = usage.getEffectiveIdleCount();
    int pendingReservations = usage.getPendingReservationCount();

    log.debug("Evaluating scale rule for SKU {}: idle={}, effectiveIdle={}, pending={}, total={}",
        usage.getUsageGroupKey(),
        usage.getIdleCount(),
        effectiveIdleCount,
        pendingReservations,
        usage.getTotalCount());

    // SCALE DOWN: Only if effective idle (after reservations) exceeds max
    if (effectiveIdleCount > scaleSpec.getMaxIdleToKeep()) {
        if (isLastActionOlderThan(scaleSpec.getCoolDownSecs() * 5)) {
            log.debug("Scale Down CoolDown skip: {}, {}",
                this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
            return Optional.empty();
        }

        int step = effectiveIdleCount - scaleSpec.getMaxIdleToKeep();
        int newSize = Math.max(usage.getTotalCount() - step, this.scaleSpec.getMinSize());

        decision = Optional.of(
            ScaleDecision.builder()
                .clusterId(this.scaleSpec.getClusterId())
                .skuId(this.scaleSpec.getSkuId())
                .desireSize(newSize)
                .maxSize(newSize)
                .minSize(newSize)
                .type(newSize == usage.getTotalCount() ? ScaleType.NoOpReachMin : ScaleType.ScaleDown)
                .build());
    }
    // SCALE UP: If effective idle is below min OR if there are pending reservations
    else if (effectiveIdleCount < scaleSpec.getMinIdleToKeep() || pendingReservations > 0) {
        if (isLastActionOlderThan(scaleSpec.getCoolDownSecs())) {
            log.debug("Scale Up CoolDown skip: {}, {}",
                this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
            return Optional.empty();
        }

        // Scale up to cover both idle deficit and pending reservations
        int idleDeficit = Math.max(0, scaleSpec.getMinIdleToKeep() - effectiveIdleCount);
        int step = idleDeficit + pendingReservations;

        int newSize = Math.min(usage.getTotalCount() + step, this.scaleSpec.getMaxSize());

        decision = Optional.of(
            ScaleDecision.builder()
                .clusterId(this.scaleSpec.getClusterId())
                .skuId(this.scaleSpec.getSkuId())
                .desireSize(newSize)
                .maxSize(newSize)
                .minSize(newSize)
                .type(newSize == usage.getTotalCount() ? ScaleType.NoOpReachMax : ScaleType.ScaleUp)
                .build());
    }

    log.info("Scale Decision for {}-{}: {} (effectiveIdle={}, pending={})",
        this.scaleSpec.getClusterId(),
        this.scaleSpec.getSkuId(),
        decision,
        effectiveIdleCount,
        pendingReservations);

    if (decision.isPresent() &&
        (decision.get().type.equals(ScaleType.ScaleDown) ||
         decision.get().type.equals(ScaleType.ScaleUp))) {
        resetLastActionInstant();
    }
    return decision;
}
```

---

## Summary of Changes

| File | Changes |
|------|---------|
| `MasterConfiguration.java` | Add `isReservationSchedulingEnabled()` config flag |
| `ResourceClusterActor.java` | Add `GetReservationAwareClusterUsageRequest`, `PendingReservationInfo`, `GetClusterUsageWithReservationsRequest` messages and handler |
| `ReservationRegistryActor.java` | Add `GetPendingReservationsForScaler`, `PendingReservationsForScalerResponse`, `PendingReservationInfoSnapshot`, expose `ConstraintGroup.getTotalRequestedWorkers()` and `size()` |
| `ExecutorStateManagerActor.java` | Add `reservationSchedulingEnabled` flag, handler for `GetClusterUsageWithReservationsRequest` |
| `ExecutorStateManagerImpl.java` | Add `reservationSchedulingEnabled` flag, `getClusterUsageWithReservations()`, `findBestGroupForUsage(SchedulingConstraints)`, `mapGroupKeyToSkuViaGroupKeyFunc()`, helper methods |
| `GetClusterUsageResponse.java` | Add `pendingReservationCount` field, `getEffectiveIdleCount()` method to `UsageByGroupKey` |
| `ResourceClusterScalerActor.java` | Add `reservationSchedulingEnabled` flag, conditional usage request, update `ClusterAvailabilityRule.apply()` |

---

## Data Flow Summary

```
1. ScalerActor.onTriggerClusterUsageRequest()
   │
   ├─ (reservationEnabled=false) ──> GetClusterUsageRequest ──> ESMActor ──> existing logic
   │
   └─ (reservationEnabled=true) ──> GetReservationAwareClusterUsageRequest
                                         │
                                         ▼
2. ResourceClusterActor.onGetReservationAwareClusterUsage()
   │
   ├─ Phase 1: Ask ReservationRegistryActor for GetPendingReservationsForScaler
   │           Response: PendingReservationsForScalerResponse
   │                     - ready: boolean
   │                     - reservations: List<PendingReservationInfoSnapshot>
   │                         - canonicalConstraintKey: String
   │                         - schedulingConstraints: SchedulingConstraints  ← Actual constraints!
   │                         - totalRequestedWorkers: int
   │                         - reservationCount: int
   │
   └─ Phase 2: Convert to PendingReservationInfo, forward GetClusterUsageWithReservationsRequest to ESMActor
                    (includes List<PendingReservationInfo> with actual SchedulingConstraints)
                                         │
                                         ▼
3. ExecutorStateManagerActor.onGetClusterUsageWithReservations()
   │
   └─ Delegate to ExecutorStateManagerImpl.getClusterUsageWithReservations()
                                         │
                                         ▼
4. ExecutorStateManagerImpl.getClusterUsageWithReservations()
   │
   ├─ computeBaseUsage(groupKeyFunc): idleCount, totalCount per SKU
   │
   ├─ computePendingCountByGroupKey(groupKeyFunc): pending scheduler requests per SKU
   │
   ├─ mapReservationsToSku(pendingReservations, groupKeyFunc):
   │   │
   │   ├─ For each PendingReservationInfo:
   │   │   ├─ findBestGroupForUsage(schedulingConstraints) ──> TaskExecutorGroupKey
   │   │   │   (uses actual SchedulingConstraints - no parsing!)
   │   │   └─ mapGroupKeyToSkuViaGroupKeyFunc(groupKey, groupKeyFunc) ──> SKU
   │   │       (uses sample TE from executorsByGroup)
   │   │
   │   └─ Aggregate: Map<SKU, pendingReservationCount>
   │
   └─ Build Response: UsageByGroupKey with idleCount, totalCount, pendingReservationCount
                                         │
                                         ▼
5. ScalerActor.ClusterAvailabilityRule.apply(UsageByGroupKey)
   │
   ├─ effectiveIdleCount = idleCount - pendingReservationCount
   │
   ├─ Scale Down: Only if effectiveIdleCount > maxIdleToKeep
   │
   └─ Scale Up: If effectiveIdleCount < minIdleToKeep OR pendingReservationCount > 0
```

---

## Key Behavioral Changes

### Scale-Up Logic (Enhanced)

**Before:**
```
IF idleCount < minIdleToKeep:
    scaleUp by (minIdleToKeep - idleCount)
```

**After:**
```
effectiveIdle = idleCount - pendingReservations
IF effectiveIdle < minIdleToKeep OR pendingReservations > 0:
    scaleUp by (minIdleToKeep - effectiveIdle) + pendingReservations
```

### Scale-Down Logic (Protected)

**Before:**
```
IF idleCount > maxIdleToKeep:
    scaleDown by (idleCount - maxIdleToKeep)
```

**After:**
```
effectiveIdle = idleCount - pendingReservations
IF effectiveIdle > maxIdleToKeep:
    scaleDown by (effectiveIdle - maxIdleToKeep)
// Pending reservations prevent premature scale-down
```

---

## Key Design Decisions

1. **Actual SchedulingConstraints passed (no parsing)**
   - `PendingReservationInfoSnapshot` includes the actual `SchedulingConstraints` object
   - `ExecutorStateManagerImpl` can directly use `findBestGroupForUsage(constraints)` without parsing constraint keys

2. **Reuses existing matching logic**
   - `findBestGroupForUsage()` uses the same `areSchedulingAttributeConstraintsSatisfied()` and fitness calculator as real scheduling
   - Only difference: doesn't require available TEs, just finds the matching group

3. **Sample TE for SKU mapping**
   - `mapGroupKeyToSkuViaGroupKeyFunc()` uses a sample TE from `executorsByGroup` to get registration
   - Applies the same `groupKeyFunc` that scaler uses for consistency

4. **Feature flag controlled**
   - `reservationSchedulingEnabled` flag propagated to all relevant components
   - Falls back to legacy behavior when disabled

---

## Testing Checklist

- [ ] Scaler receives pending reservation count per SKU
- [ ] Scale-up triggered when pending reservations exist
- [ ] Scale-down blocked when pending reservations would consume idle TEs
- [ ] Effective idle count correctly computed
- [ ] Constraint-to-SKU mapping works for size-based matching
- [ ] Constraint-to-SKU mapping works for machine-definition-based matching
- [ ] Feature flag controls new vs. legacy behavior
- [ ] New metrics emitted for reservation-aware decisions
- [ ] Backward compatible when flag is disabled
- [ ] Graceful fallback when ReservationRegistryActor query fails

