# Reservation-Based Scheduler Integration Plan

## Overview

This document outlines the integration plan for the new reservation-based scheduling system with the existing `JobActor` worker scheduling logic. The reservation system provides prioritized, batched allocation of Task Executors through the `ReservationRegistryActor`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     JobClustersManagerActor                              │
│   initialize() ──────> mantisSchedulerFactory.markAllRegistriesReady()  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            JobActor                                      │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    WorkerManager                                  │    │
│  │   submitInitialWorkers() ──> scheduler.upsertReservation(NEW_JOB)│    │
│  │   resubmitWorker()       ──> scheduler.upsertReservation(REPLACE)│    │
│  │   scaleStage()           ──> scheduler.upsertReservation(SCALE)  │    │
│  │   shutdown()             ──> scheduler.cancelReservation()       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│          MantisScheduler (interface)                                     │
│    ┌──────────────────────────┐    ┌────────────────────────────────┐  │
│    │ResourceClusterAware      │    │ResourceClusterReservationAware │  │
│    │Scheduler (existing)      │    │Scheduler (NEW)                 │  │
│    │- scheduleWorkers()       │    │- upsertReservation()           │  │
│    │- unscheduleJob()         │    │- cancelReservation()           │  │
│    └────────────┬─────────────┘    └───────────────┬────────────────┘  │
└─────────────────┼──────────────────────────────────┼────────────────────┘
                  │                                   │
                  ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     ResourceCluster (interface)                          │
│      ┌───────────────────────────────────────────────────────────┐      │
│      │  ResourceClusterAkkaImpl                                   │      │
│      │    - getTaskExecutorsFor() (existing)                      │      │
│      │    - upsertReservation() (NEW)                             │      │
│      │    - cancelReservation() (NEW)                             │      │
│      └───────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 ResourceClusters (interface)                             │
│    - getClusterFor(ClusterID)                                           │
│    - markAllRegistriesReady() (NEW) ─────────────────────────────────┐  │
└──────────────────────────────────────────────────────────────────────│──┘
                                    │                                   │
                                    ▼                                   │
┌─────────────────────────────────────────────────────────────────────────┐
│                     ResourceClusterActor                                 │
│    ┌──────────────────────────────────────────────────────────────┐    │
│    │  ReservationRegistryActor        ExecutorStateManagerActor    │    │
│    │  - UpsertReservation             - TaskExecutorBatchAssignment │◄──┘
│    │  - CancelReservation             - WorkerLaunched events       │
│    │  - MarkReady                                                   │
│    └──────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Priority Types

The reservation system supports three priority types for different scheduling scenarios:

| Priority Type | Use Case | Priority Order |
|--------------|----------|----------------|
| `REPLACE` | Worker resubmission due to failure/heartbeat timeout | Highest (processed first) |
| `SCALE` | Scale-up requests for existing jobs | Medium |
| `NEW_JOB` | Initial workers for new job submissions | Lowest |

Within the same priority type, jobs are ordered by:
1. **Tier** (ascending) - Lower tier = higher priority
2. **Timestamp** (ascending) - Earlier requests processed first (FIFO)

---

## Implementation Plan

### Phase 1: Refactor Existing Classes for Reuse

#### 1.1 Move Reservation Classes to Shared Location (No Duplication)

The existing reservation-related classes in `ResourceClusterActor.java` should be **moved** to a new shared file rather than duplicating them. This avoids code duplication and ensures consistency.

**Move FROM:** `ResourceClusterActor.java` (static inner classes)
**Move TO:** New file `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/proto/MantisResourceClusterReservationProto.java`

```java
package io.mantisrx.master.resourcecluster.proto;

import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import lombok.Builder;
import lombok.Value;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Protocol classes for the reservation-based scheduling system.
 * These classes are shared between ResourceClusterActor, ReservationRegistryActor,
 * MantisScheduler, and JobActor.
 */
public final class MantisResourceClusterReservationProto {

    private MantisResourceClusterReservationProto() {}

    /**
     * Unique identifier for a reservation (job + stage).
     */
    @Value
    @Builder
    public static class ReservationKey {
        String jobId;
        int stageNumber;
    }

    /**
     * Priority for ordering reservations within constraint groups.
     * Ordering: REPLACE < SCALE < NEW_JOB (REPLACE processed first).
     */
    @Value
    @Builder
    public static class ReservationPriority implements Comparable<ReservationPriority> {
        public enum PriorityType {
            REPLACE,  // Worker replacement due to failure (highest priority)
            SCALE,    // Scale-up request for existing job
            NEW_JOB   // New job submission (lowest priority)
        }

        PriorityType type;
        int tier;       // Job tier (lower = higher priority within same type)
        long timestamp; // FIFO ordering within same priority and tier

        @Override
        public int compareTo(ReservationPriority other) {
            int typeComparison = this.type.compareTo(other.type);
            if (typeComparison != 0) return typeComparison;
            int tierComparison = Integer.compare(this.tier, other.tier);
            if (tierComparison != 0) return tierComparison;
            return Long.compare(this.timestamp, other.timestamp);
        }
    }

    /**
     * Full reservation data including scheduling constraints and allocation requests.
     */
    @Value
    @Builder(toBuilder = true)
    public static class Reservation {
        ReservationKey key;
        SchedulingConstraints schedulingConstraints;
        String canonicalConstraintKey;
        Set<WorkerId> requestedWorkers;
        Set<TaskExecutorAllocationRequest> allocationRequests;
        int stageTargetSize;
        ReservationPriority priority;

        public boolean hasSameShape(Reservation other) {
            return other != null
                && Objects.equals(key, other.key)
                && Objects.equals(canonicalConstraintKey, other.canonicalConstraintKey)
                && Objects.equals(requestedWorkers, other.requestedWorkers)
                && stageTargetSize == other.stageTargetSize;
        }

        public int getRequestedWorkersCount() {
            return requestedWorkers != null ? requestedWorkers.size() : 0;
        }

        public static Reservation fromUpsertReservation(UpsertReservation upsert, String canonicalConstraintKey) {
            return Reservation.builder()
                .key(upsert.getReservationKey())
                .schedulingConstraints(upsert.getSchedulingConstraints())
                .canonicalConstraintKey(canonicalConstraintKey)
                .requestedWorkers(upsert.getAllocationRequests() != null ?
                    upsert.getAllocationRequests().stream()
                        .map(TaskExecutorAllocationRequest::getWorkerId)
                        .collect(Collectors.toSet())
                    : Collections.emptySet())
                .allocationRequests(upsert.getAllocationRequests() != null ?
                    upsert.getAllocationRequests() : Collections.emptySet())
                .stageTargetSize(upsert.getStageTargetSize())
                .priority(upsert.getPriority())
                .build();
        }
    }

    /**
     * Message to insert/update a reservation.
     */
    @Value
    @Builder
    public static class UpsertReservation {
        ReservationKey reservationKey;
        SchedulingConstraints schedulingConstraints;
        Set<TaskExecutorAllocationRequest> allocationRequests;
        int stageTargetSize;
        ReservationPriority priority;
    }

    /**
     * Message to cancel a reservation.
     */
    @Value
    @Builder
    public static class CancelReservation {
        ReservationKey reservationKey;
    }

    /**
     * Response to reservation cancellation.
     */
    @Value
    public static class CancelReservationAck {
        ReservationKey reservationKey;
        boolean cancelled;
    }

    /**
     * Marker message to indicate registry is ready to process reservations.
     */
    public enum MarkReady {
        INSTANCE
    }
}
```

#### 1.2 Update `ResourceClusterActor.java` and `ReservationRegistryActor.java`

Replace the static inner classes with imports from the new shared location:

**In `ResourceClusterActor.java`:**
```java
// Add imports
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;
// Or use static imports for cleaner code:
import static io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;

// Remove the following static inner classes (moved to proto file):
// - ReservationKey
// - ReservationPriority
// - Reservation
// - UpsertReservation
// - CancelReservation
// - CancelReservationAck
// - MarkReady

// Keep these as they are specific to the actor's internal operation:
// - ProcessReservationsTick
// - ForceProcessReservationsTick
// - GetPendingReservationsView
```

**In `ReservationRegistryActor.java`:**
```java
// Replace references like ResourceClusterActor.ReservationKey with direct import:
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;
// Or static import for cleaner code
```

#### 1.3 Extend `MantisScheduler` Interface (Using Shared Classes)

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/server/master/scheduler/MantisScheduler.java`

```java
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservationAck;

// Add new methods:

/**
 * Insert or update a reservation for workers.
 * @param request The reservation request (reuses existing UpsertReservation class)
 * @return Future that completes when the reservation is accepted
 */
default CompletableFuture<Ack> upsertReservation(UpsertReservation request) {
    throw new UnsupportedOperationException("Reservation-based scheduling not supported");
}

/**
 * Cancel all pending reservations for a given job stage.
 * @param request The cancel request (reuses existing CancelReservation class)
 * @return Future containing cancellation acknowledgement
 */
default CompletableFuture<CancelReservationAck> cancelReservation(CancelReservation request) {
    throw new UnsupportedOperationException("Reservation-based scheduling not supported");
}

/**
 * Check if this scheduler supports reservation-based scheduling.
 */
default boolean supportsReservationScheduling() {
    return false;
}
```

**Note:** No new DTO classes needed! We reuse `UpsertReservation`, `CancelReservation`, and `CancelReservationAck` directly.

---

### Phase 2: ResourceCluster Interface Extension

#### 2.1 Extend `ResourceCluster` Interface (Using Shared Classes)

**File:** `mantis-control-plane/mantis-control-plane-core/src/main/java/io/mantisrx/server/master/resourcecluster/ResourceCluster.java`

Add imports and new methods:

```java
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservationAck;

/**
 * Insert or update a reservation for workers in this cluster.
 * Reuses the shared UpsertReservation class from MantisResourceClusterReservationProto.
 *
 * @param request The reservation request
 * @return Future that completes when reservation is accepted
 */
default CompletableFuture<Ack> upsertReservation(UpsertReservation request) {
    throw new UnsupportedOperationException("Reservation not supported");
}

/**
 * Cancel reservations for a job stage.
 * Reuses the shared CancelReservation class from MantisResourceClusterReservationProto.
 *
 * @param request The cancellation request
 * @return Future containing cancellation response
 */
default CompletableFuture<CancelReservationAck> cancelReservation(CancelReservation request) {
    throw new UnsupportedOperationException("Reservation not supported");
}
```

#### 2.2 Extend `ResourceClusters` Interface

**File:** `mantis-control-plane/mantis-control-plane-core/src/main/java/io/mantisrx/server/master/resourcecluster/ResourceClusters.java`

Add new method:

```java
/**
 * Mark all reservation registries as ready to process reservations.
 * This should be called after master initialization is complete and all
 * existing jobs have been recovered.
 *
 * @return Future that completes when all clusters are marked ready
 */
CompletableFuture<Ack> markAllRegistriesReady();
```

#### 2.3 Implement in `ResourceClusterAkkaImpl` (Simplified - No Conversion Needed)

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ResourceClusterAkkaImpl.java`

Since we're using the shared classes directly, the implementation is much simpler - **no type conversion needed**:

```java
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservationAck;

@Override
public CompletableFuture<Ack> upsertReservation(UpsertReservation request) {
    // Direct passthrough - no conversion needed since we use shared classes!
    return Patterns.ask(resourceClusterManagerActor, request, askTimeout)
        .thenApply(Ack.class::cast)
        .toCompletableFuture();
}

@Override
public CompletableFuture<CancelReservationAck> cancelReservation(CancelReservation request) {
    // Direct passthrough - no conversion needed!
    return Patterns.ask(resourceClusterManagerActor, request, askTimeout)
        .thenApply(CancelReservationAck.class::cast)
        .toCompletableFuture();
}
```

**Note:** By reusing the same classes across all layers, we eliminate all type conversion code!

#### 2.4 Implement in `ResourceClustersAkkaImpl` (or equivalent)

Implement `markAllRegistriesReady()` using the shared `MarkReady` enum:

```java
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.MarkReady;

@Override
public CompletableFuture<Ack> markAllRegistriesReady() {
    return listActiveClusters()
        .thenCompose(clusterIds -> {
            List<CompletableFuture<Ack>> futures = clusterIds.stream()
                .map(this::getClusterFor)
                .map(cluster -> {
                    // Send MarkReady to each cluster's reservation registry
                    // Uses shared MarkReady enum from proto file
                    return Patterns.ask(
                        resourceClusterManagerActor,
                        MarkReady.INSTANCE,  // From shared proto
                        askTimeout)
                        .thenApply(Ack.class::cast)
                        .toCompletableFuture();
                })
                .collect(Collectors.toList());

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> Ack.getInstance());
        });
}
```

---

### Phase 3: New Scheduler Implementation (Using Shared Classes)

#### 3.1 Create `ResourceClusterReservationAwareScheduler`

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/server/master/scheduler/ResourceClusterReservationAwareScheduler.java`

```java
package io.mantisrx.server.master.scheduler;

import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservationAck;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationKey;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Scheduler implementation that uses the reservation-based scheduling system.
 *
 * This scheduler delegates scheduling decisions to the ReservationRegistryActor
 * which processes reservations in priority order per constraint group:
 * - REPLACE (worker failures) - highest priority
 * - SCALE (scale-up requests) - medium priority
 * - NEW_JOB (new submissions) - lowest priority
 *
 * Uses shared classes from MantisResourceClusterReservationProto - no type conversion needed.
 * WorkerLaunched events are still routed via jobMessageRouter from ExecutorStateManagerActor.
 */
@Slf4j
@RequiredArgsConstructor
public class ResourceClusterReservationAwareScheduler implements MantisScheduler {

    private final ResourceCluster resourceCluster;

    // ==================== Reservation APIs (Using Shared Classes Directly) ====================

    @Override
    public CompletableFuture<Ack> upsertReservation(UpsertReservation request) {
        ReservationKey key = request.getReservationKey();
        log.info("Upserting reservation for job {} stage {} with {} workers (priority={})",
            key.getJobId(),
            key.getStageNumber(),
            request.getAllocationRequests().size(),
            request.getPriority().getType());
        // Direct passthrough - same type used everywhere!
        return resourceCluster.upsertReservation(request);
    }

    @Override
    public CompletableFuture<CancelReservationAck> cancelReservation(CancelReservation request) {
        ReservationKey key = request.getReservationKey();
        log.info("Cancelling reservation for job {} stage {}", key.getJobId(), key.getStageNumber());
        // Direct passthrough - same type used everywhere!
        return resourceCluster.cancelReservation(request);
    }

    @Override
    public boolean supportsReservationScheduling() {
        return true;
    }

    @Override
    public boolean schedulerHandlesAllocationRetries() {
        // Reservation registry handles retries via timeout mechanism
        return true;
    }

    // ==================== Legacy APIs ====================

    @Override
    public void scheduleWorkers(BatchScheduleRequest scheduleRequest) {
        throw new UnsupportedOperationException(
            "Use upsertReservation() instead of scheduleWorkers() with reservation-based scheduling");
    }

    @Override
    public void unscheduleJob(String jobId) {
        // For reservation-based scheduling, caller should use cancelReservation per stage
        log.warn("unscheduleJob({}) called - use cancelReservation() per stage instead", jobId);
    }

    @Override
    public void unscheduleWorker(WorkerId workerId, Optional<String> hostname) {
        throw new UnsupportedOperationException(
            "unscheduleWorker not supported - use cancelReservation() for pending workers");
    }

    @Override
    public void unscheduleAndTerminateWorker(WorkerId workerId, Optional<String> hostname) {
        // Still needed for terminating running workers
        log.info("Terminating worker {}", workerId);
        resourceCluster.markTaskExecutorWorkerCancelled(workerId)
            .whenComplete((ack, ex) -> {
                if (ex != null) {
                    log.warn("Failed to mark worker {} as cancelled", workerId, ex);
                }
            });
    }

    @Override
    public void updateWorkerSchedulingReadyTime(WorkerId workerId, long when) {
        throw new UnsupportedOperationException(
            "Reservation registry handles retry timing internally");
    }

    @Override
    public void initializeRunningWorker(ScheduleRequest scheduleRequest, String hostname, String hostID) {
        // Still needed for master failover recovery
        log.info("Initializing running worker {} on host {}", scheduleRequest.getWorkerId(), hostname);
        resourceCluster.initializeTaskExecutor(TaskExecutorID.of(hostID), scheduleRequest.getWorkerId())
            .whenComplete((ack, ex) -> {
                if (ex != null) {
                    log.warn("Failed to initialize running worker {}", scheduleRequest.getWorkerId(), ex);
                }
            });
    }
}
```

---

### Phase 4: Factory Updates

#### 4.1 Add Configuration Flag

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/server/master/config/MasterConfiguration.java`

```java
@Config("mantis.scheduling.reservation.enabled")
@Default("false")
boolean isReservationSchedulingEnabled();
```

#### 4.2 Update `MantisSchedulerFactoryImpl`

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/server/master/scheduler/MantisSchedulerFactoryImpl.java`

```java
@RequiredArgsConstructor
@Slf4j
public class MantisSchedulerFactoryImpl implements MantisSchedulerFactory {

    private final ActorSystem actorSystem;
    private final ResourceClusters resourceClusters;
    private final ExecuteStageRequestFactory executeStageRequestFactory;
    private final JobMessageRouter jobMessageRouter;
    private final MasterConfiguration masterConfiguration;
    private final MetricsRegistry metricsRegistry;
    private final Map<ClusterID, MantisScheduler> actorRefMap = new ConcurrentHashMap<>();

    @Override
    public MantisScheduler forClusterID(ClusterID clusterID) {
        if (clusterID == null || Strings.isNullOrEmpty(clusterID.getResourceID())) {
            throw new RuntimeException("Invalid clusterID for MantisScheduler");
        }
        return actorRefMap.computeIfAbsent(clusterID, this::createScheduler);
    }

    private MantisScheduler createScheduler(ClusterID clusterID) {
        log.info("Creating scheduler for cluster: {} (reservationEnabled={})",
            clusterID.getResourceID(),
            masterConfiguration.isReservationSchedulingEnabled());

        if (masterConfiguration.isReservationSchedulingEnabled()) {
            return new ResourceClusterReservationAwareScheduler(
                resourceClusters.getClusterFor(clusterID));
        } else {
            return new ResourceClusterAwareScheduler(
                actorSystem.actorOf(
                    ResourceClusterAwareSchedulerActor.props(
                        masterConfiguration.getSchedulerMaxRetries(),
                        masterConfiguration.getSchedulerMaxRetries(),
                        masterConfiguration.getSchedulerIntervalBetweenRetries(),
                        resourceClusters.getClusterFor(clusterID),
                        executeStageRequestFactory,
                        jobMessageRouter,
                        metricsRegistry),
                    "scheduler-for-" + clusterID.getResourceID()),
                masterConfiguration.getSchedulerHandlesAllocationRetries());
        }
    }

    /**
     * Mark all reservation registries as ready after master initialization.
     * Should be called after all jobs have been recovered.
     *
     * @return Future that completes when all registries are ready
     */
    public CompletableFuture<Ack> markAllRegistriesReady() {
        if (masterConfiguration.isReservationSchedulingEnabled()) {
            log.info("Marking all reservation registries as ready");
            return resourceClusters.markAllRegistriesReady();
        } else {
            log.debug("Reservation scheduling disabled, skipping markAllRegistriesReady");
            return CompletableFuture.completedFuture(Ack.getInstance());
        }
    }
}
```

---

### Phase 5: JobClustersManagerActor Integration

#### 5.1 Call `markAllRegistriesReady` After Initialization

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/JobClustersManagerActor.java`

In the `initialize()` method, after all job clusters are initialized:

```java
// In initialize() method, after Observable.from(jobClusterMap.values())...toBlocking().subscribe()
// Around line 405-411

.toBlocking()
.subscribe((clusterInit) -> {
    logger.info("JobCluster {} inited with code {}", clusterInit.jobClusterName, clusterInit.responseCode);
    numJobClusterInitSuccesses.increment();
}, (error) -> {
    logger.warn("Exception initializing clusters {}", error.getMessage(), error);
    logger.error("JobClusterManagerActor had errors during initialization");
    sender.tell(new JobClustersManagerInitializeResponse(initMsg.requestId, SERVER_ERROR,
        "JobClustersManager inited with errors"), getSelf());
}, () -> {
    logger.info("JobClusterManagerActor transitioning to initialized behavior");
    getContext().become(initializedBehavior);

    // NEW: Mark all reservation registries as ready
    if (mantisSchedulerFactory != null) {
        mantisSchedulerFactory.markAllRegistriesReady()
            .whenComplete((ack, ex) -> {
                if (ex != null) {
                    logger.error("Failed to mark reservation registries as ready", ex);
                } else {
                    logger.info("All reservation registries marked ready");
                }
            });
    }

    sender.tell(new JobClustersManagerInitializeResponse(initMsg.requestId, SUCCESS,
        "JobClustersManager successfully inited"), getSelf());
});
```

---

### Phase 6: JobActor Integration (Using Shared Classes Directly)

#### 6.1 Update `WorkerManager` in `JobActor.java`

**File:** `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/jobcluster/job/JobActor.java`

Add imports for the shared classes at the top of the file:

```java
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationKey;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType;
```

##### 6.1.1 Add Field to Track Reservation Support

```java
class WorkerManager implements IWorkerManager {
    // Existing fields...

    private final boolean useReservationScheduling;

    WorkerManager(...) {
        // Existing initialization...
        this.useReservationScheduling = // from master config value
    }
}
```

##### 6.1.2 Modify `queueTasks()` Method

```java
private void queueTasks(List<IMantisWorkerMetadata> workerRequests, Optional<Long> readyAt) {
    if (useReservationScheduling) {
        queueTasksViaReservation(workerRequests, PriorityType.NEW_JOB);
    } else {
        queueTasksViaLegacyScheduler(workerRequests, readyAt);
    }
}

private void queueTasksViaLegacyScheduler(List<IMantisWorkerMetadata> workerRequests, Optional<Long> readyAt) {
    // Existing implementation - move current queueTasks logic here
    final List<ScheduleRequest> scheduleRequests = workerRequests
        .stream()
        .map(wR -> createSchedulingRequest(wR, readyAt))
        .collect(Collectors.toList());
    LOGGER.info("Queueing up batch schedule request for {} workers", workerRequests.size());
    try {
        scheduler.scheduleWorkers(new BatchScheduleRequest(scheduleRequests));
    } catch (Exception e) {
        LOGGER.error("Exception queueing tasks", e);
    }
}

/**
 * Queue tasks via reservation system using shared proto classes directly.
 * No type conversion needed - JobActor uses the same classes as ResourceClusterActor.
 */
private void queueTasksViaReservation(
        List<IMantisWorkerMetadata> workerRequests,
        PriorityType priorityType) {

    // Group by stage
    Map<Integer, List<IMantisWorkerMetadata>> byStage = workerRequests.stream()
        .collect(Collectors.groupingBy(IMantisWorkerMetadata::getStageNum));

    for (Map.Entry<Integer, List<IMantisWorkerMetadata>> entry : byStage.entrySet()) {
        int stageNum = entry.getKey();
        List<IMantisWorkerMetadata> stageWorkers = entry.getValue();

        if (stageWorkers.isEmpty()) continue;

        // Create allocation requests
        Set<TaskExecutorAllocationRequest> allocationRequests = stageWorkers.stream()
            .map(wm -> {
                ScheduleRequest sr = createSchedulingRequest(wm, Optional.empty());
                return TaskExecutorAllocationRequest.of(
                    sr.getWorkerId(),
                    sr.getSchedulingConstraints(),
                    sr.getJobMetadata(),
                    sr.getStageNum());
            })
            .collect(Collectors.toSet());

        // Get stage target size
        int stageTargetSize = mantisJobMetaData.getStageMetadata(stageNum)
            .map(IMantisStageMetadata::getNumWorkers)
            .orElse(stageWorkers.size());

        // Build reservation request using shared UpsertReservation class directly!
        UpsertReservation request = UpsertReservation.builder()
            .reservationKey(ReservationKey.builder()
                .jobId(jobId.getId())
                .stageNumber(stageNum)
                .build())
            .schedulingConstraints(stageWorkers.get(0).getSchedulingConstraints())
            .allocationRequests(allocationRequests)
            .stageTargetSize(stageTargetSize)
            .priority(buildPriority(priorityType))
            .build();

        LOGGER.info("Upserting reservation for job {} stage {} with {} workers (priority={})",
            jobId, stageNum, allocationRequests.size(), priorityType);

        scheduler.upsertReservation(request)
            .whenComplete((ack, ex) -> {
                if (ex != null) {
                    LOGGER.warn("Failed to upsert reservation for job {} stage {}", jobId, stageNum, ex);
                }
            });
    }
}

/**
 * Build priority using shared ReservationPriority class directly.
 */
private ReservationPriority buildPriority(PriorityType type) {
    // Get tier from job SLA (durationType ordinal gives tier)
    int tier = mantisJobMetaData.getJobDefinition().getJobSla().getDurationType().ordinal();

    return ReservationPriority.builder()
        .type(type)
        .tier(tier)
        .timestamp(System.currentTimeMillis())
        .build();
}
```

##### 6.1.3 Modify `resubmitWorker()` Method

```java
private void resubmitWorker(JobWorker oldWorker) throws Exception {
    LOGGER.info("Resubmitting worker {}", oldWorker.getMetadata());
    // ... existing validation and worker creation logic ...

    JobWorker newWorker = new JobWorker.Builder()
        // ... existing builder code ...
        .build();

    mantisJobMetaData.replaceWorkerMetaData(...);

    // Kill the old task
    scheduler.unscheduleAndTerminateWorker(
        oldWorkerMetadata.getWorkerId(),
        Optional.ofNullable(oldWorkerMetadata.getSlave()));

    // Queue the new worker
    if (useReservationScheduling) {
        // Use REPLACE priority (highest) for worker resubmission
        queueTasksViaReservation(
            Collections.singletonList(newWorker.getMetadata()),
            PriorityType.REPLACE);  // Direct enum reference
    } else {
        long workerResubmitTime = resubmitRateLimiter.getWorkerResubmitTime(...);
        queueTasks(Collections.singletonList(newWorker.getMetadata()), Optional.of(workerResubmitTime));
    }

    LOGGER.info("Worker {} successfully queued for scheduling", newWorker);
    numWorkerResubmissions.increment();
}
```

##### 6.1.4 Modify `scaleStage()` Method

```java
@Override
public int scaleStage(MantisStageMetadataImpl stageMetaData, int ruleMax, int ruleMin,
                      int numWorkers, String reason) {
    // ... existing validation and worker count calculation ...

    if (newNumWorkerCount > oldNumWorkers) {
        List<IMantisWorkerMetadata> workerRequests = new ArrayList<>();
        for (int i = 0; i < newNumWorkerCount - oldNumWorkers; i++) {
            try {
                int newWorkerIndex = oldNumWorkers + i;
                IMantisWorkerMetadata workerRequest = addWorker(schedInfo, stageMetaData.getStageNum(), newWorkerIndex);
                jobStore.storeNewWorker(workerRequest);
                markStageAssignmentsChanged(true);
                workerRequests.add(workerRequest);
            } catch (Exception e) {
                LOGGER.warn("Exception adding new worker for {}", stageMetaData.getJobId().getId(), e);
            }
        }

        // Queue all new workers
        if (useReservationScheduling) {
            // Use SCALE priority (medium) for scaling operations
            queueTasksViaReservation(workerRequests, PriorityType.SCALE);  // Direct enum reference
        } else {
            queueTasks(workerRequests, Optional.empty());
        }
    } else {
        // Scale down logic - unchanged
        // ...
    }

    return newNumWorkerCount;
}
```

##### 6.1.5 Modify `shutdown()` Method

```java
@Override
public void shutdown() {
    if (useReservationScheduling) {
        // Cancel all pending reservations for each stage using shared CancelReservation class
        for (IMantisStageMetadata stage : mantisJobMetaData.getStageMetadata().values()) {
            CancelReservation cancelRequest = CancelReservation.builder()
                .reservationKey(ReservationKey.builder()
                    .jobId(jobId.getId())
                    .stageNumber(stage.getStageNum())
                    .build())
                .build();

            scheduler.cancelReservation(cancelRequest)
                .whenComplete((resp, ex) -> {
                    if (ex != null) {
                        LOGGER.warn("Failed to cancel reservation for job {} stage {}",
                            jobId, stage.getStageNum(), ex);
                    } else {
                        LOGGER.debug("Cancelled reservation for job {} stage {} (cancelled={})",
                            jobId, stage.getStageNum(), resp.isCancelled());
                    }
                });
        }
    } else {
        scheduler.unscheduleJob(jobId.getId());
    }

    // Rest of existing shutdown logic...
    if (!allWorkerCompleted()) {
        terminateAllWorkersAsync();
    }
    jobSchedulingInfoBehaviorSubject.onNext(new JobSchedulingInfo(this.jobMgr.getJobId().getId(), new HashMap<>()));
    jobSchedulingInfoBehaviorSubject.onCompleted();
}
```

---

## Summary of Files to Modify/Create

### Key Design Decision: Reuse Existing Classes (No Duplication!)

By moving the existing reservation classes from `ResourceClusterActor.java` to a shared proto file, we:
- **Eliminate type conversion code** throughout the stack
- **Avoid duplication** of `ReservationKey`, `ReservationPriority`, `Reservation`, etc.
- **Ensure consistency** - same class used by JobActor, Scheduler, ResourceCluster, and Actor layers
- **Simplify maintenance** - single source of truth for reservation types

### New Files

| File | Description |
|------|-------------|
| `MantisResourceClusterReservationProto.java` | **Shared proto file** containing `ReservationKey`, `ReservationPriority`, `Reservation`, `UpsertReservation`, `CancelReservation`, `CancelReservationAck`, `MarkReady` - moved from `ResourceClusterActor.java` |
| `ResourceClusterReservationAwareScheduler.java` | New scheduler implementation using shared classes |

### Modified Files

| File | Changes |
|------|---------|
| `ResourceClusterActor.java` | Remove inner classes (moved to proto), import from `MantisResourceClusterReservationProto` |
| `ReservationRegistryActor.java` | Import from `MantisResourceClusterReservationProto` instead of `ResourceClusterActor` inner classes |
| `MantisScheduler.java` | Add `upsertReservation(UpsertReservation)`, `cancelReservation(CancelReservation)`, `supportsReservationScheduling()` |
| `ResourceCluster.java` | Add `upsertReservation(UpsertReservation)`, `cancelReservation(CancelReservation)` |
| `ResourceClusters.java` | Add `markAllRegistriesReady()` |
| `ResourceClusterAkkaImpl.java` | Implement reservation methods (direct passthrough - no conversion!) |
| `ResourceClustersAkkaImpl.java` | Implement `markAllRegistriesReady()` |
| `MasterConfiguration.java` | Add `isReservationSchedulingEnabled()` config |
| `MantisSchedulerFactoryImpl.java` | Add config toggle, create appropriate scheduler, add `markAllRegistriesReady()` |
| `JobClustersManagerActor.java` | Call `markAllRegistriesReady()` after init success |
| `JobActor.java` | Update `WorkerManager` to use reservations with priority types using shared classes |

---

## Rollout Plan

1. **Phase 1**: Deploy with `mantis.scheduling.reservation.enabled=false` (no behavior change)
2. **Phase 2**: Enable on test clusters, validate:
   - New job submission uses `NEW_JOB` priority
   - Worker resubmission uses `REPLACE` priority
   - Scale-up uses `SCALE` priority
   - Job kill cancels reservations properly
3. **Phase 3**: Gradual production rollout with feature flag
4. **Phase 4**: Enable by default, deprecate legacy path

---

## Testing Checklist

- [ ] New job submission creates reservation with `NEW_JOB` priority
- [ ] Worker failure triggers resubmission with `REPLACE` priority
- [ ] Scale-up creates reservation with `SCALE` priority
- [ ] Scale-down does not create reservations (direct termination)
- [ ] Job kill cancels all pending reservations
- [ ] Master failover correctly initializes running workers
- [ ] Priority ordering: `REPLACE` processed before `SCALE` before `NEW_JOB`
- [ ] Same priority ordered by tier then timestamp (FIFO)
- [ ] `WorkerLaunched` events correctly routed to JobActor
- [ ] Legacy scheduler path still works when config disabled

