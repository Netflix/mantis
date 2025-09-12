# Plan: Support Reservation-based Task Executors Scheduling
The objective here is to introduce a reservation registry in resource cluster and scheduler to be able to handle reliable batched scheduling for jobs workers to task exectuors.
Current Problems:
- Only support batch mode in new job creations but not job scaling.
- Unreliable cache tracking on scheduling requests.
- Resource scaling is not efficient and cannot handle large scale concurrent job scaling operations.

## Design: Reservation-based Scheduling

### Action List (v2.1)

- ReservationRegistry: add `ReservationRegistry` owned by `ResourceClusterActor`
  - Types: `ReservationKey(jobId, stageNum)`, `Reservation{ReservationKey, canonicalSchedulingConstraint, workerRequests, stageTargetSize, lastUpdatedAt, priorityEpoch}`
  - Indexes: `reservationsByKey`, `perSkuPriorityQueue` ordered by `(priorityEpoch desc)`
  - API (actor-turn only): `upsertReservations`, `completeReservation`, `cancelReservation`, `getPendingReservationsBySchedulingConstraints`, `markReady/markNotReady`, `clearAll`
  - Behavior: 
    - same reservation request doesn't change existing reservation priority.
    - init: return not ready from getPendingReservationsBySchedulingConstraints until ready signal from schduler factory.
    - reservation priority: default on request epoch; override to (0/1/2 etc. on job tiering or manual overide e.g. replace worker request).
    - same key (jobId + stageNum) with different stage target size: dedupe logic (ignore same target size && same worker request size).
    - reservation class should be immutable. Replace exisitng if needed.
    - internal loop to process reservation queue: timer + ESM signal
    - abstract and config to support different priority behavior. Default strategy setup to use epoch time on creation + override signal (e.g. priorityEpoch as 0)

- ResourceClusterActor: integrate registry with matching and TE lifecycle
  - Integrate registry calls: getTaskExecutorFor from scheduler; getUsage from RC scaler (combine state from ESM and registry).
  - Delegate calls to registry and calls between registry and ExecutorStateManager
  - Support Assignement function (currently in scheduler actor) to schedule assignments to TEs and failure handling (update reservation).

- Scheduler: simplify and become stateless on pending
  - Extend `BatchScheduleRequest` to each job stage's target size.
  - On schedule: keep the same `getTaskExecutorsFor` call to RC but no longer expect the allocation result. Only retry if RC returns a failure message indicating reservation insert failed.
  - On assignment: no longer responsible to call TE for assignment (now handled directly inside RC).
  - On leader init: in scheduler factory wait for `JobActor`s init, then call `markRegistryReady()` to RC.

- Autoscaler coupling and readiness
  - RC usage response: `{ready:boolean, bySku: {available, pending, idle=available-pending}}`; still exclude disabled from `available`
  - Scaler ignores scale-down while registry not ready; optionally prompt scaler when `pending` reservations increases materially

- API contracts to add/confirm
  - RC: `scheduleFor(allocationRequestsWithKey)` → upsert+ack to replace current `getExecutorsFor*` APIs.
  - Scheduler Factory → RC: `MarkRegistryReady`

- Observability & ops
  - Metrics: track state in reservation registry (size, pending, actions). Metrics on usage result to scaler.
  - Structured logs for upserts, assignment decisions, submit outcomes, readiness transitions

- Recovery, flags, and rollout
  - Feature-flag the registry path; retain fallback to current behavior; Add depreaction annotation and notes to old code path that can be removed later.
  - Ensure idempotent upserts and submit result handling; timeouts recover assigned-but-unlaunched

- Tests
  - Unit: registry APIs and state transition.
  - Unit: ESM lease/assign/unassign/timeout invariants
  - Integration: multi-reservation handling, failed TE assignements, init behavior, RC restart with scheduler replay, scaler polling with readiness gating

### Edge cases
- JobActor/Scheduler retries: ensure handling if JobActor/Scheduler trigger retry on scheduling requests, proper handling and dedupe on reservations.


### Reservation-based Scheduling v2.1 (Mermaid)

```mermaid
sequenceDiagram
    autonumber
    participant JA as JobActor
    participant SCH as ResourceClusterAwareSchedulerActor
    participant RC as ResourceClusterActor
    participant REG as ReservationRegistry
    participant ESM as ExecutorStateManagerImpl
    participant RCS as ResourceClusterScalerActor
    participant TE as TaskExecutor

    SCH->>RC: MarkRegistryReady() after all job actor init on leader switch
    JA->>SCH: BatchScheduleRequest(targetCount)
    SCH->>RC: getTaskExecutorsFor(key: jobId+stage, targetCount)
    RC->>REG: upsertReservations(key, targetCount)
    REG->>REG: enqueue key in perSkuPriorityQueue
    loop match reservations (periodic or triggered by TE updates)
      REG->>RC: tryMatchTaskExecutors(reservation)

      rect rgb(245, 245, 245)
      RC->>ESM: findBestFit(reservations)
      ESM-->>RC: matched (TE ids)
      RC->>TE: submitTask(executeStageRequest) with TE lease
      TE-->>RC: Ack / Fail
      RC->>REG: completeReservation(key) if success, re-enqueue reservation if failed
      end
    end

    
    

    loop Scaler poll (periodic or prompted)
        RCS->>RC: GetClusterUsage()
        RC-->>RCS: {ready, bySku: {merge state from ESM and REG}}
    end


```

## [Depreacted] Draft 1: Reservation-First Push Design

### Reservation-First Push Sequence

```mermaid
sequenceDiagram
    participant Client
    participant JCM as JobClustersManagerActor
    participant JC as JobClusterActor
    participant JA as JobActor
    participant SCH as ResourceClusterAwareSchedulerActor
    participant RC as ResourceClusterActor
    participant REG as ReservationRegistry
    participant ESM as ExecutorStateManagerImpl
    participant RCS as ResourceClusterScalerActor
    participant HOST as ResourceClusterHostActor
    participant TE as TaskExecutor

    Client->>JCM: SubmitJob
    JCM->>JC: forward
    JC->>JA: InitJob
    JA->>SCH: Request workers
    SCH->>REG: UpsertReservation
    REG->>RC: Request allocation
    RC->>ESM: Select best fit and lease
    RC->>ESM: Mark assigned and start timeout
    RC->>SCH: AssignedBatchScheduleRequestEvent
    SCH->>TE: submitTask
    TE-->>SCH: Ack or Fail
    SCH->>RC: MarkAllocationLaunched or Failed
    RC->>REG: Update counts or rollback
    RC->>ESM: Unassign on timeout

    loop Capacity loop
        RCS->>RC: GetClusterUsage
        RC-->>RCS: Usage
        RCS->>HOST: Scale up or down
        HOST-->>RC: Add or remove TEs
        TE->>RC: Register or Heartbeat
        RC->>ESM: Mark available
    end
```

### Scheduler Request Payload Change (Required)

- Update `JobActor → Scheduler: BatchScheduleRequest` to carry the reservation-relevant state for dedupe and quick convergence:
  - `targetCount` for the (jobId, stageNum, skuId) key as of this request
  - `launchedCount` known by JobActor (optional; if omitted, scheduler/RC infers from active workers)
- The scheduler will upsert these values into the `ReservationRegistry` before (or alongside) calling the existing allocation path. This allows the registry to coalesce rapid scale changes and avoid double-counting pending demand.


## ReservationRegistry (Reuse-Path) Design

Purpose
- Provide a precise, deduped ledger of demand for autoscaling and visibility while reusing the existing scheduling/assignment flow unchanged.

Key types
- **ReservationKey**: `(jobId, stageNum, skuId)` where `skuId` matches the scaler's `usageGroupKey`.
- **Reservation**:
  - `key`: ReservationKey
  - `targetCount`: int
  - `launchedCount`: int
  - `lastUpdatedAt`: long
  - `priority`: long (updated whenever target changes to reorder within sku)
- Derived: `pendingCount = max(0, targetCount - launchedCount)`

Lifecycle and integration
- JobActor emits BatchScheduleRequest to Scheduler with `(targetCount, launchedCount?)` attached.
- Scheduler resolves `skuId`, calls `upsertFromScheduler` (with retries), then proceeds with the existing `getTaskExecutorsFor(...) → submitTask(...)` flow.
- When an `AssignedBatchScheduleRequestEvent` completes without errors for a reservation key, Scheduler calls `completeReservationBatch(key, batchSize)`.
- During leader reinit, Scheduler replays upserts from JobActors and then signals `markReady()` so scaler can trust `pending`.

Autoscaler coupling
- For each `skuId`: `idle = available - pending` where `pending = sum(max(0, targetCount - launchedCount))` from the registry. Because keys use the same `skuId` as the scaler's usage, the computation aligns exactly.

Notes
- No partial allocations are recorded at the reservation layer; it only updates when batches fully succeed. Existing assignment retries, timeouts, and error handling remain unchanged in Scheduler/RC/ESM.
- Priority is updated on every target change to allow newer requests to reorder within the sku queue if/when RC later consults the registry for fairness.

ReservationRegistry Placement

- Recommendation: implement `ReservationRegistry` as a plain component owned by `ResourceClusterActor` (same actor turn), not as:
  - an inner class of `ExecutorStateManagerImpl`, and
  - not a separate actor.

- Rationale:
  - **Separation of concerns**: `ExecutorStateManagerImpl` is TE-centric (registered/available/assigned/disabled); reservations are job-centric (JobId/Stage/GroupKey target/allocated/launched). Keeping them separate preserves clarity and testability.
  - **Atomicity**: Matching requires atomic steps across registry and TE state (lease → assign → start timeout → emit assignment). Owning both within `ResourceClusterActor` preserves atomicity under a single-threaded actor turn.
  - **Lower complexity**: A separate actor would need cross-actor transactions or compensations, readiness choreography, and WAL/idempotency for recovery.

- Practical shape:
  - `ResourceClusterActor` composes:
    - `ReservationRegistry` (maps and per-GroupKey FIFO/priority queues; counters: target, allocated, launched),
    - `ExecutorStateManagerImpl` (existing TE state/selection/lease/assignment/timeout).
  - All updates flow through RC:
    - Upsert/cancel reservation → attempt allocation (best-fit + lease) → mark assigned + start timeout → push to scheduler.
    - Submit ack/fail → update registry counts; on fail/timeout → unassign and return TE to available.
  - Scaler usage: `idle = ESM.available − Registry.pending`, gated by a registry readiness flag on leader startup.
