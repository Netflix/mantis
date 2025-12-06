# Plan: Reservation Registry v3

## Goal
- Deliver reliable, strict-batch worker scheduling by moving reservation processing and task assignment into dedicated child actors under `ResourceClusterActor`.
- Preserve legacy scheduling path while enabling a pass-through mode where the resource cluster drives assignment, using the Excalidraw concept diagram as the execution blueprint.

## References
- `context/context-scheduling-logic.md`
- `context/Mantis Resource Registry.excalidraw`

## Summary of Gaps in Current Scheduling Flow
- Batch allocation from the scheduler is best-effort; no single actor owns the full reservation lifecycle, so retries become per-worker and inconsistent.
- `ExecutorStateManager` (ESM) pending state is not synchronized with the scaler, leading to inaccurate idle capacity reporting.
- Job creation honors strict batching while stage scaling mixes batch and non-batch behaviors, causing fairness and starvation issues.
- Scheduler retrying each worker independently creates no shared state for partial failures; duplicates and missing workers are possible.
- `ResourceClusterActor` already handles TE registration and heartbeats; adding reservation retry loops there would overload its dispatcher, so the new design must split responsibilities.

## Terminology
- Task Executor (TE): agent host registered to control plane from one of the resource clusters with a pre-defined container spec. Can be assigned job worker.
- Worker: a worker in one of a job's stage. A job can have multiple stages and each stage will have a list of workers with worker index and worker number to id each one. JobActor is responsible to keep track of the workers and inform scheduler to add/remove workers from scaling/creation/deletion operations.
- ESM (executor state manager): class in ResourceClusterActor tracking TE status (registration and heartbeats).

## Architectural Changes
- Introduce a child-actor hierarchy under `ResourceClusterActor`: `ReservationRegistryActor`, `AssignmentTrackerActor`, and `ExecutorStateManagerActor` (wrapping the existing ESM logic). The child actors should talk to each other via parent actor instead of using direct actor ref.
- Add a scheduler factory toggle that selects either the legacy `ResourceClusterAwareSchedulerActor` or a new pass-through scheduler that forwards batch requests directly to the resource cluster.
- Shift the batch scheduling loop out of the scheduler: the scheduler enqueues a reservation, the resource cluster allocates TEs, and a dedicated tracker assigns workers and publishes results back to the job.
- Provide a feedback channel so successful assignments notify `JobActor`, allowing it to transition workers to `STARTING` (re-purposing the status to mean "assigned and waiting for heartbeat").
- Strict batching (immutable reservation). Use detailed metrics to see the pending status. Support cancellation on reservations fully wired into job action (kill, scale down). Simpler mechanism, less error prone but won't handle extreme case where large batch starving everyone else.

## Actor Catalog

### ResourceClusterActor (parent)
- **State**
  - References to child actors (`ReservationRegistryActor`, `AssignmentTrackerActor`, `ExecutorStateManagerActor`).
  - Existing TE registration tables (disabled/available/leasing), mapped by cluster SKU.
  - Pending command queue for escalations (scale requests, disablement).
  - Configuration for dispatcher choices and polling intervals.
- **Behavior**
  - Routes incoming `BatchScheduleRequest` (from scheduler or pass-through factory) to the reservation registry.
  - Continues to process TE lifecycle events (registration, heartbeats, disable, drain) and forwards inventory changes to the registry/assignment tracker when relevant.
  - Bridges scaler requests by composing data from ESM and reservations (pending demand) before replying.
  - Supervises child actors, restarting them and rebuilding in-memory state on failure.

### ReservationRegistryActor
- **State**
  - Strict priority queue of reservations keyed by stage/job priority; each entry includes canonicalized scheduling constraints and desired worker count.
  - Reservation index by `JobId` and stage for dedupe/cancellation.
  - Rate limiter / minimum wait timestamp to avoid thrashing the dispatcher.
  - Metrics counters (queue length, successful allocations, blocked durations).
- **Behavior**
  - Enqueue reservations when jobs submit new workers or scale up; merge requests when total target size matches existing reservation.
  - Process loop (timer-driven and on-demand): peek top reservation, invoke `tryAllocateAndLease` on ESM, and react to the result.
  - On successful lease of the full batch, emit an `AssignmentTaskCreated` message to the assignment tracker and remove the reservation from the queue.
  - On failed allocation, leave the reservation in place, optionally downgrade priority based on policy.
  - Handle cancellations triggered by job scale-down, worker kill, or reservation expiry.
  - Publish queue metrics to the scaler so pending demand is visible immediately.
  - on startup this registry actor should wait for all pending reservations to be resent by the jobactor init process. Do not return partial results to scaler until this init is marked as finished via the signal from jobClustersManagerActor -> schedulerFactory


### ExecutorStateManagerActor
- **State**
  - Wraps the existing ESM tables for TE availability, leasing contracts, disabled/draining sets, and pending reservations.
  - Data snapshots used for scaler responses (ready, idle, pending by SKU).
- **Behavior**
  - Responds to `tryAllocateAndLease(reservation)` messages from the registry, performing fitness calculations and returning `LeaseAccepted` or reasons for failure.
  - Updates pending counts when reservations are leasing, ensuring the scaler subtracts pending from available.
  - Releases leases on cancellation or on assignment tracker completion/failure.
  - Emits notifications to the registry whenever inventory becomes available (TE registered, released, or re-enabled).

### AssignmentTrackerActor
- **State**
  - Map of `AssignmentTaskId -> AssignmentTask` (see below), keyed by job/stage and reservation epoch.
  - Retry budgets (configurable) and exponential backoff timers per worker.
  - Channels to route IO-intensive work (TaskExecutor RPCs) onto a dedicated IO dispatcher while control logic stays on the actor dispatcher.
  - Metrics for in-flight assignments, retry counts, and time-to-assign.
- **Behavior**
  - On `AssignmentTaskCreated`, spawn tracking for the task, moving each worker to the IO dispatcher to call `TaskExecutorGateway.submitTask`.
  - Transition assigned workers into the success set; notify `JobActor` via existing message router so it can monitor heartbeats.
  - For RPC failures or timeouts, increment retry count; if the limit is reached, emit a `ReservationRequeueRequested` event (high priority) instead of silently replacing the worker.
  - Cancel tasks when job termination or worker kill/replace commands arrive.
  - Close tasks when all workers acknowledge, releasing leases back to ESM.

### AssignmentTask (entity, tracked by AssignmentTrackerActor)
- **State Fields**
  - Original reservation payload: job/stage, list of worker descriptors, scheduling constraints.
  - `assignedWorkers`: worker ids already acknowledged by TEs.
  - `pendingWorkers`: worker ids pending assignment with per-worker retry counters and last attempt timestamps.
  - Associated leases (TE ids) from ESM to ensure release on completion/failure.
- **Behavior**
  - Moves workers from pending to assigned on success.
  - Exposes next retry timestamps to the tracker so the dispatcher timer can wake the task at the right time.

### ResourceClusterAwarePassThroughSchedulerActor (new optional scheduler)
- **State**
  - Reference to resource cluster routing endpoint.
  - Mode flag (legacy vs pass-through) configured via scheduler factory.
- **Behavior**
  - In pass-through mode, accepts `BatchScheduleRequest` and forwards a lightweight reservation envelope to `ResourceClusterActor`, skipping TE selection.
  - Handles legacy compatibility by falling back to existing behavior when feature flag is disabled.
  - Reports reservation enqueue success/failure back to the caller (`JobActor`).

## Message Flow (Strict Batch Scenario)
1. `(0)` Scheduler (legacy or pass-through) receives `BatchScheduleRequest` from `JobActor` and forwards `EnqueueReservation(job, stage, workers)` to `ResourceClusterActor`.
2. `ResourceClusterActor` pushes the reservation into `ReservationRegistryActor`, which records metadata and schedules processing (timer or immediate wake).
3. `(1)` Registry peeks the highest priority reservation and calls `ExecutorStateManagerActor.tryAllocateAndLease`. If successful for the entire batch, it proceeds; otherwise the reservation stays queued.
4. `(2)` Registry emits `AssignmentTaskCreated(reservation, leases)` to `AssignmentTrackerActor`, breaking the operation into a separate actor message.
5. `AssignmentTrackerActor` fans out per-worker assignment on the IO dispatcher, calling into `TaskExecutorGateway`.
6. `(3)` On success, the tracker sends `WorkerAssigned(job, stage, worker)` to `JobActor`, which updates worker state and monitors heartbeats.
7. `(3')` If a worker exceeds retry budget, the tracker raises `ReservationRequeueRequested` (high priority) to the registry, and the job actor is asked to orchestrate worker replacement rather than blindly reassigning the same TE.
8. On completion (all workers assigned), the tracker releases leases through ESM and marks the task finished.

## Failure and Retry Handling
- TE assignment retries are capped; once exceeded, the system re-queues a fresh reservation and lets `JobActor` manage replacement to avoid duplicate workers on partially initialized executors.
- Reservation cancellations propagate to both registry and tracker, ensuring leased TEs are released promptly.
- Registry processing has a minimum delay between attempts to prevent tight loops if inventory is unavailable.
- On actor restart, state recovery sequences reload reservations and assignments from persisted snapshots (implementation detail TBD) before resuming processing.

## Concurrency and Dispatchers
- Dispatcher threads handle registry logic and timers; IO threads handle TE RPCs (`TaskExecutorGateway.submitTask`).
- `AssignmentTrackerActor` bridges both via message passing, keeping actor state mutations single-threaded.
- Registry to ESM calls remain asynchronous (`ask`/`pipeTo`) to avoid blocking the dispatcher.

## Scheduler and API Updates
- Introduce a scheduler factory flag (`passThroughReservationScheduling`) to select between legacy scheduling and the new registry-driven flow.
- Maintain compatibility with existing APIs for job submission and scaling; the difference is purely in how the scheduler fulfills the batch request.
- Update job lifecycle so that success notifications from the tracker move workers into the `STARTING` state, aligning with the TODOs called out in the diagram.

## Observability
- New metrics: reservation queue depth, time-in-queue, assignment retry counts, assignment latency per job, registry wake-ups, and ESM lease success rate.
- Add debug tracing for `(0)` through `(3')` steps to correlate reservations, assignments, and job notifications.

## ResourceClusterActor.java Changes
- In `mantis-control-plane/mantis-control-plane-server/src/main/java/io/mantisrx/master/resourcecluster/ResourceClusterActor.java`, declare child `ActorRef`s for the reservation registry, assignment tracker, and executor state manager actors. Construct them during `preStart` (or constructor) using dedicated dispatchers and register them under predictable names for restart.
- Replace direct `ExecutorStateManager` invocations with asynchronous `tell/ask` calls to the new ESM actor. Update all scheduling handlers to work with future responses, releasing leases through the actor instead of mutating local state.
- Extend the receive pipeline to handle reservation lifecycle messages: enqueue, cancel, flush, and assignment completion. Legacy inline scheduling remains behind a feature flag; when pass-through mode is enabled, immediately forward requests to the registry actor and return an enqueue acknowledgement.
- When TE lifecycle events arrive (registration, heartbeats, disable/enable, disconnection), continue updating existing tables but also notify both ESM and registry actors so queued reservations can retry promptly.
- Update scaler request handlers (`GetClusterUsage*`, `GetClusterIdleInstances*`) to compose responses from ESM inventory plus registry pending demand. Ensure responses include pending batch sizes so autoscaler decisions remain accurate.
- Given the state restoration mode, ensure these actors do not terminate on error and should try to resume to avoid losing state.
- Emit new metrics from `ResourceClusterActorMetrics` for registry queue depth, assignment latency, and lease durations by subscribing to child actor metric events or probing their state via ask-pattern calls.

- ReservationRegistryActor: add `ReservationRegistry` owned by `ResourceClusterActor`
  - Types: `ReservationKey(jobId, stageNum)`, `Reservation{ReservationKey, canonicalSchedulingConstraint, workerRequests, stageTargetSize, lastUpdatedAt, priorityEpoch}`
  - Indexes: `reservationsByKey`, `perSchedulingConstraintPriorityQueue` ordered by `(priorityEpoch asc)`. (make sure there is proper GC on these index)
  - API (actor message behavior):
    -- `upsertReservations`: recevied from JobActor/Scheduler. insert if new. If there is existing key, replace the old reservation.
    -- `cancelReservation`: cancel an existing reservation. Remove from index tables.
    -- `getPendingReservationsView`: return a map of summary of pending reservations grouped by reservation's schedulingConstraint.
    -- `markReady`: signal from external actors to indicate the registry has received all the state (init messages) from job actors. This is important since a partial pending result to resource cluster actor and scaler can interrupt ongoing provisoin tasks during a leader switch.
  - Behavior:
    - the general reservations from job actors should be processed FIFO
    - same reservation request doesn't change existing reservation priority.
    - init: return not ready from getPendingReservationsBySchedulingConstraints until ready signal from schduler factory.
    - reservation priority: default on request epoch; override to (0/1/2 etc. on manual overide e.g. replace worker request). Leave interface to add custom logic in future too.
    - same key (jobId + stageNum) with different stage target size: dedupe logic (ignore same target size && same worker request size. Replace old request with new one if target size changed).
    - reservation class should be immutable. Replace exisitng if needed.
    - internal loop to process reservation queue: timer + ESM signal (need to be mini-batch style to avoid too many trigger from ESM).
    - abstract and config to support different priority behavior. Default strategy setup to use epoch time on creation + override signal (e.g. priorityEpoch as 0)
    - main processing logic: when reservation registry try to process the active reservations, it should go through the priority (so it honors the priority). Start with top of the queue of each "size" or "sku", check if the reservation can be fulfilled (matched) from executor state manager. Once matched, invoke resource cluster actor to do the actual assignement to TEs and finish (remove) the reservation in registry. If the target reservation cannot be matched, stop the process on this queue. Proceed to next reservation if the top one is successfully filled, otherwise stop (only top of the queue shall be processed). Error in assigning TEs tasks should be properly handled such that it moves the reservation to the tail of the queue (re-assigned priority).
    - (optional) new reservation trigger resource cluster scaler process.

- AssignmentTrackerActor: tracks and manages actual worker assignments
    - Host similar funcationality as onAssignedScheduleRequestEvent(AssignedScheduleRequestEvent event) in ResourceClusterAwareSchedulerActor; Since this is now a child actor in resourceClusterActor it doesn't need to go throgh resource cluster interface to get the connections etc. but can directly ask the parent actor for the needed objects.
    - track ongoing IO tasks to each TE and handle retry similar to logic in scheduler actor.
    - assignment tasks can be cancelled due to job kill and worker replacement.
