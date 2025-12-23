# Scheduling Logic and Resource Cluster Interactions

This document explains how job creation and job scaling translate into scheduling and resource changes, and how those changes interact with the Resource Cluster and the Resource Cluster Scaler.

## A) Job Creation → Scheduling → Task Execution

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant JCM as JobClustersManagerActor
    participant JC as JobClusterActor
    participant JA as JobActor
    participant MS as MantisScheduler
    participant RCSA as ResourceClusterAwareSchedulerActor
    participant RC as ResourceClusterActor
    participant TE as Task Executor

    Client->>JCM: SubmitJobRequest
    JCM->>JC: forward(request)
    JC->>JA: InitJob (initialize workers)
    JA->>MS: scheduleWorkers(BatchScheduleRequest)
    MS->>RCSA: BatchScheduleRequestEvent
    RCSA->>RC: getTaskExecutorsFor(allocationRequests)
    RC-->>RCSA: allocations (TaskExecutorID -> AllocationRequest)
    RCSA->>TE: submitTask(executeStageRequestFactory.of(...))
    TE-->>RCSA: Ack / Failure
    RCSA->>JA: WorkerLaunched / WorkerLaunchFailed
    Note over RC: Inventory & matching by ExecutorStateManagerImpl
```

Key notes:
- Job submission enters at `JobClustersManagerActor.onJobSubmit`, forwarded to `JobClusterActor`, then `JobActor.initialize` constructs workers.
- `JobActor` enqueues workers via batch scheduling to the resource-cluster-aware scheduler.
- `ResourceClusterAwareSchedulerActor` queries `ResourceClusterActor` for best-fit executors and pushes tasks to TEs via `TaskExecutorGateway`.

## B) Job Stage Scale-Up/Down and Resource Cluster Scaler Loop

```mermaid
sequenceDiagram
    autonumber
    participant JA as JobActor
    participant WM as WorkerManager
    participant MS as MantisScheduler
    participant RCSA as ResourceClusterAwareSchedulerActor
    participant RC as ResourceClusterActor
    participant RCS as ResourceClusterScalerActor
    participant Host as ResourceClusterHostActor
    participant TE as Task Executor

    JA->>JA: onScaleStage(StageScalingPolicy, ScalerRule min/max)
    JA->>WM: scaleStage(newNumWorkers)
    alt Scale Up
        WM->>MS: scheduleWorkers(BatchScheduleRequest)
        MS->>RCSA: BatchScheduleRequestEvent
        RCSA->>RC: getTaskExecutorsFor(...)
        RC-->>RCSA: allocations or NoResourceAvailable
        RCSA->>TE: submitTask(...)
        note over RC: pendingJobRequests updated when not enough TEs
    else Scale Down
        WM->>RC: unscheduleAndTerminateWorker(...)
    end

    loop Periodic Capacity Loop (every scalerPullThreshold)
        RCS->>RC: GetClusterUsageRequest
        RC-->>RCS: GetClusterUsageResponse (idle/total by SKU)
        RCS->>RCS: apply rules (minIdleToKeep, maxIdleToKeep, cooldown)
        alt ScaleUp decision
            RCS->>Host: ScaleResourceRequest(desired size increase)
            Host-->>RC: New TEs provisioned
            TE->>RC: TaskExecutorRegistration + Heartbeats
            RC->>RC: mark available (unless disabled)
        else ScaleDown decision
            RCS->>RC: GetClusterIdleInstancesRequest
            RC-->>RCS: idle TE list
            RCS->>Host: ScaleResourceRequest(desired size decrease, idleInstances)
            RCS->>RC: DisableTaskExecutorsRequest(idle targets during drain)
        end
    end
```

Key notes:
- Job scale-up adds workers; if TEs are insufficient, the scheduler records demand (pending) so the scaler sees fewer effective idle slots and scales up capacity.
- Scale-down removes workers and may free idle TEs; the scaler can shrink capacity by selecting idle instances to drain and disabling them during deprovisioning.

## Code Reference Map

- Submission & job initialization:
  - `JobClustersManagerActor.onJobSubmit(...)`
  - `JobActor.onJobInitialize(...)`, `initialize(...)`, `submitInitialWorkers(...)`, `queueTasks(...)`

- Scheduling (push model):
  - `ResourceClusterAwareSchedulerActor.onBatchScheduleRequestEvent(...)`, `onAssignedScheduleRequestEvent(...)`
  - `ResourceClusterActor` + `ExecutorStateManagerImpl.findBestFit(...)`, `findBestGroupBySizeNameMatch(...)`, `findBestGroupByFitnessCalculator(...)`

- Job scaling:
  - `JobActor.onScaleStage(...)` → `WorkerManager.scaleStage(...)`

- Resource-cluster autoscaling:
  - `ResourceClusterScalerActor.onTriggerClusterUsageRequest(...)`, `onGetClusterUsageResponse(...)`, `onGetClusterIdleInstancesResponse(...)`
  - `ExecutorStateManagerImpl.getClusterUsage(...)` (idle = available − pending), `pendingJobRequests`
  - `ResourceClusterActor.onTaskExecutorRegistration(...)` (TE joins, becomes available)

## Coupling Between Scheduling and Scaling

- When allocations fail due to lack of capacity, the resource cluster records pending demand per job/group; the scaler subtracts this pending from available to compute effective idle, triggering scale-up.
- New TEs register with the resource cluster and are immediately considered for subsequent scheduling waves.
