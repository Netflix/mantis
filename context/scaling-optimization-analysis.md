# Mantis Scaling Impact Optimization Analysis

## Overview

This document captures the analysis and proposed solutions for improving Mantis job scaling impact, focusing on minimizing downstream disruption and eliminating connection timing gaps during scaling events.

## Current Problems Identified

### 1. Worker State Filtering Gap (JobActor.java:1622)

**Current Implementation:**
```java
if (WorkerState.isRunningState(wm.getState())) {
    // Includes: Launched, StartInitiated, Started
    workerHosts.put(wm.getWorkerNumber(), new WorkerHost(...));
}
```

**Issue:** Workers in `Launched` and `StartInitiated` states are included in JobSchedulingInfo but not ready for connections, causing:
- Connection failures during worker startup (15-30 second window)
- Unnecessary load on starting workers
- Downstream job instability

**Solution:** Filter to only `Started` workers
```java
if (wm.getState().equals(WorkerState.Started)) {
    // Only fully ready workers
    workerHosts.put(wm.getWorkerNumber(), new WorkerHost(...));
}
```
**Problem**
Only returning started workers will cause a few side effects:
* less mini batching -> more update frequency
* for workers in launched state they must ensure to trigger the scheduling info updates when they transition into started state.


### 2. Scaling Update Storm

**Current Behavior:** When scaling out 200 workers:
```
Worker 1: StartInitiated → Started → markStageAssignmentsChanged() → Update #1
Worker 2: StartInitiated → Started → markStageAssignmentsChanged() → Update #2
...
Worker 200: ... → Update #200
```

**Result:** Up to 200 separate JobSchedulingInfo updates hammering downstream jobs

**Impact:**
- Downstream job overload during scaling events
- Connection establishment thundering herd
- System instability during critical scaling periods

### 3. Connection Timing Asymmetry

**Problem:** Workers start receiving upstream data before downstream jobs can connect:
```
T=0s:  Worker Started → receives upstream data
T=0s:  JobSchedulingInfo broadcast begins
T=1-5s: Downstream jobs attempt connections
Gap:   Worker processes data but downstream can't connect
```

## Proposed Solutions

### Solution 1: Scaling-Aware Smart Batching

**Implementation:** Detect scaling events and batch updates appropriately

```java
class ScalingAwareBatcher {
    private final AtomicInteger pendingWorkerTransitions = new AtomicInteger(0);
    private volatile boolean isInScalingMode = false;

    public void markStageAssignmentsChanged(boolean forceRefresh, String reason) {
        pendingWorkerTransitions.incrementAndGet();

        if (!forceRefresh && shouldEnterScalingMode()) {
            isInScalingMode = true;
            scheduleScalingBatch(); // 2-5 second delay based on scale
        } else {
            // Normal operation: immediate or regular batching
            refreshStageAssignmentsAndPush();
        }
    }

    private boolean shouldEnterScalingMode() {
        return pendingWorkerTransitions.get() > 5; // More than 5 workers transitioning
    }
}
```

**Benefits:**
- 200 worker scaling: 200 updates → 1-3 batched updates
- Adaptive delay based on scaling intensity
- Maintains responsiveness during normal operations

### Solution 2: Worker Readiness Grace Period

**Implementation:** Add buffer time between worker Started state and downstream availability

```java
private boolean isWorkerReadyForDownstream(IMantisWorkerMetadata worker) {
    long gracePeriodMs = 1000; // 1 second grace period
    return (System.currentTimeMillis() - worker.getStartedAt()) >= gracePeriodMs;
}
```

**Benefits:**
- Ensures workers are truly ready before downstream connections
- Reduces failed connection attempts
- Minimizes upstream/downstream timing gaps

### Solution 3: Connection Jitter (DynamicConnectionSet)

**Current Problem:** All downstream jobs attempt connections simultaneously when receiving JobSchedulingInfo updates

**Implementation:** Add randomized delay to connection establishment

```java
// In DynamicConnectionSet.java connection creation
int jitterDelayMs = random.nextInt(5000); // 0-5 second jitter
return Observable.timer(jitterDelayMs, TimeUnit.MILLISECONDS)
    .flatMap(tick -> {
        // Create actual connection after jitter delay
        RemoteRxConnection<T> connection = toObservableFunc.call(endpoint, ...);
        return connection.getObservable();
    });
```

**Benefits:**
- Spreads connection load over time
- Prevents worker overload during scaling
- Maintains overall throughput while reducing peaks

## Implementation Strategy

### Phase 1: Immediate (Low Risk)
1. **Started-state filtering** - Single line change in JobActor.java:1622
2. **Connection jitter** - Add randomization to DynamicConnectionSet

### Phase 2: Short-term (Medium Risk)
1. **Scaling detection batching** - Smart batching based on worker transition rate
2. **Worker readiness grace period** - 1-second buffer for downstream readiness

### Phase 3: Long-term (Enhanced)
1. **Configuration-driven optimization** - Tunable parameters for different environments
2. **Advanced batching strategies** - Threshold-based and adaptive batching

## Configuration Options

```java
// Scaling detection
mantis.worker.scaling.transition.threshold=5
mantis.worker.scaling.detection.window.ms=10000
mantis.worker.scaling.batch.delay.ms=2000

// Worker readiness
mantis.worker.downstream.readiness.grace.ms=1000

// Connection behavior
mantis.connection.establishment.jitter.max.ms=5000

// Update frequency
mantis.worker.update.significant.change.threshold=5
```

## Risk Assessment

### Low Risk Changes
- **Started-state filtering**: Single line change, fail-safe direction
- **Connection jitter**: Delays connections but doesn't change logic
- **Grace period**: Ensures readiness rather than assuming it

### Medium Risk Changes
- **Batching logic**: Changes update frequency, needs careful configuration
- **State transition handling**: Affects JobSchedulingInfo broadcasting timing

### Mitigation Strategies
1. **Feature flags** for gradual rollout
2. **Extensive monitoring** of update rates and connection patterns
3. **Fallback mechanisms** to existing behavior
4. **Configuration-driven** approach for environment-specific tuning

## Expected Impact

### Before Optimization
- **Connection failures**: 15-30 seconds during worker startup
- **Scaling updates**: 200 individual updates for 200 worker scaling
- **System load**: Thundering herd during scaling events

### After Optimization
- **Connection failures**: Eliminated through proper state filtering
- **Scaling updates**: 1-3 batched updates for 200 worker scaling
- **System load**: Distributed over time through jitter and batching

## Monitoring and Observability

### Key Metrics to Track
1. **Worker state transition rates** - Detect scaling events
2. **JobSchedulingInfo update frequency** - Monitor batching effectiveness
3. **Connection establishment success rates** - Track connection health
4. **Downstream job latency** - Measure scaling impact on consumers

### Alerting Thresholds
- JobSchedulingInfo update rate > 10/minute (potential batching issue)
- Connection failure rate > 5% during scaling (readiness gap)
- Worker state transition rate > 50/minute (scaling event detection)

## Conclusion

The proposed optimizations transform problematic scaling behavior from:
- **200 individual updates causing downstream overload**
- **30-second connection failure windows**
- **Simultaneous connection thundering herds**

To:
- **1-3 batched updates with controlled timing**
- **Immediate connection success through proper filtering**
- **Distributed connection establishment through jitter**

These changes maintain system simplicity while dramatically improving scaling behavior, following the guiding principles of "simpler is better" and "don't introduce cascading failure risk."


## Updated Proposed Solutions Notes
Based on the analysis so far, i am proposing a solution as below:
- In jobActor only return started workers to update job scheduling info.
- introduce a mechanism to reduce the interruptions on frequent scheduling info updates.
- to decide whether to update the scheduling info, job actor should check the current state including how many in accepted/startInitiated/lauched then apply some logic to best batch the updates with delay to reduce needed update frequency.
