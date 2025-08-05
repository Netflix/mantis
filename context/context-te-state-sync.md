# Problem
We recently hit a weird case that seems like a potential bug, or maybe a limitation.

We have a 2 stage job, where for a short time all stage 2 workers were pulling events from what they thought was a stage 1 worker, but ended up being a worker for an unrelated job.

As far as I can tell from the logs and code, it appears that:

A stage 1 worker, say A-worker-1 on task executor TE-1 fails hard (looks like a JVM segfault)
A minute or so later, TE-1 re-registers itself and is marked available
A minute later B-worker-2 is mapped to TE-1
At this point Job A still has A-worker-1 mapped to TE-1 in its scheduling info, so workers in stage 2 of A are pulling events from B-worker-2
This persists for 2 more minutes, when A-worker-1 hits a heartbeat timeout and is failed by the JobActor.
The main issue: Even though TE-1 is bound to A-worker-1 according to the JobActor, when it fails hard and comes back up, it is re-registered as available. I would expect there to be state knowing that TE-1 should not be marked as available.


# Extra info
I think i understand the problem now and it is a definite a bug of out of sync states between resource cluster tracking and job actors (this is not the first bug we found/fixed on this topic).
For our use cases we DO recycle containers in the resource clusters to avoid waiting for full container provision so we need the capability to mark a previous assigned container back to available pool.
I can think of two potential options here:

1. broadcast a message to job actors when a previously used TE is re-marked as available so that any existing job actor schedulingInfo can be refreshed. (Perf/load is a concern here though given the potential broadcast blast radius).
2. Mark the TE available only after default heartbeat timeout expires. Less efficient but probably a good enough brute force fix for the short term.

# Solution Analysis and Implementation

## Root Cause Analysis
The issue occurs due to state synchronization problems between two key components:
- **ResourceClusterActor**: Tracks TaskExecutor availability and immediately marks TEs as available upon re-registration
- **JobActor**: Maintains scheduling info and periodically checks heartbeats to clean up failed workers

**Race Condition**: When TE-1 fails hard and reconnects, ResourceClusterActor marks it available immediately, but JobActor still has stale scheduling info pointing stage 2 workers to TE-1.

## Implemented Solution: Selective WorkerEvent Notification (Solution 1A)

### Architecture
**Message Flow**: ResourceClusterActor → JobMessageRouter → JobClusterActor → JobActor
**Event Type**: New `TaskExecutorReconnectedEvent` extends `WorkerEvent`
**Targeting**: Uses existing WorkerId routing to affect only the specific job that had a worker on the reconnected TE

### Key Components

#### 1. TaskExecutorReconnectedEvent
```java
// New WorkerEvent type indicating TE reconnection
public class TaskExecutorReconnectedEvent implements WorkerEvent {
    private final WorkerId previousWorkerId;
    private final TaskExecutorID taskExecutorID;
    // Provides targeted notification to affected job
}
```

#### 2. TaskExecutorState Tracking
```java
// Added to TaskExecutorState.java
@Nullable
private WorkerId previousWorkerId;

// Store previous WorkerId on disconnection
boolean onDisconnection() {
    previousWorkerId = getWorkerId();
    // ... existing logic
}
```

#### 3. ResourceClusterActor Notification Logic
```java
// In onTaskExecutorRegistration()
WorkerId previousWorkerId = state.getPreviousWorkerId();
if (stateChange && previousWorkerId != null) {
    log.info("Task executor {} reconnected, was previously running worker {}. Notifying job for scheduling refresh.",
             taskExecutorID, previousWorkerId);
    jobMessageRouter.routeWorkerEvent(new TaskExecutorReconnectedEvent(previousWorkerId, taskExecutorID));
    state.clearPreviousWorkerId();
}
```

#### 4. JobActor Event Handler
```java
// In processEvent()
if (event instanceof TaskExecutorReconnectedEvent reconnectEvent) {
    LOGGER.info("Received TaskExecutorReconnectedEvent for worker {} on executor {}. " +
                "Refreshing stage assignments to prevent stale TE mappings.",
                reconnectEvent.getWorkerId(), reconnectEvent.getTaskExecutorID());
    
    // Force refresh of stage assignments to update stale TaskExecutor mappings
    markStageAssignmentsChanged(true);
    refreshStageAssignmentsAndPush();
    return;
}
```

### Performance Analysis

| Solution | Messages Sent | Routing Efficiency | Implementation Complexity |
|----------|---------------|-------------------|-------------------------|
| **1A: Targeted** | 1 per affected job | Highest | Low |
| **1B: JobCluster Broadcast** | 1 per JobCluster | Medium | Low |
| **1C: Reverse Lookup** | N per affected jobs | High | Medium |
| **Naive Broadcast** | All active jobs | Lowest | Low |

### Implementation Benefits

- **✅ Minimal Performance Impact**: Only 1 message per affected job vs broadcast to all jobs
- **✅ Immediate Resolution**: No waiting period - fixes race condition instantly  
- **✅ Leverages Existing Infrastructure**: Uses established JobMessageRouter and WorkerEvent patterns
- **✅ Surgical Precision**: Only affects jobs that actually had workers on the reconnected TE
- **✅ Simple Implementation**: Low complexity, follows existing code patterns

### Files Modified/Created

1. **NEW**: `TaskExecutorReconnectedEvent.java` - New WorkerEvent type for targeted notifications
2. **MODIFIED**: `TaskExecutorState.java` - Added previousWorkerId tracking and methods
3. **MODIFIED**: `ResourceClusterActor.java` - Added reconnection notification logic  
4. **MODIFIED**: `JobActor.java` - Added event handler for scheduling info refresh
5. **NEW**: `TaskExecutorReconnectedEventTest.java` - Unit tests for verification

### Verification Results
- ✅ Code compiles successfully with only deprecation warnings
- ✅ Unit tests pass 
- ✅ Follows existing code patterns and conventions
- ✅ Minimal changes to existing codebase

This implementation provides an efficient, targeted solution to the TaskExecutor state synchronization problem while maintaining system performance and leveraging existing infrastructure.
