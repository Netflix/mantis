# Mantis Connection Mechanisms

This document captures the connection mechanisms used in Mantis for both stage-to-stage connections within jobs and job-to-job connections across different jobs.

## Stage-to-Stage Connections (Intra-Job)

### Overview
Stage-to-stage connections handle data flow between processing stages within the same Mantis job (e.g., Stage 1 → Stage 2).

### Key Components

#### 1. Worker Discovery API
- **Endpoint**: `GET /assignmentresults/{jobId}?sendHB=true`
- **Protocol**: Server-Sent Events (SSE)
- **Response**: Stream of `JobSchedulingInfo` objects
- **File**: `mantis-control-plane-client/MantisMasterClientApi.java:697-742`

#### 2. Connection Management
- **DynamicConnectionSet** (`mantis-remote-observable/DynamicConnectionSet.java:180-266`)
  - Manages adding/removing worker connections dynamically
  - Handles worker scaling events in real-time
  - Provides load balancing with multiple connections per endpoint

- **Reconciliator** (`mantis-remote-observable/reconciliator/Reconciliator.java:154-183`)
  - Ensures expected connections match actual connections
  - Handles connection drift and failures
  - Automatically reconnects with backoff logic

#### 3. Worker Execution
- **WorkerConsumerRemoteObservable** (`mantis-runtime/WorkerConsumerRemoteObservable.java:58-96`)
  - Creates connections based on stage type (Keyed vs Scalar)
  - Handles different connection patterns for different stage configurations

### Connection Flow
1. **Discovery**: Stage 2 workers subscribe to SSE stream for `JobSchedulingInfo`
2. **Endpoint Resolution**: Extract Stage 1 worker endpoints (host, port, state)
3. **Connection Creation**: Create multiple connections per endpoint for load balancing
4. **Dynamic Updates**: Add/remove connections as Stage 1 workers scale
5. **Reconciliation**: Continuously verify expected vs actual connections

### Data Structures
- **JobSchedulingInfo**: Contains worker assignment mapping per stage
- **WorkerAssignments**: Maps stage numbers to worker host information
- **WorkerHost**: Individual worker endpoint details (host, port, state)
- **Endpoint**: Connection endpoint with unique identifier

## Job-to-Job Connections (Inter-Job)

### Overview
Job-to-job connections enable downstream jobs to consume data from upstream jobs' output streams, creating multi-job processing pipelines.

### Key Components

#### 1. Job Discovery APIs
- **Job Cluster Resolution**: `GET /namedjobs/{jobCluster}` → current job ID
- **Job Scheduling**: `GET /assignmentresults/{jobId}?sendHB=true` → job worker info
- **Sink Discovery**: `getSinkStageNum(jobId)` → sink stage number
- **Worker Locations**: `getSinkLocations(jobId, sinkStage)` → worker endpoints

#### 2. Connection Client
- **JobSource** (`mantis-connector-job-source/JobSource.java`)
  - Primary connector for job-to-job connections
  - Uses `TargetInfo` configuration for connection parameters

- **MantisSSEJob**
  - SSE client for consuming upstream job data
  - Handles connection lifecycle and reconnection

#### 3. Configuration
```java
JobSource.TargetInfo targetInfo = new TargetInfoBuilder()
    .withSourceJobName("UpstreamJobName")    // Target job name
    .withQuery("select * from stream")       // MQL query filter
    .withClientId("unique-client-id")        // Connection identifier
    .withSamplePerSec(100)                   // Sampling rate
    .withBroadcastMode(false)                // Connection mode
    .build();
```

### Connection Flow
1. **Job Resolution**: Job name → `/namedjobs/{jobCluster}` → current job ID
2. **Sink Discovery**: Job ID → `getSinkStageNum(jobId)` → sink stage number
3. **Worker Discovery**: Job ID + stage → `getSinkLocations()` → worker endpoints
4. **SSE Connection**: Direct HTTP connection to upstream job's sink workers
5. **Data Consumption**: Stream data via SSE with optional MQL filtering

## Key Differences

| Aspect | Stage-to-Stage | Job-to-Job |
|--------|----------------|------------|
| **Scope** | Within same job | Across different jobs |
| **Discovery API** | JobSchedulingInfo stream | Job Discovery APIs |
| **Target** | Next stage workers | Sink stage of upstream job |
| **Protocol** | TCP/RxNetty | SSE over HTTP |
| **Addressing** | Stage number | Job name/cluster |
| **Configuration** | Stage config in job definition | TargetInfo with MQL queries |
| **Connection Management** | DynamicConnectionSet + Reconciliator | MantisSSEJob client |
| **Load Balancing** | Multiple connections per endpoint | SSE client-side balancing |

## Architecture Benefits

### Stage-to-Stage
- **Low latency**: Direct TCP connections between co-located workers
- **High throughput**: Multiple connections with custom networking
- **Dynamic scaling**: Real-time connection updates via SSE scheduling stream
- **Fault tolerance**: Automatic reconnection with backoff

### Job-to-Job
- **Isolation**: Jobs remain independent with loose coupling
- **Flexibility**: MQL queries enable data filtering and transformation
- **Standardization**: HTTP/SSE provides standard protocol
- **Discoverability**: Job names provide logical addressing

## Implementation Files

### Stage-to-Stage Core Files
- `mantis-runtime-executor/WorkerExecutionOperationsNetworkStage.java:656-697`
- `mantis-remote-observable/DynamicConnectionSet.java:180-266`
- `mantis-remote-observable/reconciliator/Reconciliator.java:154-183`
- `mantis-control-plane-client/MantisMasterClientApi.java:697-742`

### Job-to-Job Core Files
- `mantis-connectors/mantis-connector-job-source/src/main/java/io/mantisrx/connector/job/source/JobSource.java`
- `mantis-client/src/main/java/io/mantisrx/client/MantisSSEJob.java`
- `mantis-client/src/main/java/io/mantisrx/client/MantisClient.java`

## Worker Readiness and Scaling Gap Mitigation

### Problem Statement
During job scaling operations, there is a timing gap between when new workers appear in the JobSchedulingInfo stream and when they are actually ready to accept connections. This causes downstream consumers to attempt connections to unavailable workers, leading to:
- Connection failures and retries
- Increased latency during scaling events
- Resource waste from failed connection attempts
- Potential cascading failures in large deployments
