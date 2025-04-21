# Mantis Control Plane Core

This module contains core components for the Mantis Control Plane, including the Flink RPC system integration.

## Flink RPC Dependencies

This module directly depends on the Flink RPC libraries:
- `org.apache.flink:flink-rpc-core` (API dependency)
- `org.apache.flink:flink-rpc-akka` (compileOnly dependency, provided by copyLibs task)

## Dependency Graph

The following diagram shows how `org.apache.flink:flink-rpc-akka` and `org.apache.flink:flink-rpc-core` are used in the Mantis project and what modules depend on them:

```mermaid
graph TD
    flink-rpc-core["org.apache.flink:flink-rpc-core"]
    flink-rpc-akka["org.apache.flink:flink-rpc-akka"]

    mantis-control-plane-core --> flink-rpc-core
    mantis-control-plane-core -.-> flink-rpc-akka

    mantis-server-worker-client --> mantis-control-plane-core
    mantis-connector-publish --> mantis-control-plane-core
    mantis-control-plane-client --> mantis-control-plane-core
    mantis-connector-job-source --> mantis-control-plane-core

    mantis-control-plane-server --> mantis-control-plane-core
    mantis-server-agent --> mantis-control-plane-core

    classDef direct fill:#f9f,stroke:#333,stroke-width:2px;
    classDef indirect fill:#bbf,stroke:#333,stroke-width:1px;

    class flink-rpc-core,flink-rpc-akka direct;
    class mantis-control-plane-core direct;
    class mantis-server-worker-client,mantis-connector-publish,mantis-control-plane-client,mantis-connector-job-source,mantis-control-plane-server,mantis-server-agent indirect;
```

Note: The dotted line from mantis-control-plane-core to flink-rpc-akka represents a runtime dependency rather than a direct compile-time dependency. The flink-rpc-core module does not directly depend on Akka or Scala libraries, while flink-rpc-akka does.

## Key Classes and Their Relationships

```mermaid
classDiagram
    class RpcGateway {
        <<interface>>
    }
    class RpcEndpoint {
        <<abstract>>
    }
    class TaskExecutorGateway {
        <<interface>>
        +submitTask(ExecuteStageRequest) CompletableFuture~Ack~
        +cacheJobArtifacts(CacheJobArtifactsRequest) CompletableFuture~Ack~
        +cancelTask(WorkerId) CompletableFuture~Ack~
        +requestThreadDump() CompletableFuture~String~
        +isRegistered() CompletableFuture~Boolean~
    }
    class TaskExecutor {
        -TaskExecutorID taskExecutorID
        -ClassLoaderHandle classLoaderHandle
        -TaskExecutorRegistration taskExecutorRegistration
        +start()
        +awaitRunning() CompletableFuture~Void~
        +closeAsync() CompletableFuture~Void~
    }
    class ResourceClusterGateway {
        <<interface>>
        +registerTaskExecutor(TaskExecutorRegistration) CompletableFuture~Ack~
        +heartBeatFromTaskExecutor(TaskExecutorHeartbeat) CompletableFuture~Ack~
        +notifyTaskExecutorStatusChange(TaskExecutorStatusChange) CompletableFuture~Ack~
        +disconnectTaskExecutor(TaskExecutorDisconnection) CompletableFuture~Ack~
    }
    class ResourceClusterGatewayAkkaImpl {
        -ActorRef resourceClusterManagerActor
        -Duration askTimeout
        +registerTaskExecutor(TaskExecutorRegistration) CompletableFuture~Ack~
        +heartBeatFromTaskExecutor(TaskExecutorHeartbeat) CompletableFuture~Ack~
        +notifyTaskExecutorStatusChange(TaskExecutorStatusChange) CompletableFuture~Ack~
        +disconnectTaskExecutor(TaskExecutorDisconnection) CompletableFuture~Ack~
    }

    RpcGateway <|-- TaskExecutorGateway
    RpcEndpoint <|-- TaskExecutor
    TaskExecutorGateway <|.. TaskExecutor
    ResourceClusterGateway <|.. ResourceClusterGatewayAkkaImpl
```

## Usage Pattern

The Flink RPC system is used in Mantis for communication between components, particularly for task execution:

1. **RPC System Loading**:
   - `MantisAkkaRpcSystemLoader` loads the Akka RPC implementation
   - `CleanupOnCloseRpcSystem` wraps the RPC system to clean up resources when closed

2. **Task Executor Communication**:
   - `TaskExecutorGateway` interface extends Flink's `RpcGateway` for task executor communication
   - `TaskExecutor` class implements `TaskExecutorGateway` and extends `RpcEndpoint`

3. **Resource Cluster Management**:
   - `ResourceClusterAkkaImpl` and `ResourceClusterGatewayAkkaImpl` use Akka actors but interact with the Flink RPC system
   - They provide methods to get `TaskExecutorGateway` instances for communication with task executors
