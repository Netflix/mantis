The Mantis Runtime is consists of two components:

1. A single **Mantis Master** which coordinates the execution of Mantis Jobs.
1. Independent **Mantis Jobs** which receive streams of events as input, transform events one at a
   time, and produce streams of events as output.

This page assumes familiarity with Mantis Job high-level concepts. An introduction can be found in
[Writing Mantis Jobs](../../developing/writingjobs). This page presents internal details for Mantis Jobs.

## Mantis Job Components
A Mantis Job consists of three components. Each one is based on a cold Observable that emits
events to the next Observer in the Observable chain:

1. **Source**

    The Source component is an `RxFunction` that consumes data in a streaming, non-blocking,
    backpressure-aware manner from an external service.

1. **Processing Stage**

    A Processing Stage component is based on an `RxFunction`. This is where
    event transformations take place. There can be many Processing Stages in a Mantis Job.

1. **Sink**

    The Sink component is based on an `RxAction`. It asynchronously emits results of the
    final Processing Stage to an external service.

!!! note
    Mantis Jobs can consume events from typical external services such as APIs, databases, and
    Kafka topics. Mantis Jobs can also consume events emitted by other Mantis Jobs. This is
    referred to in Mantis as **job chaining**.

## Runtime Lifecycle
The entry point for a Mantis Job is the Mantis Worker. The Mantis Master starts three primary
services on a Mantis Worker when the Master boots the Worker up:

1. The **virtual machine worker** service interacts with the underlying substrate, currently
   Mesos. This service subscribes to task updates and registers the Mantis Worker with Mesos
   executor callbacks to launch Mantis Jobs.

1. The **heartbeat service** sends HTTP heartbeat requests to notify the Mantis Master that the
   worker is alive and available to process events.

1. The **stage executor** dynamically loads bytecode for a Mantis Job, creates an in-memory
   representation of all the metadata required to execute events for that Mantis Job, and
   processes events for the current Processing Stage.

### Job Master Stage
The Job Master autoscales Processing Stages. It can autoscale such stages independently of each
other. If the configuration of a Job indicates that any Processing Stage is autoscalable, Mantis
will automatically add a Job Master as the initial processing stage of the Job. This is a hidden
stage that Job owners do not explicitly manage; instead, Mantis will create and configure a
`JobMasterService`. This service creates a subscription to worker metrics via the
`WorkerMetricHandler` and a `MetricsClient` which receives metrics over HTTP via SSE and sends
them over to the `JobAutoScaler`.

#### Job Autoscaler
The Job autoscaler is based on a [PID controller](https://en.wikipedia.org/wiki/PID_controller).
Within this autoscaler are three controllers for CPU, memory, and network resources which
continuously calculate an error value and apply corrections. Once the autoscaler makes a prediction,
it delegates an API call to the Mantis Master to perform the scaling action on resources for a
Processing stage.

### Single-Stage and Multi-Stage Jobs
A Job with only one Processing Stage is a single-stage Job. In such a case, the entire Job (Source,
Processing Stage, and Sink) will execute on the current worker node.

A Job with more than one Processing Stage is a multi-stage Job. In such a Job, the stage executor
will first inspect the current component. If the current component is a Source, then the executor
will execute it as a Source. Otherwise, it will inspect the context again to determine if current
component is a Sink. If so, it will acquire a port and create a `SinkPublisher` to publish events to
the next Job. Finally, if the component is a normal Processing Stage, then the executor will execute
its transformations.
