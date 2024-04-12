## Is Mantis just another stream processing engine like Flink or Spark ?

Mantis can be thought of as a stream processing engine + a container cloud for execution.

Mantis jobs do not run in isolation but are part of an ecosystem of jobs. This allows jobs to collaborate
with other jobs to form complex streaming microservices system. Allowing jobs to communicate with other jobs
promotes code and data reuse thus improving efficiency and productivity as users do not have to `re-invent
the wheel`.

Additionally, Mantis jobs can, with the help of the mantis-publish library, source data directly from production systems
in an on-demand fashion. This provides a cost effective option to access rich operational data which otherwise would
be prohibitive to transport and store (Imagine having to transport and store request and response bodies for every single
request)

So the answer is a qualified `yes` but it is much more than a stream processing engine.   

## Does Mantis process events in micro batches or one at a time ?

By default Mantis Jobs process events one at a time. Users can however create their own micro batches
using ReactiveX operators.

## What kind of message guarantees does Mantis support ?

The default delivery semantic is at-most once. This is intentional as the majority of the use cases on Mantis
are latency sensitive (think alerting.) where determining an aggregate trend in subsecond latency is preferred to
data completeness albeit with potential delay. 

In cases where the source of data is Kafka, Mantis supports at-least once semantics.

The exactly-once or effectively once semantic support is currently not available in Open Source.

## Do Mantis Jobs operate in event time or clock time ?

By default Mantis jobs operate with clock time. 

## How does Mantis deal with backpressure ?

Mantis relies on the [backpressure](https://github.com/ReactiveX/RxJava/wiki/Backpressure) support built-in to RxJava.
This ensures an upstream operator in the execution DAG will not send more events down until the downstream 
consumer is ready. 

Thus backpressure bubbles up along the DAG to the edge where data is being fetched from across the network.
At this point, the strategy depends on whether the external source of data is `cold` source like a queue or 
database or a `hot` source like data sent via [mantis-publish].
 
For [cold] sources like Kafka no more events are read until the previous batch of events have been acknowledged.
For [hot] sources events are first buffered and then as a last resort dropped.

One effective way to deal with backpressure in Mantis, is to enable [autoscale] on the job with a strategy to scale up on backpressure. 
 
## Is it possible to use the second [stage] of a three-stage [Mantis Job] as the [source job] for another [MQL] job?

Jobs connect to the [sink] stage of other jobs by default, so while it may be technically possible
to rig up a solution where you connect to an intermediate stage instead, this is not supported as
an out-of-the-box solution. You may instead want to reorganize your original job, perhaps splitting
it into two jobs.

<!-- Do not edit below this line -->
