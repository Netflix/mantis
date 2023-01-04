## Introduction
This directory contains code that forms the core of mantis with logic for specifying a mantis job,
how various operators are stored in stages, how stages communicate with each other, parameters to
configure jobs...

## Package `io.mantisrx.runtime.core`
This is a relatively new package built to act as the new API for writing new mantis jobs. It contains
interfaces and implementations for a new mantis domain specific language (DSL) that is inspired by
stream processing APIs from other engines like apache-flink.

The idea behind the new API is to model a mantis job (stream processing pipeline) as a
directed-acyclic -graph^1 (DAG) similar to many other stream processing engines and also abstract
the physical layout of the job from the processing logic.

An important consideration is removing RxJava `Observable` interface which is heavily coupled to
writing a mantis job. This coupling made it difficult to update the RxJava version 1.0 to latest.

#### Directed Acyclic Graph (DAG)
The entire data processing pipeline is modeled as a DAG where the `nodes` correspond to a data stream
`MantisStream`. Transforms/Operators^2 like `map`, `filter`, `flatmap` are directed `edges` in the graph.
This produces a new `MantisStream` `node`. The nodes take configurations like node name, serializers,
stage and worker parallelism, operator name.

Modeling the entire job as a graph first allows for flexibility in the API and hopefully prevents
costly refactors in the future. We believe an added benefit is a simplified and consistent mental
model for a `MantisStream`. This is then converted to a existing implementation of `MantisJob` to
be passed on. The physical layout is decided now and the transforms chained together unless there
is a `materialize`.

^1 -- barring a few operators that are implemented as self-loops.
^2 -- used interchangeably with operators.
