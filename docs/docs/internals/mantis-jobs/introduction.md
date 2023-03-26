A [Mantis Job] is a JVM-based stream processing application that takes in an [Observable] stream of data items, [transforms] this stream by using [RxJava] operators, and then outputs the results as another Observable stream.

[RxJava] Observables can be visualized by using “marble diagrams”:

![RxJava Observables can be represented by “marble diagrams”](../../images/observables.svg)

You can combine multiple RxJava operators to transform an Observable stream of items in many ways:

![An incoming stream is processed by a Mantis Job composed of the operators map and merge, to compose an output stream](../../images/complex.svg)

The above diagram shows a Mantis Job composed of two operators that process an input stream to compose an output stream. The first operator, `map`, emits a new Observable for each item emitted by the source Observable. The second operator, `merge`, emits each item emitted by those Observables as a fresh Observable stream.

There is an enormous wealth of ways in which you can transform streams of data by using RxJava Observable operators.

If the volume of data to be processed is too large for a single [worker] to handle, you can “divide and conquer” by grouping and dividing the operators across various [processing stages], as in the following diagram:

![An incoming stream is processed by a Mantis Job composed of three stages, one composed of the groupBy operator, the second by the window and reduce operators, and the third composed of the merge operator, together resulting in an output stream](../../images/stages.svg)

## Mantis Job Clusters

You define a [Job Cluster] before you submit [Jobs]. A Job Cluster is a containing entity for Jobs.
It defines [metadata] and certain service-level agreements ([SLA]s). Job Clusters ease Job lifecycle
management and Job revisioning.

For example, by setting the SLA of a Job Cluster to Min=1 and Max=1, you ensure that exactly one Job
instance is always running for that Cluster. The Job Cluster also has default Job [parameters] that
any new Jobs submitted to the Cluster inherit. You can update new Job [artifacts] into the Job
Cluster so that the next Job submission picks up the latest version.

## Components of a Mantis Job

A Mantis Job has three [components], each of which has a corresponding chapter in this
documentation:

1. [**Source**](../source) — Fetches data from an external source and makes it available in the form of
   an Observable.

1. [**Processing Stage**](../processing-stage) — Transforms the Observable stream by means of a variety of operators. In the new [DSL], we can think of it as the middle instead of a combination of stages — [read more](../../../reference/dsl)

1. [**Sink**](../sink) — Pushes the resulting Observable out in the form of a fresh stream.

!!! note

    There is an alternate implementation that allows writing a Mantis Job as a series of operators operating directly on a `MantisStream` instance which abstracts information about RxJava, Observables from the user and offers a simpler way (hopefully 🤞) to write mantis jobs. Please see [Mantis DSL docs](../../../reference/dsl) for more details

![A Mantis Job is composed of three components: the “Source” fetches data in a streaming, non-blocking, back-pressure-aware manner; the “Stage” transforms the incoming stream by applying high-level functions such as map, scan, reduce, et cetera; the “Sink” pushes the results of the transformation in a non-blocking, asynchronous manner.](../../images/job-components.svg)

## Directory Structure of a Mantis Job

In addition to the source files, Mantis requires some meta-files to be present in the Job artifact.

Here is a sample directory structure:
```
src/
  - main/
    - java/
      - com/
        - myorg/
          - MyJob.java
    - resources/
      - META-INF/
        - services/
          - io.mantisrx.runtime.MantisJobProvider
      - job.properties
      - job-test.properties
```

* **`io.mantisrx.runtime.MantisJobProvider`** *(required)* — lists the fully-qualified name of the Java class that implements the `MantisJobProvider` interface
* **`job.properties`** and **`job-test.properties`** *(optional)* — required only if you are initializing the platform via the `.lifecycle()` method

## Creating a Mantis Job

To create a Mantis Job, call `MantisJob…create()`. When you do so, interpolate the following builder
methods. The first three — `.source()`, `.stage()`, and `.sink()` — are required, they must be the
first three of the methods that you call, and you must call them in that order:

1. <code>.source(<var>AbstractJobSource</var>)</code> — required, see [The Source Component](../source)
1. <code>.stage(<var>Stage</var>, <var>stageConfig</var>)</code> — required, (call this one or more times) see [The Processing Stage Component](../processing-stage). Alternatively, <br/>
   <code>.filter|.map|.flatMap|.keyBy</code> — required, (call these operators one or more times) an alternative to `.stage` to simplify job description. See [Mantis DSL](../../../reference/dsl)
1. <code>.sink(<var>Sink</var>)</code> — required, see [The Sink Component](../sink)
1. <code>.lifecycle(<var>Lifecycle</var>)</code> — optional, allows for start-up and shut-down hooks
1. <code>.parameterDefinition(<var>ParameterDefinition</var>)</code> — optional, (call this zero to many times) define any [parameters](/glossary#parameter) your job requires here
1. <code>.metadata(<var>Metadata</var>)</code> — optional, (call this zero to many times) define your job name and description here

| of this class          | this method                 | returns an object of this class |
| ----------------       | --------------------------  | ------------------------------- |
| `MantisStream` ⟶      | `init()` ⟶                 | `MantisStream`                  |
| `MantisStream` ⟶      | `source()` ⟶               | `MantisStream`                  |
| `MantisStream` ⟶      | `sink()` ⟶                 | `MantisStream`                  |
| `MantisStream` ⟶      | `map, flatMap, ...` ⟶      | `MantisStream`                  |
| `MantisStream` ⟶      | `keyBy()` ⟶                | `KeyedMantisStream`             |
| `KeyedMantisStream` ⟶ | `map, window, ...` ⟶       | `KeyedMantisStream`             |
| `KeyedMantisStream` ⟶ | `reduce` ⟶                 | `MantisStream`                  |
| `MantisJob` ⟶         | `source()` ⟶               | `SourceHolder`                  |
| `SourceHolder` ⟶      | `stage()` ⟶                | `Stages`                        |
| `Stages` ⟶            | `stage()` ⟶                | `Stages`                        |
| `Stages` ⟶            | `sink()` ⟶                 | `Config`                        |
| `Config` ⟶            | `lifecycle()` ⟶            | `Config`                        |
| `Config` ⟶            | `parameterDefinition()` ⟶  | `Config`                        |
| `Config` ⟶            | `metadata()` ⟶             | `Config`                        |
| `Config` ⟶            | `create()` ⟶               | `Job`                           |


### Lifecycle Management

You can establish start-up and shut-down procedures for your Mantis Job by means of the
`.lifecycle().` builder method.

Pass this method a `Lifecycle` object, that is to say, an object that implements the following
methods:

* **`startup()`** — initialize arbitrary application configs, perform dependency injection, and any long running or shared service libraries.
* **`shutdown()`** — gracefully close connections, shut down long running or shared service libraries, and general process cleanup.
* **`getServiceLocator()`** — returns a `ServiceLocator` that implements the `service(key)` method. Implement this method to return your dependency injection object such as Guice.

### Defining Parameters

To create a Parameter in order to pass it to the `.parameterDefinition()` builder method of the
`MantisJob` builder, use the following <code>new <var>ParameterVariety</var>()…build()</code> builder
methods:

* <code>.name(<var>string</var>)</code> — a user-friendly name for your Parameter
* <code>.description(<var>string</var>)</code> — a user-friendly description of your Parameter
* <code>.defaultValue(<var>value</var>)</code> — the value of this Parameter if the Job does not override it
* <code>.validator(<var>Validator</var>)</code> — a way of checking the proposed Parameter values for validity so bad values can be rejected before you submit the Job
* <code>.required()</code> — call this builder method if the Job must provide a value for this Parameter

There are some built-in Parameter varieties you can choose from that correspond to common data types:

* `BooleanParameter`
* `DoubleParameter`
* `IntParameter`
* `StringParameter`

#### Validators

<!-- 
A `Validator` has a description, available via its `getDescription()` method, and a validation
function, available via its `getValidator()` method. That function takes a potential value for
your parameter as input and returns a `Validation`. A `Validation` has the following methods:

* `isFailedValidation()` — indicates whether or not the potential parameter value is invalid
* `getFailedValidationReason()` — if `isFailedValidation()` is `true`, this function returns a string that indicates why the validation failed
-->

There are some standard Validators you can choose from that cover some common varieties of parameters:

* <code>Validators.range(<var>start</var>, <var>end</var>)</code> — will reject as invalid Parameter values that do not lie between the indicated *start* and *end* numerical values (where *start* and *end* themselves are valid Parameter values)
* `Validators.notNullOrEmpty()` — will reject empty strings or null values
* `Validators.alwaysPass()` — will not reject any Parameter values as invalid

#### Example

For example:

```java
myStringParameter = new StringParameter()
    .name("MyParameter")
    .description("This is a human-friendlydescription of my parameter")
    .validator(Validators.notNullOrEmpty())
    .defaultValue("SomeValue")
    .required()
    .build();
```

### Defining Metadata

To create metadata in order to pass it to the `.metadata()` builder method of the `MantisJob`
builder, use the following `new Metadata.Builder()…build()` builder methods:

* <code>.name(<var>string</var>)</code>
* <code>.description(<var>string</var>)</code>

For example:

```java
myMetadata = new Metadata.Builder()
    .name("MyMetadata")
    .description("Description of my metadata")
    .build();
```

<!-- Do not edit below this line -->
<!-- START -->
<!-- This section comes from the file "reference_links". It is automagically inserted into other files by means of the "refgen" script, also in the "docs/" directory. Edit this section only in the "reference_links" file, not in any of the other files in which it is included, or your edits will be overwritten. -->
[artifact]:                ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifacts]:               ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact file]:           ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact files]:          ../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[autoscale]:               ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaled]:              ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscales]:              ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaling]:             ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[scalable]:                ../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[AWS]:                     javascript:void(0)          "Amazon Web Services"
[backpressure]:            ../glossary#backpressure      "Backpressure refers to a set of possible strategies for coping with ReactiveX Observables that produce items more rapidly than their observers consume them."
[Binary compression]:      ../glossary#binarycompression
[broadcast]:               ../glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[broadcast mode]:          ../glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[Cassandra]:               ../glossary#cassandra         "Apache Cassandra is an open source, distributed database management system."
[cluster]:                 ../glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[clusters]:                ../glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[cold]:                    ../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observable]:         ../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observables]:        ../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[component]:               ../glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[components]:              ../glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[custom source]:           ../glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[custom sources]:          ../glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[executor]:                ../glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[executors]:               ../glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[fast property]: ../glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[fast properties]: ../glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[Fenzo]:                   ../glossary#fenzo             "Fenzo is a Java library that implements a generic task scheduler for Mesos frameworks."
[grouped]:                 ../glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[grouped data]:            ../glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[GRPC]:                    ../glossary#grpc              "gRPC is an open-source RPC framework using Protocol Buffers."
[hot]:                     ../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observable]:          ../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observables]:         ../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[JMC]:                     ../glossary#jmc               "Java Mission Control is a tool from Oracle with which developers can monitor and manage Java applications."
[job]:                     ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[jobs]:                    ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis job]:              ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis jobs]:             ../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[job cluster]:             ../glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[job clusters]:            ../glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[Job Master]:              ../glossary#jobmaster         "If a job is configured with autoscaling, Mantis will add a Job Master component to it as its initial component. This component will send metrics back to Mantis to help it govern the autoscaling process."
[Mantis Master]:           ../glossary#mantismaster      "The Mantis Master coordinates the execution of [Mantis Jobs] and starts the services on each Worker."
[Kafka]:                   ../glossary#kafka             "Apache Kafka is a large-scale, distributed streaming platform."
[keyed data]:              ../glossary#keyed             "Grouped (or keyed) data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[Keystone]:                ../glossary#keystone          "Keystone is Netflix’s data backbone, a stream processing platform that focuses on data analytics."
[label]:                   ../glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[labels]:                  ../glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[Log4j]:                   ../glossary#log4j             "Log4j is a Java-based logging framework."
[Apache Mesos]:            ../glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[Mesos]:                   ../glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[metadata]:                ../glossary#metadata          "Mantis inserts metadata into its Job payload. This may include information about where the data came from, for instance. You can define additional metadata to include in the payload when you establish the Job Cluster."
[meta message]:            ../glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[meta messages]:           ../glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[migration strategy]:      ../glossary#migration
[migration strategies]:    ../glossary#migration
[MRE]:                     ../glossary#mre               "Mantis Publish (a.k.a. Mantis Realtime Events, or MRE) is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Publish]:          ../glossary#mantispublish     "Mantis Publish is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Query Language]:   ../glossary#mql               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[MQL]:                     ../glossary#mql               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[Observable]:              ../glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[Observables]:             ../glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[parameter]:               ../glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[parameters]:              ../glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[Processing Stage]:        ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[Processing Stages]:       ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stage]:                   ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stages]:                  ../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[property]:                ../glossary#property          "A property is a particular named data value found within events in an event stream."
[properties]:              ../glossary#property          "A property is a particular named data value found within events in an event stream."
[Reactive Stream]:         ../glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[Reactive Streams]:        ../glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[ReactiveX]:               ../glossary#reactivex         "ReactiveX is a software technique for transforming, combining, reacting to, and managing streams of data. RxJava is an example of a library that implements this technique."
[RxJava]:                  ../glossary#rxjava            "RxJava is the Java implementation of ReactiveX, a software technique for transforming, combining, reacting to, and managing streams of data."
[downsample]:              ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sample]:                  ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampled]:                 ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[samples]:                 ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampling]:                ../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[scalar]:                  ../glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[scalar data]:             ../glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[Sink]:                    ../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sinks]:                   ../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sink component]:          ../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[service-level agreement]:  ../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[service-level agreements]: ../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[SLA]:                     ../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[Source]:                  ../glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Sources]:                 ../glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Source Job]:              ../glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Source Jobs]:             ../glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Spinnaker]: ../glossary#spinnaker "Spinnaker is a set of resources that help you deploy and manage resources in the cloud."
[SSE]:                     ../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent event]:       ../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent events]:      ../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[transform]:               ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformed]:             ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transforms]:              ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformation]:          ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformations]:         ../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transient]:               ../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient job]:           ../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient jobs]:          ../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[WebSocket]:               ../glossary#websocket         "WebSocket is a two-way, interactive communication channel that works over HTTP. In the Mantis context, it is an alternative to SSE."
[Worker]:                  ../glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Workers]:                 ../glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Zookeeper]:               ../glossary#zookeeper         "Apache Zookeeper is an open-source server that maintains configuration information and other services required by distributed applications."
<!-- END -->
