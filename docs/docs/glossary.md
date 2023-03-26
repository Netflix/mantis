artifact file {#artifact}
:   Each [Mantis Job] has an associated artifact file that contains its source code and JSON configuration.

autoscaling {#autoscaling}
:   You can establish an autoscaling policy for each [component] of your [Mantis Job] that governs how Mantis adjusts the number of [workers] assigned to that component as its workload changes.<br />⇒ See [Mantis Job Autoscaling](autoscaling)

backpressure {#backpressure}
:   Backpressure refers to a set of possible strategies for coping with [ReactiveX]  [Observables] that produce items more rapidly than their observers consume them.<br />⇒ See [ReactiveX.io: backpressure operators](http://reactivex.io/documentation/operators/backpressure.html)

Binary compression {#binarycompression}
:   <span class="tbd">has a particular meaning in the Mantis context... see (Connecting to a Source Job)[/writingjobs/source#connecting-to-a-source-job]</span>

Broadcast mode {#broadcast}
:   In broadcast mode, each [worker] of your [Mantis Job] gets all the data from all workers of the [Source Job] rather than having that data distributed equally among the workers of your Job.<br />⇒ See [Source Job Sources: Broadcast Mode](writingjobs/source#broadcast-mode)

Cassandra {#cassandra}
:   Apache Cassandra is an open source, distributed database management system.<br />⇒ See [Apache Cassandra Documentation](http://cassandra.apache.org/doc/latest/).

Cluster {#cluster}
:   A Job Cluster is a containing entity for [Mantis Jobs]. It defines metadata and certain [service-level agreements]. Job Clusters ease job lifecycle management and job revisioning.
:   A Mantis Cluster is a group of cloud container instances that hold your Mantis resources.

cold Observables {#cold}
:   A cold [ReactiveX]  [Observable] waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it.

component {#component}
:   A Mantis [Job] is composed of three types of component: a [Source], one or more [Processing Stages], and a [Sink].

Custom source {#customsource}
:   In contrast to a [Source Job], which is a built-in variety of [Source]  [component] designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics.

DSL {#dsl}
: Domain Specific Language. Set of operators and functions supported by Mantis Platform for users to specify their realtime data processing pipeline.

Executor {#executor}
:   The stage executor is responsible for loading the bytecode for a [Mantis Job] and then executing its [stages] and [workers] in a coordinated fashion. In the [Mesos] UI, workers are also referred to as executors.

Fenzo {#fenzo}
:   Fenzo is a Java library that implements a generic task scheduler for [Mesos] frameworks.<br />⇒ See [the Fenzo documentation](https://github.com/Netflix/Fenzo/wiki).

grouped data {#grouped}
:   Grouped (or keyed) data is distinguished from [scalar] data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a [RxJava]  [`GroupedObservable`](http://reactivex.io/RxJava/javadoc/rx/observables/GroupedObservable.html) or by a `MantisGroup`.

GRPC {#grpc}
:   GRPC is an open-source RPC framework using Protocol Buffers.<br />⇒ See [GRPC.io](https://grpc.io/docs/)

hot Observables {#hot}
:   A hot [ReactiveX]  [Observable] may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items.

JMC {#jmc}
:   **Java Mission Control** is a tool from Oracle with which developers can monitor and manage Java applications.<br />⇒ See [Java Components](https://docs.oracle.com/javacomponents/index.html)

Job {#job}
:   A Mantis Job takes in a stream of data, transforms it by using [RxJava] operators, and then outputs the results as another stream. It is composed of a [Source], one or more [Processing Stages], and a [Sink].<br />⇒ See [Writing Mantis Jobs](writingjobs)

Job Cluster {#jobcluster}
:   *see [Cluster]*

Job Master {#jobmaster}
:   If a job is configured with [autoscaling], Mantis will add a Job Master component to it as its initial component. This component will send metrics back to Mantis to help it govern the autoscaling process.

Kafka {#kafka}
:   Apache Kafka is a large-scale, distributed streaming platform.<br />⇒ See [Apache Kafka](http://kafka.apache.org).

keyed data {#keyed}
:   *see [grouped data]*

Keystone {#keystone}
:   Keystone is Netflix’s data backbone, a stream processing platform that focuses on data analytics.<br />⇒ See [Keystone Real-time Stream Processing Platform](https://medium.com/netflix-techblog/keystone-real-time-stream-processing-platform-a3ee651812a)

label {#label}
:   A label is a text key/value pair that you can add to a [Job Cluster] or to an individual [Job] to make it easier to search for or group.

Log4j {#log4j}
:   Log4j is a Java-based logging framework.<br />⇒ See [Apache Log4j](https://logging.apache.org/log4j/2.x/)

Mantis Master {#mantismaster}
:   The Mantis Master coordinates the execution of [Mantis Jobs] and starts the services on each [Worker].

Mesos {#mesos}
:   Apache Mesos is an open-source technique for balancing resources across frameworks in clusters.<br />⇒ See [Apache Mesos](https://mesos.apache.org/documentation/latest/)

Metadata {#metadata}
:   Mantis inserts metadata into its [Job] payload. This may include information about where the data came from, for instance. You can define additional metadata to include in the payload when you establish the [Job Cluster].

meta message {#metamessage}
:   A [Source Job] may occasionally inject meta messages into its data stream that indicate things like data drops.

migration strategy {#migration}
:   <span class="tbd">define</span>

Mantis Publish {#mantispublish}
:   Mantis Publish (internally at Netflix known as Mantis Realtime Events or MRE) is a library that your application can use to stream events into Mantis while respecting [MQL] filters.<br />⇒ See [MQL](MQL).

MQL {#MQL}
:   You use **Mantis Query Language** to define filters and other data processing that Mantis applies to a [Source] data stream at its point of origin, so as to reduce the amount of data going over the wire.

Observable {#observable}
:   In [ReactiveX] an `Observable` is the method of processing a stream of data in a way that facilitates its [transformation] and consumption by observers. Observables come in [hot] and [cold] varieties. There is also a `GroupedObservable` that is specialized to [grouped] data.<br />⇒ See [ReactiveX.io: Observable](http://reactivex.io/documentation/observable.html).

Parameter {#parameter}
:   A [Mantis Job] may accept parameters that modify its behavior. You can define these in your [Job Cluster] definition, and set their values on a per-Job basis.

Processing Stage {#stage}
:   A Processing Stage component of a Mantis [Job] transforms the [RxJava]  [Observables] it obtains from the [Source] component. A Job with only one Processing Stage is called a single-stage Job.<br />⇒ See [The Processing Stage Component](writingjobs/stage)

property {#property}
:   A property is a particular named data value found within events in an event stream.

Reactive Streams {#reactivestreams}
:   Reactive Streams is the latest advance of the [ReactiveX] project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with [backpressure].<br />⇒ See [Reactive Streams](http://www.reactive-streams.org).

ReactiveX {#reactivex}
:   ReactiveX is a software technique for transforming, combining, reacting to, and managing streams of data. [RxJava] is an example of a library that implements this technique.<br />⇒ See [reactivex.io](http://reactivex.io/).

RxJava {#rxjava}
:   RxJava is the Java implementation of [ReactiveX], a software technique for transforming, combining, reacting to, and managing streams of data.<br />⇒ See [reactivex.io](http://reactivex.io/).

sampling {#sampling}
:   Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values.<br />⇒ See [MQL: Sampling](MQL/sampling)

scalar data {#scalar}
:   Scalar data is distinguished from keyed or [grouped] data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary [ReactiveX]  [Observable].

Sink {#sink}
:   The Sink is the final component of a Mantis [Job]. It takes the [Observables] that has been transformed by the [Processing Stage] and outputs it in the form of a new data stream.<br />⇒ See [The Sink Component](writingjobs/sink)

SLA {#sla}
:   A **service-level agreement**, in the Mantis context, is defined on a per-[Cluster] basis. You use it to configure how many [Jobs] in the cluster will be in operation at any time, among other things.

Source {#source}
:   The Source component of a Mantis [Job] fetches data from a source outside of Mantis and makes it available to the [Processing Stage] component in the form of an [RxJava]  [Observable]. There are two varieties of Source: a [Source Job] and a [custom source].<br />⇒ See [The Source Component](writingjobs/source)

Source Job {#sourcejob}
:   A Source Job is a Mantis [Job] that you can use as a [Source], which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data.<br />⇒ See [Mantis Source Jobs](internals/sourcejobs)

server-sent events {#sse}
:   Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE [Sink].<br />⇒ See [Wikipedia: Server-sent events](https://en.wikipedia.org/wiki/Server-sent_events)

Transformation {#transformation}
:   A transformation acts on each datum from a stream or [Observables] of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between [scalar] and [grouped] forms.

transient jobs {#transient}
:   A transient (or ephemeral) [Mantis Job] is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects.

WebSocket {#websocket}
:   WebSocket is a two-way, interactive communication channel that works over HTTP. In the Mantis context, it is an alternative to [SSE].<br />⇒ See [WebSocket.org](https://www.websocket.org/).

Worker {#worker}
:   A worker is the smallest unit of work that is scheduled within a Mantis [component]. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its [autoscaling] policy.

Zookeeper {#zookeeper}
:   Apache Zookeeper is an open-source server that maintains configuration information and other services required by distributed applications.<br />⇒ See [Apache ZooKeeper](https://zookeeper.apache.org/)

<!-- Do not edit below this line -->
