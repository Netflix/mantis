This page introduces the [MQL] operators, including a syntax summary, usage examples, and a brief
description of each operator.

For the most part these operations use their [RxJava] analogs:

| MQL operator            | RxJava operator |
| ----------------------- | --------------- |
| [`select`](#select)     | [`map`](http://reactivex.io/documentation/operators/map.html) |
| [`window`](#window)     | [`window`](http://reactivex.io/documentation/operators/window.html) |
| [`where`](#where)       | [`filter`](http://reactivex.io/documentation/operators/filter.html) |
| [`group by`](#group-by) | [`groupBy`](http://reactivex.io/documentation/operators/groupby.html) |
| [`order by`](#order-by) | n/a |
| [`limit`](#limit)       | [`take`](http://reactivex.io/documentation/operators/take.html) |

For this reason you may find [the ReactiveX documentation](http://reactivex.io/intro.html) useful as
well, though it is a goal of MQL to provide you with a layer of abstraction such that you will not
need to use or think about [ReactiveX] when writing queries.

MQL attempts to stay true to the SQL syntax in order to reduce friction for new users writing
queries and to allow them to leverage their experiences with SQL.

An essential concept in MQL is the “[property],” which can take on one of several forms:

* `property.name.here`
* `e["this"]["accesses"]["nested"]["properties"]`
* `15`
* `"string literal"`

Most of the MQL operators use these forms to refer to properties within the event stream, as well as
string and numeric literals.

The last detail of note concerns unbounded vs. discrete streams. ReactiveX [Observables] that are
not expected to close are an unbounded stream and must be bounded/discretized in order for certain
operators to function with them. The `window` operator is useful for partitioning unbounded streams
into discrete bounded streams for operators such as aggregate, group by, or order by.

## `select`

**Syntax:** <code>select <var>property</var> from <var>source</var></code>

**Examples:**

* `"select * from servo"`
* `"select nf.node from servo"`
* `"select e["tags"]["nf.cluster"] from servo"`

The `select` operator allows you to project data into a new object by specifying which properties
should be carried forward into the output. Properties will bear the same name as was used to select
them. In the case of numbers and string literals their value will also be their name. In the case of
nested properties the result will be a top-level object joined with dots.

For example, the following `select` example…

* `"select "string literal", 45, e["tags"]["cluster"], nf.node from servo"`

…would result in an object like:

```json
{
  "string literal": "string literal",
  45: 45,
  "tags.cluster": "mantisagent",
  "nf.node": "i-123456"
}
```

!!! warning
    Be careful to avoid collisions between the top-level objects with dotted names and the nested
    objects that result in top-level properties with dotted names.

### Aggregates

**Syntax:** <code>"select <var>aggregate</var>(<var>property</var>) from <var>source</var>"</code>

**Examples:**

* `"select count(e["node"]) from servo window 60"`
* `"select count(e["node"]) from servo window 60 where e["metrics"]["latency"] > 350"`
* `"select average(e["metrics"]["latency"]), e["node"] from servo window 10"`

Supported Aggregates:

- `Min`
- `Max`
- `Average`
- `Count`
- `Sum`

Aggregates add analysis capabilities to MQL in order to allow you to answer interesting questions
about data in real-time. They can be intermixed with regular properties in order to select
properties and compute information about those properties, such as the last example above which
computes average latency on a per-node basis in 10 second windows.

!!! note
    Aggregates require that the stream on which they operate be discretized. You can ensure this
    either by feeding MQL a [cold Observable] in its context or by using the `window` operator on an
    unbounded stream.

You can use the `distinct` operator on a property to operate only on unique items within the window.
This is particularly useful with the `count` aggregate if you want to count the number of items with
some distinct property value in that window. For example:

* `"select count(distinct esn) from stream window 10 1"`

## `from`

**Syntax:** <code>"select <var>something</var> from <var>source</var>"</code>

**Example:**

* `"select * from servo"`

The `from` clause indicates to MQL from which Observable it should draw data. This requires some
explanation, as it bears different meaning in different contexts.

Directly against queryable sources the `from` clause refers to the source Observable no matter which
name is given. The operator is in fact optional, and the name of the source is arbitrary in this
context, and the clause will be inserted for you if you omit it.

When you use MQL as a library, the *`source`* corresponds with the names in the context map
parameter. The second parameter to `eval-mql()` is a `Map<String, Observable<T>>` and the `from`
clause will attempt to fetch the `Observable` from this `Map`.

## `window`

**Syntax:** <code>"WINDOW <var>integer</var>"</code>

**Examples:**

* `"select node, latency from servo window 10"`
* `"select MAX(latency) from servo window 60"`

The `window` clause divides an otherwise unbounded stream of data into discrete time-bounded
streams. The *`integer`* parameter is the number of seconds over which to perform this bounding. For
example `"select * from observable window 10"` will produce 10-second windows of data.

This discretization of streams is important for use with aggregate operations as well as group-by
and order-by clauses which cannot be executed on, and will hang on, unbounded streams.

## `where`

**Syntax:** <code>"select <var>property</var> from <var>source</var> where <var>predicate</var>"</code>

**Examples:** 

* `"select * from servo where node == "i-123456" AND e["metrics"]["latency"] > 350"`
* `"select * from servo where (node == "i-123456" AND e["metrics"]["latency"] > 350) OR node == "1-abcdef""`
* `"select * from servo where node ==~ /i-123/"`
* `"select * from servo where e["metrics"]["latency"] != null`
* `"select * from servo where e["list_of_requests"][*]["status"] == "success"`

The `where` clause filters any events out of the stream which do not match a given predictate.
Predicates support `AND` and `OR` operations. Binary operators supported are `=`, `==`, `<>`, `!=`,
`<`, `<=`, `>`, `>=`, `==~`. The first two above are both equality, and either of the next two
represent not-equal. You can use the last of those operators, `==~`, with a regular expression as
in: <code>"where <var>property</var> ==~ /<var>regex</var>/"</code> (any Java regular expression will
suffice). To take an event with certain attribute, use `e["{{key}}"] != null`.

If the event contains a list field, you can use the `[*]` operator to match objects inside that list.
For example, say the events have a field called `list_of_requests` and each item in the list has a field
called `status`. Then, the condition `e["list_of_requests"][*]["status"] == "success"` will return true
if at least 1 item has `status` equals `success`. Further, you can combine multiple conditions on the
list. For example,
```
e["list_of_requests"][*]["status"] == "success" AND e["list_of_requests"][*]["url"] == "abc"
```
This condition returns true if at least 1 item has `status` equals `success` and `url` equals `abc`.

## `group by`

**Syntax:** <code>"GROUP BY <var>property</var>"</code>

**Examples:**

* `"select node, latency from servo where latency > 300.0 group by node"`
* `"select MAX(latency), e["node"] from servo group by node"`

`group by` groups values in the output according to some property. This is particularly useful in
conjunction with aggregate operators in which one can compute aggregate values over a group. For
example, the following query calculates the maximum latency observed for each node in 60-second
windows:

* `"select MAX(latency), e["node"] from servo window 60 group by node"`

!!! note
    The `group by` clause requires that the stream on which it operates be discretized. You can
    ensure this either by feeding MQL a [cold Observable] in its context or by using the `window`
    operator on an unbounded stream.

## `order by`

**Syntax:** <code>"ORDER BY <var>property</var>"</code>

**Example:**

* `"select node, latency from servo group by node order by latency"`

The `order by` operator orders the results in the inner-most Observable by the specified property.
For example, the query `"select * from observable window 5 group by nf.node order by latency"` would
produce an Observable of Observables (windows) of Observables (groups). The events within the groups
would be ordered by their latency property.

!!! note
    The `order by` clause requires that the stream on which it operates be discretized. You can
    ensure this either by feeding MQL a [cold Observable] in its context or by using the `window`
    operator on an unbounded stream.

## `limit`

**Syntax:** <code>"LIMIT <var>integer</var>"</code>

**Examples:**

* `"select * from servo limit 10"`
* `"select AVERAGE(latency) from servo window 60 limit 10"`

The limit operator takes as a parameter a single integer and bounds the number of results to ≤
*`integer`*.

!!! note
    Limit does *not* discretize the stream for earlier operators such as `group by`, `order by`,
    aggregates.

<!-- Do not edit below this line -->
<!-- START -->
<!-- This section comes from the file "reference_links". It is automagically inserted into other files by means of the "refgen" script, also in the "docs/" directory. Edit this section only in the "reference_links" file, not in any of the other files in which it is included, or your edits will be overwritten. -->
[artifact]:                ../../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifacts]:               ../../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact file]:           ../../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact files]:          ../../glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[autoscale]:               ../../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaled]:              ../../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscales]:              ../../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaling]:             ../../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[scalable]:                ../../glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[AWS]:                     javascript:void(0)          "Amazon Web Services"
[backpressure]:            ../../glossary#backpressure      "Backpressure refers to a set of possible strategies for coping with ReactiveX Observables that produce items more rapidly than their observers consume them."
[Binary compression]:      ../../glossary#binarycompression
[broadcast]:               ../../glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[broadcast mode]:          ../../glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[Cassandra]:               ../../glossary#cassandra         "Apache Cassandra is an open source, distributed database management system."
[cluster]:                 ../../glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[clusters]:                ../../glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[cold]:                    ../../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observable]:         ../../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observables]:        ../../glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[component]:               ../../glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[components]:              ../../glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[custom source]:           ../../glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[custom sources]:          ../../glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[executor]:                ../../glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[executors]:               ../../glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[fast property]: ../../glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[fast properties]: ../../glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[Fenzo]:                   ../../glossary#fenzo             "Fenzo is a Java library that implements a generic task scheduler for Mesos frameworks."
[grouped]:                 ../../glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[grouped data]:            ../../glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[GRPC]:                    ../../glossary#grpc              "gRPC is an open-source RPC framework using Protocol Buffers."
[hot]:                     ../../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observable]:          ../../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observables]:         ../../glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[JMC]:                     ../../glossary#jmc               "Java Mission Control is a tool from Oracle with which developers can monitor and manage Java applications."
[job]:                     ../../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[jobs]:                    ../../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis job]:              ../../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis jobs]:             ../../glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[job cluster]:             ../../glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[job clusters]:            ../../glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[Job Master]:              ../../glossary#jobmaster         "If a job is configured with autoscaling, Mantis will add a Job Master component to it as its initial component. This component will send metrics back to Mantis to help it govern the autoscaling process."
[Mantis Master]:           ../../glossary#mantismaster      "The Mantis Master coordinates the execution of [Mantis Jobs] and starts the services on each Worker."
[Kafka]:                   ../../glossary#kafka             "Apache Kafka is a large-scale, distributed streaming platform."
[keyed data]:              ../../glossary#keyed             "Grouped (or keyed) data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[Keystone]:                ../../glossary#keystone          "Keystone is Netflix’s data backbone, a stream processing platform that focuses on data analytics."
[label]:                   ../../glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[labels]:                  ../../glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[Log4j]:                   ../../glossary#log4j             "Log4j is a Java-based logging framework."
[Apache Mesos]:            ../../glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[Mesos]:                   ../../glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[metadata]:                ../../glossary#metadata          "Mantis inserts metadata into its Job payload. This may include information about where the data came from, for instance. You can define additional metadata to include in the payload when you establish the Job Cluster."
[meta message]:            ../../glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[meta messages]:           ../../glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[migration strategy]:      ../../glossary#migration
[migration strategies]:    ../../glossary#migration
[MRE]:                     ../../glossary#mre               "Mantis Publish (a.k.a. Mantis Realtime Events, or MRE) is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Publish]:          ../../glossary#mantispublish     "Mantis Publish is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Query Language]:   ../../glossary#MQL               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[MQL]:                     ../../glossary#MQL               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[Observable]:              ../../glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[Observables]:             ../../glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[parameter]:               ../../glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[parameters]:              ../../glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[Processing Stage]:        ../../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[Processing Stages]:       ../../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stage]:                   ../../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stages]:                  ../../glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[property]:                ../../glossary#property          "A property is a particular named data value found within events in an event stream."
[properties]:              ../../glossary#property          "A property is a particular named data value found within events in an event stream."
[Reactive Stream]:         ../../glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[Reactive Streams]:        ../../glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[ReactiveX]:               ../../glossary#reactivex         "ReactiveX is a software technique for transforming, combining, reacting to, and managing streams of data. RxJava is an example of a library that implements this technique."
[RxJava]:                  ../../glossary#rxjava            "RxJava is the Java implementation of ReactiveX, a software technique for transforming, combining, reacting to, and managing streams of data."
[downsample]:              ../../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sample]:                  ../../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampled]:                 ../../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[samples]:                 ../../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampling]:                ../../glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[scalar]:                  ../../glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[scalar data]:             ../../glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[Sink]:                    ../../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sinks]:                   ../../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sink component]:          ../../glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[service-level agreement]:  ../../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[service-level agreements]: ../../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[SLA]:                     ../../glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[Source]:                  ../../glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Sources]:                 ../../glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Source Job]:              ../../glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Source Jobs]:             ../../glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Spinnaker]: ../../glossary#spinnaker "Spinnaker is a set of resources that help you deploy and manage resources in the cloud."
[SSE]:                     ../../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent event]:       ../../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent events]:      ../../glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[transform]:               ../../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformed]:             ../../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transforms]:              ../../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformation]:          ../../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformations]:         ../../glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transient]:               ../../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient job]:           ../../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient jobs]:          ../../glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[WebSocket]:               ../../glossary#websocket         "WebSocket is a two-way, interactive communication channel that works over HTTP. In the Mantis context, it is an alternative to SSE."
[Worker]:                  ../../glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Workers]:                 ../../glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Zookeeper]:               ../../glossary#zookeeper         "Apache Zookeeper is an open-source server that maintains configuration information and other services required by distributed applications."
<!-- END -->
