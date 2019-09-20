
Some desktops have trouble dealing with [SSE]. In such a case you can use [WebSocket] to connect to
a [Job] (other API features are only available via the Mantis REST API).

The WebSocket API runs from the same servers as the Mantis REST API, and you can reach it by using
the `ws://` or `wss://` protocol and port 7102.

## Connecting to Job Output (Sink)

* Append this to the WebSocket URI to connect to a Job by name: <code>/jobconnectbyname/<var>JobName</var></code>

* Append this to the WebSocket URI to connect to a specific running Job ID: <code>/jobconnectbyid/<var>JobID</var></code>

Upon connecting, the server starts writing messages that are coming in from the corresponding Jobs. 

You can append query parameters to the WebSocket URI (preceded by “`?`”) as is the case in a REST
API. Use this to pass any [Sink]  [parameters] your Job accepts. All Jobs accept the
“<code>sampleMSec=<var>mSecs</var></code>” Sink parameter which limits the rate at which the Sink output
is sampled by the server.

## Submitting and Connecting to Job Output

* Append this to the WebSocket URI to submit and connect to the submitted Job: `/jobsubmitandconnect`

Note that you will have to send one message on the WebSocket, which is the same JSON payload that
you would send as a POST body for
[the equivalent REST API](index.md#submitting-a-job-based-on-a-job-cluster). The server then submits
the Job and then starts writing messages into the WebSocket as it gets output from the Job.

You can append query parameters to the WebSocket URI (preceded by “`?`”) as is the case in a REST
API. Use this to pass any Sink parameters your Job accepts. All Jobs accept the
“<code>sampleMSec=<var>mSecs</var></code>” sink parameter which limits the rate at which the Sink output
is sampled by the server.

### Example: Javascript to Connect a Job
```html
<!DOCTYPE html>  
<meta charset="utf-8" />  
<title>WebSocket Test</title>  
<script language="javascript" type="text/javascript">  
var wsUri = "ws://your-domain.com/jobconnectbyname/YourJobName?sampleMSec=2000";
var output;  
function init() { 
  output = document.getElementById("output"); 
  testWebSocket(); 
}
function testWebSocket() { 
  websocket = new WebSocket(wsUri);
  websocket.onopen = function(evt) { onOpen(evt) }; 
  websocket.onclose = function(evt) { onClose(evt) }; 
  websocket.onmessage = function(evt) { onMessage(evt) }; 
  websocket.onerror = function(evt) { onError(evt) }; 
}  
function onOpen(evt) {
  writeToScreen("CONNECTED"); 
}
function onClose(evt) { 
  writeToScreen("DISCONNECTED"); 
}
function onMessage(evt) {
  writeToScreen('<span style="color: blue;">RESPONSE: ' + evt.data+'</span>');
}
function onError(evt) {
  writeToScreen('<span style="color: red;">ERROR:</span> ' + evt.data);
}
function doSend(message) { 
  websocket.send(message); 
}
function writeToScreen(message) { 
  var pre = document.createElement("p"); 
  pre.style.wordWrap = "break-word"; 
  pre.innerHTML = message; output.appendChild(pre); 
}
window.addEventListener("load", init, true);  
</script>  
<h2>WebSocket Test</h2>  
<div id="output"></div>
```

### Example: Javascript to Submit a Job and Connect to It
```html
<!DOCTYPE html>  
<meta charset="utf-8" />  
<title>WebSocket Test</title>  
<script language="javascript" type="text/javascript">  
var wsUri = "ws://your-domain.com/jobsubmitandconnect/"; 
var output;  
function init() { 
  output = document.getElementById("output"); 
  testWebSocket(); 
}
function testWebSocket() { 
  websocket = new WebSocket(wsUri);
  websocket.onopen = function(evt) { onOpen(evt) }; 
  websocket.onclose = function(evt) { onClose(evt) }; 
  websocket.onmessage = function(evt) { onMessage(evt) }; 
  websocket.onerror = function(evt) { onError(evt) }; 
}  
function onOpen(evt) { 
  writeToScreen("CONNECTED"); 
  // Change this to your job's submit json content.
  // See job submit REST API above for another example.
  doSend("{\n" +
                "  \"name\":\"Outliers-mock3\",\n" +
                "  \"version\":\"\",\n" +
                "  \"parameters\":[],\n" +
                "  \"jobSla\":{\"runtimeLimitSecs\":0,\"durationType\":\"Transient\",\"userProvidedType\":\"{\\\"unique\\\":\\\"foobar\\\"}\"},\n" +
                "  \"subscriptionTimeoutSecs\":\"90\",\n" +
                "  \"jobJarFileLocation\":null,\n" +
                "  \"schedulingInfo\":{\"stages\":{\"1\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":2048.0,\"diskMB\":1.0,\"scalable\":\"true\"}}}\n" +
                "}");
}  
function onClose(evt) { 
  writeToScreen("DISCONNECTED"); 
}
function onMessage(evt) {
  writeToScreen('<span style="color: blue;">RESPONSE: ' + evt.data+'</span>');
}
function onError(evt) {
  writeToScreen('<span style="color: red;">ERROR:</span> ' + evt.data);
}
function doSend(message) { 
  websocket.send(message); 
}
function writeToScreen(message) { 
  var pre = document.createElement("p"); 
  pre.style.wordWrap = "break-word"; 
  pre.innerHTML = message; output.appendChild(pre); 
}
window.addEventListener("load", init, true);  
</script>  
<h2>WebSocket Test</h2>  
<div id="output"></div>
```

<!-- Do not edit below this line -->
<!-- START -->
<!-- This section comes from the file "reference_links". It is automagically inserted into other files by means of the "refgen" script, also in the "docs/" directory. Edit this section only in the "reference_links" file, not in any of the other files in which it is included, or your edits will be overwritten. -->
[artifact]:                /mantis/glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifacts]:               /mantis/glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact file]:           /mantis/glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[artifact files]:          /mantis/glossary#artifact          "Each Mantis Job has an associated artifact file that contains its source code and JSON configuration."
[autoscale]:               /mantis/glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaled]:              /mantis/glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscales]:              /mantis/glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[autoscaling]:             /mantis/glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[scalable]:                /mantis/glossary#autoscaling       "You can establish an autoscaling policy for each component of your Mantis Job that governs how Mantis adjusts the number of workers assigned to that component as its workload changes."
[AWS]:                     javascript:void(0)          "Amazon Web Services"
[backpressure]:            /mantis/glossary#backpressure      "Backpressure refers to a set of possible strategies for coping with ReactiveX Observables that produce items more rapidly than their observers consume them."
[Binary compression]:      /mantis/glossary#binarycompression
[broadcast]:               /mantis/glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[broadcast mode]:          /mantis/glossary#broadcast         "In broadcast mode, each worker of your job gets all the data from all workers of the Source Job rather than having that data distributed equally among the workers of your job."
[Cassandra]:               /mantis/glossary#cassandra         "Apache Cassandra is an open source, distributed database management system."
[cluster]:                 /mantis/glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[clusters]:                /mantis/glossary#cluster           "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[cold]:                    /mantis/glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observable]:         /mantis/glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[cold Observables]:        /mantis/glossary#cold              "A cold ReactiveX Observable waits until an observer subscribes to it before it begins to emit items. This means the observer is guaranteed to see the whole Observable sequence from the beginning. This is in contrast to a hot Observable, which may begin emitting items as soon as it is created, even before observers have subscribed to it."
[component]:               /mantis/glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[components]:              /mantis/glossary#component         "A Mantis Job is composed of three types of component: a Source, one or more Processing Stages, and a Sink."
[custom source]:           /mantis/glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[custom sources]:          /mantis/glossary#customsource      "In contrast to a Source Job, which is a built-in variety of Source component designed to pull data from a common sort of data source, a custom source typically accesses data from less-common sources or has unusual delivery guarantee semantics."
[executor]:                /mantis/glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[executors]:               /mantis/glossary#executor          "The stage executor is responsible for loading the bytecode for a Mantis Job and then executing its stages and workers in a coordinated fashion. In the Mesos UI, workers are also referred to as executors."
[fast property]: /mantis/glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[fast properties]: /mantis/glossary#fastproperties "Fast properties allow you to change the behavior of Netflix services without recompiling and redeploying them."
[Fenzo]:                   /mantis/glossary#fenzo             "Fenzo is a Java library that implements a generic task scheduler for Mesos frameworks."
[grouped]:                 /mantis/glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[grouped data]:            /mantis/glossary#grouped           "Grouped data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[GRPC]:                    /mantis/glossary#grpc              "gRPC is an open-source RPC framework using Protocol Buffers."
[hot]:                     /mantis/glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observable]:          /mantis/glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[hot Observables]:         /mantis/glossary#hot               "A hot ReactiveX Observable may begin emitting items as soon as it is created, even before observers have subscribed to it. This means the observer may miss items that were emitted before the observer subscribed. This is in contrast to a cold Observable, which waits until an observer subscribes to it before it begins to emit items."
[JMC]:                     /mantis/glossary#jmc               "Java Mission Control is a tool from Oracle with which developers can monitor and manage Java applications."
[job]:                     /mantis/glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[jobs]:                    /mantis/glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis job]:              /mantis/glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[Mantis jobs]:             /mantis/glossary#job               "A Mantis Job takes in a stream of data, transforms it by using RxJava operators, and then outputs the results as another stream. It is composed of a Source, one or more Processing Stages, and a Sink."
[job cluster]:             /mantis/glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[job clusters]:            /mantis/glossary#jobcluster        "A Mantis Job Cluster is a containing entity for Mantis Jobs. It defines metadata and certain service-level agreements. Job Clusters ease job lifecycle management and job revisioning."
[Job Master]:              /mantis/glossary#jobmaster         "If a job is configured with autoscaling, Mantis will add a Job Master component to it as its initial component. This component will send metrics back to Mantis to help it govern the autoscaling process."
[Mantis Master]:           /mantis/glossary#mantismaster      "The Mantis Master coordinates the execution of [Mantis Jobs] and starts the services on each Worker."
[Kafka]:                   /mantis/glossary#kafka             "Apache Kafka is a large-scale, distributed streaming platform."
[keyed data]:              /mantis/glossary#keyed             "Grouped (or keyed) data is distinguished from scalar data in that each datum is accompanied by a key that indicates what group it belongs to. Grouped data can be processed by a RxJava GroupedObservable or by a MantisGroup."
[Keystone]:                /mantis/glossary#keystone          "Keystone is Netflix’s data backbone, a stream processing platform that focuses on data analytics."
[label]:                   /mantis/glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[labels]:                  /mantis/glossary#label             "A label is a text key/value pair that you can add to a Job Cluster or to an individual Job to make it easier to search for or group."
[Log4j]:                   /mantis/glossary#log4j             "Log4j is a Java-based logging framework."
[Apache Mesos]:            /mantis/glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[Mesos]:                   /mantis/glossary#mesos             "Apache Mesos is an open-source technique for balancing resources across frameworks in clusters."
[metadata]:                /mantis/glossary#metadata          "Mantis inserts metadata into its Job payload. This may include information about where the data came from, for instance. You can define additional metadata to include in the payload when you establish the Job Cluster."
[meta message]:            /mantis/glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[meta messages]:           /mantis/glossary#metamessage       "A Source Job may occasionally inject meta messages into its data stream that indicate things like data drops."
[migration strategy]:      /mantis/glossary#migration
[migration strategies]:    /mantis/glossary#migration
[MRE]:                     /mantis/glossary#mre               "Mantis Publish (a.k.a. Mantis Realtime Events, or MRE) is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Publish]:          /mantis/glossary#mantispublish     "Mantis Publish is a library that your application can use to stream events into Mantis while respecting MQL filters."
[Mantis Query Language]:   /mantis/glossary#mql               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[MQL]:                     /mantis/glossary#mql               "You use Mantis Query Language to define filters and other data processing that Mantis applies to a Source data stream at its point of origin, so as to reduce the amount of data going over the wire."
[Observable]:              /mantis/glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[Observables]:             /mantis/glossary#observable        "In ReactiveX an Observable is the method of processing a stream of data in a way that facilitates its transformation and consumption by observers. Observables come in hot and cold varieties. There is also a GroupedObservable that is specialized to grouped data."
[parameter]:               /mantis/glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[parameters]:              /mantis/glossary#parameter         "A Mantis Job may accept parameters that modify its behavior. You can define these in your Job Cluster definition, and set their values on a per-Job basis."
[Processing Stage]:        /mantis/glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[Processing Stages]:       /mantis/glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stage]:                   /mantis/glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[stages]:                  /mantis/glossary#stage             "A Processing Stage component of a Mantis Job transforms the RxJava Observables it obtains from the Source component."
[property]:                /mantis/glossary#property          "A property is a particular named data value found within events in an event stream."
[properties]:              /mantis/glossary#property          "A property is a particular named data value found within events in an event stream."
[Reactive Stream]:         /mantis/glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[Reactive Streams]:        /mantis/glossary#reactivestreams   "Reactive Streams is the latest advance of the ReactiveX project. It is an API for manipulating streams of asynchronous data in a non-blocking fashion, with backpressure."
[ReactiveX]:               /mantis/glossary#reactivex         "ReactiveX is a software technique for transforming, combining, reacting to, and managing streams of data. RxJava is an example of a library that implements this technique."
[RxJava]:                  /mantis/glossary#rxjava            "RxJava is the Java implementation of ReactiveX, a software technique for transforming, combining, reacting to, and managing streams of data."
[downsample]:              /mantis/glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sample]:                  /mantis/glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampled]:                 /mantis/glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[samples]:                 /mantis/glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[sampling]:                /mantis/glossary#sampling          "Sampling is an MQL strategy for mitigating data volume issues. There are two sampling strategies: Random and Sticky. Random sampling uniformly downsamples the source stream to a percentage of its original volume. Sticky sampling selectively samples data from the source stream based on key values."
[scalar]:                  /mantis/glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[scalar data]:             /mantis/glossary#scalar            "Scalar data is distinguished from keyed or grouped data in that it is not categorized into groups by key. Scalar data can be processed by an ordinary ReactiveX Observable."
[Sink]:                    /mantis/glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sinks]:                   /mantis/glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[Sink component]:          /mantis/glossary#sink              "The Sink is the final component of a Mantis Job. It takes the Observable that has been transformed by the Processing Stage and outputs it in the form of a new data stream."
[service-level agreement]:  /mantis/glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[service-level agreements]: /mantis/glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[SLA]:                     /mantis/glossary#sla               "A service-level agreement, in the Mantis context, is defined on a per-Cluster basis. You use it to configure how many Jobs in the cluster will be in operation at any time, among other things."
[Source]:                  /mantis/glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Sources]:                 /mantis/glossary#source            "The Source component of a Mantis Job fetches data from a source outside of Mantis and makes it available to the Processing Stage component in the form of an RxJava Observable. There are two varieties of Source: a Source Job and a custom source."
[Source Job]:              /mantis/glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Source Jobs]:             /mantis/glossary#sourcejob         "A Source Job is a Mantis Job that you can use as a Source, which wraps a data source external to Mantis and makes it easier for you to create a job that observes its data."
[Spinnaker]: /mantis/glossary#spinnaker "Spinnaker is a set of resources that help you deploy and manage resources in the cloud."
[SSE]:                     /mantis/glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent event]:       /mantis/glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[server-sent events]:      /mantis/glossary#sse               "Server-sent events (SSE) are a way for a browser to receive automatic updates from a server through an HTTP connection. Mantis includes an SSE Sink."
[transform]:               /mantis/glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformed]:             /mantis/glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transforms]:              /mantis/glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformation]:          /mantis/glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transformations]:         /mantis/glossary#transformation    "A transformation acts on each datum from a stream or Observables of data, changing it in some manner before passing it along as a new stream or Observable. Transformations may change data between scalar and grouped forms."
[transient]:               /mantis/glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient job]:           /mantis/glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[transient jobs]:          /mantis/glossary#transient         "A transient (or ephemeral) Mantis Job is automatically killed by Mantis after a certain amount of time has passed since the last subscriber to the job disconnects."
[WebSocket]:               /mantis/glossary#websocket         "WebSocket is a two-way, interactive communication channel that works over HTTP. In the Mantis context, it is an alternative to SSE."
[Worker]:                  /mantis/glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Workers]:                 /mantis/glossary#worker            "A worker is the smallest unit of work that is scheduled within a Mantis component. You can configure how many resources Mantis allocates to each worker, and Mantis will adjust the number of workers your Mantis component needs based on its autoscaling policy."
[Zookeeper]:               /mantis/glossary#zookeeper         "Apache Zookeeper is an open-source server that maintains configuration information and other services required by distributed applications."
<!-- END -->
