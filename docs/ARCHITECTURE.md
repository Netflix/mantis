# Mantis API Architecture
The primary objective of Mantis API is to provide a horizontally scalable front to the Mantis Master. The role of the API is to relieve pressure from the Master bvy reducing the total number of incoming connections.

There are three primary pieces of functionality;

1. Proxying Calls to Mantis Master
1. Streaming data from Mantis Jobs on behalf of client
1. Proxying data between regions using the tunnel

These pieces are wired together using Zuul's concept of a `ChannelInitializer` which is a subclass of Netty's concept of the same name. The [MantisApiServerChannelInitializer](src/main/java/io/mantisrx/api/initializers/MantisApiServerChannelInitializer.java)
sets up the Netty channel and then adds handlers based on the route. Each of these pieces of functionality are discussed in turn below.

## Proxying calls to Mantis Master
The API by default will proxy any unintercepted HTTP request to the Mantis Master. This is handled by the underlying [Zuul](https://github.com/netflix/zuul) implementation, we
treat the Mantis Master as the Zuul Origin `api`, which treats all requests beginning with `/api` as calls to the specified origin. We always want this origin to be our current leader in the Manits Master cluster.

There are three aspects of the api which are germain to proxying to Mantis Master:

### Static Server List and Leader Election
Zuul typically uses Eureka to discover instances of the service to which it is proxying. This doesn't work for Mantis as we have a specific leader to which we want to proxy.
To this end we use Zuul's static server list feature, which we update via the `masterClient.getMasterMonitor()` in [MantisServerStartup.java](../src/main/java/io/mantisrx/api/MantisServerStartup.java).
Here we observe the Master Monitor and set the `api.ribbon.listOfServers` Note the origin name `api` at the beginning of this.

### Routes
[Routes](../src/main/java/io/mantisrx/api/filters/Routes.java) affect this, and may cause a call to be routed to other "filters". Anything directed to `ZuulEndPointRunner.PROXY_ENDPOINT_FILTER_NAME` will
be proxied to the master.

### Filters
Zuul Filters may manipulate either incoming requests before they're proxied, or outgoinmg requests after they are proxied. We have as a policy attempted to minimize the amount of manipulation. The reason
for this is simple, we want the API defined by `mantismasterv2` to define the standard, however we must support existing client expectations. Therefore minimizing differences in master responses and API responses
achieves this.

See [src/main/java/io/mantisrx/api/filters](../src/main/java/io/mantisrx/api/filters) for a list of all filters loaded at runtime. Note that these may be incoming, outgoing, or endpoints.

## Streaming Data
The api is tasked with providing an interface to end users, via websocket/sse, to access data streaming from jobs.
This is achieved through the [io.mantisrx.api.push](../src/main/java/io/mantisrx/api/push) namespace which contains a few critical components.

* PushConnectionDetails is a domain object which fully specifies the target (including cross regional) of a streaming data request.
* ConnectionBroker is responsible for acquiring (and potentially multiplexing) data for a PushConnectionDetails instance and returning an `Observable<String>`
* MantisSSEHandler is responsible for determining if the request is a websocket upgrade and the `Observable<String>` serving server sent events if it is not an upgrade.
* MantisWebSocketFrameHandler is responsible for serving the `Observable<String>` over websocket.

## Cross Regional Data
The ConnectionBroker also handles cross-regional data streams with the help of the contents of the [io.mantisrx.api.tunnel](../src/main/java/io/mantisrx/api/tunnel) namespace.
In the open source world we do not have a tunnel provider, it is up to the user to implement `MantisCrossRegionalClient`. Internally we have a Metatron based implementation which mutually auths the servers before
exchanging data over SSL.
