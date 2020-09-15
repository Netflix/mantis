The Mantis Publish runtime flow consists of three phases: connecting, event processing, and event delivery.

## Phase 1: Connecting

Mantis Publish will only stream an event from your application into Mantis if there is a subscriber to that
specific application instance with an MQL query that matches the event. Any [Mantis Job] can connect
to these applications. However, it is a best practice to have [Source Jobs] connect to Mantis Publish
applications. This is because Source Jobs provide several conveniences over regular Mantis Jobs,
such as multiplexing events and connection management. By leveraging Source Jobs as an intermediary,
Mantis Jobs are able to consume events from an external source without having to worry about
lower-level details of that external source.

This is possible through job chaining, which Mantis provides by default. When connecting to a
Mantis Publish application, downstream Mantis Jobs will send subscription requests with an MQL query via HTTP to a Source
Job. The Source Job will store these subscriptions in memory. These subscriptions are then fetched by upstream
applications at the edge running the Mantis Publish library. Once the upstream edge Mantis Publish application
is aware of the subscription, it will start pushing events downstream into the Source Job.

!!! tip
    The Mantis Publish library not only handles subscriptions, but also takes care of discovering Source Job workers
    so you do not have to worry about Source Job rebalancing/autoscaling.
    For more information about Source Jobs see
    [Mantis Source Jobs](../../../internals/source-jobs).

### Creating Stream Subscriptions

Clients such as Mantis Jobs connect to a Mantis Publish application by submitting a subscription represented
by an HTTP request. Mantis Publish’s `StreamManager` maintains these subscriptions in-memory. The
`StreamManager` manages internal resources such as subscriptions, streams, and internal processing
queues.

Clients can create subscriptions to different event streams. There are two types:

1. **`default` streams** contain events emitted by applications that use the Mantis Publish library.
1. **`log` streams** contain events which may not be core to the application, such as log or general infrastructure events.

## Phase 2: Event Processing

Event processing within Mantis Publish takes place in two steps: event ingestion and event dispatch.

### Event Ingestion

Event ingestion begins at the edge, in your application, by invoking `EventPublisher#publish` which places the
event onto an internal queue for dispatching.

### Event Dispatch

Events are dispatched by a drainer thread created by the Event Publisher. The drainer will drain events
from the internal queue previously populated by `EventPublisher#publish`, perform some transformations,
and finally dispatch events over the network and into Mantis.

Events are transformed by an `EventProcessor` which processes events one at a time.
Transformation includes the following steps:

1. Masks sensitive fields in the event.
    - Sensitive fields are referenced by a blacklist defined by a configuration.
    - This blacklist is a comma-delimited string of keys you wish to blacklist in your event.
1. Evaluates the MQL query of each subscription and builds a list of matching subscriptions.
1. For each matching subscription, enriches the event with a superset of fields from the MQL query
   from all the other matching subscriptions (see the following diagram).
1. Sends this enriched event to all of the subscribers (see
   [Event Delivery](#phase-3-event-delivery) below for the details).

!!! info
    More Mantis Publish configuration options can be found [in the mantis-publish configuration class](https://github.com/Netflix/mantis-publish/blob/d69d549335cd74149395e9a48780dd702bbc1b82/mantis-publish-core/src/main/java/io/mantisrx/publish/config/MrePublishConfiguration.java).

## Phase 3: Event Delivery

Mantis Publish delivers events on-demand. When a client subscribes to a Mantis Job that issues an MQL query,
the Event Publisher delivers the event using non-blocking I/O.
