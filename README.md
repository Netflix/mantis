# mantis-publish

[![Build Status](https://img.shields.io/travis/Netflix/mantis-publish.svg)](https://travis-ci.org/Netflix/mantis-publish)
[![Version](https://img.shields.io/bintray/v/netflixoss/maven/mantis-publish.svg)](https://bintray.com/netflixoss/maven/mantis-publish/_latestVersion)
[![OSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/mantis-publish.svg)](https://github.com/Netflix/mantis-publish)
[![License](https://img.shields.io/github/license/Netflix/mantis-publish.svg)](https://www.apache.org/licenses/LICENSE-2.0)

Library for publishing events into Mantis.

## Dynamic Properties

Dynamic properties are split into two categories: user-level and internal.

Prefix: `mantis.realtime.events.publish`

### User-level Dynamic Properties

| name | type | description | default |
| ---- | ---- | ----------- | ------- |
| enabled | boolean | Enables the Mantis publisher client| true |

### Channel Dynamic Properties

Prefix: `mantis.realtime.events.publish.channel`

| name | type | description | default |
| ---- | ---- | ----------- | ------- |
| gzip.enabled | boolean | Enables gzip compression for request bodies and adds `Content-Encoding: gzip` to request headers | true |
| idleTimeout.sec | int | The socket timeout for channels that haven't read or written any requests for at least this time | 300 |
| httpChunkSize.bytes | int | The maximum size of http requests | 32768 |
| writeTimeout.sec | int | The request timeout for channel sending http requests | 1 |
| flushInterval.msec | long | The longest time for events to accumulate before sending. This config works in conjunction with `io.mantisrx.realtime.events.netty.flushIntervalBytes` | 50 |
| flushInterval.bytes | int | The largest batch of events to accumulate before sending. This config works in conjunction with `io.mantisrx.realtime.events.netty.flushIntervalMs` | 524288 |
| lowWriteBufferWatermark.bytes | int | Pass-through for modifying Netty's write buffer low watermark. If the number of bytes queued in the write buffer exceeds the high water mark, `Channel#isWritable()` will start to return `false` | 524288 |
| highWriteBufferWatermark.bytes | int | Pass-through for modifying Netty's write buffer high watermark. If the number of bytes queued in the write buffer exceeds the high water mark and then dropped down below the low water mark, `Channel#isWritable()` will start to return `true` again | 524288 |
| ioThreads | int | The number of I/O threads to allocate to Netty | 1 |
| compressionThreads | int | The number of CPU threads to allocate to compressing request payloads | 1 |

## Metrics

| name | tags | type | description |
| ---- | ---- | ---- | ----------- |
| writeSuccess | channel | counter | The number of successful Netty writes into its internal buffer |
| writeFailure | channel | counter | The number of failures in writing to Netty's internal buffer |
| eventsDropped | channel, reason | counter | The number of events dropped as a result of the Netty channel being unwritable at the time of sending an event |
| writeTime | channel | timer | The time it takes to write an event into Netty's internal buffer |
| droppedBatches | channel | counter | The number of batches dropped as a result of a non-success http response from sending an event |
| connectionSuccess | channel | counter | The number of successful outbound connections established |
| connectionFailure | channel | counter | The number of failures in establishing outbound connections |
| liveConnections | channel | gauge | The current number of live connections |
| bufferSize | channel | gauge | The current number Bytes occupying a netty channel's internal buffer |
| encodeTime | channel, encoder | timer | The time it takes to gzip compress a batch of events |
| batchSize | channel | gauge | The number of events in a batch being flushed out of Netty's internal buffers |
| batchFlushTime | channel | timer | The time it takes to send a batch of events out of Netty's internal buffer and over the network |
| flushSuccess | channel | counter | The number of successful in Netty flushes |
| flushFailure | channel | counter | The number of failures in Netty flushes |
