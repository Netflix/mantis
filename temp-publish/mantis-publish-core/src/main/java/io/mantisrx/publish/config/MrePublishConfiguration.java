/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.publish.config;

import java.util.Map;

import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;


public interface MrePublishConfiguration {

    /**
     * Determine if event processing is enabled.
     * <p>
     * Property: <code>mantis.publish.enabled</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MRE_CLIENT_ENABLED_PROP
     */
    boolean isMREClientEnabled();

    /**
     * Allows events to simultaneously be sent to an external system outside of Mantis.
     * <p>
     * Property: <code>mantis.publish.tee.enabled</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MRE_CLIENT_TEE_ENABLED_PROP
     */
    boolean isTeeEnabled();

    /**
     * Specifies which external stream name tee will write to.
     * <p>
     * Property: <code>mantis.publish.tee.stream</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MRE_CLIENT_TEE_STREAM_NAME_PROP
     */
    String teeStreamName();

    /**
     * Specifies the deployed name of the app or microservice running this client
     * <p>
     * Property: <code>mantis.publish.app.name</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MRE_CLIENT_APP_NAME_PROP
     */
    String appName();

    /**
     * Comma separated list of field names where the value will be obfuscated.
     * <p>
     * Property: <code>mantis.publish.blacklist</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MRE_CLIENT_BLACKLIST_KEYS_PROP
     */
    String blackListedKeysCSV();

    /**
     * Maximum number of streams this application can create.
     * <p>
     * Property: <code>mantis.publish.max.num.streams</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MAX_NUM_STREAMS_NAME
     */
    int maxNumStreams();

    /**
     * Maximum duration in seconds for the stream to be considered inactive if there are no events.
     * <p>
     * Property: <code>mantis.publish.stream.inactive.duration.threshold.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#STREAM_INACTIVE_DURATION_THRESHOLD_NAME
     */
    long streamInactiveDurationThreshold();

    /**
     * Default maximum number of subscriptions per stream. After the limit is reached, further subscriptions on that
     * stream are rejected.
     * <p>
     * Property: <code>mantis.publish.max.subscriptions.per.stream.default</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MAX_SUBSCRIPTIONS_COUNT_PROP
     */
    int maxSubscriptionCount();

    /**
     * Size of the blocking queue to hold events to be pushed for the specific stream.
     * <p>
     * Property: <code>mantis.publish.{stream_name}.stream.queue.size</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#PER_STREAM_QUEUE_SIZE_FORMAT
     */
    int streamQueueSize(String streamName);

    /**
     * Overrides the default maximum number of subscriptions for the specific stream.
     * <p>
     * Property: <code>mantis.publish.max.subscriptions.stream.{stream_name}</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MAX_SUBS_PER_STREAM_FORMAT
     * @see #maxSubscriptionCount()
     */
    int maxSubscriptions(String streamName);

    /**
     * List of Mantis Job clusters per stream configured to receive data for this app
     *
     * @return streamName to Job cluster mapping
     *
     * @deprecated Use {@link MantisJobDiscovery#getStreamNameToJobClusterMapping(String)} instead.
     */
    @Deprecated
    Map<String, String> streamNameToJobClusterMapping();

    /**
     * @deprecated Use {@link MantisJobDiscovery#getJobCluster(String, String)} instead.
     */
    @Deprecated
    String mantisJobCluster(String streamName);

    /**
     * Interval in milliseconds when events are drained from the stream queue and delegated to underlying transmitter
     * for sending.
     * <p>
     * Property: <code>mantis.publish.drainer.interval.msec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#DRAINER_INTERVAL_MSEC_PROP
     */
    int drainerIntervalMsec();

    /**
     * Interval in seconds when subscriptions are fetched. In the default implementation, subscriptions are fetched
     * over http from the workers returned by Discovery API.
     * <p>
     * Property: <code>mantis.publish.subs.refresh.interval.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#SUBS_REFRESH_INTERVAL_SEC_PROP
     */
    int subscriptionRefreshIntervalSec();

    /**
     * Duration in seconds between a subscription is last fetched and when it is removed.
     * <p>
     * Property: <code>mantis.publish.subs.expiry.interval.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#SUBS_EXPIRY_INTERVAL_SEC_PROP
     */
    int subscriptionExpiryIntervalSec();

    /**
     * Duration in seconds between workers are refreshed for a job cluster.
     * <p>
     * Property: <code>mantis.publish.jobdiscovery.refresh.interval.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#JOB_DISCOVERY_REFRESH_INTERVAL_SEC_PROP
     */
    int jobDiscoveryRefreshIntervalSec();

    /**
     * Duration in seconds between job cluster mapping is refreshed for the current application.
     * <p>
     * Property: <code>mantis.publish.jobcluster.mapping.refresh.interval.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#JOB_CLUSTER_MAPPING_REFRESH_INTERVAL_SEC_PROP
     */
    int jobClusterMappingRefreshIntervalSec();

    /**
     * Determine if event processing should operate on a deep copy of the event. Otherwise the event object is processed directly.
     * <p>
     * Property: <code>mantis.publish.deepcopy.eventmap.enabled</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#DEEPCOPY_EVENT_MAP_ENABLED_PROP
     */
    boolean isDeepCopyEventMapEnabled();

    /**
     * Discovery API hostname to
     * - retrieve Job Cluster configured to receive events from this MRE publish client
     * - retrieve Mantis Workers for a Job Cluster
     * <p>
     * Property: <code>mantis.publish.discovery.api.hostname</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#DISCOVERY_API_HOSTNAME_PROP
     */
    String discoveryApiHostname();

    /**
     * Discovery API port to
     * - retrieve Job Cluster configured to receive events from this MRE publish client
     * - retrieve Mantis Workers for a Job Cluster
     * <p>
     * Property: <code>mantis.publish.discovery.api.port</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#DISCOVERY_API_PORT_PROP
     */
    int discoveryApiPort();

    /**
     * Maximum number of mantis workers to fetch subscription from. Workers are randomly chosen from the list returned
     * by Discovery API.
     * <p>
     * Property: <code>mantis.publish.subs.refresh.max.num.workers</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#MAX_NUM_WORKERS_FOR_SUB_REFRESH
     */
    default int maxNumWorkersToFetchSubscriptionsFrom() {
        return 3;
    }

    /**
     * Additional query params to pass to the api call to fetch subscription. It should be of the form "param1=value1&param2=value2".
     * <p>
     * Property: <code>mantis.publish.subs.fetch.query.params.string</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#SUBS_FETCH_QUERY_PARAMS_STR_PROP
     */
    default String subscriptionFetchQueryParams() {
        return "";
    }

    /**
     * Netty channel configuration for pushing events. Determine if events should be gzip encoded when send over the channel.
     * <p>
     * Property: <code>mantis.publish.channel.gzip.enabled</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_GZIP_ENABLED_PROP
     */
    boolean getGzipEnabled();

    /**
     * Netty channel configuration for pushing events. Write idle timeout in seconds for the channel.
     * <p>
     * Property: <code>mantis.publish.channel.idleTimeout.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_IDLE_TIMEOUT_SEC_PROP
     */
    int getIdleTimeoutSeconds();

    /**
     * Netty channel configuration for pushing events. Chunked size in bytes of the channel content. It is used by
     * HttpObjectAggregator.
     * <p>
     * Property: <code>mantis.publish.channel.httpChunkSize.bytes</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_HTTP_CHUNK_SIZE_BYTES_PROP
     * @see io.netty.handler.codec.http.HttpObjectAggregator
     */
    int getHttpChunkSize();

    /**
     * Netty channel configuration for pushing events. Write timeout in seconds for the channel.
     * <p>
     * Property: <code>mantis.publish.channel.writeTimeout.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_WRITE_TIMEOUT_SEC_PROP
     */
    int getWriteTimeoutSeconds();

    /**
     * Netty channel configuration for pushing events. Maximum duration in milliseconds between content flushes.
     * <p>
     * Property: <code>mantis.publish.channel.flushInterval.msec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_FLUSH_INTERVAL_MSEC
     */
    long getFlushIntervalMs();

    /**
     * Netty channel configuration for pushing events. Content is flushed when aggregated event size is above this threshold.
     * <p>
     * Property: <code>mantis.publish.channel.flushInterval.bytes</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_FLUSH_INTERVAL_BYTES
     */
    int getFlushIntervalBytes();

    /**
     * Netty channel configuration for pushing events. Used for setting write buffer watermark.
     * <p>
     * Property: <code>mantis.publish.channel.lowWriteBufferWatermark.bytes</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_LOW_WRITE_BUFFER_WATERMARK_BYTES
     * @see io.netty.channel.WriteBufferWaterMark
     */
    int getLowWriteBufferWatermark();

    /**
     * Netty channel configuration for pushing events. Used for setting write buffer watermark.
     * <p>
     * Property: <code>mantis.publish.channel.lowWriteBufferWatermark.bytes</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_LOW_WRITE_BUFFER_WATERMARK_BYTES
     * @see io.netty.channel.WriteBufferWaterMark
     */
    int getHighWriteBufferWatermark();

    /**
     * Netty channel configuration for pushing events. Number of threads in the eventLoopGroup.
     * <p>
     * Property: <code>mantis.publish.channel.ioThreads</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_IO_THREADS
     */
    int getIoThreads();

    /**
     * Netty channel configuration for pushing events. Number of threads in the encoderEventLoopGroup when gzip is
     * enabled.
     * <p>
     * Property: <code>mantis.publish.channel.compressionThreads</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#CHANNEL_COMPRESSION_THREADS
     */
    int getCompressionThreads();

    /**
     * Size of the pool of Mantis workers to push events to.
     * <p>
     * Property: <code>mantis.publish.workerpool.capacity</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#WORKER_POOL_CAPACITY_PROP
     */
    int getWorkerPoolCapacity();

    /**
     * Duration in seconds between Mantis workers are refreshed in the pool.
     * <p>
     * Property: <code>mantis.publish.workerpool.refresh.internal.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#WORKER_POOL_REFRESH_INTERVAL_SEC_PROP
     */
    int getWorkerPoolRefreshIntervalSec();

    /**
     * Number of errors to receive from a Mantis worker before it is blacklisted in the pool.
     * <p>
     * Property: <code>mantis.publish.workerpool.worker.error.quota</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#WORKER_POOL_WORKER_ERROR_QUOTA_PROP
     */
    int getWorkerPoolWorkerErrorQuota();

    /**
     * Duration in seconds after which a blacklisted Mantis worker may be reconsidered for selection.
     * <p>
     * Property: <code>mantis.publish.workerpool.worker.error.timeout.sec</code>
     * <p>
     * @see SampleArchaiusMrePublishConfiguration#WORKER_POOL_WORKER_ERROR_TIMEOUT_SEC
     */
    int getWorkerPoolWorkerErrorTimeoutSec();
}
