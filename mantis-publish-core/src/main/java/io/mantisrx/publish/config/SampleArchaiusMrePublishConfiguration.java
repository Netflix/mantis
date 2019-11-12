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

import java.util.HashMap;
import java.util.Map;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SampleArchaiusMrePublishConfiguration implements MrePublishConfiguration {

    public static final String PROP_PREFIX = "mantis.publish";
    public static final String PUBLISH_JOB_CLUSTER_PROP_PREFIX = PROP_PREFIX + ".jobcluster.";
    public static final String DEEPCOPY_EVENT_MAP_ENABLED_PROP = PROP_PREFIX + ".deepcopy.eventmap.enabled";
    public static final String MRE_CLIENT_ENABLED_PROP = PROP_PREFIX + ".enabled";
    public static final String MRE_CLIENT_APP_NAME_PROP = PROP_PREFIX + ".app.name";
    public static final String MRE_CLIENT_TEE_ENABLED_PROP = PROP_PREFIX + "tee.enabled";
    public static final String MRE_CLIENT_TEE_STREAM_NAME_PROP = PROP_PREFIX + "tee.stream";
    public static final String MRE_CLIENT_BLACKLIST_KEYS_PROP = PROP_PREFIX + ".blacklist";
    public static final String MAX_SUBSCRIPTIONS_COUNT_PROP = PROP_PREFIX + ".max.subscriptions.per.stream.default";
    public static final String DRAINER_INTERVAL_MSEC_PROP = PROP_PREFIX + ".drainer.interval.msec";
    public static final String JOB_DISCOVERY_REFRESH_INTERVAL_SEC_PROP = PROP_PREFIX + ".jobdiscovery.refresh.interval.sec";
    public static final String JOB_CLUSTER_MAPPING_REFRESH_INTERVAL_SEC_PROP = PROP_PREFIX + ".jobcluster.mapping.refresh.interval.sec";
    public static final String SUBS_REFRESH_INTERVAL_SEC_PROP = PROP_PREFIX + ".subs.refresh.interval.sec";
    public static final String SUBS_EXPIRY_INTERVAL_SEC_PROP = PROP_PREFIX + ".subs.expiry.interval.sec";
    public static final String SUBS_FETCH_QUERY_PARAMS_STR_PROP = PROP_PREFIX + ".subs.fetch.query.params.string";
    public static final String DISCOVERY_API_HOSTNAME_PROP = PROP_PREFIX + ".discovery.api.hostname";
    public static final String DISCOVERY_API_PORT_PROP = PROP_PREFIX + ".discovery.api.port";
    public static final String MAX_NUM_WORKERS_FOR_SUB_REFRESH = PROP_PREFIX + ".subs.refresh.max.num.workers";
    public static final String MAX_SUBS_PER_STREAM_FORMAT = PROP_PREFIX + ".max.subscriptions.stream.%s";
    public static final String PER_STREAM_QUEUE_SIZE_FORMAT = PROP_PREFIX + ".%s.stream.queue.size";
    public static final String MAX_NUM_STREAMS_NAME = PROP_PREFIX + ".max.num.streams";
    public static final String STREAM_INACTIVE_DURATION_THRESHOLD_NAME = PROP_PREFIX + ".stream.inactive.duration.threshold.sec";
    // Worker Pool properties.
    public static final String WORKER_POOL_PROP_PREFIX = PROP_PREFIX + ".workerpool.";
    public static final String WORKER_POOL_CAPACITY_PROP = WORKER_POOL_PROP_PREFIX + "capacity";
    public static final String WORKER_POOL_REFRESH_INTERVAL_SEC_PROP = WORKER_POOL_PROP_PREFIX + "refresh.interval.sec";
    public static final String WORKER_POOL_WORKER_ERROR_QUOTA_PROP = WORKER_POOL_PROP_PREFIX + "worker.error.quota";
    public static final String WORKER_POOL_WORKER_ERROR_TIMEOUT_SEC = WORKER_POOL_PROP_PREFIX + "worker.error.timeout.sec";
    // Event Channel properties.
    public static final String CHANNEL_PROP_PREFIX = PROP_PREFIX + ".channel.";
    public static final String CHANNEL_GZIP_ENABLED_PROP = CHANNEL_PROP_PREFIX + "gzip.enabled";
    public static final String CHANNEL_IDLE_TIMEOUT_SEC_PROP = CHANNEL_PROP_PREFIX + "idleTimeout.sec";
    public static final String CHANNEL_HTTP_CHUNK_SIZE_BYTES_PROP = CHANNEL_PROP_PREFIX + "httpChunkSize.bytes";
    public static final String CHANNEL_WRITE_TIMEOUT_SEC_PROP = CHANNEL_PROP_PREFIX + "writeTimeout.sec";
    public static final String CHANNEL_FLUSH_INTERVAL_MSEC = CHANNEL_PROP_PREFIX + "flushInterval.msec";
    public static final String CHANNEL_FLUSH_INTERVAL_BYTES = CHANNEL_PROP_PREFIX + "flushInterval.bytes";
    public static final String CHANNEL_LOW_WRITE_BUFFER_WATERMARK_BYTES = CHANNEL_PROP_PREFIX + "lowWriteBufferWatermark.bytes";
    public static final String CHANNEL_HIGH_WRITE_BUFFER_WATERMARK_BYTES = CHANNEL_PROP_PREFIX + "highWriteBufferWatermark.bytes";
    public static final String CHANNEL_IO_THREADS = CHANNEL_PROP_PREFIX + "ioThreads";
    public static final String CHANNEL_COMPRESSION_THREADS = CHANNEL_PROP_PREFIX + "compressionThreads";

    private final PropertyRepository propRepo;
    private final Property<Boolean> gzipEnabled;
    private final Property<Integer> idleTimeoutSeconds;
    private final Property<Integer> httpChunkSize;
    private final Property<Integer> writeTimeoutSeconds;
    private final Property<Long> flushIntervalMs;
    private final Property<Integer> flushIntervalBytes;
    private final Property<Integer> lowWriteBufferWatermark;
    private final Property<Integer> highWriteBufferWatermark;
    private final Property<Integer> ioThreads;
    private final Property<Integer> compressionThreads;

    private final Property<Boolean> deepCopyEventMapEnabled;
    private final Property<Boolean> mreClientEnabled;
    private final Property<String> appName;
    private final Property<Boolean> mreClientTeeEnabled;
    private final Property<String> mreClientTeeStreamName;
    private final Property<String> blacklistedKeys;
    private final Property<Integer> maxNumWorkersForSubsRefresh;
    private final Property<Integer> maxNumStreams;
    private final Property<Long> streamInactiveDurationThreshold;
    private final Property<Integer> maxSubscriptionCount;
    private final Map<String, Property<Integer>> maxSubsByStreamType = new HashMap<>();
    private final Map<String, Property<Integer>> queueSizeByStreamType = new HashMap<>();
    private final Map<String, Property<String>> jobClusterByStreamType = new HashMap<>();
    private final Property<String> subsFetchQueryParamStr;
    private final Property<String> discoveryApiHostnameProp;
    private final Property<Integer> discoveryApiPortProp;
    private final Property<Integer> drainerIntervalMSecProp;
    private final Property<Integer> jobDiscoveryRefreshIntervalSecProp;
    private final Property<Integer> jobClusterMappingRefreshIntervalSecProp;
    private final Property<Integer> subscriptionRefreshIntervalSecProp;
    private final Property<Integer> subscriptionExpiryIntervalSecProp;

    private final Property<Integer> workerPoolRefreshIntervalSec;
    private final Property<Integer> workerPoolCapacity;
    private final Property<Integer> workerPoolWorkerErrorQuota;
    private final Property<Integer> workerPoolWorkerErrorTimeoutSec;

    private static final Logger LOG = LoggerFactory.getLogger(SampleArchaiusMrePublishConfiguration.class);

    public SampleArchaiusMrePublishConfiguration(final PropertyRepository propertyRepository) {
        this.propRepo = propertyRepository;

        this.mreClientEnabled =
                propertyRepository.get(MRE_CLIENT_ENABLED_PROP, Boolean.class)
                        .orElse(true);
        this.appName =
                propertyRepository.get(MRE_CLIENT_APP_NAME_PROP, String.class)
                        .orElse("unknownApp");
        this.mreClientTeeEnabled =
                propertyRepository.get(MRE_CLIENT_TEE_ENABLED_PROP, Boolean.class)
                        .orElse(false);
        this.mreClientTeeStreamName =
                propertyRepository.get(MRE_CLIENT_TEE_STREAM_NAME_PROP, String.class)
                        .orElse("default_stream");
        this.blacklistedKeys =
                propertyRepository.get(MRE_CLIENT_BLACKLIST_KEYS_PROP, String.class)
                        .orElse("param.password");

        this.maxNumWorkersForSubsRefresh =
                propertyRepository.get(MAX_NUM_WORKERS_FOR_SUB_REFRESH, Integer.class)
                        .orElse(3);
        this.maxNumStreams =
                propertyRepository.get(MAX_NUM_STREAMS_NAME, Integer.class)
                        .orElse(5);
        this.streamInactiveDurationThreshold =
                propertyRepository.get(STREAM_INACTIVE_DURATION_THRESHOLD_NAME, Long.class)
                        .orElse(24L * 60L * 60L);
        this.maxSubscriptionCount =
                propertyRepository.get(MAX_SUBSCRIPTIONS_COUNT_PROP, Integer.class)
                        .orElse(20);
        this.deepCopyEventMapEnabled =
                propertyRepository.get(DEEPCOPY_EVENT_MAP_ENABLED_PROP, Boolean.class)
                                  .orElse(true);

        jobClusterByStreamType.put(StreamType.DEFAULT_EVENT_STREAM, propRepo.get(PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.DEFAULT_EVENT_STREAM, String.class)
                .orElse("SharedMrePublishEventSource"));
        jobClusterByStreamType.put(StreamType.LOG_EVENT_STREAM, propRepo.get(PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.LOG_EVENT_STREAM, String.class)
                .orElse("SharedPushLogEventSource"));
        this.drainerIntervalMSecProp = propRepo.get(DRAINER_INTERVAL_MSEC_PROP, Integer.class)
                .orElse(100);
        this.jobDiscoveryRefreshIntervalSecProp = propRepo.get(JOB_DISCOVERY_REFRESH_INTERVAL_SEC_PROP, Integer.class)
                .orElse(10);
        this.jobClusterMappingRefreshIntervalSecProp = propRepo.get(JOB_CLUSTER_MAPPING_REFRESH_INTERVAL_SEC_PROP, Integer.class)
                .orElse(60);
        this.subscriptionRefreshIntervalSecProp = propRepo.get(SUBS_REFRESH_INTERVAL_SEC_PROP, Integer.class)
                .orElse(1);
        this.subscriptionExpiryIntervalSecProp = propRepo.get(SUBS_EXPIRY_INTERVAL_SEC_PROP, Integer.class)
                .orElse(5 * 60);
        this.subsFetchQueryParamStr = propRepo.get(SUBS_FETCH_QUERY_PARAMS_STR_PROP, String.class)
                .orElse("");
        this.discoveryApiHostnameProp = propRepo.get(DISCOVERY_API_HOSTNAME_PROP, String.class)
                .orElse("127.0.0.1");
        this.discoveryApiPortProp = propRepo.get(DISCOVERY_API_PORT_PROP, Integer.class)
                .orElse(80);

        this.gzipEnabled =
                propRepo.get(CHANNEL_GZIP_ENABLED_PROP, Boolean.class)
                        .orElse(true);
        this.idleTimeoutSeconds =
                propRepo.get(CHANNEL_IDLE_TIMEOUT_SEC_PROP, Integer.class)
                        .orElse(300);           // 5 minutes
        this.httpChunkSize =
                propRepo.get(CHANNEL_HTTP_CHUNK_SIZE_BYTES_PROP, Integer.class)
                        .orElse(32768);         // 32 KiB
        this.writeTimeoutSeconds =
                propRepo.get(CHANNEL_WRITE_TIMEOUT_SEC_PROP, Integer.class)
                        .orElse(1);
        this.flushIntervalMs =
                propRepo.get(CHANNEL_FLUSH_INTERVAL_MSEC, Long.class)
                        .orElse(50L);
        this.flushIntervalBytes =
                propRepo.get(CHANNEL_FLUSH_INTERVAL_BYTES, Integer.class)
                        .orElse(512 * 1024);    // 500 KiB
        this.lowWriteBufferWatermark =
                propRepo.get(CHANNEL_LOW_WRITE_BUFFER_WATERMARK_BYTES, Integer.class)
                        .orElse(1572864);       // 1.5 MiB
        this.highWriteBufferWatermark =
                propRepo.get(CHANNEL_HIGH_WRITE_BUFFER_WATERMARK_BYTES, Integer.class)
                        .orElse(2097152);       // 2 MiB
        this.ioThreads =
                propRepo.get(CHANNEL_IO_THREADS, Integer.class)
                        .orElse(1);
        this.compressionThreads =
                propRepo.get(CHANNEL_COMPRESSION_THREADS, Integer.class)
                        .orElse(1);

        this.workerPoolCapacity =
                propRepo.get(WORKER_POOL_CAPACITY_PROP, Integer.class)
                        .orElse(1000);
        this.workerPoolRefreshIntervalSec =
                propRepo.get(WORKER_POOL_REFRESH_INTERVAL_SEC_PROP, Integer.class)
                        .orElse(10);
        this.workerPoolWorkerErrorQuota =
                propRepo.get(WORKER_POOL_WORKER_ERROR_QUOTA_PROP, Integer.class)
                        .orElse(60);
        this.workerPoolWorkerErrorTimeoutSec =
                propRepo.get(WORKER_POOL_WORKER_ERROR_TIMEOUT_SEC, Integer.class)
                        .orElse(300);
    }

    @Override
    public boolean isMREClientEnabled() {
        return mreClientEnabled.get();
    }

    @Override
    public String appName() {
        return appName.get();
    }

    @Override
    public boolean isTeeEnabled() {
        return mreClientTeeEnabled.get();
    }

    @Override
    public String teeStreamName() {
        return mreClientTeeStreamName.get();
    }

    @Override
    public String blackListedKeysCSV() {
        return blacklistedKeys.get();
    }

    @Override
    public int maxNumStreams() {
        return maxNumStreams.get();
    }

    @Override
    public long streamInactiveDurationThreshold() {
        return streamInactiveDurationThreshold.get();
    }

    @Override
    public int maxSubscriptionCount() {
        return maxSubscriptionCount.get();
    }

    @Override
    public boolean isDeepCopyEventMapEnabled() {
        return deepCopyEventMapEnabled.get();
    }

    @Override
    public int streamQueueSize(final String streamName) {
        queueSizeByStreamType.putIfAbsent(streamName,
                propRepo.get(String.format(PER_STREAM_QUEUE_SIZE_FORMAT, streamName), Integer.class)
                        .orElse(1000));
        Property<Integer> streamQueueSizeProp = queueSizeByStreamType.get(streamName);
        return streamQueueSizeProp.get();
    }

    @Override
    public int maxSubscriptions(final String streamName) {
        maxSubsByStreamType.putIfAbsent(streamName,
                propRepo.get(String.format(MAX_SUBS_PER_STREAM_FORMAT, streamName), Integer.class)
                        .orElse(maxSubscriptionCount()));
        Property<Integer> maxSubsProp = maxSubsByStreamType.get(streamName);
        return maxSubsProp.get();
    }

    @Override
    public Map<String, String> streamNameToJobClusterMapping() {

        Map<String, String> mapping = new HashMap<>();
        // TBD: call discovery api to fetch mappings from stream to job cluster for current app
        mapping.put(StreamType.DEFAULT_EVENT_STREAM, mantisJobCluster(StreamType.DEFAULT_EVENT_STREAM));
        mapping.put(StreamType.LOG_EVENT_STREAM, mantisJobCluster(StreamType.LOG_EVENT_STREAM));
        mapping.put(StreamType.REQUEST_EVENT_STREAM, mantisJobCluster(StreamType.DEFAULT_EVENT_STREAM));
       // mapping.put(StreamJobClusterMap.DEFAULT_STREAM_KEY,mantisJobCluster(StreamType.DEFAULT_EVENT_STREAM));

        return mapping;
    }

    @Override
    public String mantisJobCluster(final String streamName) {
        jobClusterByStreamType.putIfAbsent(streamName,
                propRepo.get(PUBLISH_JOB_CLUSTER_PROP_PREFIX + streamName, String.class)
                        .orElse("JobClusterNotConfiguredFor" + streamName));
        Property<String> jobClusterProp = jobClusterByStreamType.get(streamName);
        return jobClusterProp.get();
    }

    @Override
    public int drainerIntervalMsec() {
        return drainerIntervalMSecProp.get();
    }

    @Override
    public int subscriptionRefreshIntervalSec() {
        return subscriptionRefreshIntervalSecProp.get();
    }

    @Override
    public int subscriptionExpiryIntervalSec() {
        return subscriptionExpiryIntervalSecProp.get();
    }

    @Override
    public int jobDiscoveryRefreshIntervalSec() {
        return jobDiscoveryRefreshIntervalSecProp.get();
    }

    @Override
    public int jobClusterMappingRefreshIntervalSec() {
        return jobClusterMappingRefreshIntervalSecProp.get();
    }

    @Override
    public String discoveryApiHostname() {
        return discoveryApiHostnameProp.get();
    }

    @Override
    public int discoveryApiPort() {
        return discoveryApiPortProp.get();
    }

    @Override
    public int maxNumWorkersToFetchSubscriptionsFrom() {
        return maxNumWorkersForSubsRefresh.get();
    }

    @Override
    public String subscriptionFetchQueryParams() {
        return subsFetchQueryParamStr.get();
    }

    @Override
    public boolean getGzipEnabled() {
        return gzipEnabled.get();
    }

    @Override
    public int getIdleTimeoutSeconds() {
        return idleTimeoutSeconds.get();
    }

    @Override
    public int getHttpChunkSize() {
        return httpChunkSize.get();
    }

    @Override
    public int getWriteTimeoutSeconds() {
        return writeTimeoutSeconds.get();
    }

    @Override
    public long getFlushIntervalMs() {
        return flushIntervalMs.get();
    }

    @Override
    public int getFlushIntervalBytes() {
        return flushIntervalBytes.get();
    }

    @Override
    public int getLowWriteBufferWatermark() {
        return lowWriteBufferWatermark.get();
    }

    @Override
    public int getHighWriteBufferWatermark() {
        return highWriteBufferWatermark.get();
    }

    @Override
    public int getIoThreads() {
        return ioThreads.get();
    }

    @Override
    public int getCompressionThreads() {
        return compressionThreads.get();
    }

    @Override
    public int getWorkerPoolCapacity() {
        return workerPoolCapacity.get();
    }

    @Override
    public int getWorkerPoolRefreshIntervalSec() {
        return workerPoolRefreshIntervalSec.get();
    }

    @Override
    public int getWorkerPoolWorkerErrorQuota() {
        return workerPoolWorkerErrorQuota.get();
    }

    @Override
    public int getWorkerPoolWorkerErrorTimeoutSec() {
        return workerPoolWorkerErrorTimeoutSec.get();
    }
}
