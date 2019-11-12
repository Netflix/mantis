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


public interface MrePublishConfiguration {

    boolean isMREClientEnabled();

    /**
     * Allows events to simultaneously be sent to an external system outside of Mantis.
     */
    boolean isTeeEnabled();

    /**
     * Specifies which external stream name tee will write to.
     */
    String teeStreamName();

    /**
     * Specifies the deployed name of the app or microservice running this client
     * @return Application name
     */
    String appName();

    String blackListedKeysCSV();

    int maxNumStreams();

    long streamInactiveDurationThreshold();

    int maxSubscriptionCount();

    int streamQueueSize(String streamName);

    int maxSubscriptions(String streamName);

    /**
     * List of Mantis Job clusters per stream configured to receive data for this app
     *
     * @return streamName to Job cluster mapping
     */
    Map<String, String> streamNameToJobClusterMapping();

    String mantisJobCluster(String streamName);

    int drainerIntervalMsec();

    int subscriptionRefreshIntervalSec();

    int subscriptionExpiryIntervalSec();

    int jobDiscoveryRefreshIntervalSec();

    int jobClusterMappingRefreshIntervalSec();

    boolean isDeepCopyEventMapEnabled();

    /**
     * Discovery API hostname to
     * - retrieve Job Cluster configured to receive events from this MRE publish client
     * - retrieve Mantis Workers for a Job Cluster
     *
     * @return discovery API hostname
     */
    String discoveryApiHostname();

    /**
     * Discovery API port to
     * - retrieve Job Cluster configured to receive events from this MRE publish client
     * - retrieve Mantis Workers for a Job Cluster
     *
     * @return discovery API port
     */
    int discoveryApiPort();

    default int maxNumWorkersToFetchSubscriptionsFrom() {
        return 3;
    }

    default String subscriptionFetchQueryParams() {
        return "";
    }

    boolean getGzipEnabled();

    int getIdleTimeoutSeconds();

    int getHttpChunkSize();

    int getWriteTimeoutSeconds();

    long getFlushIntervalMs();

    int getFlushIntervalBytes();

    int getLowWriteBufferWatermark();

    int getHighWriteBufferWatermark();

    int getIoThreads();

    int getCompressionThreads();

    int getWorkerPoolCapacity();

    int getWorkerPoolRefreshIntervalSec();

    int getWorkerPoolWorkerErrorQuota();

    int getWorkerPoolWorkerErrorTimeoutSec();
}
