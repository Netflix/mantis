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

package io.mantisrx.publish;

import com.netflix.mantis.discovery.proto.StreamJobClusterMap;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.core.SubscriptionFactory;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class handles the logic for stream -> job cluster discovery. A class that extends this abstract class is
 * expected to perform the actual fetch given a stream and cluster names. The storage of the subscriptions is off loaded
 * to the stream manager (See {@link StreamManager}). Thus, the stream manager is the source of truth of current active
 * subscriptions. For the same reason, the stream manager also stores which streams are registered. This class will only
 * fetch subscriptions for streams that are registered.
 */
public abstract class AbstractSubscriptionTracker implements SubscriptionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSubscriptionTracker.class);

    private final MrePublishConfiguration mrePublishConfiguration;
    private final Registry registry;
    private final MantisJobDiscovery jobDiscovery;
    private final StreamManager streamManager;
    private final Counter refreshSubscriptionInvokedCount;
    private final Counter refreshSubscriptionSuccessCount;
    private final Counter refreshSubscriptionFailedCount;
    private final Counter staleSubscriptionRemovedCount;
    private volatile Map<String, Long> streamLastFetchedTs = new HashMap<>();

    public AbstractSubscriptionTracker(MrePublishConfiguration mrePublishConfiguration,
                                       Registry registry,
                                       MantisJobDiscovery jobDiscovery,
                                       StreamManager streamManager) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.registry = registry;
        this.jobDiscovery = jobDiscovery;
        this.streamManager = streamManager;
        this.refreshSubscriptionInvokedCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "refreshSubscriptionInvokedCount");
        this.refreshSubscriptionSuccessCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "refreshSubscriptionSuccessCount");
        this.refreshSubscriptionFailedCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "refreshSubscriptionFailedCount");
        this.staleSubscriptionRemovedCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "staleSubscriptionRemovedCount");
    }

    void propagateSubscriptionChanges(String streamName, Set<MantisServerSubscription> curr) {
        Set<String> previousIds = getCurrentSubIds(streamName);

        for (MantisServerSubscription newSub : curr) {
            // Add new subscription not present previously
            if (!previousIds.contains(newSub.getSubscriptionId())) {
                try {
                    Optional<Subscription> subscription = SubscriptionFactory.getSubscription(
                            newSub.getSubscriptionId(), newSub.getQuery());
                    if (subscription.isPresent()) {
                        streamManager.addStreamSubscription(subscription.get());
                    } else {
                        LOG.info("will not add invalid subscription {}", newSub);
                    }
                } catch (Throwable t) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("failed to add subscription {}", newSub, t);
                    }
                }
            }

            previousIds.remove(newSub.getSubscriptionId());
        }

        // Remove previous subscriptions no longer present.
        previousIds.stream().forEach(prevId -> {
            try {
                streamManager.removeStreamSubscription(prevId);
            } catch (Throwable t) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("failed to remove subscription {}", prevId);
                }
            }
        });
    }


    private void cleanupStaleSubscriptions(String streamName) {
        Long lastFetched = streamLastFetchedTs.get(streamName);
        if (lastFetched != null) {
            boolean hasStaleSubscriptionsData = (System.currentTimeMillis() - lastFetched) >
                    mrePublishConfiguration.subscriptionExpiryIntervalSec() * 1000;
            if (hasStaleSubscriptionsData) {
                LOG.info("removing stale subscriptions data for stream {} (created {})", streamName, lastFetched);
                staleSubscriptionRemovedCount.increment();
                streamLastFetchedTs.remove(streamName);
                propagateSubscriptionChanges(streamName, Collections.emptySet());
            }
        }
    }

    /**
     * Get current set of subscriptions for a streamName for given jobCluster.
     *
     * @param streamName name of MRE stream
     * @param jobCluster Mantis Job Cluster name
     *
     * @return Optional of MantisServerSubscriptionEnvelope on successful retrieval, else empty
     */
    public abstract Optional<MantisServerSubscriptionEnvelope> fetchSubscriptions(String streamName, String jobCluster);

    @Override
    public void refreshSubscriptions() {
        refreshSubscriptionInvokedCount.increment();
        // refresh subscriptions only if the Publish client is enabled and has streams registered by
        // MantisEventPublisher
        boolean mantisPublishEnabled = mrePublishConfiguration.isMREClientEnabled();
        Set<String> registeredStreams = streamManager.getRegisteredStreams();
        boolean subscriptionsFetchedForStream = false;
        if (mantisPublishEnabled && !registeredStreams.isEmpty()) {
            Map<String, String> streamJobClusterMap =
                    jobDiscovery.getStreamNameToJobClusterMapping(mrePublishConfiguration.appName());
            for (Map.Entry<String, String> e : streamJobClusterMap.entrySet()) {
                String streamName = e.getKey();
                LOG.debug("processing stream {} and currently registered Streams {}", streamName, registeredStreams);
                if (registeredStreams.contains(streamName)
                        || StreamJobClusterMap.DEFAULT_STREAM_KEY.equals(streamName)) {
                    subscriptionsFetchedForStream = true;
                    String jobCluster = e.getValue();
                    try {
                        Optional<MantisServerSubscriptionEnvelope> subsEnvelopeO = fetchSubscriptions(
                                streamName, jobCluster);
                        if (subsEnvelopeO.isPresent()) {
                            MantisServerSubscriptionEnvelope subsEnvelope = subsEnvelopeO.get();
                            propagateSubscriptionChanges(streamName, subsEnvelope.getSubscriptions());
                            LOG.debug("{} subscriptions updated to {}", streamName, subsEnvelope);
                            streamLastFetchedTs.put(streamName, System.currentTimeMillis());
                            refreshSubscriptionSuccessCount.increment();
                        } else {
                            // cleanup stale subsEnvelope if we haven't seen a subscription refresh for
                            // subscriptionExpiryIntervalSec from the Mantis workers
                            cleanupStaleSubscriptions(streamName);
                            refreshSubscriptionFailedCount.increment();
                        }
                    } catch (Exception exc) {
                        LOG.info("refresh subscriptions failed for {} {}", streamName, jobCluster, exc);
                        refreshSubscriptionFailedCount.increment();
                    }
                } else {
                    LOG.debug("will not fetch subscriptions for un-registered stream {}", streamName);
                }
            }
            if (!subscriptionsFetchedForStream) {
                LOG.warn("No server side mappings found for one or more streams {} ", registeredStreams);
            }
        } else {
            LOG.debug("subscription refresh skipped (client enabled {} registered streams {})",
                    mantisPublishEnabled, registeredStreams);
        }
    }

    protected Set<String> getCurrentSubIds(String streamName) {
        return streamManager.getStreamSubscriptions(streamName).stream().map(Subscription::getSubscriptionId)
                .collect(Collectors.toSet());
    }
}
