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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.netflix.mantis.discovery.proto.StreamJobClusterMap;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.core.SubscriptionFactory;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractSubscriptionTracker implements SubscriptionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSubscriptionTracker.class);
    private static final MantisServerSubscriptionEnvelope DEFAULT_EMPTY_SUB_ENVELOPE = new MantisServerSubscriptionEnvelope(Collections.emptyList());

    private final MrePublishConfiguration mrePublishConfiguration;
    private final Registry registry;
    private final StreamManager streamManager;
    private final Counter refreshSubscriptionInvokedCount;
    private final Counter refreshSubscriptionSuccessCount;
    private final Counter refreshSubscriptionFailedCount;
    private final Counter staleSubscriptionRemovedCount;

    private volatile Map<String, StreamSubscriptions> previousSubscriptions = new HashMap<>();

    public AbstractSubscriptionTracker(MrePublishConfiguration mrePublishConfiguration,
                                      Registry registry,
                                      StreamManager streamManager) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.registry = registry;
        this.streamManager = streamManager;
        this.refreshSubscriptionInvokedCount = SpectatorUtils.buildAndRegisterCounter(registry, "refreshSubscriptionInvokedCount");
        this.refreshSubscriptionSuccessCount = SpectatorUtils.buildAndRegisterCounter(registry, "refreshSubscriptionSuccessCount");
        this.refreshSubscriptionFailedCount = SpectatorUtils.buildAndRegisterCounter(registry, "refreshSubscriptionFailedCount");
        this.staleSubscriptionRemovedCount = SpectatorUtils.buildAndRegisterCounter(registry, "staleSubscriptionRemovedCount");
    }



    void propagateSubscriptionChanges(Set<MantisServerSubscription> prev, Set<MantisServerSubscription> curr) {
        Set<MantisServerSubscription> prevSubsNotInCurr = new HashSet<>(prev);
        prevSubsNotInCurr.removeAll(curr);
        prevSubsNotInCurr.stream().forEach(subToRemove -> {
            try {
                Optional<Subscription> subscription = SubscriptionFactory.getSubscription(subToRemove.getSubscriptionId(), subToRemove.getQuery());
                if (subscription.isPresent()) {
                    streamManager.removeStreamSubscription(subscription.get());
                } else {
                    LOG.warn("unexpected to find invalid subscription to remove {}", subToRemove);
                }
            } catch (Throwable t) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("failed to remove subscription {}", subToRemove, t);
                }
            }
        });

        Set<MantisServerSubscription> currSubsNotInPrev = new HashSet<>(curr);
        currSubsNotInPrev.removeAll(prev);
        currSubsNotInPrev.stream().forEach(subToAdd -> {
            try {
                Optional<Subscription> subscription = SubscriptionFactory.getSubscription(subToAdd.getSubscriptionId(), subToAdd.getQuery());
                if (subscription.isPresent()) {
                    streamManager.addStreamSubscription(subscription.get());
                } else {
                    LOG.info("will not add invalid subscription {}", subToAdd);
                }
            } catch (Throwable t) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("failed to add subscription {}", subToAdd, t);
                }
            }
        });
    }



    private void cleanupStaleSubscriptions(String streamName) {
        StreamSubscriptions streamSubscriptions = previousSubscriptions.get(streamName);
        if (streamSubscriptions != null) {
            boolean hasStaleSubscriptionsData = (System.currentTimeMillis() - streamSubscriptions.getCreateTimeMs()) > mrePublishConfiguration.subscriptionExpiryIntervalSec() * 1000;
            if (hasStaleSubscriptionsData) {
                LOG.info("removing stale subscriptions data for stream {} ({} created {})", streamName, streamSubscriptions.getSubsEnvelope(), streamSubscriptions.getCreateTimeMs());
                staleSubscriptionRemovedCount.increment();
                StreamSubscriptions removedSubs = previousSubscriptions.remove(streamName);
                propagateSubscriptionChanges(removedSubs.getSubsEnvelope().getSubscriptions(), Collections.emptySet());
            }
        }
    }

    /**
     * Get current set of subscriptions for a streamName for given jobCluster
     * @param streamName name of MRE stream
     * @param jobCluster Mantis Job Cluster name
     * @return Optional of MantisServerSubscriptionEnvelope on successful retrieval, else empty
     */
    public abstract Optional<MantisServerSubscriptionEnvelope> fetchSubscriptions(String streamName, String jobCluster);

    @Override
    public void refreshSubscriptions() {
        refreshSubscriptionInvokedCount.increment();
        // refresh subscriptions only if the Publish client is enabled and has streams registered by MantisEventPublisher
        boolean mantisPublishEnabled = mrePublishConfiguration.isMREClientEnabled();
        Set<String> registeredStreams = streamManager.getRegisteredStreams();
        boolean subscriptionsFetchedForStream = false;
        if (mantisPublishEnabled && !registeredStreams.isEmpty()) {
            Map<String, String> streamJobClusterMap = mrePublishConfiguration.streamNameToJobClusterMapping();
            for (Map.Entry<String, String> e : streamJobClusterMap.entrySet()) {
                String streamName = e.getKey();
                LOG.debug("processing stream {} and currently registered Streams", streamName, registeredStreams);
                if (registeredStreams.contains(streamName) || StreamJobClusterMap.DEFAULT_STREAM_KEY.equals(streamName)) {
                    subscriptionsFetchedForStream = true;
                    String jobCluster = e.getValue();
                    try {
                        Optional<MantisServerSubscriptionEnvelope> subsEnvelopeO = fetchSubscriptions(streamName, jobCluster);
                        if (subsEnvelopeO.isPresent()) {
                            MantisServerSubscriptionEnvelope subsEnvelope = subsEnvelopeO.get();
                            propagateSubscriptionChanges(previousSubscriptions
                                            .getOrDefault(streamName, new StreamSubscriptions(streamName, DEFAULT_EMPTY_SUB_ENVELOPE))
                                            .getSubsEnvelope().getSubscriptions(),
                                    subsEnvelope.getSubscriptions());
                            LOG.debug("{} subscriptions updated to {}", streamName, subsEnvelope);
                            previousSubscriptions.put(streamName, new StreamSubscriptions(streamName, subsEnvelope));
                            refreshSubscriptionSuccessCount.increment();
                        } else {
                            // cleanup stale subsEnvelope if we haven't seen a subscription refresh for subscriptionExpiryIntervalSec from the Mantis workers
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
            if(!subscriptionsFetchedForStream) {
                LOG.warn("No server side mappings found for one or more streams {} ", registeredStreams);
            }
        } else {
            LOG.debug("subscription refresh skipped (client enabled {} registered streams {})", mantisPublishEnabled, registeredStreams);
        }
    }

    public Optional<MantisServerSubscriptionEnvelope> getCurrentSubs(String stream) {
        return Optional
                .ofNullable(previousSubscriptions.get(stream))
                .map(StreamSubscriptions::getSubsEnvelope);
    }

    static class StreamSubscriptions {

        private final String streamName;
        private final MantisServerSubscriptionEnvelope subsEnvelope;
        private transient final long createTimeMs;

        public StreamSubscriptions(final String streamName, final MantisServerSubscriptionEnvelope subsEnvelope) {
            this.streamName = streamName;
            this.subsEnvelope = subsEnvelope;
            this.createTimeMs = System.currentTimeMillis();
        }

        public String getStreamName() {
            return streamName;
        }

        public MantisServerSubscriptionEnvelope getSubsEnvelope() {
            return subsEnvelope;
        }

        public long getCreateTimeMs() {
            return createTimeMs;
        }
    }
}
