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

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.internal.metrics.StreamMetrics;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamManager {

    private static final Logger LOG = LoggerFactory.getLogger(StreamManager.class);

    private final Registry registry;
    private final MrePublishConfiguration config;

    private final Counter mantisStreamCreateFailed;

    private final ConcurrentMap<String, ConcurrentSkipListSet<Subscription>> streamSubscriptionsMap;
    private final ConcurrentMap<String, List<String>> subscriptionIdToStreamsMap;
    private final ConcurrentMap<String, BlockingQueue<Event>> streamQueuesMap;
    private final ConcurrentMap<String, StreamMetrics> streamMetricsMap;

    public StreamManager(Registry registry, MrePublishConfiguration mrePublishConfiguration) {
        this.registry = registry;
        this.config = mrePublishConfiguration;

        this.mantisStreamCreateFailed =
                SpectatorUtils.buildAndRegisterCounter(this.registry, "mantisStreamCreateFailed");

        this.streamSubscriptionsMap = new ConcurrentHashMap<>();
        this.subscriptionIdToStreamsMap = new ConcurrentHashMap<>();
        this.streamQueuesMap = new ConcurrentHashMap<>();
        this.streamMetricsMap = new ConcurrentHashMap<>();
    }

    Optional<BlockingQueue<Event>> registerStream(
            final String streamName) {

        if (!streamQueuesMap.containsKey(streamName)) {
            cleanupInactiveStreamQueues();

            if (streamQueuesMap.keySet().size() >= config.maxNumStreams()) {
                LOG.debug("failed to create queue for stream {} (MAX_NUM_STREAMS {} exceeded)",
                        streamName,
                        config.maxNumStreams());
                mantisStreamCreateFailed.increment();
                return Optional.empty();
            }

            int qSize = config.streamQueueSize(streamName);

            LOG.info("creating queue for stream {} (size: {})", streamName, qSize);
            synchronized (streamQueuesMap) {
                streamQueuesMap.putIfAbsent(streamName, new LinkedBlockingQueue<>(qSize));
                // Stream metrics are created and registered only after
                // an app tries to emit an event to that stream.
                // Having a subscription for a stream does not create the StreamMetrics.
                streamMetricsMap.putIfAbsent(streamName, new StreamMetrics(registry, streamName));
            }
        }

        return Optional.ofNullable(streamQueuesMap.get(streamName));
    }

    private boolean isStreamInactive(long lastEventTs) {
        // Consider Stream inactive if no events seen on the stream
        // for over 1 day (default FP value).
        long secondsSinceLastEvent = TimeUnit.SECONDS.convert(
                System.nanoTime() - lastEventTs,
                TimeUnit.NANOSECONDS);

        return secondsSinceLastEvent > config.streamInactiveDurationThreshold();
    }

    private void cleanupInactiveStreamQueues() {
        List<String> streamsToRemove = new ArrayList<>(5);
        streamQueuesMap.keySet().stream()
                .forEach(streamName ->
                        getStreamMetrics(streamName).ifPresent(m -> {
                            long lastEventTs = m.getLastEventOnStreamTimestamp();
                            if (lastEventTs != 0 && isStreamInactive(lastEventTs)) {
                                streamsToRemove.add(streamName);
                            }
                        })
                );

        streamsToRemove.stream().forEach(stream -> {
            synchronized (streamQueuesMap) {
                streamQueuesMap.remove(stream);
                streamMetricsMap.remove(stream);
            }
        });
    }

    Optional<BlockingQueue<Event>> getQueueForStream(final String streamName) {
        return Optional.ofNullable(streamQueuesMap.get(streamName));
    }

    Optional<StreamMetrics> getStreamMetrics(final String streamName) {
        return Optional.ofNullable(streamMetricsMap.get(streamName));
    }

    boolean hasSubscriptions(final String streamName) {
        return Optional.ofNullable(streamSubscriptionsMap.get(streamName))
                .map(subs -> !subs.isEmpty())
                .orElse(false);
    }

    private List<String> sanitizeStreamSubjects(final List<String> subjects) {
        return subjects.stream()
                .map(s -> {
                    if (s.toLowerCase().equals("observable") ||
                            s.toLowerCase().equals("stream")) {
                        // Translate the legacy default stream names to map to the default stream.
                        return StreamType.DEFAULT_EVENT_STREAM;
                    } else {
                        return s;
                    }
                }).collect(Collectors.toList());
    }

    private void handleDuplicateSubscriptionId(final Subscription sub) {
        String subId = sub.getSubscriptionId();
        Optional.ofNullable(subscriptionIdToStreamsMap.get(subId))
                .ifPresent(streams -> removeSubscriptionId(streams, subId));
    }

    synchronized void addStreamSubscription(final Subscription sub) {
        List<String> streams = sanitizeStreamSubjects(sub.getSubjects());
        LOG.info("adding subscription {} with streams {}", sub, streams);

        handleDuplicateSubscriptionId(sub);

        for (String stream : streams) {
            streamSubscriptionsMap.putIfAbsent(stream, new ConcurrentSkipListSet<>());

            ConcurrentSkipListSet<Subscription> subs = streamSubscriptionsMap.get(stream);
            int maxSubs = config.maxSubscriptions(stream);

            // Remove any existing subs with same subscriptionId (if any).
            subs.removeIf(s -> s.getSubscriptionId().equals(sub.getSubscriptionId()));

            subs.add(sub);
            int numSubs = subs.size();

            if (numSubs > maxSubs) {
                // Cleanup any subscriptions we might have added to another stream before hitting
                // the limit on subs for this stream.
                removeStreamSubscription(sub);
                LOG.warn("QUERY SUBSCRIPTION REJECTED: Number of subscriptions for stream {} exceeded max {}. " +
                                "Increase (default mre.publish.max.subscriptions.per.stream.default or " +
                                " mre.publish.max.subscriptions.stream.<streamName>) to allow more queries. Removed {}",
                         stream, maxSubs, sub);
                getStreamMetrics(stream).ifPresent(m ->
                        m.getMantisQueryRejectedCounter().increment());
            }

            getStreamMetrics(stream).ifPresent(m ->
                    m.getMantisActiveQueryCountGauge().set((double) numSubs));
        }

        subscriptionIdToStreamsMap.put(sub.getSubscriptionId(), streams);
    }

    private void removeSubscriptionId(final List<String> streams, final String subscriptionId) {
        for (String stream : streams) {
            if (streamSubscriptionsMap.containsKey(stream)) {
                final ConcurrentSkipListSet<Subscription> subs =
                        streamSubscriptionsMap.get(stream);

                if (subs != null) {
                    subs.removeIf(sub -> sub.getSubscriptionId().equals(subscriptionId));

                    getStreamMetrics(stream).ifPresent(m ->
                            m.getMantisActiveQueryCountGauge().set((double) subs.size()));

                    if (subs.isEmpty()) {
                        streamSubscriptionsMap.remove(stream);
                    }
                }
            }
        }
    }

    synchronized boolean removeStreamSubscription(final String subscriptionId) {
        LOG.info("removing subscription {}", subscriptionId);
        List<String> streams =
                subscriptionIdToStreamsMap.getOrDefault(subscriptionId, Collections.emptyList());
        removeSubscriptionId(streams, subscriptionId);
        subscriptionIdToStreamsMap.remove(subscriptionId);

        return true;
    }

    synchronized boolean removeStreamSubscription(final Subscription sub) {
        LOG.info("removing subscription {}", sub);
        final List<String> streams = sanitizeStreamSubjects(sub.getSubjects());
        removeSubscriptionId(streams, sub.getSubscriptionId());
        subscriptionIdToStreamsMap.remove(sub.getSubscriptionId());

        return true;
    }

    /**
     * List of the all the subscriptions for a stream name regardless of whether events are published to it or not
     *
     * @param streamName
     *
     * @return
     */
    Set<Subscription> getStreamSubscriptions(final String streamName) {
        return streamSubscriptionsMap.getOrDefault(streamName, new ConcurrentSkipListSet<>());
    }

    /**
     * Returns a list of all stream names registered from MantisEventPublisher
     *
     * @return
     */
    Set<String> getRegisteredStreams() {
        return streamQueuesMap.keySet();
    }
}
