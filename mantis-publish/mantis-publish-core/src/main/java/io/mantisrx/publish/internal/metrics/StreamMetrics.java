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

package io.mantisrx.publish.internal.metrics;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.CardinalityLimiters;
import com.netflix.spectator.impl.AtomicDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


public class StreamMetrics {

    /**
     * Bounds the number of distinct {@code subscriptionId} tag values registered for the
     * {@code mantisEventsFiltered} counter. Matches the limit used by {@code MqlEvalStage} on
     * the Mantis source-job side (Netflix-internal PR #949).
     */
    private static final int SUBSCRIPTION_ID_CARDINALITY_LIMIT = 50;

    private final String streamName;
    private final Registry registry;
    private final Function<String, String> subscriptionIdLimiter =
            CardinalityLimiters.mostFrequent(SUBSCRIPTION_ID_CARDINALITY_LIMIT);
    private final ConcurrentHashMap<String, Counter> mantisEventsFilteredCounters = new ConcurrentHashMap<>();

    private final Counter mantisEventsDroppedCounter;
    private final Counter mantisEventsDroppedProcessingExceptionCounter;
    private final Counter mantisEventsProcessedCounter;
    private final Counter mantisEventsSkippedCounter;
    private final Counter mantisQueryRejectedCounter;
    private final Counter mantisQueryFailedCounter;
    private final Counter mantisQueryProjectionFailedCounter;
    private final AtomicDouble mantisEventsQueuedGauge;
    private final AtomicDouble mantisActiveQueryCountGauge;
    private final Timer mantisEventsProcessTimeTimer;

    private final AtomicLong lastEventOnStreamTimestamp = new AtomicLong(0L);

    public StreamMetrics(Registry registry, final String streamName) {
        this.streamName = streamName;
        this.registry = registry;

        this.mantisEventsDroppedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisEventsDropped", "stream", streamName, "reason", "publisherQueueFull");
        this.mantisEventsDroppedProcessingExceptionCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisEventsDropped", "stream", streamName, "reason", "processingException");
        this.mantisEventsProcessedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisEventsProcessed", "stream", streamName);
        this.mantisEventsSkippedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisEventsSkipped", "stream", streamName);
        this.mantisQueryRejectedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisQueryRejected", "stream", streamName);
        this.mantisQueryFailedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisQueryFailed", "stream", streamName);
        this.mantisQueryProjectionFailedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisQueryProjectionFailed", "stream", streamName);
        this.mantisEventsQueuedGauge = SpectatorUtils.buildAndRegisterGauge(
                registry, "mantisEventsQueued", "stream", streamName);
        this.mantisActiveQueryCountGauge = SpectatorUtils.buildAndRegisterGauge(
                registry, "mantisActiveQueryCount", "stream", streamName);
        this.mantisEventsProcessTimeTimer = SpectatorUtils.buildAndRegisterTimer(
                registry, "mantisEventsProcessTime", "stream", streamName);

        updateLastEventOnStreamTimestamp();
    }

    public String getStreamName() {
        return streamName;
    }

    public Counter getMantisEventsDroppedCounter() {
        return mantisEventsDroppedCounter;
    }

    public Counter getMantisEventsDroppedProcessingExceptionCounter() {
        return mantisEventsDroppedProcessingExceptionCounter;
    }

    public Counter getMantisEventsProcessedCounter() {
        return mantisEventsProcessedCounter;
    }

    public Counter getMantisEventsSkippedCounter() {
        return mantisEventsSkippedCounter;
    }

    public Counter getMantisQueryRejectedCounter() {
        return mantisQueryRejectedCounter;
    }

    public Counter getMantisQueryFailedCounter() {
        return mantisQueryFailedCounter;
    }

    public Counter getMantisQueryProjectionFailedCounter() {
        return mantisQueryProjectionFailedCounter;
    }

    /**
     * Returns the per-subscription "events filtered out" counter for the given
     * subscription id. The subscriptionId tag value is cardinality-limited; ids
     * beyond the limit collapse into a single "--others--" bucket. Counters are
     * cached one per (limited) id for the lifetime of this StreamMetrics.
     *
     * @param subscriptionId the id of the subscription whose query did not match
     * @return a Spectator Counter tagged stream=<streamName>, subscriptionId=<limited id>
     */
    public Counter getMantisEventsFilteredCounter(String subscriptionId) {
        // CardinalityLimiters.mostFrequent(...) is backed by a non-thread-safe LinkedHashMap;
        // this class is shared across concurrently-processed events, so apply() must be
        // synchronized to avoid corrupting its internal LRU state.
        String limitedId;
        synchronized (subscriptionIdLimiter) {
            limitedId = subscriptionIdLimiter.apply(subscriptionId);
        }
        return mantisEventsFilteredCounters.computeIfAbsent(
                limitedId,
                lid -> SpectatorUtils.buildAndRegisterCounter(
                        registry, "mantisEventsFiltered", "stream", streamName, "subscriptionId", lid));
    }

    public AtomicDouble getMantisEventsQueuedGauge() {
        return mantisEventsQueuedGauge;
    }

    public AtomicDouble getMantisActiveQueryCountGauge() {
        return mantisActiveQueryCountGauge;
    }

    public Timer getMantisEventsProcessTimeTimer() {
        return mantisEventsProcessTimeTimer;
    }

    public void updateLastEventOnStreamTimestamp() {
        lastEventOnStreamTimestamp.set(System.nanoTime());
    }

    public long getLastEventOnStreamTimestamp() {
        return lastEventOnStreamTimestamp.get();
    }
}
