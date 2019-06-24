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

import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.impl.AtomicDouble;


public class StreamMetrics {

    private final String streamName;

    private final Counter mantisEventsDroppedCounter;
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

        this.mantisEventsDroppedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "eventsDropped", "stream", streamName, "reason", "publisherQueueFull");
        this.mantisEventsProcessedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisStreamEventsProcessed", "stream", streamName);
        this.mantisEventsSkippedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisStreamEventsSkippedCounter", "stream", streamName);
        this.mantisQueryRejectedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisStreamQueryRejectedCounter", "stream", streamName);
        this.mantisQueryFailedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisStreamQueryFailedCounter", "stream", streamName);
        this.mantisQueryProjectionFailedCounter = SpectatorUtils.buildAndRegisterCounter(
                registry, "mantisStreamQueryProjectionFailedCounter", "stream", streamName);
        this.mantisEventsQueuedGauge = SpectatorUtils.buildAndRegisterGauge(
                registry, "mantisStreamEventsQueued", "stream", streamName);
        this.mantisActiveQueryCountGauge = SpectatorUtils.buildAndRegisterGauge(
                registry, "mantisStreamActiveQueryCount", "stream", streamName);
        this.mantisEventsProcessTimeTimer = SpectatorUtils.buildAndRegisterTimer(
                registry, "mantisStreamEventsProcessTime", "stream", streamName);

        updateLastEventOnStreamTimestamp();
    }

    public String getStreamName() {
        return streamName;
    }

    public Counter getMantisEventsDroppedCounter() {
        return mantisEventsDroppedCounter;
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
