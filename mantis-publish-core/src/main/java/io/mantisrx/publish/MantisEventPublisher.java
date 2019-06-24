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

import java.util.Optional;
import java.util.concurrent.BlockingQueue;

import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.metrics.StreamMetrics;


/**
 * An {@link EventPublisher} that publishes events into Mantis.
 */
public class MantisEventPublisher implements EventPublisher {

    private final MrePublishConfiguration mrePublishConfiguration;
    private final StreamManager streamManager;

    public MantisEventPublisher(MrePublishConfiguration mrePublishConfiguration,
                                StreamManager streamManager) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.streamManager = streamManager;
    }

    @Override
    public void publish(final Event event) {
        publish(StreamType.DEFAULT_EVENT_STREAM, event);
    }

    @Override
    public void publish(final String streamName, final Event event) {
        if (!isEnabled()) {
            return;
        }

        final Optional<BlockingQueue<Event>> streamQ = streamManager.createIfAbsentQueueForStream(streamName);

        if (streamQ.isPresent()) {
            final Optional<StreamMetrics> streamMetricsO = streamManager.getStreamMetrics(streamName);
            if (hasSubscriptions(streamName) || isTeeEnabled()) {
                boolean success = streamQ.get().offer(event);
                if (!success) {
                    streamMetricsO.ifPresent(m -> m.getMantisEventsDroppedCounter().increment());
                } else {
                    streamMetricsO.ifPresent(m -> m.getMantisEventsProcessedCounter().increment());
                }
            } else {
                // Don't enqueue the event if there are no active subscriptions for this stream.
                streamMetricsO.ifPresent(m -> {
                    m.getMantisActiveQueryCountGauge().set(0.0);
                    m.getMantisEventsSkippedCounter().increment();
                });
            }
        }
    }

    @Override
    public boolean hasSubscriptions(final String streamName) {
        return streamManager.hasSubscriptions(streamName);
    }

    private boolean isEnabled() {
        return mrePublishConfiguration.isMREClientEnabled();
    }

    private boolean isTeeEnabled() {
        return mrePublishConfiguration.isTeeEnabled();
    }
}
