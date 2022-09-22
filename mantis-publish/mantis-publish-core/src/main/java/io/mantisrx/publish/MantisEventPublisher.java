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

import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.api.PublishStatus;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.metrics.StreamMetrics;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link EventPublisher} that publishes events into Mantis.
 */
public class MantisEventPublisher implements EventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(MantisEventPublisher.class);

    private final MrePublishConfiguration mrePublishConfiguration;
    private final StreamManager streamManager;

    public MantisEventPublisher(MrePublishConfiguration mrePublishConfiguration,
                                StreamManager streamManager) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.streamManager = streamManager;
    }

    @Override
    public CompletionStage<PublishStatus> publish(final String streamName, final Event event) {

        if (!isEnabled()) {
            return CompletableFuture.completedFuture(PublishStatus.SKIPPED_CLIENT_NOT_ENABLED);
        }

        if (event == null || event.isEmpty()) {
            return CompletableFuture.completedFuture(PublishStatus.SKIPPED_INVALID_EVENT);
        }

        final Optional<BlockingQueue<Event>> streamQ = streamManager.registerStream(streamName);

        if (streamQ.isPresent()) {
            final Optional<StreamMetrics> streamMetricsO = streamManager.getStreamMetrics(streamName);
            if (hasSubscriptions(streamName) || isTeeEnabled()) {
                boolean success = streamQ.get().offer(event);
                if (!success) {
                    streamMetricsO.ifPresent(m -> m.getMantisEventsDroppedCounter().increment());
                    return CompletableFuture.completedFuture(PublishStatus.FAILED_QUEUE_FULL);
                } else {
                    streamMetricsO.ifPresent(m -> m.getMantisEventsProcessedCounter().increment());
                    // TODO - propagate a Promise of PublishStatus with the Event and update status async after network send to Mantis
                    return CompletableFuture.completedFuture(PublishStatus.ENQUEUED);
                }
            } else {
                // Don't enqueue the event if there are no active subscriptions for this stream.
                streamMetricsO.ifPresent(m -> {
                    m.getMantisActiveQueryCountGauge().set(0.0);
                    m.getMantisEventsSkippedCounter().increment();
                });
                return CompletableFuture.completedFuture(PublishStatus.SKIPPED_NO_SUBSCRIPTIONS);
            }
        } else {
            // failed to register stream, this could happen if max stream limit is exceeded
            return CompletableFuture.completedFuture(PublishStatus.FAILED_STREAM_NOT_REGISTERED);
        }
    }

    @Override
    public boolean hasSubscriptions(final String streamName) {
        if (!isEnabled()) {
            LOG.debug("Mantis publish client is not enabled");
            return false;
        }
        // register this stream for tracking subscriptions
        streamManager.registerStream(streamName);
        return streamManager.hasSubscriptions(streamName);
    }

    private boolean isEnabled() {
        return mrePublishConfiguration.isMREClientEnabled();
    }

    private boolean isTeeEnabled() {
        return mrePublishConfiguration.isTeeEnabled();
    }
}
