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

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


class EventDrainer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(EventDrainer.class);

    public static final String LOGGING_CONTEXT_KEY = "mantisLogCtx";
    static final String DEFAULT_THREAD_NAME = "mantisDrainer";
    public static final String LOGGING_CONTEXT_VALUE = DEFAULT_THREAD_NAME;

    private final MrePublishConfiguration config;
    private final Timer mantisEventDrainTimer;
    private final StreamManager streamManager;
    private final EventProcessor eventProcessor;
    private final EventTransmitter eventTransmitter;
    private final Clock clock;


    EventDrainer(MrePublishConfiguration config,
                 StreamManager streamManager,
                 Registry registry,
                 EventProcessor eventProcessor,
                 EventTransmitter eventTransmitter,
                 Clock clock) {
        this.config = config;
        this.mantisEventDrainTimer =
                SpectatorUtils.buildAndRegisterTimer(registry, "mrePublishEventDrainTime");

        this.streamManager = streamManager;
        this.eventProcessor = eventProcessor;
        this.eventTransmitter = eventTransmitter;
        this.clock = clock;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting drainer thread.");
        }
    }

    @Override
    public void run() {
        try {
            MDC.put(LOGGING_CONTEXT_KEY, LOGGING_CONTEXT_VALUE);
            final long startTime = clock.millis();
            Set<String> streams = streamManager.getRegisteredStreams();

            for (String stream : streams) {
                final List<Event> streamEventList = new ArrayList<>();

                int queueDepth = 0;
                try {
                    final Optional<BlockingQueue<Event>> streamQueueO = streamManager.getQueueForStream(stream);

                    if (streamQueueO.isPresent()) {
                        BlockingQueue<Event> streamEventQ = streamQueueO.get();
                        streamEventQ.drainTo(streamEventList);

                        queueDepth = streamEventList.size();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Queue drained size: {} for stream {}", queueDepth, stream);
                        }

                        final int finalQueueDepth = queueDepth;
                        streamManager.getStreamMetrics(stream)
                                .ifPresent(m -> {
                                    m.getMantisEventsQueuedGauge().set((double) finalQueueDepth);
                                    if (finalQueueDepth > 0) {
                                        m.updateLastEventOnStreamTimestamp();
                                    }
                                });

                        streamEventList.stream()
                                .map(e -> process(stream, e))
                                .filter(Objects::nonNull)
                                .forEach(e -> eventTransmitter.send(e, stream));
                        streamEventList.clear();
                    }
                } catch (Exception e) {
                    LOG.warn("Exception processing events for stream {}", stream, e);
                    final int finalQueueDepth = queueDepth;
                    streamManager.getStreamMetrics(stream)
                            .ifPresent(m -> {
                                m.getMantisEventsDroppedProcessingExceptionCounter().increment(finalQueueDepth);
                            });
                }
            }
            final long processingTime = clock.millis() - startTime;
            mantisEventDrainTimer.record(processingTime, TimeUnit.MILLISECONDS);
        } finally {
            MDC.remove(LOGGING_CONTEXT_KEY);
        }
    }

    private Event process(String stream, Event event) {
        final long startTime = clock.millis();
        Event processedEvent = eventProcessor.process(stream, event);

        final long processingTime = clock.millis() - startTime;
        streamManager.getStreamMetrics(stream)
                .ifPresent(m -> m.getMantisEventsProcessTimeTimer().record(processingTime, TimeUnit.MILLISECONDS));

        return processedEvent;
    }
}
