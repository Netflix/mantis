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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class processes a {@link Event} for a specific stream. Semantics of event processing are defined by
 * {@link EventProcessor#process(String, Event)}.
 * <p>
 * Default streams are defined in {@link StreamType}.
 */
class EventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EventProcessor.class);

    private final MrePublishConfiguration config;
    private final StreamManager streamManager;
    private final Tee tee;

    private final Random randomGenerator;
    private final AtomicBoolean errorLogEnabled;

    EventProcessor(MrePublishConfiguration config, StreamManager streamManager, Tee tee) {
        this.config = config;
        this.streamManager = streamManager;
        this.tee = tee;
        this.randomGenerator = new Random();
        this.errorLogEnabled = new AtomicBoolean(true);
    }

    /**
     * Processes an event for a stream.
     * <p>
     * Event Processing:
     * <p>
     * 1. Mask sensitive fields in the event as defined by {@link MrePublishConfiguration#blackListedKeysCSV()}.
     * 2. Check in-memory cache of {@link Subscription}s to find subscriptions whose query match the event.
     * 3. Build a *superset* of fields from *all* matching subscriptions into a single event.o
     *
     * @return a {@link MantisEvent} which is an internal representation (specific to MRE) of the {@link Event}.
     */
    public Event process(String stream, Event event) {
        LOG.debug("Entering EventProcessor#onNext: {}", event);

        boolean isEnabled = config.isMREClientEnabled();
        if (!isEnabled) {
            LOG.debug("Mantis Realtime Events Publisher is disabled."
                    + "Set the property defined in your MrePublishConfiguration object to true to enable.");
            return null;
        }

        // make a deep copy before proceeding to avoid altering the user provided map.
        if (config.isDeepCopyEventMapEnabled()) {
            event = new Event(event.getMap(), true);
        }

        maskSensitiveFields(event);

        if (config.isTeeEnabled()) {
            tee.tee(config.teeStreamName(), event);
        }

        List<Subscription> matchingSubscriptions = new ArrayList<>();
        if (streamManager.hasSubscriptions(stream)) {
            final Set<Subscription> streamSubscriptions = streamManager.getStreamSubscriptions(stream);

            for (Subscription s : streamSubscriptions) {
                try {
                    if (s.matches(event)) {
                        matchingSubscriptions.add(s);
                    }
                } catch (Exception e) {
                    streamManager.getStreamMetrics(stream)
                            .ifPresent(m -> m.getMantisQueryFailedCounter().increment());

                    // Send errors only for a sample of events.
                    int rndNo = randomGenerator.nextInt(1_000_000);
                    if (rndNo < 10) {
                        sendError(s, e.getMessage());
                    }
                }
            }
        }

        Event projectedEvent = null;
        if (!matchingSubscriptions.isEmpty()) {
            projectedEvent = projectSupersetEvent(stream, matchingSubscriptions, event);
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("no matching subscriptions");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit EventProcessor#onNext: {}", event);
        }

        return projectedEvent;
    }

    /**
     * Masks fields of an {@link Event} contained in a blacklist.
     */
    void maskSensitiveFields(Event event) {
        String blacklistKeys = config.blackListedKeysCSV();
        List<String> blacklist =
                Arrays.stream(blacklistKeys.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());

        blacklist.stream()
                .filter(key -> event.get(key) != null)
                .forEach(key -> event.set(key, "***"));
    }

    private void sendError(Subscription subscription, String errorMessage) {
        // TODO
        //        String clientId = subIdToClientIdMap.get(subscription.getSubscriptionId());
        //        final List<String> subscriptionIds = Collections.singletonList(subscription.getSubscriptionId());
        //
        //        final Event errorEvent = new Event();
        //        errorEvent.set("msg", errorMessage);
        //        errorEvent.set("matched-clients", subscriptionIds.toString());
        //
        //        PublishSubject<Event> subject = clientIdToClientSessionMap.get(clientId);
        //        if (subject != null) {
        //            subject.onNext(errorEvent);
        //        } else {
        //            if (LOG.isDebugEnabled()) {
        //                LOG.debug("no client session for client {}", clientId);
        //            }
        //        }
    }

    private Event projectSupersetEvent(final String streamName,
                                             final List<Subscription> matchingSubscriptions,
                                             final Event event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter EventProcessor#projectSupersetEvent {}  event: {}", matchingSubscriptions, event);
        }

        Event projectedEvent = null;
        try {
            if (!matchingSubscriptions.isEmpty()) {
                projectedEvent = matchingSubscriptions.get(0).projectSuperset(matchingSubscriptions, event);
            }
        } catch (Exception e) {
            // Log only the first error so as to avoid flooding the log files.
            if (errorLogEnabled.get()) {
                String queries = matchingSubscriptions.stream()
                        .map(Subscription::getRawQuery)
                        .collect(Collectors.joining(", "));
                LOG.error("Failed to project Event {} for queries: {}", event, queries);
                errorLogEnabled.set(false);
            }
            streamManager.getStreamMetrics(streamName).ifPresent(m ->
                    m.getMantisQueryProjectionFailedCounter().increment());
        }

        Event augmentedEvent = null;
        if (projectedEvent != null && !projectedEvent.isEmpty()) {
            augmentedEvent = enrich(projectedEvent, streamName, matchingSubscriptions);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Projected event is empty. skipping");
            }
        }

        return augmentedEvent;
    }

    private Event enrich(Event projectedEvent,
                         String streamName,
                         List<Subscription> matchingSubscriptions) {
        projectedEvent.set("type", "EVENT");
        projectedEvent.set("mantisStream", streamName);

        List<String> subIdList = new ArrayList<>(matchingSubscriptions.size());
        for (Subscription res : matchingSubscriptions) {
            subIdList.add(res.getSubscriptionId());
        }
        projectedEvent.set("matched-clients", subIdList);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated event string: {}", projectedEvent);
        }

        return projectedEvent;
    }
}
