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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;
import rx.schedulers.Schedulers;


/**
 * Initializes the Mantis Realtime Events Publisher and its internal components.
 * <p>
 * Components:
 * <p>
 * 1. {@link MantisEventPublisher} is the user-facing object for publishing events into Mantis.
 * 2. {@link SubscriptionTracker} maintains and updates a list of subscriptions.
 * 3. {@link MantisJobDiscovery} maintains and updates a list of Mantis Jobs and their workers.
 * <p>
 * This class has several configuration options. See {@link MrePublishConfiguration} for more information on
 * how to set configuration options and defaults.
 */
public class MrePublishClientInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(MrePublishClientInitializer.class);

    private final MrePublishConfiguration config;
    private final Registry registry;
    private final StreamManager streamManager;
    private final EventPublisher eventPublisher;
    private final SubscriptionTracker subscriptionsTracker;
    private final EventTransmitter eventTransmitter;
    private final Tee tee;

    private final List<Subscription> subscriptions = new ArrayList<>();

    public MrePublishClientInitializer(
            MrePublishConfiguration config,
            Registry registry,
            StreamManager streamManager,
            EventPublisher eventPublisher,
            SubscriptionTracker subscriptionsTracker,
            EventTransmitter eventTransmitter,
            Tee tee) {

        this.config = config;
        this.registry = registry;
        this.streamManager = streamManager;
        this.eventPublisher = eventPublisher;
        this.subscriptionsTracker = subscriptionsTracker;
        this.eventTransmitter = eventTransmitter;
        this.tee = tee;
    }

    /**
     * Starts internal components for the Mantis Realtime Events Publisher.
     */
    public void start() {
        this.subscriptions.add(setupSubscriptionTracker(subscriptionsTracker));
        this.subscriptions.add(setupDrainer(streamManager, eventTransmitter, tee));
    }

    /**
     * Safely shuts down internal components for the Mantis Realtime Events Publisher.
     */
    public void stop() {
        Iterator<Subscription> iterator = subscriptions.iterator();
        while (iterator.hasNext()) {
            Subscription sub = iterator.next();
            if (sub != null && !sub.isUnsubscribed()) {
                sub.unsubscribe();
            }
        }
        subscriptions.clear();
    }

    public EventPublisher getEventPublisher() {
        return eventPublisher;
    }

    private Subscription setupDrainer(StreamManager streamManager, EventTransmitter transmitter, Tee tee) {
        EventProcessor eventProcessor = new EventProcessor(config, streamManager, tee);
        EventDrainer eventDrainer =
                new EventDrainer(config, streamManager, registry, eventProcessor, transmitter, Clock.systemUTC());

        return Schedulers
                .newThread()
                .createWorker()
                .schedulePeriodically(eventDrainer::run, 1, config.drainerIntervalMsec(), TimeUnit.MILLISECONDS);
    }

    private Subscription setupSubscriptionTracker(SubscriptionTracker subscriptionsTracker) {
        final AtomicLong subscriptionsRefreshTimeMs = new AtomicLong(System.currentTimeMillis());
        final int subRefreshIntervalMs = config.subscriptionRefreshIntervalSec() * 1000;

        return Schedulers
                .newThread()
                .createWorker()
                .schedulePeriodically(() -> {
                    try {
                        if ((System.currentTimeMillis() - subscriptionsRefreshTimeMs.get()) > subRefreshIntervalMs) {
                            subscriptionsTracker.refreshSubscriptions();
                            subscriptionsRefreshTimeMs.set(System.currentTimeMillis());
                        }
                    } catch (Exception e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("failed to refresh subscriptions", e);
                        }
                    }
                }, 1, 1, TimeUnit.SECONDS);
    }
}
