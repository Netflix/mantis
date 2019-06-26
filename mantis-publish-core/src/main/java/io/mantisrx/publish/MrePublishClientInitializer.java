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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the Mantis Realtime Events Publisher and its internal components.
 * <p>
 * Components:
 * <p>
 * 1. {@link MantisEventPublisher} is the user-facing object for publishing events into Mantis.
 * 2. {@link SubscriptionTracker} maintains and updates a list of scheduledFutures.
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

    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
    private static final ScheduledThreadPoolExecutor DRAINER_EXECUTOR =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "MantisDrainer"));
    private static final ScheduledThreadPoolExecutor SUBSCRIPTIONS_EXECUTOR =
            new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "MantisSubscriptionsTracker"));

    static {
        DRAINER_EXECUTOR.setRemoveOnCancelPolicy(true);
        SUBSCRIPTIONS_EXECUTOR.setRemoveOnCancelPolicy(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            DRAINER_EXECUTOR.shutdown();
            SUBSCRIPTIONS_EXECUTOR.shutdown();
        }));
    }

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
        this.scheduledFutures.add(setupSubscriptionTracker(subscriptionsTracker));
        this.scheduledFutures.add(setupDrainer(streamManager, eventTransmitter, tee));
    }

    /**
     * Safely shuts down internal components for the Mantis Realtime Events Publisher.
     */
    public void stop() {
        Iterator<ScheduledFuture<?>> iterator = scheduledFutures.iterator();
        while (iterator.hasNext()) {
            ScheduledFuture<?> next = iterator.next();
            if (next != null && !next.isCancelled()) {
                next.cancel(false);
            }
        }
        scheduledFutures.clear();
    }

    public EventPublisher getEventPublisher() {
        return eventPublisher;
    }

    private ScheduledFuture<?> setupDrainer(StreamManager streamManager, EventTransmitter transmitter, Tee tee) {
        EventProcessor eventProcessor = new EventProcessor(config, streamManager, tee);
        EventDrainer eventDrainer =
                new EventDrainer(config, streamManager, registry, eventProcessor, transmitter, Clock.systemUTC());

        return DRAINER_EXECUTOR.scheduleAtFixedRate(eventDrainer::run,
                0, config.drainerIntervalMsec(), TimeUnit.MILLISECONDS);
    }

    private ScheduledFuture<?> setupSubscriptionTracker(SubscriptionTracker subscriptionsTracker) {
        return SUBSCRIPTIONS_EXECUTOR.scheduleAtFixedRate(() -> {
            try {
                subscriptionsTracker.refreshSubscriptions();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("failed to refresh scheduledFutures", e);
                }
            }
        }, 1, config.subscriptionRefreshIntervalSec(), TimeUnit.SECONDS);
    }
}
