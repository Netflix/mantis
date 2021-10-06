/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.publish.providers;

import com.netflix.spectator.api.Registry;
import io.mantisrx.publish.EventTransmitter;
import io.mantisrx.publish.MrePublishClientInitializer;
import io.mantisrx.publish.StreamManager;
import io.mantisrx.publish.SubscriptionTracker;
import io.mantisrx.publish.Tee;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.config.MrePublishConfiguration;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Provides an instance of a Mantis {@link MrePublishClientInitializer}.
 *
 * This class can be created standalone via its constructor or created with any injector implementation.
 */
@Singleton
public class MrePublishClientInitializerProvider implements Provider<MrePublishClientInitializer> {

    private final MrePublishConfiguration config;

    private final Registry registry;

    private final StreamManager streamManager;

    private final EventPublisher eventPublisher;

    private final SubscriptionTracker subscriptionTracker;

    private final EventTransmitter eventTransmitter;

    private final Tee tee;

    @Inject
    public MrePublishClientInitializerProvider(
            MrePublishConfiguration config,
            Registry registry,
            StreamManager streamManager,
            EventPublisher eventPublisher,
            SubscriptionTracker subscriptionTracker,
            EventTransmitter eventTransmitter,
            Tee tee) {
        this.config = config;
        this.registry = registry;
        this.streamManager = streamManager;
        this.eventPublisher = eventPublisher;
        this.subscriptionTracker = subscriptionTracker;
        this.eventTransmitter = eventTransmitter;
        this.tee = tee;
    }

    @Override
    @Singleton
    public MrePublishClientInitializer get() {
        MrePublishClientInitializer clientInitializer =
                new MrePublishClientInitializer(
                        config,
                        registry,
                        streamManager,
                        eventPublisher,
                        subscriptionTracker,
                        eventTransmitter,
                        tee);

        clientInitializer.start();

        return clientInitializer;
    }
}
