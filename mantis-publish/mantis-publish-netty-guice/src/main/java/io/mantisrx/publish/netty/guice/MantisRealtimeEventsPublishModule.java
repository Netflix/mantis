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

package io.mantisrx.publish.netty.guice;

import com.google.inject.AbstractModule;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.publish.DefaultSubscriptionTracker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.EventTransmitter;
import io.mantisrx.publish.MrePublishClientInitializer;
import io.mantisrx.publish.StreamManager;
import io.mantisrx.publish.SubscriptionTracker;
import io.mantisrx.publish.Tee;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.netty.pipeline.HttpEventChannel;
import io.mantisrx.publish.netty.pipeline.HttpEventChannelManager;
import io.mantisrx.publish.netty.transmitters.ChoiceOfTwoEventTransmitter;
import io.mantisrx.publish.providers.EventPublisherProvider;
import io.mantisrx.publish.providers.MantisJobDiscoveryProvider;
import io.mantisrx.publish.providers.MrePublishClientInitializerProvider;
import io.mantisrx.publish.providers.StreamManagerProvider;
import io.mantisrx.publish.providers.TeeProvider;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;


public class MantisRealtimeEventsPublishModule extends AbstractModule {

    @Singleton
    private static class EventTransmitterProvider implements Provider<EventTransmitter> {

        private final MrePublishConfiguration configuration;

        private final Registry registry;

        private final MantisJobDiscovery jobDiscovery;

        @Inject
        public EventTransmitterProvider(
                MrePublishConfiguration configuration,
                Registry registry,
                MantisJobDiscovery jobDiscovery) {

            this.configuration = configuration;
            this.registry = registry;
            this.jobDiscovery = jobDiscovery;
        }

        @Override
        public EventTransmitter get() {
            HttpEventChannelManager channelManager = new HttpEventChannelManager(registry, configuration);
            EventChannel eventChannel = new HttpEventChannel(registry, channelManager);
            return new ChoiceOfTwoEventTransmitter(configuration, registry, jobDiscovery, eventChannel);
        }
    }

    @Singleton
    private static class SubscriptionTrackerProvider implements Provider<SubscriptionTracker> {

        private final MrePublishConfiguration configuration;

        private final Registry registry;

        private final StreamManager streamManager;

        private final MantisJobDiscovery jobDiscovery;

        @Inject
        public SubscriptionTrackerProvider(
                MrePublishConfiguration configuration,
                Registry registry,
                StreamManager streamManager,
                MantisJobDiscovery jobDiscovery) {

            this.configuration = configuration;
            this.registry = registry;
            this.streamManager = streamManager;
            this.jobDiscovery = jobDiscovery;
        }

        @Override
        public SubscriptionTracker get() {
            return new DefaultSubscriptionTracker(
                    configuration,
                    registry,
                    jobDiscovery,
                    streamManager,
                    HttpClient.create(registry));
        }
    }

    @Singleton
    private static class MrePublishConfigProvider implements Provider<MrePublishConfiguration> {

        private final PropertyRepository propertyRepository;

        @Inject
        public MrePublishConfigProvider(PropertyRepository propertyRepository) {
            this.propertyRepository = propertyRepository;
        }

        @Override
        public MrePublishConfiguration get() {
            return new SampleArchaiusMrePublishConfiguration(propertyRepository);
        }
    }

    @Override
    protected void configure() {
        bind(MrePublishConfiguration.class).toProvider(MrePublishConfigProvider.class).asEagerSingleton();
        bind(StreamManager.class).toProvider(StreamManagerProvider.class).asEagerSingleton();
        bind(EventPublisher.class).toProvider(EventPublisherProvider.class).asEagerSingleton();
        bind(MantisJobDiscovery.class).toProvider(MantisJobDiscoveryProvider.class).asEagerSingleton();
        bind(EventTransmitter.class).toProvider(EventTransmitterProvider.class).asEagerSingleton();
        bind(Tee.class).toProvider(TeeProvider.class).asEagerSingleton();
        bind(SubscriptionTracker.class).toProvider(SubscriptionTrackerProvider.class).asEagerSingleton();
        bind(MrePublishClientInitializer.class).toProvider(MrePublishClientInitializerProvider.class).asEagerSingleton();
    }
}
