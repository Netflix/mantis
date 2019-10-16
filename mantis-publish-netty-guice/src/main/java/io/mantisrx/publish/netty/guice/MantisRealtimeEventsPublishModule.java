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

package io.mantisrx.publish.netty.guice;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.publish.DefaultSubscriptionTracker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.EventTransmitter;
import io.mantisrx.publish.MantisEventPublisher;
import io.mantisrx.publish.MrePublishClientInitializer;
import io.mantisrx.publish.NoOpTee;
import io.mantisrx.publish.StreamManager;
import io.mantisrx.publish.SubscriptionTracker;
import io.mantisrx.publish.Tee;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.internal.discovery.MantisJobDiscoveryCachingImpl;
import io.mantisrx.publish.internal.discovery.MantisJobDiscoveryStaticImpl;
import io.mantisrx.publish.internal.discovery.mantisapi.DefaultMantisApiClient;
import io.mantisrx.publish.netty.pipeline.HttpEventChannel;
import io.mantisrx.publish.netty.pipeline.HttpEventChannelManager;
import io.mantisrx.publish.netty.transmitters.ChoiceOfTwoEventTransmitter;


public class MantisRealtimeEventsPublishModule extends AbstractModule {

    @Singleton
    private static class MrePublishClientInitializerProvider implements Provider<MrePublishClientInitializer> {

        @Inject
        private MrePublishConfiguration config;

        @Inject
        private PropertyRepository propertyRepository;

        @Inject
        private Registry registry;

        @Inject
        private StreamManager streamManager;

        @Inject
        private EventPublisher eventPublisher;

        @Inject
        private MantisJobDiscovery jobDiscovery;

        @Inject
        private SubscriptionTracker subscriptionsTracker;

        @Inject
        private EventTransmitter eventTransmitter;

        @Inject
        private Tee tee;

        @Override
        public MrePublishClientInitializer get() {
            MrePublishClientInitializer mreClient =
                    new MrePublishClientInitializer(
                            config,
                            registry,
                            streamManager,
                            eventPublisher,
                            subscriptionsTracker,
                            eventTransmitter,
                            tee);

            mreClient.start();

            return mreClient;
        }
    }

    @Singleton
    private static class EventTransmitterProvider implements Provider<EventTransmitter> {

        @Inject
        private MrePublishConfiguration configuration;

        @Inject
        private Registry registry;

        @Inject
        private MantisJobDiscovery jobDiscovery;

        @Override
        public EventTransmitter get() {
            HttpEventChannelManager channelManager = new HttpEventChannelManager(registry, configuration);
            EventChannel eventChannel = new HttpEventChannel(registry, channelManager);
            return new ChoiceOfTwoEventTransmitter(configuration, registry, jobDiscovery, eventChannel);
        }
    }

    @Singleton
    private static class StreamManagerProvider implements Provider<StreamManager> {

        @Inject
        private MrePublishConfiguration configuration;

        @Inject
        private Registry registry;

        @Override
        public StreamManager get() {
            return new StreamManager(registry, configuration);
        }
    }

    @Singleton
    private static class EventPublisherProvider implements Provider<EventPublisher> {

        @Inject
        private MrePublishConfiguration config;

        @Inject
        private StreamManager streamManager;

        @Override
        public EventPublisher get() {
            return new MantisEventPublisher(config, streamManager);
        }
    }

    @Singleton
    private static class TeeProvider implements Provider<Tee> {

        @Inject
        Registry registry;


        @Override
        public Tee get() {
            return new NoOpTee(registry);
        }
    }

    @Singleton
    private static class SubscriptionTrackerProvider implements Provider<SubscriptionTracker> {

        @Inject
        private MrePublishConfiguration configuration;

        @Inject
        private Registry registry;

        @Inject
        private StreamManager streamManager;

        @Inject
        private MantisJobDiscovery jobDiscovery;

        @Override
        public SubscriptionTracker get() {
            return new DefaultSubscriptionTracker(
                    configuration,
                    registry,
                    streamManager,
                    HttpClient.create(registry),
                    jobDiscovery);
        }
    }

    @Singleton
    private static class MrePublishConfigProvider implements Provider<MrePublishConfiguration> {

        @Inject
        private PropertyRepository propertyRepository;
        
        @Override
        public MrePublishConfiguration get() {
            return new SampleArchaiusMrePublishConfiguration(propertyRepository);
        }
    }

    @Singleton
    private static class MantisJobDiscoveryProvider implements Provider<MantisJobDiscovery> {
        @Inject
        private MrePublishConfiguration configuration;

        @Inject
        private Registry registry;

        @Override
        public MantisJobDiscovery get() {
            return new MantisJobDiscoveryCachingImpl(configuration, registry,
                    new DefaultMantisApiClient(configuration, HttpClient.create(registry)));
            // for local testing swap in the static impl.
            //return new MantisJobDiscoveryStaticImpl("127.0.0.1",9090);

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
