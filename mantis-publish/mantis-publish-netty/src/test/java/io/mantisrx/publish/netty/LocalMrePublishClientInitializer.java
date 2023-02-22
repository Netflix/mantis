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

package io.mantisrx.publish.netty;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.discovery.proto.StageWorkers;
import io.mantisrx.publish.DefaultSubscriptionTracker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.EventTransmitter;
import io.mantisrx.publish.MantisEventPublisher;
import io.mantisrx.publish.MrePublishClientInitializer;
import io.mantisrx.publish.StreamManager;
import io.mantisrx.publish.SubscriptionTracker;
import io.mantisrx.publish.Tee;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.netty.pipeline.HttpEventChannel;
import io.mantisrx.publish.netty.pipeline.HttpEventChannelManager;
import io.mantisrx.publish.netty.transmitters.ChoiceOfTwoEventTransmitter;
import io.netty.util.ResourceLeakDetector;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class LocalMrePublishClientInitializer {

    private static final Map<String, String> streamJobClusterMap =
            Collections.singletonMap(StreamType.DEFAULT_EVENT_STREAM, "StartPlayLogBlobSource2-4");

    private static MrePublishConfiguration testConfig() {
        DefaultSettableConfig settableConfig = new DefaultSettableConfig();
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.DEFAULT_EVENT_STREAM, streamJobClusterMap.get(StreamType.DEFAULT_EVENT_STREAM));
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.SUBS_REFRESH_INTERVAL_SEC_PROP, 5);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.JOB_DISCOVERY_REFRESH_INTERVAL_SEC_PROP, 5);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.DISCOVERY_API_HOSTNAME_PROP, "127.0.0.1");
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.DISCOVERY_API_PORT_PROP, 9090);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.CHANNEL_GZIP_ENABLED_PROP, true);

        PropertyRepository propertyRepository = DefaultPropertyFactory.from(settableConfig);
        return new SampleArchaiusMrePublishConfiguration(propertyRepository);
    }

    private static MantisJobDiscovery createJobDiscovery() {
        MantisJobDiscovery jobDiscovery = mock(MantisJobDiscovery.class);
        when(jobDiscovery.getCurrentJobWorkers(anyString()))
                .thenReturn(
                        Optional.of(new JobDiscoveryInfo(
                                        "cluster",
                                        "id",
                                        Collections.singletonMap(
                                                1,
                                                new StageWorkers(
                                                        "cluster",
                                                        "id",
                                                        1,
                                                        Collections.singletonList(
                                                                new MantisWorker("127.0.0.1", 9090)
                                                        )
                                                )
                                        )
                                )
                        )
                );

        return jobDiscovery;
    }

    public static void main(String[] args) throws InterruptedException {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        MrePublishConfiguration mrePublishConfiguration = testConfig();
        Registry registry = new DefaultRegistry();
        HttpClient httpClient = HttpClient.create(registry);
        MantisJobDiscovery jobDiscovery = createJobDiscovery();
        StreamManager streamManager = new StreamManager(registry, mrePublishConfiguration);
        MantisEventPublisher mantisEventPublisher = new MantisEventPublisher(mrePublishConfiguration, streamManager);
        SubscriptionTracker subscriptionsTracker =
                new DefaultSubscriptionTracker(mrePublishConfiguration, registry, jobDiscovery, streamManager, httpClient);
        HttpEventChannelManager channelManager = new HttpEventChannelManager(registry, mrePublishConfiguration);
        EventChannel eventChannel = new HttpEventChannel(registry, channelManager);
        EventTransmitter transmitter =
                new ChoiceOfTwoEventTransmitter(mrePublishConfiguration, registry, jobDiscovery, eventChannel);
        Tee tee = mock(Tee.class);
        doNothing().when(tee).tee(anyString(), any(Event.class));

        MrePublishClientInitializer mreClient = new MrePublishClientInitializer(
                mrePublishConfiguration,
                registry,
                streamManager,
                mantisEventPublisher,
                subscriptionsTracker,
                transmitter,
                tee);

        mreClient.start();
        EventPublisher eventPublisher = mreClient.getEventPublisher();
        // Periodically publish a test event
        final Event event = new Event();
        final CountDownLatch latch = new CountDownLatch(1000);
        event.set("testKey", "testValue");
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "EventPublisherTest"));
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(() -> {
            eventPublisher.publish(event);
            latch.countDown();
        }, 1, 1, TimeUnit.SECONDS);

        latch.await();
        if (!scheduledFuture.isCancelled()) {
            scheduledFuture.cancel(true);
        }
        mreClient.stop();
    }
}
