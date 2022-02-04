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

import static io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration.MAX_SUBS_PER_STREAM_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AbstractSubscriptionTrackerTest {
    private SettableConfig config;
    private StreamManager streamManager;
    private TestSubscriptionTracker subscriptionTracker;

    @BeforeEach
    public void setup() {
        config = new DefaultSettableConfig();
        PropertyRepository propertyRepository = DefaultPropertyFactory.from(config);
        SampleArchaiusMrePublishConfiguration archaiusConfiguration = new SampleArchaiusMrePublishConfiguration(propertyRepository);
        Registry registry = new DefaultRegistry();
        MantisJobDiscovery mockJobDiscovery = mock(MantisJobDiscovery.class);
        Map<String, String> streamJobClusterMap = new HashMap<>();
        streamJobClusterMap.put(StreamType.DEFAULT_EVENT_STREAM, "RequestEventSubTrackerTestJobCluster");
        streamJobClusterMap.put("requestStream", "RequestEventSubTrackerTestJobCluster");
        when(mockJobDiscovery.getStreamNameToJobClusterMapping(anyString())).thenReturn(streamJobClusterMap);

        streamManager = new StreamManager(registry, archaiusConfiguration);
        subscriptionTracker = new TestSubscriptionTracker(archaiusConfiguration, registry, mockJobDiscovery, streamManager);
    }

    @Test
    public void testDiscardSubscriptionsBeyondMax() {
        config.setProperty(String.format(MAX_SUBS_PER_STREAM_FORMAT, StreamType.DEFAULT_EVENT_STREAM), 2);

        streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM);
        List<MantisServerSubscription> nextSubs = ImmutableList.of(
                new MantisServerSubscription("id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("id3", "select * from defaultStream where id = 3", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        Set<Subscription> subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        Set<String> subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        Set<String> expected = ImmutableSet.of("id1", "id2");
        assertEquals(expected, subIds);
    }


    @Test
    public void testMaxSubscriptionCountChange() {
        config.setProperty(String.format(MAX_SUBS_PER_STREAM_FORMAT, StreamType.DEFAULT_EVENT_STREAM), 2);

        streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM);
        List<MantisServerSubscription> nextSubs = ImmutableList.of(
                new MantisServerSubscription("id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("id3", "select * from defaultStream where id = 3", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        Set<Subscription> subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        Set<String> subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        Set<String> expected = ImmutableSet.of("id1", "id2");
        assertEquals(expected, subIds);

        config.setProperty(String.format(MAX_SUBS_PER_STREAM_FORMAT, StreamType.DEFAULT_EVENT_STREAM), 4);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("id3", "select * from defaultStream where id = 3", null),
                new MantisServerSubscription("id4", "select * from defaultStream where id = 4", null),
                new MantisServerSubscription("id5", "select * from defaultStream where id = 5", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("id1", "id2", "id3", "id4");
        assertEquals(expected, subIds);
    }

    @Test
    public void testSubscriptionUpdate() {
        streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM);
        List<MantisServerSubscription> nextSubs = ImmutableList.of(
                new MantisServerSubscription("id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("id3", "select * from defaultStream where id = 3", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        Set<Subscription> subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        Set<String> subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        Set<String> expected = ImmutableSet.of("id1", "id2", "id3");
        assertEquals(expected, subIds);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("id3", "select * from defaultStream where id = 3", null),
                new MantisServerSubscription("id4", "select * from defaultStream where id = 4", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("id1", "id2", "id3", "id4");
        assertEquals(expected, subIds);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("id4", "select * from defaultStream where id = 4", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("id2", "id4");
        assertEquals(expected, subIds);
    }

    @Test
    public void testUpdateMultipleStreams() {
        String requestStream = "requestStream";

        streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM);
        streamManager.registerStream(requestStream);
        List<MantisServerSubscription> nextSubs = ImmutableList.of(
                new MantisServerSubscription("default_id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("default_id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("default_id3", "select * from defaultStream, requestStream where id = 3", null)
        );

        List<MantisServerSubscription> nextRequestSubs = ImmutableList.of(
                new MantisServerSubscription("request_id1", "select * from requestStream where id = 1", null),
                new MantisServerSubscription("request_id2", "select * from requestStream where id = 2", null),
                new MantisServerSubscription("default_id3", "select * from defaultStream, requestStream where id = 3", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs,
                requestStream, nextRequestSubs));
        subscriptionTracker.refreshSubscriptions();

        Set<Subscription> subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        Set<String> subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        Set<String> expected = ImmutableSet.of("default_id1", "default_id2", "default_id3");
        assertEquals(expected, subIds);

        subs = streamManager.getStreamSubscriptions(requestStream);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("request_id1", "request_id2", "default_id3");
        assertEquals(expected, subIds);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("default_id1", "select * from defaultStream where id = 1", null)
        );

        nextRequestSubs = ImmutableList.of(
                new MantisServerSubscription("request_id1", "select * from requestStream where id = 1", null),
                new MantisServerSubscription("request_id2", "select * from requestStream where id = 2", null),
                new MantisServerSubscription("request_id4", "select * from requestStream where id = 4", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs,
                requestStream, nextRequestSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("default_id1");
        assertEquals(expected, subIds);

        subs = streamManager.getStreamSubscriptions(requestStream);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("request_id1", "request_id2", "request_id4");
        assertEquals(expected, subIds);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("default_id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("default_id3", "select * from defaultStream, requestStream where id = 3", null)
        );

        nextRequestSubs = ImmutableList.of(
                new MantisServerSubscription("request_id1", "select * from requestStream where id = 1", null),
                new MantisServerSubscription("request_id2", "select * from requestStream where id = 2", null),
                new MantisServerSubscription("request_id4", "select * from requestStream where id = 4", null),
                new MantisServerSubscription("default_id3", "select * from defaultStream, requestStream where id = 3", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs,
                requestStream, nextRequestSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("default_id1", "default_id3");
        assertEquals(expected, subIds);

        subs = streamManager.getStreamSubscriptions(requestStream);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("request_id1", "request_id2", "request_id4", "default_id3");
        assertEquals(expected, subIds);
    }

    @Test
    public void testUpdateMultipleStreamsWithUnionSubscriptions() {
        String requestStream = "requestStream";

        streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM);
        streamManager.registerStream(requestStream);
        List<MantisServerSubscription> nextSubs = ImmutableList.of(
                new MantisServerSubscription("default_id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("default_id2", "select * from defaultStream where id = 2", null),
                new MantisServerSubscription("request_id1", "select * from requestStream where id = 1", null),
                new MantisServerSubscription("request_id2", "select * from requestStream where id = 2", null),
                new MantisServerSubscription("default_id3", "select * from defaultStream, requestStream where id = 3", null)
        );

        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs,
                requestStream, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        Set<Subscription> subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        Set<String> subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        Set<String> expected = ImmutableSet.of("default_id1", "default_id2", "default_id3");
        assertEquals(expected, subIds);

        subs = streamManager.getStreamSubscriptions(requestStream);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("request_id1", "request_id2", "default_id3");
        assertEquals(expected, subIds);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("default_id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("request_id1", "select * from requestStream where id = 1", null),
                new MantisServerSubscription("request_id2", "select * from requestStream where id = 2", null),
                new MantisServerSubscription("request_id4", "select * from requestStream where id = 4", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs,
                requestStream, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("default_id1");
        assertEquals(expected, subIds);

        subs = streamManager.getStreamSubscriptions(requestStream);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("request_id1", "request_id2", "request_id4");
        assertEquals(expected, subIds);

        nextSubs = ImmutableList.of(
                new MantisServerSubscription("default_id1", "select * from defaultStream where id = 1", null),
                new MantisServerSubscription("request_id1", "select * from requestStream where id = 1", null),
                new MantisServerSubscription("request_id2", "select * from requestStream where id = 2", null),
                new MantisServerSubscription("request_id4", "select * from requestStream where id = 4", null),
                new MantisServerSubscription("default_id3", "select * from defaultStream, requestStream where id = 3", null)
        );
        subscriptionTracker.setNextSubscriptions(ImmutableMap.of(StreamType.DEFAULT_EVENT_STREAM, nextSubs,
                requestStream, nextSubs));
        subscriptionTracker.refreshSubscriptions();

        subs = streamManager.getStreamSubscriptions(StreamType.DEFAULT_EVENT_STREAM);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("default_id1", "default_id3");
        assertEquals(expected, subIds);

        subs = streamManager.getStreamSubscriptions(requestStream);
        subIds = subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toSet());
        expected = ImmutableSet.of("request_id1", "request_id2", "request_id4", "default_id3");
        assertEquals(expected, subIds);
    }

    public static class TestSubscriptionTracker extends AbstractSubscriptionTracker {
        private Map<String, List<MantisServerSubscription>> nextSubscriptions;

        public TestSubscriptionTracker(
                MrePublishConfiguration mrePublishConfiguration,
                Registry registry,
                MantisJobDiscovery jobDiscovery,
                StreamManager streamManager) {

            super(mrePublishConfiguration, registry, jobDiscovery, streamManager);
        }

        public void setNextSubscriptions(Map<String, List<MantisServerSubscription>> subscriptions) {
            this.nextSubscriptions = subscriptions;
        }

        @Override
        public Optional<MantisServerSubscriptionEnvelope> fetchSubscriptions(String streamName, String jobCluster) {
            if (nextSubscriptions != null && !nextSubscriptions.isEmpty()) {
                return Optional.of(new MantisServerSubscriptionEnvelope(nextSubscriptions.get(streamName)));
            } else {
                return Optional.empty();
            }
        }
    }
}
