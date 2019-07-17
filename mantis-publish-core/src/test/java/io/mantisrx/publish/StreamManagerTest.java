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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.core.SubscriptionFactory;
import com.netflix.spectator.api.DefaultRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class StreamManagerTest {

    private SettableConfig config;
    private StreamManager streamManager;

    @BeforeEach
    void setUp() {
        config = new DefaultSettableConfig();
        PropertyRepository propertyRepository = DefaultPropertyFactory.from(config);
        SampleArchaiusMrePublishConfiguration archaiusConfiguration = new SampleArchaiusMrePublishConfiguration(propertyRepository);
        streamManager = new StreamManager(new DefaultRegistry(), archaiusConfiguration);
    }

    @Test
    void testCreateStreamQueue() {
        final String streamName = "testStream";
        streamManager.registerStream(streamName);

        assertFalse(streamManager.hasSubscriptions(streamName));
        assertTrue(streamManager.getQueueForStream(streamName).isPresent());

        Set<Subscription> streamSubscriptions =
                streamManager.getStreamSubscriptions(streamName);
        assertEquals(0, streamSubscriptions.size());

        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());
    }

    @Test
    void testAddStreamSubscription() {
        final String streamName = StreamType.DEFAULT_EVENT_STREAM;

        Optional<Subscription> subscriptionO =
                SubscriptionFactory.getSubscription("subId1", "false");

        assertTrue(subscriptionO.isPresent());
        streamManager.addStreamSubscription(subscriptionO.get());

        assertTrue(streamManager.hasSubscriptions(streamName));

        Set<Subscription> streamSubscriptions =
                streamManager.getStreamSubscriptions(streamName);
        assertEquals(1, streamSubscriptions.size());
        for (final Subscription s : streamSubscriptions) {
            assertEquals(s, subscriptionO.get());
        }
        assertFalse(streamManager.getQueueForStream(streamName).isPresent());
        assertFalse(streamManager.getStreamMetrics(streamName).isPresent());
        assertTrue(streamManager.getRegisteredStreams().isEmpty());
    }

    @Test
    void testInvalidSubscription() {
        Optional<Subscription> subscriptionO = SubscriptionFactory.getSubscription("subId1", "SELECT * FROM stream WHERE true SAMPLE {\\\"strategy\\\":\\\"RANDOM\\\", \\\"threshold\\\":200}");
        assertFalse(subscriptionO.isPresent());
    }

    @Test
    void testAddRemoveStreamSubscription() {
        final String streamName = "testStream";
        // create stream queue
        streamManager.registerStream(streamName);

        // add subscription
        Optional<Subscription> subscriptionO = SubscriptionFactory
                .getSubscription("subId2", "SELECT a,b FROM " + streamName);
        assertTrue(subscriptionO.isPresent());

        streamManager.addStreamSubscription(subscriptionO.get());

        assertTrue(streamManager.hasSubscriptions(streamName));

        Set<Subscription> streamSubscriptions =
                streamManager.getStreamSubscriptions(streamName);
        assertEquals(1, streamSubscriptions.size());
        for (final Subscription s : streamSubscriptions) {
            assertEquals(s, subscriptionO.get());
        }
        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());

        streamManager.removeStreamSubscription(subscriptionO.get());
        assertFalse(streamManager.hasSubscriptions(streamName));

        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());
    }

    @Test
    void testAddRemoveStreamSubscription2() {
        final String streamName = "testStream";
        // create stream queue
        streamManager.registerStream(streamName);

        // add subscription
        Optional<Subscription> subscriptionO = SubscriptionFactory
                .getSubscription("subId2", "SELECT a,b FROM " + streamName);
        assertTrue(subscriptionO.isPresent());

        streamManager.addStreamSubscription(subscriptionO.get());

        assertTrue(streamManager.hasSubscriptions(streamName));

        Set<Subscription> streamSubscriptions = streamManager.getStreamSubscriptions(streamName);
        assertEquals(1, streamSubscriptions.size());
        for (final Subscription s : streamSubscriptions) {
            assertEquals(s, subscriptionO.get());
        }
        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());

        // add subscription with duplicate subscriptionId should replace old subscription
        Optional<Subscription> subscriptionO2 = SubscriptionFactory
                .getSubscription("subId2", "SELECT a,b,c FROM " + streamName);
        assertTrue(subscriptionO2.isPresent());

        streamManager.addStreamSubscription(subscriptionO2.get());

        assertTrue(streamManager.hasSubscriptions(streamName));

        Set<Subscription> streamSubscriptions2 = streamManager.getStreamSubscriptions(streamName);
        assertEquals(1, streamSubscriptions2.size());
        Iterator<Subscription> iterator = streamSubscriptions2.iterator();
        assertEquals(subscriptionO2.get(), iterator.next());

        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());

        // remove sub
        streamManager.removeStreamSubscription(subscriptionO.get());
        assertFalse(streamManager.hasSubscriptions(streamName));
        assertEquals(0, streamManager.getStreamSubscriptions(streamName).size());

        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());
    }

    @Test
    void testAddRemoveStreamSubscriptionId() {
        final String streamName = "testStream";
        // create stream queue
        streamManager.registerStream(streamName);

        // add subscription
        final String subId = "subId3";
        Optional<Subscription> subscriptionO = SubscriptionFactory
                .getSubscription(subId, "SELECT * FROM " + streamName);
        assertTrue(subscriptionO.isPresent());

        streamManager.addStreamSubscription(subscriptionO.get());

        assertTrue(streamManager.hasSubscriptions(streamName));

        Set<Subscription> streamSubscriptions =
                streamManager.getStreamSubscriptions(streamName);
        assertEquals(1, streamSubscriptions.size());
        for (final Subscription s : streamSubscriptions) {
            assertEquals(s, subscriptionO.get());
        }
        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());

        streamManager.removeStreamSubscription(subId);
        assertFalse(streamManager.hasSubscriptions(streamName));

        assertTrue(streamManager.getQueueForStream(streamName).isPresent());
        assertTrue(streamManager.getStreamMetrics(streamName).isPresent());
        assertEquals(1, streamManager.getRegisteredStreams().size());
        assertEquals(streamName, streamManager.getRegisteredStreams().iterator().next());
    }

    @Test
    void testStreamLimits() throws InterruptedException {
        final String streamName = "testStream";
        // create stream queue
        int maxStreams = 5;
        config.setProperty(SampleArchaiusMrePublishConfiguration.MAX_NUM_STREAMS_NAME, maxStreams);
        for (int i = 0; i < maxStreams; i++) {
            config.setProperty(
                    String.format(SampleArchaiusMrePublishConfiguration.PER_STREAM_QUEUE_SIZE_FORMAT, streamName + "1"),
                    maxStreams);
            assertTrue(streamManager
                    .registerStream(streamName + i).isPresent());
        }
        // stream should not get created since limit is reached
        assertFalse(streamManager
                .registerStream(streamName + "Trigger").isPresent());
        assertEquals(maxStreams, streamManager.getRegisteredStreams().size());

        // Override inactive stream duration and wait for all streams
        // to become inactive before adding new streams
        long inactiveMillis = 500L;
        config.setProperty(
                SampleArchaiusMrePublishConfiguration.STREAM_INACTIVE_DURATION_THRESHOLD_NAME,
                inactiveMillis / 1000L);
        Thread.sleep(inactiveMillis * 2);
        for (int i = 0; i < maxStreams; i++) {
            assertTrue(streamManager
                    .registerStream(streamName + "New" + i).isPresent());
        }
        assertEquals(maxStreams, streamManager.getRegisteredStreams().size());
        // revert the inactive stream duration threshold
        config.setProperty(
                SampleArchaiusMrePublishConfiguration.STREAM_INACTIVE_DURATION_THRESHOLD_NAME, 24 * 60 * 60);
    }
}
