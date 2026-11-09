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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.spectator.api.DefaultRegistry;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.internal.metrics.StreamMetrics;
import io.mantisrx.publish.internal.mql.MQLSubscription;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class EventProcessorTest {

    private StreamManager streamManager;
    private EventProcessor eventProcessor;

    @BeforeEach
    void setUp() {
        SettableConfig config = new DefaultSettableConfig();
        config.setProperty(SampleArchaiusMrePublishConfiguration.MRE_CLIENT_BLACKLIST_KEYS_PROP, "param.password");
        PropertyRepository repository =
                DefaultPropertyFactory.from(config);
        streamManager = mock(StreamManager.class);
        MrePublishConfiguration mrePublishConfiguration = new SampleArchaiusMrePublishConfiguration(repository);
        Tee tee = mock(Tee.class);
        doNothing().when(tee).tee(anyString(), any(Event.class));

        eventProcessor = new EventProcessor(mrePublishConfiguration, streamManager, tee);
    }

    @Test
    void shouldReturnEnrichedEventForStream() throws Exception {
        when(streamManager.hasSubscriptions(anyString())).thenReturn(true);
        SettableConfig config = new DefaultSettableConfig();
        PropertyRepository repository =
                DefaultPropertyFactory.from(config);
        MrePublishConfiguration mrePublishConfiguration = new SampleArchaiusMrePublishConfiguration(repository);
        Subscription subscription = new MQLSubscription("id", "select * where true");
        Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
        subscriptions.add(subscription);
        when(streamManager.getStreamSubscriptions(anyString())).thenReturn(subscriptions);

        Event event = new Event();
        event.set("k1", "v1");
        Event actual = eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);
        // Single event with a `select * where true` yields the single event.
        assertEquals(StreamType.DEFAULT_EVENT_STREAM, actual.get("mantisStream"));
        assertEquals("EVENT", actual.get("type"));
        assertEquals("v1", actual.get("k1"));
        assertEquals(1, ((ArrayList)actual.get("matched-clients")).size());
        assertEquals("id", ((ArrayList)actual.get("matched-clients")).get(0));
    }

    @Test
    void shouldReturnEmptyEventForStream() throws Exception {
        when(streamManager.hasSubscriptions(anyString())).thenReturn(false);

        Event event = new Event();
        event.set("k1", "v1");
        Event actual = eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);
        // No subscriptions
        assertNull(actual);

        Subscription subscription = mock(MQLSubscription.class);
        when(subscription.matches(any(Event.class))).thenReturn(false);
        Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
        subscriptions.add(subscription);
        when(streamManager.getStreamSubscriptions(anyString())).thenReturn(subscriptions);
        actual = eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);
        // A subscription exists but doesn't match.
        assertNull(actual);
    }

    @Test
    void shouldIncrementMantisEventsFilteredForNonMatchingSubscription() throws Exception {
        StreamMetrics metrics = new StreamMetrics(new DefaultRegistry(), StreamType.DEFAULT_EVENT_STREAM);
        when(streamManager.getStreamMetrics(anyString())).thenReturn(Optional.of(metrics));
        when(streamManager.hasSubscriptions(anyString())).thenReturn(true);

        Subscription subscription = mock(MQLSubscription.class);
        when(subscription.getSubscriptionId()).thenReturn("nonMatchingSub");
        when(subscription.matches(any(Event.class))).thenReturn(false);
        Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
        subscriptions.add(subscription);
        when(streamManager.getStreamSubscriptions(anyString())).thenReturn(subscriptions);

        Event event = new Event();
        event.set("k1", "v1");
        Event actual = eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);

        assertNull(actual);
        assertEquals(1, metrics.getMantisEventsFilteredCounter("nonMatchingSub").count());
    }

    @Test
    void shouldNotIncrementMantisEventsFilteredForMatchingSubscription() throws Exception {
        StreamMetrics metrics = new StreamMetrics(new DefaultRegistry(), StreamType.DEFAULT_EVENT_STREAM);
        when(streamManager.getStreamMetrics(anyString())).thenReturn(Optional.of(metrics));
        when(streamManager.hasSubscriptions(anyString())).thenReturn(true);

        Subscription subscription = new MQLSubscription("matchingSub", "select * where true");
        Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
        subscriptions.add(subscription);
        when(streamManager.getStreamSubscriptions(anyString())).thenReturn(subscriptions);

        Event event = new Event();
        event.set("k1", "v1");
        eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);

        assertEquals(0, metrics.getMantisEventsFilteredCounter("matchingSub").count());
    }

    @Test
    void shouldOnlyIncrementMantisEventsFilteredForNonMatchingSubscriptionInMixedSet() throws Exception {
        StreamMetrics metrics = new StreamMetrics(new DefaultRegistry(), StreamType.DEFAULT_EVENT_STREAM);
        when(streamManager.getStreamMetrics(anyString())).thenReturn(Optional.of(metrics));
        when(streamManager.hasSubscriptions(anyString())).thenReturn(true);

        Subscription matchingSubscription = new MQLSubscription("matchingSub", "select * where true");
        Subscription nonMatchingSubscription = new MQLSubscription("nonMatchingSub", "select * where false");
        Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
        subscriptions.add(matchingSubscription);
        subscriptions.add(nonMatchingSubscription);
        when(streamManager.getStreamSubscriptions(anyString())).thenReturn(subscriptions);

        Event event = new Event();
        event.set("k1", "v1");
        Event actual = eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);

        assertEquals(0, metrics.getMantisEventsFilteredCounter("matchingSub").count());
        assertEquals(1, metrics.getMantisEventsFilteredCounter("nonMatchingSub").count());
        assertEquals(StreamType.DEFAULT_EVENT_STREAM, actual.get("mantisStream"));
    }

    @Test
    void shouldNotIncrementMantisEventsFilteredWhenMatchesThrows() throws Exception {
        StreamMetrics metrics = new StreamMetrics(new DefaultRegistry(), StreamType.DEFAULT_EVENT_STREAM);
        when(streamManager.getStreamMetrics(anyString())).thenReturn(Optional.of(metrics));
        when(streamManager.hasSubscriptions(anyString())).thenReturn(true);

        Subscription subscription = mock(MQLSubscription.class);
        when(subscription.getSubscriptionId()).thenReturn("throwingSub");
        when(subscription.matches(any(Event.class))).thenThrow(new RuntimeException("query failed"));
        Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
        subscriptions.add(subscription);
        when(streamManager.getStreamSubscriptions(anyString())).thenReturn(subscriptions);

        Event event = new Event();
        event.set("k1", "v1");
        Event actual = eventProcessor.process(StreamType.DEFAULT_EVENT_STREAM, event);

        assertNull(actual);
        assertEquals(1, metrics.getMantisQueryFailedCounter().count());
        assertEquals(0, metrics.getMantisEventsFilteredCounter("throwingSub").count());
    }

    @Test
    void shouldMaskSensitiveFields() {
        Map<String, Object> data = new HashMap<>();
        data.put("param.password", "hunter2");
        data.put("myname", "mantis");
        Event re = new Event(data);

        eventProcessor.maskSensitiveFields(re);
        assertSame("***", re.get("param.password"));
        assertEquals(re.get("myname"), "mantis");
    }

    @Test
    void shouldHandleJava8Time() {
        Map<String, Object> data = new HashMap<>();
        data.put("myname", "mantis");
        data.put("time", Instant.parse("2000-01-01T00:01:00.00Z"));
        Event re = new Event(data);

        assertEquals("{\"myname\":\"mantis\",\"time\":946684860.000000000}", re.toJsonString());
    }
}
