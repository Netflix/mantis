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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.PublishStatus;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import org.junit.Before;
import org.junit.Test;


public class MantisEventPublisherTest {

    private MrePublishConfiguration testConfig(boolean mantisPublishClientEnabled) {
        DefaultSettableConfig settableConfig = new DefaultSettableConfig();
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.MRE_CLIENT_ENABLED_PROP, mantisPublishClientEnabled);
        PropertyRepository propertyRepository = DefaultPropertyFactory.from(settableConfig);
        return new SampleArchaiusMrePublishConfiguration(propertyRepository);
    }

    private StreamManager streamManager;

    @Before
    public void setup() {
        streamManager = mock(StreamManager.class);
    }

    @Test
    public void testPublisherEnqueue() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        PublishStatus status = eventPublisher.publish(new Event());
        assertEquals(PublishStatus.ENQUEUED, status);
    }

    @Test
    public void testPublishMessageSkipStatusWhenDisabled() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = false;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        PublishStatus status = eventPublisher.publish(new Event());
        assertEquals(PublishStatus.SKIPPED_CLIENT_NOT_ENABLED, status);
    }

    @Test
    public void testPublishMessageSkipStatusWhenNoSubscriptions() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(false);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        PublishStatus status = eventPublisher.publish(new Event());
        assertEquals(PublishStatus.SKIPPED_NO_SUBSCRIPTIONS, status);
    }

    @Test
    public void testPublishMessageFailStatusWhenStreamNotRegistered() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.empty());
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        PublishStatus status = eventPublisher.publish(new Event());
        assertEquals(PublishStatus.FAILED_STREAM_NOT_REGISTERED, status);
    }

    @Test
    public void testPublishMessageFailStatusWhenQueueFull() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.ofNullable(new LinkedBlockingQueue<>(1)));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        PublishStatus status = eventPublisher.publish(new Event());
        assertEquals(PublishStatus.ENQUEUED, status);
        PublishStatus status2 = eventPublisher.publish(new Event());
        assertEquals(PublishStatus.FAILED_QUEUE_FULL, status2);
    }
}
