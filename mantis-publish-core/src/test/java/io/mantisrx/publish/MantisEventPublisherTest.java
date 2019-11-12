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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    private void assertStatusAsync(PublishStatus expected, CompletionStage<PublishStatus> actualF) {
        final CountDownLatch latch = new CountDownLatch(1);
        actualF.whenComplete((s, t) -> {
            try {
                assertEquals(expected, s);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
        try {
            assertTrue("timed out waiting for status callback", latch.await(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private Event testEvent() {
        return new Event(new HashMap<>(Collections.singletonMap("k", "v")), true);
    }

    @Test
    public void testPublishStatusEnqueued() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        CompletionStage<PublishStatus> statusF = eventPublisher.publish(new Event(new HashMap<>(Collections.singletonMap("k", "v"))));
        assertStatusAsync(PublishStatus.ENQUEUED, statusF);
    }

    @Test
    public void testPublishStatusSkipOnDisabled() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = false;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        CompletionStage<PublishStatus> statusF = eventPublisher.publish(testEvent());
        assertStatusAsync(PublishStatus.SKIPPED_CLIENT_NOT_ENABLED, statusF);
    }

    @Test
    public void testPublishStatusSkipOnInvalidEvent() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        CompletionStage<PublishStatus> statusF = eventPublisher.publish(new Event());
        assertStatusAsync(PublishStatus.SKIPPED_INVALID_EVENT, statusF);
    }

    @Test
    public void testPublishStatusSkipOnNoSubscriptions() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.of(new LinkedBlockingDeque<>()));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(false);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        CompletionStage<PublishStatus> statusF = eventPublisher.publish(testEvent());
        assertStatusAsync(PublishStatus.SKIPPED_NO_SUBSCRIPTIONS, statusF);
    }

    @Test
    public void testPublishStatusFailOnStreamNotRegistered() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.empty());
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        CompletionStage<PublishStatus> statusF = eventPublisher.publish(testEvent());
        assertStatusAsync(PublishStatus.FAILED_STREAM_NOT_REGISTERED, statusF);
    }

    @Test
    public void testPublishStatusFailOnQueueFull() {
        when(streamManager.registerStream(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(Optional.ofNullable(new LinkedBlockingQueue<>(1)));
        when(streamManager.hasSubscriptions(StreamType.DEFAULT_EVENT_STREAM)).thenReturn(true);
        boolean publishEnabled = true;
        MantisEventPublisher eventPublisher = new MantisEventPublisher(testConfig(publishEnabled), streamManager);
        CompletionStage<PublishStatus> statusF = eventPublisher.publish(testEvent());
        statusF.whenComplete((s, t) -> assertEquals(PublishStatus.ENQUEUED, s));
        CompletionStage<PublishStatus> statusF2 = eventPublisher.publish(testEvent());
        statusF2.whenComplete((s, t) -> assertEquals(PublishStatus.FAILED_QUEUE_FULL, s));
    }
}
