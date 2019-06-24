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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.proto.MantisEvent;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class EventDrainerTest {

    private StreamManager streamManager;
    private EventProcessor processor;
    private EventTransmitter transmitter;
    private EventDrainer drainer;

    @BeforeEach
    void setup() {
        MrePublishConfiguration config = mock(MrePublishConfiguration.class);
        Registry registry = new DefaultRegistry();
        streamManager = mock(StreamManager.class);
        processor = mock(EventProcessor.class);
        transmitter = mock(EventTransmitter.class);
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        drainer = new EventDrainer(
                config,
                streamManager,
                registry,
                processor,
                transmitter,
                clock);

        Set<String> streams = new HashSet<>();
        streams.add("requestEvents");
        when(streamManager.getAllStreams()).thenReturn(streams);
    }

    @AfterEach
    void teardown() {
    }

    @Test
    void shouldDrainAndSendForExistingSubscribers() {
        when(streamManager.hasSubscriptions(anyString())).thenReturn(true);
        BlockingQueue<Event> events = new LinkedBlockingQueue<>();
        Event event = new Event();
        event.set("k1", "v1");
        events.offer(event);
        when(streamManager.getQueueForStream(anyString()))
                .thenReturn(Optional.of(events));
        when(processor.process(anyString(), any(Event.class)))
                .thenReturn(mock(MantisEvent.class));

        drainer.run();

        verify(transmitter, times(1))
                .send(any(MantisEvent.class), any());
    }

    @Test
    void shouldDrainAndNoopForNonexistentSubscribers() {
        when(streamManager.hasSubscriptions(anyString())).thenReturn(false);

        drainer.run();

        verify(transmitter, times(0))
                .send(any(MantisEvent.class), any());
    }
}