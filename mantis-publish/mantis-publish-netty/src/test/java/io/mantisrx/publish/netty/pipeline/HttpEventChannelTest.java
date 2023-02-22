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

package io.mantisrx.publish.netty.pipeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.netty.proto.MantisEvent;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class HttpEventChannelTest {

    private static MantisWorker mantisWorker;

    private HttpEventChannelManager channelManager;
    private HttpEventChannel eventChannel;
    private Channel channel;

    @BeforeAll
    static void setupAll() {
        mantisWorker = new MantisWorker("127.0.0.1", 9090);
    }

    @BeforeEach
    void setup() {
        Registry registry = new DefaultRegistry();

        channelManager = mock(HttpEventChannelManager.class);
        channel = mock(Channel.class);
        when(channel.writeAndFlush(any(MantisEvent.class))).thenReturn(mock(ChannelFuture.class));
        when(channel.write(any(MantisEvent.class))).thenReturn(mock(ChannelFuture.class));
        when(channelManager.findOrCreate(any(InetSocketAddress.class))).thenReturn(channel);
        eventChannel = new HttpEventChannel(registry, channelManager);
    }

    @Test
    void shouldWriteOverActiveAndWritableChannel() {
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);
        when(channel.bytesBeforeUnwritable()).thenReturn(10L);

        eventChannel.send(mantisWorker, new Event());
        verify(channel, times(1)).writeAndFlush(any(MantisEvent.class));
    }

    @Test
    void shouldNotWriteOverInactiveChannel() {
        when(channel.isActive()).thenReturn(false);
        when(channel.isWritable()).thenReturn(true);

        eventChannel.send(mantisWorker, new Event());
        verify(channel, times(0)).writeAndFlush(any(MantisEvent.class));
    }

    @Test
    void shouldNotWriteOverActiveAndUnwritableChannel() {
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(false);

        eventChannel.send(mantisWorker, new Event());
        verify(channel, times(0)).writeAndFlush(any(MantisEvent.class));
    }
}
