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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.DefaultRegistry;
import io.mantisrx.publish.netty.proto.MantisEvent;
import io.mantisrx.publish.netty.proto.MantisEventEnvelope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class MantisEventAggregatorTest {

    private Clock clock;
    private ChannelHandlerContext ctx;
    private MantisEventAggregator aggregator;

    @BeforeEach
    void setup() {
        ctx = mock(ChannelHandlerContext.class);
        clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
        aggregator = new MantisEventAggregator(new DefaultRegistry(), clock, true, 50, 10);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        ChannelPromise promise = mock(ChannelPromise.class);
        when(ctx.channel().voidPromise()).thenReturn(promise);
        when(ctx.channel().remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9090));
        when(ctx.alloc()).thenReturn(mock(PooledByteBufAllocator.class));
        when(ctx.alloc().directBuffer()).thenReturn(mock(ByteBuf.class));
        when(ctx.writeAndFlush(any(FullHttpRequest.class))).thenReturn(mock(ChannelFuture.class));
    }

    @Test
    void shouldBuildValidHeaders() throws IOException {
        MantisEventEnvelope envelope =
                new MantisEventEnvelope(clock.millis(), "origin", new ArrayList<>());
        envelope.addEvent(new MantisEvent(1, "v"));

        FullHttpRequest request = aggregator.buildRequest(ctx, envelope);

        HttpHeaders expectedHeaders = new DefaultHttpHeaders();
        expectedHeaders.add(HttpHeaderNames.ACCEPT, HttpHeaderValues.APPLICATION_JSON);
        expectedHeaders.add(HttpHeaderNames.ORIGIN, "localhost");
        request.headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);
        expectedHeaders.add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        // Netty wraps and overrides String#equals type with its own StringEntry.
        for (int i = 0; i < expectedHeaders.entries().size(); i++) {
            assertEquals(
                    expectedHeaders.entries().get(i).getKey(),
                    request.headers().entries().get(i).getKey());
            assertEquals(
                    expectedHeaders.entries().get(i).getValue(),
                    request.headers().entries().get(i).getValue());
        }
    }

    @Test
    void shouldForwardMessageAboveSizeThreshold() {
        MantisEvent event = new MantisEvent(1, "12345");
        aggregator.write(ctx, event, ctx.channel().voidPromise());
        aggregator.write(ctx, event, ctx.channel().voidPromise());
        // message1 + message2 > 10 Bytes; forwards message1, clear state, and add message2.
        verify(ctx, times(1)).writeAndFlush(any(FullHttpRequest.class));
    }


    @Test
    void shouldBatchMessagesBelowSizeThreshold() {
        MantisEvent event = new MantisEvent(1, "12345");
        aggregator.write(ctx, event, ctx.channel().voidPromise());
        // message1 < 10 Bytes; hold until size threshold (or timeout) reached.
        // Also forwards the original promise.
        verify(ctx, times(0)).write(any(FullHttpRequest.class), any());
    }


    @Test
    void shouldForwardUnacceptedMessage() {
        aggregator.write(ctx, new byte[1], ctx.channel().voidPromise());
        verify(ctx, times(1)).write(any(), any());
    }

    @Test
    void shouldForwardOversizedMessages() {
        MantisEvent event = new MantisEvent(1, Arrays.toString(new byte[11]));
        // message1 > 10 Bytes; don't propagate an exception to the caller (so we don't
        // close the connection), but instead warn.
        aggregator.write(ctx, event, ctx.channel().voidPromise());
        verify(ctx, times(1)).writeAndFlush(any(FullHttpRequest.class));
    }
}