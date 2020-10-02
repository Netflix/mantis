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

import static io.netty.channel.ChannelHandler.Sharable;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.mantisrx.publish.internal.exceptions.NonRetryableException;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.internal.exceptions.RetryableException;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles incoming responses for sending an event via {@code POST} request.
 * <p>
 * This class propagates {@code 4xx} errors as {@link NonRetryableException}s and {@code 5xx} errors as
 * {@link RetryableException}s.
 * <p>
 * For runtime exceptions, such as ones thrown by a {@link MessageToMessageEncoder} or
 * {@link ChannelOutboundHandlerAdapter}, or Netty user events, such as an {@link IdleStateEvent},
 * this class will explicitly close the connection.
 */
@Sharable
class HttpEventChannelHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpEventChannelHandler.class);

    private final Counter droppedBatches;

    /**
     * Creates a new instance.
     */
    HttpEventChannelHandler(Registry registry) {
        this.droppedBatches =
                SpectatorUtils.buildAndRegisterCounter(
                        registry, "droppedBatches", "channel", HttpEventChannel.CHANNEL_TYPE);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        HttpStatusClass status = msg.status().codeClass();
        if (status != HttpStatusClass.SUCCESS) {
            droppedBatches.increment();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("got http response. status: {}, headers: {}, message: {}",
                    msg.status().codeAsText(),
                    msg.headers().entries().toString(),
                    msg.content().toString(UTF_8));
        }
    }

    /**
     * Handles user events for inbound and outbound events of the channel pipeline.
     * <p>
     * This method handles the following events:
     * <p>
     * 1. {@link IdleStateEvent}s if no data has been written for some time.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            LOG.warn("closing idle channel");
            ctx.channel().close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.debug("caught exception from handler", cause);
        droppedBatches.increment();

        if (!(cause instanceof RetryableException || cause instanceof NonRetryableException)) {
            ctx.channel().close();
        }
    }
}
