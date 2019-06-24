/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connectors.publish.source.http;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NettyExceptionHandler extends SimpleChannelInboundHandler<HttpRequest> {

    private final Map<String, String> responseHeaders = new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(NettyExceptionHandler.class);

    // MetricGroupId metricGroupId;
    // Counter invalidRequestCount;

    public NettyExceptionHandler() {

        //        metricGroupId = new MetricGroupId(METRIC_GROUP + "_incoming");
        //
        //        Metrics m = new Metrics.Builder().id(metricGroupId).addCounter("InvalidRequestCount").build();
        //
        //        m = MetricsRegistry.getInstance().registerAndGet(m);
        //
        //        invalidRequestCount = m.getCounter("InvalidRequestCount");


    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest message) {
        // we can't deal with this message. No one in the pipeline handled it. Log it.
        logger.warn("Unknown message received: {}", message);
        //        invalidRequestCount.increment();
        sendResponse(
                ctx,
                false,
                message + " Bad request received.",
                HttpResponseStatus.BAD_REQUEST, responseHeaders)
        ;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        //     invalidRequestCount.increment();
        logger.warn("Unhandled exception", cause);
        sendResponse(
                ctx,
                false,
                "Internal server error: " + cause.getMessage(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                responseHeaders);
    }

    /**
     * Sends the given response and status code to the given channel.
     *
     * @param channelHandlerContext identifying the open channel
     * @param keepAlive             If the connection should be kept alive.
     * @param message               which should be sent
     * @param statusCode            of the message to send
     * @param headers               additional header values
     */
    public static CompletableFuture<Void> sendResponse(
            ChannelHandlerContext channelHandlerContext,
            boolean keepAlive,
            String message,
            HttpResponseStatus statusCode,
            Map<String, String> headers) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);

        response.headers().set(CONTENT_TYPE, "application/json");

        for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
            response.headers().set(headerEntry.getKey(), headerEntry.getValue());
        }

        if (keepAlive) {
            response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        byte[] buf = message.getBytes(StandardCharsets.UTF_8);
        ByteBuf b = Unpooled.copiedBuffer(buf);
        HttpUtil.setContentLength(response, buf.length);

        // write the initial line and the header.
        channelHandlerContext.write(response);

        channelHandlerContext.write(b);

        ChannelFuture lastContentFuture = channelHandlerContext.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        // close the connection, if no keep-alive is needed
        if (!keepAlive) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }

        return toCompletableFuture(lastContentFuture);
    }

    private static CompletableFuture<Void> toCompletableFuture(final ChannelFuture channelFuture) {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                completableFuture.complete(null);
            } else {
                completableFuture.completeExceptionally(future.cause());
            }
        });
        return completableFuture;
    }
}
