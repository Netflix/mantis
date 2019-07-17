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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.internal.exceptions.RetryableException;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.publish.netty.proto.MantisEvent;
import io.mantisrx.publish.netty.proto.MantisEventEnvelope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Aggregates {@link MantisEvent}s for a configurable amount of time or size. After either time or size threshold
 * is met, this class will batch up the events into a {@link MantisEventEnvelope} and set that as a HTTP request
 * body, specifically within {@link FullHttpRequest#content()} as a {@link ByteBuf}.
 */
class MantisEventAggregator extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MantisEventAggregator.class);

    private final Clock clock;

    private final Timer batchFlushTime;
    private final Counter droppedBatches;
    private final Counter flushSuccess;
    private final Counter flushFailure;
    private final AtomicDouble batchSize;
    private final ObjectWriter objectWriter;
    private final boolean compress;
    private ScheduledFuture<?> writerTimeout;
    private long flushIntervalMs;
    private int flushIntervalBytes;
    private MantisEventEnvelope currentMessage;
    private int currentMessageSize;

    /**
     * Creates a new instance.
     */
    MantisEventAggregator(Registry registry,
                          Clock clock,
                          boolean compress,
                          long flushIntervalMs,
                          int flushIntervalBytes) {

        this.clock = clock;

        this.batchFlushTime =
                SpectatorUtils.buildAndRegisterTimer(
                        registry, "batchFlushTime", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.droppedBatches =
                SpectatorUtils.buildAndRegisterCounter(
                        registry, "droppedBatches", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.flushSuccess =
                SpectatorUtils.buildAndRegisterCounter(
                        registry, "flushSuccess", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.flushFailure =
                SpectatorUtils.buildAndRegisterCounter(
                        registry, "flushFailure", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.batchSize =
                SpectatorUtils.buildAndRegisterGauge(
                        registry, "batchSize", "channel", HttpEventChannel.CHANNEL_TYPE);

        this.flushIntervalMs = flushIntervalMs;
        this.flushIntervalBytes = flushIntervalBytes;
        this.compress = compress;
        this.objectWriter = new ObjectMapper().writer();
        this.currentMessage = new MantisEventEnvelope(
                clock.millis(),
                "origin",   // TODO: Get origin from context.
                new ArrayList<>());
    }

    private boolean acceptOutboundMessage(Object msg) {
        return msg instanceof MantisEvent;
    }

    /**
     * Writes a message into an internal buffer and returns a promise.
     * <p>
     * If it's not time to write a batch, then return a {@code successful} promise.
     * If it's time to write a batch, then return the promise resulting from the batch write operation,
     * which could be {@code successful} or {@code failure}.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (acceptOutboundMessage(msg)) {
            MantisEvent event = (MantisEvent) msg;
            int eventSize = event.size();

            if (currentMessageSize + eventSize > flushIntervalBytes) {
                writeBatch(ctx, promise);
            } else {
                promise.setSuccess();
            }

            currentMessage.addEvent(event);
            currentMessageSize += event.size();
        } else {
            ctx.write(msg, promise);
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        writerTimeout = ctx.executor()
                .scheduleAtFixedRate(
                        new WriterTimeoutTask(ctx),
                        flushIntervalMs,
                        flushIntervalMs,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (writerTimeout != null) {
            writerTimeout.cancel(false);
            writerTimeout = null;
        }
    }

    /**
     * Aggregates {@link MantisEvent}s into a {@link MantisEventEnvelope} and packages it up into a
     * {@link FullHttpRequest} to be sent to the next channel handler in the pipeline.
     * <p>
     * This method will set the status of the promise based on the
     * result of the {@link ChannelHandlerContext#writeAndFlush(Object)} operation.
     */
    void writeBatch(ChannelHandlerContext ctx, ChannelPromise promise) {
        try {
            FullHttpRequest request = buildRequest(ctx, currentMessage);
            int eventListSize = currentMessage.getEventList().size();
            batchSize.set((double) eventListSize);
            final long start = clock.millis();
            ctx.writeAndFlush(request).addListener(future -> {
                final long end = clock.millis();
                batchFlushTime.record(end - start, TimeUnit.MILLISECONDS);
                if (future.isSuccess()) {
                    promise.setSuccess();
                    flushSuccess.increment();
                } else {
                    promise.setFailure(new RetryableException(future.cause().getMessage()));
                    flushFailure.increment();
                }
            });
        } catch (IOException e) {
            LOG.debug("unable to serialize batch", e);
            droppedBatches.increment();
            ctx.fireExceptionCaught(e);
        } finally {
            currentMessage = new MantisEventEnvelope(
                    clock.millis(),
                    "origin",   // TODO: Get origin from context.
                    new ArrayList<>());
            currentMessageSize = 0;
        }
    }

    /**
     * Returns a {@link FullHttpRequest} with headers, body, and event timestamp set.
     */
    FullHttpRequest buildRequest(ChannelHandlerContext ctx, MantisEventEnvelope event)
            throws IOException {
        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
        String urlString = "http://" + address.getHostString() + ':' + address.getPort() + "/api/v1/events";
        URI uri = URI.create(urlString);

        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                uri.getRawPath(),
                ctx.alloc().directBuffer());

        request.headers().add(HttpHeaderNames.ACCEPT, HttpHeaderValues.APPLICATION_JSON);
        request.headers().add(HttpHeaderNames.ORIGIN, "localhost");
        if (compress) {
            request.headers().add(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);
        }
        request.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        event.setTs(clock.millis());

        try (OutputStream bbos = new ByteBufOutputStream(request.content())) {
            objectWriter.writeValue(bbos, event);
        }

        return request;
    }

    private class WriterTimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        WriterTimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isActive()) {
                LOG.debug("channel not active");
                return;
            }

            if (ctx.channel().isWritable() && currentMessage != null && currentMessageSize != 0) {
                writeBatch(ctx, ctx.channel().newPromise());
            }
        }
    }
}
