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

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.internal.exceptions.NonRetryableException;
import io.mantisrx.publish.internal.exceptions.RetryableException;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.netty.proto.MantisEvent;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link EventChannel} backed by Netty. All I/O operations are asynchronous.
 */
public class HttpEventChannel implements EventChannel {

    /**
     * Defines Netty as an {@link EventChannel} channel type.
     */
    public static final String CHANNEL_TYPE = "netty";
    private static final Logger LOG = LoggerFactory.getLogger(HttpEventChannel.class);
    private final Registry registry;

    private final Counter writeSuccess;
    private final Counter writeFailure;
    private final Counter nettyChannelDropped;

    private final Timer nettyWriteTime;

    private final HttpEventChannelManager channelManager;

    /**
     * Creates an instance.
     */
    public HttpEventChannel(
            Registry registry,
            HttpEventChannelManager channelManager) {

        this.registry = registry;

        this.writeSuccess =
                SpectatorUtils.buildAndRegisterCounter(
                        this.registry, "writeSuccess", "channel", CHANNEL_TYPE);
        this.writeFailure =
                SpectatorUtils.buildAndRegisterCounter(
                        this.registry, "writeFailure", "channel", CHANNEL_TYPE);
        this.nettyChannelDropped =
                SpectatorUtils.buildAndRegisterCounter(
                        this.registry, "mantisEventsDropped", "channel", CHANNEL_TYPE, "reason", "nettyBufferFull");

        this.nettyWriteTime =
                SpectatorUtils.buildAndRegisterTimer(
                        this.registry, "writeTime", "channel", CHANNEL_TYPE);

        this.channelManager = channelManager;
    }

    /**
     * Writes an event over the network via {@code POST} request, typically to a Mantis Source Job worker instance.
     *
     * @param worker a {@link MantisWorker} representing remote host.
     * @param event  the output to write over the network.
     *
     * @return a {@link Future<Void>} which could indicate a Success or Retryable/NonRetryable exception.
     */
    @Override
    public CompletableFuture<Void> send(MantisWorker worker, Event event) {
        InetSocketAddress address = worker.toInetSocketAddress();
        Channel channel = channelManager.findOrCreate(address);

        CompletableFuture<Void> future = new CompletableFuture<>();
        if (channel.isActive()) {
            if (channel.isWritable()) {
                LOG.debug("channel is writable: {} bytes remaining", channel.bytesBeforeUnwritable());
                final long nettyStart = registry.clock().wallTime();
                // TODO: Channel#setAttribute(future), complete (or exceptionally) in HttpEventChannelHandler.
                MantisEvent mantisEvent = new MantisEvent(1, event.toJsonString());
                channel.writeAndFlush(mantisEvent).addListener(f -> {
                    if (f.isSuccess()) {
                        writeSuccess.increment();
                        future.complete(null);
                    } else {
                        LOG.debug("failed to send event over netty channel", f.cause());
                        writeFailure.increment();
                        future.completeExceptionally(new RetryableException(f.cause().getMessage()));
                    }
                });
                final long nettyEnd = registry.clock().wallTime();
                nettyWriteTime.record(nettyEnd - nettyStart, TimeUnit.MILLISECONDS);
            } else {
                LOG.debug("channel not writable: {} bytes before writable", channel.bytesBeforeWritable());
                nettyChannelDropped.increment();
                future.completeExceptionally(
                        new RetryableException("channel not writable: "
                                + channel.bytesBeforeWritable() + " bytes before writable"));
            }
        } else {
            future.completeExceptionally(new NonRetryableException("channel not active"));
        }

        return future;
    }

    /**
     * Returns the buffer size as a percentage utilization of the channel's internal transport. For example, see
     * {@link HttpEventChannel} which uses a Netty {@link Channel} for its underlying transport which will return the
     * underlying Netty Channel NIO buffer size.
     * <p>
     * An {@link EventChannel} may have many underlying connections which implement the same transport. For example,
     * the {@link HttpEventChannel} may have many Netty {@link Channel}s to connect to external hosts. Each of these
     * Netty Channels will have their own buffers.
     *
     * @param worker a {@link MantisWorker} which is used to query for the buffer size of a specific internal transport.
     */
    @Override
    public double bufferSize(MantisWorker worker) {
        InetSocketAddress address = worker.toInetSocketAddress();
        Channel channel = channelManager.findOrCreate(address);
        return channel.bytesBeforeUnwritable() / channel.config().getWriteBufferHighWaterMark();
    }

    @Override
    public void close(MantisWorker worker) {
        InetSocketAddress address = worker.toInetSocketAddress();
        channelManager.close(address);
    }
}
