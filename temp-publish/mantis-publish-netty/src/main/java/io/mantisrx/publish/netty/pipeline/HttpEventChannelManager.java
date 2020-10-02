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

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.config.MrePublishConfiguration;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages a cache of Netty {@link Channel}s. It configures and creates channels,
 * adds event listeners to channels, and returns them to the caller when queried for by address.
 * Channels are associated with a single {@link EventLoopGroup}.
 */
public class HttpEventChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(HttpEventChannel.class);

    private final Counter connectionSuccess;
    private final Counter connectionFailure;
    private final AtomicDouble liveConnections;
    private final AtomicDouble nettyChannelBufferSize;

    private final int lowWriteBufferWatermark;
    private final int highWriteBufferWatermark;

    private final EventLoopGroup eventLoopGroup;
    private final EventLoopGroup encoderEventLoopGroup;
    private final Bootstrap bootstrap;
    private final ConcurrentMap<String, Channel> channels;

    /**
     * Creates a new instance.
     */
    public HttpEventChannelManager(
            Registry registry,
            MrePublishConfiguration config) {

        this.connectionSuccess =
                SpectatorUtils.buildAndRegisterCounter(
                        registry, "connectionSuccess", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.connectionFailure =
                SpectatorUtils.buildAndRegisterCounter(
                        registry, "connectionFailure", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.liveConnections =
                SpectatorUtils.buildAndRegisterGauge(
                        registry, "liveConnections", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.nettyChannelBufferSize =
                SpectatorUtils.buildAndRegisterGauge(
                        registry, "bufferSize", "channel", HttpEventChannel.CHANNEL_TYPE);

        this.lowWriteBufferWatermark = config.getLowWriteBufferWatermark();
        this.highWriteBufferWatermark = config.getHighWriteBufferWatermark();

        this.eventLoopGroup = new NioEventLoopGroup(config.getIoThreads());

        boolean gzipEnabled = config.getGzipEnabled();
        if (gzipEnabled) {
            this.encoderEventLoopGroup = new DefaultEventLoopGroup(config.getCompressionThreads());
        } else {
            this.encoderEventLoopGroup = null;
        }

        this.bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, MantisMessageSizeEstimator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(
                                lowWriteBufferWatermark, highWriteBufferWatermark))
                .handler(new HttpEventChannelInitializer(
                        registry, config, encoderEventLoopGroup));
        this.channels = new ConcurrentHashMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread(this.eventLoopGroup::shutdownGracefully));
    }

    /**
     * Returns an existing channel or creates and caches a new channel.
     * <p>
     * The newly-cached channel may not be operational as creation is asynchronous.
     * <p>
     * Since the connect operation is asynchronous, callers must:
     * <p>
     * 1. check the channel's state before writing
     * 2. invalidate the cache on connection closed/refused exceptions in the supplied future.
     * <p>
     * This method registers a {@link ChannelFutureListener} to listen for {@link Channel#close()} events.
     * On a {@code close()} event, this channel manager instance will deregister the closed Netty channel
     * by removing it from its {@link HttpEventChannelManager#channels} cache.
     */
    Channel findOrCreate(InetSocketAddress address) {
        Channel channel = find(address);

        if (channel == null) {
            LOG.debug("creating new channel for {}", address);
            ChannelFuture channelFuture = bootstrap.connect(address);
            channel = channelFuture.channel();

            channels.put(getHostPortString(address), channel);

            // Add listener to handle connection closed events, which could happen on exceptions.
            channel.closeFuture().addListener(future -> {
                LOG.debug("closing channel for {}", address);
                channels.remove(getHostPortString(address));
                liveConnections.set((double) channels.size());
            });

            // Add listener to handle the result of the connection event.
            channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    LOG.debug("connection success for {}", address);
                    connectionSuccess.increment();
                    liveConnections.set((double) channels.size());
                } else {
                    LOG.debug("failed to connect to {}", address);
                    connectionFailure.increment();
                }
            });
        }

        nettyChannelBufferSize.set(highWriteBufferWatermark - channel.bytesBeforeUnwritable());

        return channel;
    }

    private Channel find(InetSocketAddress address) {
        return channels.get(getHostPortString(address));
    }

    /**
     * Request to close the Netty channel at the given address.
     * <p>
     * Note that we don't need to explicitly remove the channel from the
     * {@link HttpEventChannelManager#channels} cache because it adds a {@link ChannelFutureListener}
     * upon {@link Channel} creation (in {@link HttpEventChannelManager#findOrCreate(InetSocketAddress)})
     * to listen for {@link Channel#close()} events. On a {@code close()} event,
     * the channel will be removed from the cache.
     */
    void close(InetSocketAddress address) {
        Channel channel = find(address);
        if (channel != null) {
            channel.close();
        }
    }

    private String getHostPortString(InetSocketAddress address) {
        return address.getHostString() + ':' + address.getPort();
    }
}
