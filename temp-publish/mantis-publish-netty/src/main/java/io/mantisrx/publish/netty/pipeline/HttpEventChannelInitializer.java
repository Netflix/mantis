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

import java.time.Clock;

import io.mantisrx.publish.config.MrePublishConfiguration;
import com.netflix.spectator.api.Registry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class HttpEventChannelInitializer extends ChannelInitializer<Channel> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpEventChannel.class);

    private final Registry registry;

    private final boolean gzipEnabled;
    private final long flushIntervalMs;
    private final int flushIntervalBytes;
    private final int idleTimeoutSeconds;
    private final int httpChunkSize;
    private final int writeTimeoutSeconds;

    private final EventLoopGroup encoderEventLoopGroup;

    HttpEventChannelInitializer(
            Registry registry,
            MrePublishConfiguration config,
            EventLoopGroup encoderEventLoopGroup) {

        this.registry = registry;

        this.gzipEnabled = config.getGzipEnabled();
        this.idleTimeoutSeconds = config.getIdleTimeoutSeconds();
        this.httpChunkSize = config.getHttpChunkSize();
        this.writeTimeoutSeconds = config.getWriteTimeoutSeconds();
        this.flushIntervalMs = config.getFlushIntervalMs();
        this.flushIntervalBytes = config.getFlushIntervalBytes();

        this.encoderEventLoopGroup = encoderEventLoopGroup;
    }

    @Override
    protected void initChannel(Channel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(
                new LoggingHandler());
        pipeline.addLast("http-client-codec",
                new HttpClientCodec());
        if (gzipEnabled) {
            pipeline.addLast(encoderEventLoopGroup, "gzip-encoder",
                    new GzipEncoder(registry));
        }
        pipeline.addLast("http-object-aggregator",
                new HttpObjectAggregator(httpChunkSize));
        pipeline.addLast("write-timeout-handler",
                new WriteTimeoutHandler(writeTimeoutSeconds));
        pipeline.addLast("mantis-event-aggregator",
                new MantisEventAggregator(
                        registry,
                        Clock.systemUTC(),
                        gzipEnabled,
                        flushIntervalMs,
                        flushIntervalBytes));
        pipeline.addLast("idle-channel-handler",
                new IdleStateHandler(0, idleTimeoutSeconds, 0));
        pipeline.addLast("event-channel-handler",
                new HttpEventChannelHandler(registry));

        LOG.debug("initializing channel with pipeline: {}", pipeline.toMap());
    }
}
