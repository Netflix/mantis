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

package io.mantisrx.connector.publish.source.http;

import io.mantisrx.connector.publish.core.QueryRegistry;
import io.mantisrx.runtime.Context;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.subjects.Subject;


public class NettySourceHttpServer implements SourceHttpServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettySourceHttpServer.class);

    private final NioEventLoopGroup workerGroup;
    private final NioEventLoopGroup bossGroup;

    private Runnable nettyServerRunnable;
    private volatile boolean isInitialized = false;
    private volatile boolean isStarted = false;
    private final MeterRegistry metricsRegistry;

    public NettySourceHttpServer(Context context, int threadCount, MeterRegistry metricsRegistry) {
        this.bossGroup = new NioEventLoopGroup(threadCount);
        this.workerGroup = new NioEventLoopGroup();
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    public void init(QueryRegistry queryRegistry, Subject<String, String> eventSubject, int port) {
        if (!isInitialized) {
            nettyServerRunnable = () -> {
                try {
                    ServerBootstrap b = new ServerBootstrap();
                    b.option(ChannelOption.SO_BACKLOG, 1024);
                    b.group(bossGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(new HttpServerInitializer(queryRegistry, eventSubject, metricsRegistry));
                    Channel ch = b.bind(port).sync().channel();
                    ch.closeFuture().sync();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage());
                } finally {
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                }
            };

            isInitialized = true;
        }
    }

    @Override
    public void startServer() {
        if (isInitialized && !isStarted) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(nettyServerRunnable);

            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownServer));

            isStarted = true;
        } else {
            throw new IllegalStateException("Server already started");
        }
    }

    @Override
    public void shutdownServer() {
        if (isInitialized && isStarted) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
