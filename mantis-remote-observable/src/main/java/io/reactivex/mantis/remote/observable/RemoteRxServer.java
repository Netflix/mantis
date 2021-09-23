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

package io.reactivex.mantis.remote.observable;

import io.mantisrx.server.core.ServiceRegistry;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.reactivex.mantis.remote.observable.ingress.IngressPolicies;
import io.reactivex.mantis.remote.observable.ingress.IngressPolicy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfiguratorComposite;
import mantis.io.reactivex.netty.server.RxServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RemoteRxServer {

    private static final Logger logger = LoggerFactory.getLogger(RemoteRxServer.class);
    private static boolean enableHeartBeating = true;
    private static boolean enableNettyLogging = false;
    private static boolean enableCompression = true;
    private static int maxFrameLength = 5242880; // 5 MB max frame
    private static int writeBufferTimeMSec = 100; // 100 millisecond buffer
    private RxServer<RemoteRxEvent, List<RemoteRxEvent>> server;
    private RxMetrics metrics;
    private int port;

    RemoteRxServer(RxServer<RemoteRxEvent, List<RemoteRxEvent>> server, RxMetrics metrics) {
        this.server = server;
        this.metrics = metrics;
        loadFastProperties();
    }

    // Note, this constructor is only to support
    // integration with modern implementation
    // during migration
    public RemoteRxServer() {
        metrics = new RxMetrics();
    }

    @SuppressWarnings("rawtypes")
    public RemoteRxServer(Builder builder) {
        port = builder.getPort();
        // setup configuration state for server
        Map<String, ServeConfig> configuredObservables = new HashMap<String, ServeConfig>();
        // add configs
        for (ServeConfig config : builder.getObservablesConfigured()) {
            String observableName = config.getName();
            logger.debug("RemoteRxServer configured with remote observable: " + observableName);
            configuredObservables.put(observableName, config);
        }
        metrics = new RxMetrics();
        // create server
        RxServer<RemoteRxEvent, List<RemoteRxEvent>> server
                = RxNetty.newTcpServerBuilder(port, new RemoteObservableConnectionHandler(configuredObservables, builder.getIngressPolicy(),
                metrics, writeBufferTimeMSec))
                .pipelineConfigurator(new PipelineConfiguratorComposite<RemoteRxEvent, List<RemoteRxEvent>>(
                        new PipelineConfigurator<RemoteRxEvent, RemoteRxEvent>() {
                            @Override
                            public void configureNewPipeline(ChannelPipeline pipeline) {
                                if (enableNettyLogging) {
                                    pipeline.addFirst(new LoggingHandler(LogLevel.ERROR)); // uncomment to enable debug logging
                                }
                                if (enableHeartBeating) {
                                    pipeline.addLast("idleStateHandler", new IdleStateHandler(10, 2, 0));
                                    pipeline.addLast("heartbeat", new HeartbeatHandler());
                                }
                                if (enableCompression) {
                                    pipeline.addLast("gzipInflater", new JdkZlibEncoder(ZlibWrapper.GZIP));
                                    pipeline.addLast("gzipDeflater", new JdkZlibDecoder(ZlibWrapper.GZIP));
                                }
                                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4)); // 4 bytes to encode length
                                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4)); // max frame = half MB

                            }
                        }, new BatchedRxEventPipelineConfigurator()))
                .channelOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024, 5 * 1024 * 1024))

                .build();

        this.server = server;
        logger.info("RemoteRxServer started on port: " + port);
    }

    public RxMetrics getMetrics() {
        return metrics;
    }

    public void start() {
        server.start();
    }

    public void startAndWait() {
        server.startAndWait();
        logger.info("RemoteRxServer shutdown on port: " + port);
    }

    public void shutdown() {
        try {
            server.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("RemoteRxServer shutdown on port: " + port);
    }

    public void blockUntilServerShutdown() {
        try {
            server.waitTillShutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("RemoteRxServer shutdown on port: " + port);
    }

    private void loadFastProperties() {
        String enableHeartBeatingStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.enableHeartBeating", "true");
        if (enableHeartBeatingStr.equals("false")) {
            enableHeartBeating = false;
        }

        String enableNettyLoggingStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.enableLogging", "false");
        if (enableNettyLoggingStr.equals("true")) {
            enableNettyLogging = true;
        }

        String enableCompressionStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.enableCompression", "true");
        if (enableCompressionStr.equals("false")) {
            enableCompression = false;
        }

        String maxFrameLengthStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.maxFrameLength", "5242880");
        if (maxFrameLengthStr != null && maxFrameLengthStr.length() > 0) {
            maxFrameLength = Integer.parseInt(maxFrameLengthStr);
        }

        String writeBufferTimeMSecStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.writeBufferTimeMSec", "100");
        if (writeBufferTimeMSecStr != null && writeBufferTimeMSecStr.length() > 0) {
            writeBufferTimeMSec = Integer.parseInt(maxFrameLengthStr);
        }
    }

    public static class Builder {

        private int port;
        @SuppressWarnings("rawtypes")
        private Set<ServeConfig> observablesConfigured
                = new HashSet<ServeConfig>();
        private IngressPolicy ingressPolicy = IngressPolicies.allowAll();

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder ingressPolicy(IngressPolicy ingressPolicy) {
            this.ingressPolicy = ingressPolicy;
            return this;
        }

        public <T> Builder addObservable(ServeObservable<T> configuration) {
            observablesConfigured.add(configuration);
            return this;
        }

        public <T> Builder addObservable(ServeNestedObservable<T> configuration) {
            observablesConfigured.add(configuration);
            return this;
        }

        public <K, V> Builder addObservable(ServeGroupedObservable<K, V> configuration) {
            observablesConfigured.add(configuration);
            return this;
        }

        public RemoteRxServer build() {
            return new RemoteRxServer(this);
        }

        int getPort() {
            return port;
        }

        @SuppressWarnings("rawtypes")
        Set<ServeConfig> getObservablesConfigured() {
            return observablesConfigured;
        }

        IngressPolicy getIngressPolicy() {
            return ingressPolicy;
        }
    }
}
