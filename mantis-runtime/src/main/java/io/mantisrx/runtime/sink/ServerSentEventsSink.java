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

package io.mantisrx.runtime.sink;

import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.sink.predicate.Predicate;
import io.mantisrx.server.core.ServiceRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.reactivex.mantis.network.push.PushServerSse;
import io.reactivex.mantis.network.push.PushServers;
import io.reactivex.mantis.network.push.Routers;
import io.reactivex.mantis.network.push.ServerConfig;
import io.reactivex.mantis.network.push.Router;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.server.HttpServer;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;


public class ServerSentEventsSink<T> implements SelfDocumentingSink<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSentEventsSink.class);
    private final Func2<Map<String, List<String>>, Context, Void> subscribeProcessor;
    private final BehaviorSubject<Integer> portObservable = BehaviorSubject.create();
    private final Func1<T, String> encoder;
    private final Func1<Throwable, String> errorEncoder;
    private final Predicate<T> predicate;
    private Func2<Map<String, List<String>>, Context, Void> requestPreprocessor;
    private Func2<Map<String, List<String>>, Context, Void> requestPostprocessor;
    private int port = -1;
    private final MantisPropertiesLoader propService;
    private final Router<T> router;

    private PushServerSse<T, Context> pushServerSse;
    private HttpServer<ByteBuf, ServerSentEvent> httpServer;

    public ServerSentEventsSink(Func1<T, String> encoder) {
        this(encoder, null, null);
    }

    ServerSentEventsSink(Func1<T, String> encoder,
                         Func1<Throwable, String> errorEncoder,
                         Predicate<T> predicate) {
        if (errorEncoder == null) {
            // default
            errorEncoder = Throwable::getMessage;
        }
        this.encoder = encoder;
        this.errorEncoder = errorEncoder;
        this.predicate = predicate;
        this.propService = ServiceRegistry.INSTANCE.getPropertiesService();
        this.subscribeProcessor = null;
        this.router = null;
    }

    ServerSentEventsSink(Builder<T> builder) {
        this.encoder = builder.encoder;
        this.errorEncoder = builder.errorEncoder;
        this.predicate = builder.predicate;
        this.requestPreprocessor = builder.requestPreprocessor;
        this.requestPostprocessor = builder.requestPostprocessor;
        this.subscribeProcessor = builder.subscribeProcessor;
        this.propService = ServiceRegistry.INSTANCE.getPropertiesService();
        this.router = builder.router;
    }

    @Override
    public Metadata metadata() {

        StringBuilder description = new StringBuilder();
        description.append("HTTP server streaming results using Server-sent events.  The sink"
            + " supports optional subscription (GET) parameters to change the events emitted"
            + " by the stream.  A sampling interval can be applied to the stream using"
            + " the GET parameter sample=numSeconds.  This will limit the stream rate to"
            + " events-per-numSeconds.");
        if (predicate != null && predicate.getDescription() != null) {
            description.append("  Predicate description: ").append(predicate.getDescription());
        }

        return new Metadata.Builder()
            .name("Server Sent Event Sink")
            .description(description.toString())
            .build();
    }

    private boolean runNewSseServerImpl(String jobName) {
        String legacyServerString = propService.getStringValue("mantis.sse.newServerImpl", "true");
        String legacyServerStringPerJob = propService.getStringValue(jobName + ".mantis.sse.newServerImpl", "false");
        return Boolean.parseBoolean(legacyServerString) || Boolean.parseBoolean(legacyServerStringPerJob);
    }

    private int numConsumerThreads() {
        String consumerThreadsString = propService.getStringValue("mantis.sse.numConsumerThreads", "1");
        return Integer.parseInt(consumerThreadsString);
    }

    private int maxChunkSize() {
        String maxChunkSize = propService.getStringValue("mantis.sse.maxChunkSize", "1000");
        return Integer.parseInt(maxChunkSize);
    }

    private int maxReadTime() {
        String maxChunkSize = propService.getStringValue("mantis.sse.maxReadTimeMSec", "250");
        return Integer.parseInt(maxChunkSize);
    }

    private int maxNotWritableTimeSec() {
        String maxNotWritableTimeSec = propService.getStringValue("mantis.sse.maxNotWritableTimeSec", "-1");
        return Integer.parseInt(maxNotWritableTimeSec);
    }

    private int bufferCapacity() {
        String bufferCapacityString = propService.getStringValue("mantis.sse.bufferCapacity", "25000");
        return Integer.parseInt(bufferCapacityString);
    }

    private boolean useSpsc() {
        String useSpsc = propService.getStringValue("mantis.sse.spsc", "false");
        return Boolean.parseBoolean(useSpsc);
    }

    @Override
    public void call(Context context, PortRequest portRequest, final Observable<T> observable) {
        port = portRequest.getPort();
        if (runNewSseServerImpl(context.getWorkerInfo().getJobClusterName())) {
            LOG.info("Serving modern HTTP SSE server sink on port: " + port);

            String serverName = "SseSink";
            ServerConfig.Builder<T> config = new ServerConfig.Builder<T>()
                .name(serverName)
                .groupRouter(router != null ? router : Routers.roundRobinSse(serverName, encoder))
                .port(port)
                .metricsRegistry(context.getMetricsRegistry())
                .maxChunkTimeMSec(maxReadTime())
                .maxChunkSize(maxChunkSize())
                .bufferCapacity(bufferCapacity())
                .numQueueConsumers(numConsumerThreads())
                .useSpscQueue(useSpsc())
                .maxChunkTimeMSec(getBatchInterval())
                .maxNotWritableTimeSec(maxNotWritableTimeSec());
            if (predicate != null) {
                config.predicate(predicate.getPredicate());
            }
            pushServerSse = PushServers.infiniteStreamSse(config.build(), observable,
                requestPreprocessor, requestPostprocessor,
                subscribeProcessor, context, true);
            pushServerSse.start();
        } else {
            LOG.info("Serving legacy HTTP SSE server sink on port: " + port);

            int batchInterval = getBatchInterval();
            httpServer = RxNetty.newHttpServerBuilder(
                    port,
                    new ServerSentEventRequestHandler<>(
                        observable,
                        encoder,
                        errorEncoder,
                        predicate,
                        requestPreprocessor,
                        requestPostprocessor,
                        context,
                        batchInterval))
                .pipelineConfigurator(PipelineConfigurators.<ByteBuf>serveSseConfigurator())
                .channelOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024, 5 * 1024 * 1024))
                .build();
            httpServer.start();
        }
        portObservable.onNext(port);
    }

    @Override
    public void close() throws IOException {
        if (pushServerSse != null) {
            pushServerSse.shutdown();
        } else if (httpServer != null) {
            try {
                httpServer.shutdown();
            } catch (InterruptedException e) {
                throw new IOException(String.format("Failed to shut down the http server %s", httpServer), e);
            }
        }
    }

    private int getBatchInterval() {
        //default flush interval
        String flushIntervalMillisStr =
            ServiceRegistry.INSTANCE.getPropertiesService()
                .getStringValue("mantis.sse.batchInterval", "100");
        LOG.info("Read fast property mantis.sse.batchInterval" + flushIntervalMillisStr);
        return Integer.parseInt(flushIntervalMillisStr);
    }

    private int getHighWaterMark() {
        String jobName = propService.getStringValue("JOB_NAME", "default");
        int highWaterMark = 5 * 1024 * 1024;
        String highWaterMarkStr = propService.getStringValue(
            jobName + ".sse.highwater.mark",
            Integer.toString(5 * 1024 * 1024));
        LOG.info("Read fast property:" + jobName + ".sse.highwater.mark ->" + highWaterMarkStr);
        try {
            highWaterMark = Integer.parseInt(highWaterMarkStr);
        } catch (Exception e) {
            LOG.error("Error parsing string " + highWaterMarkStr + " exception " + e.getMessage());
        }
        return highWaterMark;
    }

    public int getServerPort() {
        return port;
    }

    /**
     * Notifies you when the mantis job is available to listen to, for use when you want to
     * write unit or regressions tests with the local runner that verify the output.
     */
    public Observable<Integer> portConnections() {
        return portObservable;
    }

    public static class Builder<T> {

        private Func1<T, String> encoder;
        private Func2<Map<String, List<String>>, Context, Void> requestPreprocessor;
        private Func2<Map<String, List<String>>, Context, Void> requestPostprocessor;
        private Func1<Throwable, String> errorEncoder = Throwable::getMessage;
        private Predicate<T> predicate;
        private Func2<Map<String, List<String>>, Context, Void> subscribeProcessor;
        private Router<T> router;

        public Builder<T> withEncoder(Func1<T, String> encoder) {
            this.encoder = encoder;
            return this;
        }

        public Builder<T> withErrorEncoder(Func1<Throwable, String> errorEncoder) {
            this.errorEncoder = errorEncoder;
            return this;
        }

        public Builder<T> withPredicate(Predicate<T> predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder<T> withRequestPreprocessor(Func2<Map<String, List<String>>, Context, Void> preProcessor) {
            this.requestPreprocessor = preProcessor;
            return this;
        }

        public Builder<T> withSubscribePreprocessor(
            Func2<Map<String, List<String>>, Context, Void> subscribeProcessor) {
            this.subscribeProcessor = subscribeProcessor;
            return this;
        }

        public Builder<T> withRequestPostprocessor(Func2<Map<String, List<String>>, Context, Void> postProcessor) {
            this.requestPostprocessor = postProcessor;
            return this;
        }

        public Builder<T> withRouter(Router<T> router) {
            this.router = router;
            return this;
        }

        public ServerSentEventsSink<T> build() {
            return new ServerSentEventsSink<>(this);
        }
    }
}
