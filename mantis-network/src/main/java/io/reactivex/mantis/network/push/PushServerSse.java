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

package io.reactivex.mantis.network.push;

import com.mantisrx.common.utils.MantisSSEConstants;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.compression.CompressionUtils;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.mql.jvm.interfaces.MQLServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerRequest;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import mantis.io.reactivex.netty.protocol.http.server.RequestHandler;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import mantis.io.reactivex.netty.server.RxServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;


public class PushServerSse<T, S> extends PushServer<T, ServerSentEvent> {

    private static final Logger logger = LoggerFactory.getLogger(PushServerSse.class);

    public static final String PUSH_SERVER_METRIC_GROUP_NAME = "PushServerSse";
    public static final String PUSH_SERVER_LEGACY_METRIC_GROUP_NAME = "ServerSentEventRequestHandler";
    public static final String PROCESSED_COUNTER_METRIC_NAME = "processedCounter";
    public static final String DROPPED_COUNTER_METRIC_NAME = "droppedCounter";
    public static final String CLIENT_ID_TAG_NAME = "clientId";
    public static final String SOCK_ADDR_TAG_NAME = "sockAddr";

    private Func2<Map<String, List<String>>, S, Void> requestPreprocessor;
    private Func2<Map<String, List<String>>, S, Void> requestPostprocessor;
    private final Func2<Map<String, List<String>>, S, Void> subscribeProcessor;

    private S processorState;
    private Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate;
    private boolean supportLegacyMetrics;
    private MetricsRegistry metricsRegistry;

    public PushServerSse(PushTrigger<T> trigger, ServerConfig<T> config,
                         PublishSubject<String> serverSignals,
                         Func2<Map<String, List<String>>, S, Void> requestPreprocessor,
                         Func2<Map<String, List<String>>, S, Void> requestPostprocessor,
                         final Func2<Map<String, List<String>>, S, Void> subscribeProcessor,
                         S state, boolean supportLegacyMetrics) {
        super(trigger, config, serverSignals);
        this.metricsRegistry = config.getMetricsRegistry();
        this.predicate = config.getPredicate();
        this.processorState = state;
        this.requestPostprocessor = requestPostprocessor;
        this.requestPreprocessor = requestPreprocessor;
        this.subscribeProcessor = subscribeProcessor;
        this.supportLegacyMetrics = supportLegacyMetrics;
    }

    private Metrics registerSseMetrics(String uniqueClientId, String socketAddrStr) {
        final BasicTag clientIdTag = new BasicTag(CLIENT_ID_TAG_NAME, Optional.ofNullable(uniqueClientId).orElse("none"));
        final BasicTag sockAddrTag = new BasicTag(SOCK_ADDR_TAG_NAME, Optional.ofNullable(socketAddrStr).orElse("none"));

        final String metricGroup = supportLegacyMetrics ? PUSH_SERVER_LEGACY_METRIC_GROUP_NAME : PUSH_SERVER_METRIC_GROUP_NAME;
        Metrics sseSinkMetrics = new Metrics.Builder()
            .id(metricGroup, clientIdTag, sockAddrTag)
            .addCounter(PROCESSED_COUNTER_METRIC_NAME)
            .addCounter(DROPPED_COUNTER_METRIC_NAME)
            .build();

        sseSinkMetrics = metricsRegistry.registerAndGet(sseSinkMetrics);

        return sseSinkMetrics;
    }

    @Override
    public RxServer<?, ?> createServer() {

        RxServer<HttpServerRequest<String>, HttpServerResponse<ServerSentEvent>> server = RxNetty.newHttpServerBuilder(port,
                new RequestHandler<String, ServerSentEvent>() {
                    @Override
                    public Observable<Void> handle(
                        HttpServerRequest<String> request,
                        final HttpServerResponse<ServerSentEvent> response) {

                        final Map<String, List<String>> queryParameters = request.getQueryParameters();
                        final Counter sseProcessedCounter;
                        final Counter sseDroppedCounter;

                        // heartbeat state
                        boolean enableHeartbeats = false;

                        boolean enableBinaryOutput = false;

                        final AtomicLong heartBeatReadIdleSec = new AtomicLong(2);

                        SerializedSubject<String, String> metaMsgSubject = PublishSubject.<String>create().toSerialized();

                        final AtomicLong metaMessagesFreqMSec = new AtomicLong(1000);

                        boolean enableMetaMessages = false;
                        final AtomicLong lastWriteTime = new AtomicLong();
                        Subscription heartbeatSubscription = null;
                        Subscription metaMsgSubscription = null;

                        // sample state
                        boolean enableSampling = false;
                        long samplingTimeMsec = 0;

                        // client state
                        String groupId = null;
                        String slotId = null;
                        String id = null;
                        Func1<T, Boolean> predicateFunction = null;
                        String availabilityZone = null;

                        if (predicate != null) {
                            predicateFunction = predicate.call(queryParameters);
                        }

                        byte[] delimiter = CompressionUtils.MANTIS_SSE_DELIMITER_BINARY;

                        if (queryParameters != null && !queryParameters.isEmpty()) {

                            if (queryParameters.containsKey(MantisSSEConstants.ID)) {
                                id = queryParameters.get(MantisSSEConstants.ID).get(0);
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.SLOT_ID)) {
                                slotId = queryParameters.get(MantisSSEConstants.SLOT_ID).get(0);
                            }
                            // support groupId and clientId for grouping
                            if (queryParameters.containsKey(MantisSSEConstants.GROUP_ID)) {
                                groupId = queryParameters.get(MantisSSEConstants.GROUP_ID).get(0);
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.CLIENT_ID)) {
                                groupId = queryParameters.get(MantisSSEConstants.CLIENT_ID).get(0);
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.HEARTBEAT_SEC)) {
                                heartBeatReadIdleSec.set(Long.parseLong(queryParameters.get(MantisSSEConstants.HEARTBEAT_SEC).get(0)));
                                if (heartBeatReadIdleSec.get() < 1) {
                                    throw new IllegalArgumentException("Sampling rate too low: " + samplingTimeMsec);
                                }
                                enableHeartbeats = true;
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION)) {
                                String enableBinaryOutputStr = queryParameters.get(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION).get(0);
                                if ("true".equalsIgnoreCase(enableBinaryOutputStr)) {
                                    logger.info("Binary compression requested");
                                    enableBinaryOutput = true;
                                }
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.ENABLE_PINGS)) {
                                String enablePings = queryParameters.get(MantisSSEConstants.ENABLE_PINGS).get(0);
                                if ("true".equalsIgnoreCase(enablePings)) {
                                    enableHeartbeats = true;
                                }
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.ENABLE_META_MESSAGES)) {
                                String enableMetaMessagesStr = queryParameters.get(MantisSSEConstants.ENABLE_META_MESSAGES).get(0);
                                if ("true".equalsIgnoreCase(enableMetaMessagesStr)) {
                                    enableMetaMessages = true;
                                }
                            }

                            if (queryParameters.containsKey(MantisSSEConstants.META_MESSAGES_SEC)) {
                                metaMessagesFreqMSec.set(Long.parseLong(queryParameters.get(MantisSSEConstants.META_MESSAGES_SEC).get(0)));
                                if (metaMessagesFreqMSec.get() < 250) {
                                    throw new IllegalArgumentException("Meta message frequence rate too low: " + metaMessagesFreqMSec.get());
                                }
                                enableMetaMessages = true;

                            }

                            if (queryParameters.containsKey(MantisSSEConstants.SAMPLE)) {
                                samplingTimeMsec = Long.parseLong(queryParameters.get(MantisSSEConstants.SAMPLE).get(0)) * 1000;
                                if (samplingTimeMsec < 50) {
                                    throw new IllegalArgumentException("Sampling rate too low: " + samplingTimeMsec);
                                }
                                enableSampling = true;
                            }
                            if (queryParameters.containsKey(MantisSSEConstants.SAMPLE_M_SEC)) {
                                samplingTimeMsec = Long.parseLong(queryParameters.get(MantisSSEConstants.SAMPLE_M_SEC).get(0));
                                if (samplingTimeMsec < 50) {
                                    throw new IllegalArgumentException("Sampling rate too low: " + samplingTimeMsec);
                                }
                                enableSampling = true;
                            }

                            if (queryParameters.containsKey(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER)) {
                                String rawDelimiter = queryParameters.get(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER).get(0);
                                if (rawDelimiter != null && !rawDelimiter.isEmpty()) {
                                    delimiter = rawDelimiter.getBytes();
                                }
                            }

                            if (queryParameters.containsKey(MantisSSEConstants.MQL)) {
                                String query = queryParameters.get(MantisSSEConstants.MQL).get(0);
                                if (MQLServer.parses(query)) {
                                    Query q = MQLServer.parse(query);
                                    predicateFunction = (T datum) -> datum instanceof Map ? q.matches((Map) datum) : true;
                                }
                            }

                            if (queryParameters.containsKey(MantisSSEConstants.AVAILABILITY_ZONE)) {
                                availabilityZone = queryParameters.get(MantisSSEConstants.AVAILABILITY_ZONE).get(0);
                            }
                        }

                        InetSocketAddress socketAddress = (InetSocketAddress) response.getChannel().remoteAddress();

                        Metrics metrics;
                        if (groupId == null) {
                            String address = socketAddress.getAddress().toString();
                            metrics = registerSseMetrics(address, address);
                        } else {
                            metrics = registerSseMetrics(groupId, socketAddress.getAddress().toString());
                        }
                        sseProcessedCounter = metrics.getCounter(PROCESSED_COUNTER_METRIC_NAME);
                        sseDroppedCounter = metrics.getCounter(DROPPED_COUNTER_METRIC_NAME);

                        response.getHeaders().set("Access-Control-Allow-Origin", "*");
                        response.getHeaders().set("content-type", "text/event-stream");
                        response.getHeaders().set("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
                        response.getHeaders().set("Pragma", "no-cache");
                        response.flush();

                        if (queryParameters != null && requestPreprocessor != null) {
                            requestPreprocessor.call(queryParameters, processorState);
                        }

                        if (enableMetaMessages && metaMessagesFreqMSec.get() > 0) {
                            logger.info("Enabling Meta messages, interval : " + metaMessagesFreqMSec.get() + " ms");
                            metaMsgSubscription = metaMsgSubject
                                .throttleLast(metaMessagesFreqMSec.get(), TimeUnit.MILLISECONDS)
                                .doOnNext((String t) -> {
                                        if (t != null && !t.isEmpty()) {
                                            long currentTime = System.currentTimeMillis();
                                            ByteBuf data = response.getAllocator().buffer().writeBytes(t.getBytes());
                                            response.writeAndFlush(new ServerSentEvent(data));
                                            lastWriteTime.set(currentTime);
                                        }
                                    }
                                ).subscribe();
                        }

                        if (enableHeartbeats && heartBeatReadIdleSec.get() > 0) {
                            logger.info("Enabling hearts, interval: " + heartBeatReadIdleSec);
                            heartbeatSubscription = Observable
                                .interval(2, heartBeatReadIdleSec.get(), TimeUnit.SECONDS)
                                .doOnNext((Long t1) -> {
                                        long currentTime = System.currentTimeMillis();
                                        long diff = (currentTime - lastWriteTime.get()) / 1000;
                                        if (diff > heartBeatReadIdleSec.get()) {
                                            ByteBuf data = response.getAllocator().buffer().writeBytes("ping".getBytes());
                                            response.writeAndFlush(new ServerSentEvent(data));
                                            lastWriteTime.set(currentTime);
                                        }
                                    }
                                ).subscribe();
                        }
                        Action0 connectionClosedCallback = null;
                        if (queryParameters != null && requestPostprocessor != null) {
                            connectionClosedCallback = new Action0() {
                                @Override
                                public void call() {
                                    requestPostprocessor.call(queryParameters, processorState);
                                }
                            };
                        }

                        class SubscribeCallback implements Action0 {

                            @Override
                            public void call() {
                                if (queryParameters != null && subscribeProcessor != null) {
                                    subscribeProcessor.call(queryParameters, processorState);
                                }
                            }
                        }

                        return manageConnectionWithCompression(response, socketAddress.getHostString(), socketAddress.getPort(), groupId,
                            slotId, id, lastWriteTime,
                            enableHeartbeats, heartbeatSubscription, enableSampling, samplingTimeMsec, metaMsgSubject, metaMsgSubscription,
                            predicateFunction, connectionClosedCallback, sseProcessedCounter,
                            sseDroppedCounter,
                            new SubscribeCallback(), enableBinaryOutput, true, delimiter, availabilityZone);
                    }
                })
            .pipelineConfigurator(PipelineConfigurators.serveSseConfigurator())
            .channelOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024, 5 * 1024 * 1024))

            .build();
        return server;
    }


}
