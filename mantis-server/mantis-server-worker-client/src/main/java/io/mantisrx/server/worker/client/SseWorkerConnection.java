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

package io.mantisrx.server.worker.client;

import static com.mantisrx.common.utils.MantisMetricStringConstants.DROP_OPERATOR_INCOMING_METRIC_GROUP;

import com.mantisrx.common.utils.MantisSSEConstants;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.compression.CompressionUtils;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.runtime.parameter.SinkParameter;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivx.mantis.operators.DropOperator;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import mantis.io.reactivex.netty.metrics.MetricEventsListenerFactory;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.PublishSubject;


public class SseWorkerConnection {

    private static final Logger logger = LoggerFactory.getLogger(SseWorkerConnection.class);
    private static final String metricNamePrefix = DROP_OPERATOR_INCOMING_METRIC_GROUP;

    private static MetricEventsListenerFactory metricEventsListenerFactory;

    protected final PublishSubject<Boolean> shutdownSubject = PublishSubject.create();
    final AtomicLong lastDataReceived = new AtomicLong(System.currentTimeMillis());
    private final String connectionType;
    private final String hostname;
    private final int port;
    private final MetricGroupId metricGroupId;
    private final Counter pingCounter;

    private final boolean reconnectUponConnectionReset;
    private final Action1<Boolean> updateConxStatus;
    private final Action1<Boolean> updateDataRecvngStatus;
    private final Action1<Throwable> connectionResetHandler;
    private final long dataRecvTimeoutSecs;
    private final CopyOnWriteArraySet<MetricGroupId> metricsSet;
    private final int bufferSize;
    private final SinkParameters sinkParameters;
    private final boolean disablePingFiltering;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isReceivingData = new AtomicBoolean(false);
    HttpClient<ByteBuf, ServerSentEvent> client;
    private boolean compressedBinaryInputEnabled = false;
    private volatile boolean isShutdown = false;
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic =
            new Func1<Observable<? extends Throwable>, Observable<?>>() {
                @Override
                public Observable<?> call(Observable<? extends Throwable> attempts) {
                    if (!reconnectUponConnectionReset)
                        return Observable.empty();
                    return attempts
                            .zipWith(Observable.range(1, Integer.MAX_VALUE), new Func2<Throwable, Integer, Integer>() {
                                @Override
                                public Integer call(Throwable t1, Integer integer) {
                                    return integer;
                                }
                            })
                            .flatMap(new Func1<Integer, Observable<?>>() {
                                @Override
                                public Observable<?> call(Integer integer) {
                                    if (isShutdown) {
                                        logger.info("{}: Is shutdown, stopping retries", getName());
                                        return Observable.empty();
                                    }
                                    long delay = 2 * (integer > 10 ? 10 : integer);
                                    logger.info("{}: retrying conx after sleeping for {} secs", getName(), delay);
                                    return Observable.timer(delay, TimeUnit.SECONDS);
                                }
                            });
                }
            };
    private long lastDataDropValue = 0L;
    public SseWorkerConnection(final String connectionType,
                               final String hostname,
                               final Integer port,
                               final Action1<Boolean> updateConxStatus,
                               final Action1<Boolean> updateDataRecvngStatus,
                               final Action1<Throwable> connectionResetHandler,
                               final long dataRecvTimeoutSecs,
                               final boolean reconnectUponConnectionReset,
                               final CopyOnWriteArraySet<MetricGroupId> metricsSet,
                               final int bufferSize,
                               final SinkParameters sinkParameters,
                               final MetricGroupId metricGroupId) {
        this(connectionType, hostname, port, updateConxStatus, updateDataRecvngStatus, connectionResetHandler,
                dataRecvTimeoutSecs, reconnectUponConnectionReset, metricsSet, bufferSize, sinkParameters, false,
                metricGroupId);
    }
    public SseWorkerConnection(final String connectionType,
                               final String hostname,
                               final Integer port,
                               final Action1<Boolean> updateConxStatus,
                               final Action1<Boolean> updateDataRecvngStatus,
                               final Action1<Throwable> connectionResetHandler,
                               final long dataRecvTimeoutSecs,
                               final boolean reconnectUponConnectionReset,
                               final CopyOnWriteArraySet<MetricGroupId> metricsSet,
                               final int bufferSize,
                               final SinkParameters sinkParameters,
                               final boolean disablePingFiltering,
                               final MetricGroupId metricGroupId) {
        this.connectionType = connectionType;
        this.hostname = hostname;
        this.port = port;

        this.metricGroupId = metricGroupId;
        final MetricGroupId connHealthMetricGroup = new MetricGroupId("ConnectionHealth");
        Metrics m = new Metrics.Builder()
                .id(connHealthMetricGroup)
                .addCounter("pingCount")
                .build();
        this.pingCounter = m.getCounter("pingCount");

        this.updateConxStatus = updateConxStatus;
        this.updateDataRecvngStatus = updateDataRecvngStatus;
        this.connectionResetHandler = connectionResetHandler;
        this.dataRecvTimeoutSecs = dataRecvTimeoutSecs;
        this.reconnectUponConnectionReset = reconnectUponConnectionReset;
        this.metricsSet = metricsSet;
        this.bufferSize = bufferSize;
        this.sinkParameters = sinkParameters;
        if (this.sinkParameters != null) {
            this.compressedBinaryInputEnabled = isCompressedBinaryInputEnabled(this.sinkParameters.getSinkParams());
        }
        this.disablePingFiltering = disablePingFiltering;
    }

    private boolean isCompressedBinaryInputEnabled(List<SinkParameter> sinkParams) {
        for (SinkParameter sinkParam : sinkParams) {
            if (MantisSSEConstants.MANTIS_ENABLE_COMPRESSION.equals(sinkParam.getName()) && "true".equalsIgnoreCase(sinkParam.getValue())) {
                return true;
            }
        }
        return false;
    }

    public static void useMetricListenersFactory(MetricEventsListenerFactory factory) {
        metricEventsListenerFactory = factory;
    }

    public String getName() {
        return "Sse" + connectionType + "Connection: " + hostname + ":" + port;
    }

    public synchronized void close() throws Exception {
        logger.info("Closing sse connection to " + hostname + ":" + port);
        if (isShutdown)
            return;
        shutdownSubject.onNext(true);
        shutdownSubject.onCompleted();
        isShutdown = true;
        closeConnected();
    }

    private <I, O> HttpClientBuilder<I, O> newHttpClientBuilder(String host, int port) {
        HttpClientBuilder<I, O> builder =
            new MantisHttpClientBuilder<I, O>(host, port).withMaxConnections(1000).enableWireLogging(LogLevel.DEBUG);
        if (null != metricEventsListenerFactory) {
            builder.withMetricEventsListenerFactory(metricEventsListenerFactory);
        }
        return builder;
    }

    public synchronized Observable<MantisServerSentEvent> call() {
        if (isShutdown)
            return Observable.empty();

        client = this.<ByteBuf, ServerSentEvent>newHttpClientBuilder(hostname, port)
                .pipelineConfigurator(PipelineConfigurators.<ByteBuf>clientSseConfigurator())
                //.enableWireLogging(LogLevel.ERROR)
                .withNoConnectionPooling()
                .build();

        StringBuilder sp = new StringBuilder();

        String delimiter = sinkParameters == null
                ? null
                : sinkParameters.getSinkParams().stream()
                        .filter(s -> s.getName()
                                .equalsIgnoreCase(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER))
                        .findFirst()
                        .map(SinkParameter::getValue)
                        .orElse(null);

        if (sinkParameters != null) {
            sp.append(sinkParameters.toString());
        }



        sp.append(sp.length() == 0 ? getDefaultSinkParams("?") : getDefaultSinkParams("&"));

        String uri = "/" + sp.toString();
        logger.info(getName() + ": Using uri: " + uri);
        return
                client.submit(HttpClientRequest.createGet(uri))
                        .takeUntil(shutdownSubject)
                        .takeWhile((serverSentEventHttpClientResponse) -> !isShutdown)
                        .filter((HttpClientResponse<ServerSentEvent> response) -> {
                            if (!response.getStatus().reasonPhrase().equals("OK"))
                                logger.warn(getName() + ":Trying to continue after unexpected response from sink: "
                                        + response.getStatus().reasonPhrase());
                            return response.getStatus().reasonPhrase().equals("OK");
                        })
                        .flatMap((HttpClientResponse<ServerSentEvent> response) -> {
                            if (!isConnected.getAndSet(true)) {
                                if (updateConxStatus != null)
                                    updateConxStatus.call(true);
                            }
                            return streamContent(response, updateDataRecvngStatus, dataRecvTimeoutSecs, delimiter);
                        })
                        .doOnError((Throwable throwable) -> {
                            // Only reset connection status, do not close SSE http client.
                            // otherwise it would cause infinite retry loop by the retryWhen below
                            resetConnected();
                            logger.warn("{}: Error on getting response from SSE server: {}",
                                getName(), throwable.getMessage());
                            connectionResetHandler.call(throwable);
                        })
                        .retryWhen(retryLogic)
                        .doOnError((Throwable throwable) -> {
                            closeConnected();
                            logger.error("{}: non-retryable error on getting response from SSE server: ",
                                getName(), throwable);
                            connectionResetHandler.call(throwable);
                        })
                        .doOnCompleted(this::closeConnected);
    }

    /***
     * close SSE connection status and close the SSE http client.
     */
    private void closeConnected() {
        // explicitly close the connection
        ((MantisHttpClientImpl<?, ?>)client).closeConn();
        resetConnected();
    }

    private void resetConnected() {
        ((MantisHttpClientImpl<?, ?>)client).resetConn();
        if (isConnected.getAndSet(false)) {
            if (updateConxStatus != null)
                updateConxStatus.call(false);
        }
        if (isReceivingData.compareAndSet(true, false))
            if (updateDataRecvngStatus != null)
                synchronized (updateDataRecvngStatus) {
                    updateDataRecvngStatus.call(false);
                }
    }

    protected Observable<MantisServerSentEvent> streamContent(HttpClientResponse<ServerSentEvent> response,
                                                            final Action1<Boolean> updateDataRecvngStatus,
                                                            final long dataRecvTimeoutSecs, String delimiter) {
        long interval = Math.max(1, dataRecvTimeoutSecs / 2);
        if (updateDataRecvngStatus != null) {
            Observable.interval(interval, interval, TimeUnit.SECONDS)
                    .doOnNext((Long aLong) -> {
                        if (!isShutdown) {
                            if (hasDataDrop() || System.currentTimeMillis() > (lastDataReceived.get() + dataRecvTimeoutSecs * 1000)) {
                                if (isReceivingData.compareAndSet(true, false))
                                    synchronized (updateDataRecvngStatus) {
                                        updateDataRecvngStatus.call(false);
                                    }
                            } else {
                                if (isConnected.get() && isReceivingData.compareAndSet(false, true))
                                    synchronized (updateDataRecvngStatus) {
                                        updateDataRecvngStatus.call(true);
                                    }
                            }
                        }
                    })
                    .takeUntil(shutdownSubject)
                    .takeWhile((o) -> !isShutdown)
                    .doOnCompleted(() -> {
                        if (isReceivingData.compareAndSet(true, false))
                            synchronized (updateDataRecvngStatus) {
                                updateDataRecvngStatus.call(false);
                            }
                    })
                    .subscribe();
        }
        return response.getContent()
                .lift(new DropOperator<ServerSentEvent>(metricGroupId))
                .rebatchRequests(this.bufferSize <= 0 ? 1 : this.bufferSize)
                .flatMap((ServerSentEvent t1) -> {
                    lastDataReceived.set(System.currentTimeMillis());
                    if (isConnected.get() && isReceivingData.compareAndSet(false, true))
                        if (updateDataRecvngStatus != null)
                            synchronized (updateDataRecvngStatus) {
                                updateDataRecvngStatus.call(true);
                            }

                    if (t1.hasEventType() && t1.getEventTypeAsString().startsWith("error:")) {
                        return Observable.error(new SseException(ErrorType.Retryable, "Got error SSE event: " + t1.contentAsString()));
                    }
                    return Observable.just(t1.contentAsString());
                }, 1)
                .filter(data -> {
                    if (data.startsWith("ping")) {
                        pingCounter.increment();
                        return this.disablePingFiltering;
                    }
                    return true;
                })
                .flatMapIterable((data) -> {
                    boolean useSnappy = true;
                    return CompressionUtils.decompressAndBase64Decode(data, compressedBinaryInputEnabled, useSnappy, delimiter);
                }, 1)
                .takeUntil(shutdownSubject)
                .takeWhile((event) -> !isShutdown);
    }

    private boolean hasDataDrop() {
        final Collection<Metrics> metrics = MetricsRegistry.getInstance().getMetrics(metricNamePrefix);
        long totalDataDrop = 0L;
        if (metrics != null && !metrics.isEmpty()) {
            //logger.info("Got " + metrics.size() + " metrics for DropOperator");
            for (Metrics m : metrics) {
                final Counter dropped = m.getCounter("" + DropOperator.Counters.dropped);
                final Counter onNext = m.getCounter("" + DropOperator.Counters.onNext);
                if (dropped != null)
                    totalDataDrop += dropped.value();
            }
        }
        if (totalDataDrop > lastDataDropValue) {
            lastDataDropValue = totalDataDrop;
            return true;
        }
        return false;
    }

    private String getDefaultSinkParams(String prefix) {
        String groupId = System.getenv("JOB_ID");
        String slotId = System.getenv("WORKER_INDEX");
        String id = System.getenv("WORKER_NUMBER");

        if (groupId != null && !groupId.isEmpty() && slotId != null && !slotId.isEmpty() && id != null && !id.isEmpty())
            return prefix + "groupId=" + groupId + "&slotId=" + slotId + "&id=" + id;
        return "";
    }


    private static enum ErrorType {
        Retryable,
        Unknown
    }

    private static class SseException extends RuntimeException {

        private final ErrorType type;

        private SseException(ErrorType type, String message) {
            super(type + ": " + message);
            this.type = type;
        }

        private SseException(ErrorType type, String message, Throwable cause) {
            super(type + ": " + message, cause);
            this.type = type;
        }
    }
}
