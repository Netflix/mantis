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

package io.mantisrx.client;

import static com.mantisrx.common.utils.MantisMetricStringConstants.DROP_OPERATOR_INCOMING_METRIC_GROUP;

import com.mantisrx.common.utils.NettyUtils;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.core.ServiceRegistry;
import io.mantisrx.server.worker.client.SseWorkerConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.reactivx.mantis.operators.DropOperator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;


public class SseSinkConnectionFunction implements SinkConnectionFunc<MantisServerSentEvent> {

    private static final String DEFAULT_BUFFER_SIZE_STR = "0";
    private static final Logger logger = LoggerFactory.getLogger(SseSinkConnectionFunction.class);
    private static final CopyOnWriteArraySet<String> metricsSet = new CopyOnWriteArraySet<>();
    private static final String metricGroupName;
    private static MeterRegistry meterRegistry;

    private static final Action1<Throwable> defaultConxResetHandler = new Action1<Throwable>() {
        @Override
        public void call(Throwable throwable) {
            logger.warn("Retrying reset connection");
            try {Thread.sleep(500);} catch (InterruptedException ie) {
                logger.debug("Interrupted waiting for retrying connection");
            }
        }
    };

    static {
        // NJ
        // Use single netty thread
        NettyUtils.setNettyThreads();

        metricGroupName = DROP_OPERATOR_INCOMING_METRIC_GROUP + "_SseSinkConnectionFunction_withBuffer";
        metricsSet.add(metricGroupName);
        logger.info("SETTING UP METRICS PRINTER THREAD");
        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    Set<String> metricGroups = new HashSet<>(metricsSet);
                    if (!metricGroups.isEmpty()) {
                        for (String metricGroup : metricGroups) {
                            final Meter meter = meterRegistry.find(metricGroup).meter();
                            if (meter != null) {
                                Counter onNext = meterRegistry.counter(metricGroup + "_" + DropOperator.Counters.onNext);
                                Counter onError = meterRegistry.counter(metricGroup + "_" + DropOperator.Counters.onError);
                                Counter onComplete = meterRegistry.counter(metricGroup + "_" + DropOperator.Counters.onComplete);
                                Counter dropped = meterRegistry.counter(metricGroup + "_" + DropOperator.Counters.dropped);

                                logger.info(metricGroup + ": onNext=" + onNext.count() + ", onError=" + onError.count() +
                                                ", onComplete=" + onComplete.count() + ", dropped=" + dropped.count()
                                        // + ", buffered=" + buffered.value()
                                );
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Unexpected error in metrics printer thread: " + e.getMessage(), e);
                }
            }
        }, 60, 60, TimeUnit.SECONDS);


    }

    private final boolean reconnectUponConnectionRest;
    private final Action1<Throwable> connectionResetHandler;
    private final SinkParameters sinkParameters;
    private final int bufferSize;

    public SseSinkConnectionFunction(boolean reconnectUponConnectionRest, Action1<Throwable> connectionResetHandler, MeterRegistry meterRegistry) {
        this(reconnectUponConnectionRest, connectionResetHandler, null, meterRegistry);
    }

    public SseSinkConnectionFunction(boolean reconnectUponConnectionRest, Action1<Throwable> connectionResetHandler, SinkParameters sinkParameters, MeterRegistry meterRegistry) {
        this.reconnectUponConnectionRest = reconnectUponConnectionRest;
        this.connectionResetHandler = connectionResetHandler == null ? defaultConxResetHandler : connectionResetHandler;
        this.sinkParameters = sinkParameters;
        String bufferSizeStr = ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantisClient.buffer.size", DEFAULT_BUFFER_SIZE_STR);
        bufferSize = Integer.parseInt(Optional.ofNullable(bufferSizeStr).orElse(DEFAULT_BUFFER_SIZE_STR));
        this.meterRegistry = meterRegistry;

    }

    @Override
    public SinkConnection<MantisServerSentEvent> call(String hostname, Integer port) {
        return call(hostname, port, null, null, 5);
    }

    @Override
    public SinkConnection<MantisServerSentEvent> call(final String hostname, final Integer port,
                                                      final Action1<Boolean> updateConxStatus,
                                                      final Action1<Boolean> updateDataRecvngStatus,
                                                      final long dataRecvTimeoutSecs,
                                                      final boolean disablePingFiltering) {

        return new SinkConnection<MantisServerSentEvent>() {
            private final SseWorkerConnection workerConn =
                    new SseWorkerConnection("Sink", hostname, port, updateConxStatus, updateDataRecvngStatus,
                            connectionResetHandler, dataRecvTimeoutSecs, reconnectUponConnectionRest, metricsSet,
                            bufferSize, sinkParameters, disablePingFiltering,metricGroupName);

            @Override
            public String getName() {
                return workerConn.getName();
            }

            @Override
            public void close() throws Exception {
                workerConn.close();
            }

            @Override
            public Observable<MantisServerSentEvent> call() {
                return workerConn.call();
            }
        };
    }
}
