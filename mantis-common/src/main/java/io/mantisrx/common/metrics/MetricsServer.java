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

package io.mantisrx.common.metrics;

import io.mantisrx.common.metrics.measurement.CounterMeasurement;
import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import io.mantisrx.common.metrics.measurement.Measurements;
import io.mantisrx.common.metrics.spectator.MetricId;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.server.HttpServer;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerRequest;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import mantis.io.reactivex.netty.protocol.http.server.RequestHandler;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;


public class MetricsServer {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServer.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer<ByteBuf, ServerSentEvent> server;
    private int port;
    private Map<String, String> tags;
    private long publishRateInSeconds;

    public MetricsServer(int port, long publishRateInSeconds, Map<String, String> tags) {
        this.port = port;
        this.publishRateInSeconds = publishRateInSeconds;
        this.tags = tags;
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jdk8Module());
    }

    private Observable<Measurements> measurements(long timeFrequency) {
        final MetricsRegistry registry = MetricsRegistry.getInstance();
        return
                Observable.interval(0, timeFrequency, TimeUnit.SECONDS)
                        .flatMap(new Func1<Long, Observable<Measurements>>() {
                            @Override
                            public Observable<Measurements> call(Long t1) {
                                long timestamp = System.currentTimeMillis();
                                List<Measurements> measurements = new ArrayList<>();
                                for (Metrics metrics : registry.metrics()) {
                                    Collection<CounterMeasurement> counters = new LinkedList<>();
                                    Collection<GaugeMeasurement> gauges = new LinkedList<>();

                                    for (Entry<MetricId, Counter> counterEntry : metrics.counters().entrySet()) {
                                        Counter counter = counterEntry.getValue();
                                        counters.add(new CounterMeasurement(counterEntry.getKey().metricName(), counter.value()));
                                    }
                                    for (Entry<MetricId, Gauge> gaugeEntry : metrics.gauges().entrySet()) {
                                        gauges.add(new GaugeMeasurement(gaugeEntry.getKey().metricName(), gaugeEntry.getValue().doubleValue()));
                                    }
                                    measurements.add(new Measurements(metrics.getMetricGroupId().id(),
                                            timestamp, counters, gauges, tags));
                                }
                                return Observable.from(measurements);
                            }
                        });
    }

    public void start() {

        final Observable<Measurements> measurements = measurements(publishRateInSeconds);

        logger.info("Starting metrics server on port: " + port);
        server = RxNetty.createHttpServer(
                port,
                new RequestHandler<ByteBuf, ServerSentEvent>() {
                    @Override
                    public Observable<Void> handle(HttpServerRequest<ByteBuf> request, final HttpServerResponse<ServerSentEvent> response) {

                        final Map<String, List<String>> queryParameters = request.getQueryParameters();

                        final List<String> namesToFilter = new LinkedList<>();
                        logger.info("got query params {}", queryParameters);
                        if (queryParameters != null && queryParameters.containsKey("name")) {
                            namesToFilter.addAll(queryParameters.get("name"));
                        }

                        Observable<Measurements> filteredObservable = measurements
                                .filter(new Func1<Measurements, Boolean>() {
                                    @Override
                                    public Boolean call(Measurements measurements) {
                                        if (!namesToFilter.isEmpty()) {
                                            // check filters
                                            for (String name : namesToFilter) {
                                                if (name.indexOf('*') != -1) {
                                                    // check for ends with
                                                    if (name.indexOf('*') == 0 && measurements.getName().endsWith(name.substring(1)))
                                                        return true;
                                                    // check for starts with
                                                    if (name.indexOf('*') > 0 && measurements.getName().startsWith(name.substring(0, name.indexOf('*')))) {
                                                        return true;
                                                    }
                                                }
                                                if (measurements.getName().equals(name)) {
                                                    return true; // filter match
                                                }
                                            }
                                            return false; // not found in filters
                                        } else {
                                            return true; // no filters provided
                                        }
                                    }
                                });

                        return filteredObservable.flatMap(new Func1<Measurements, Observable<Void>>() {
                            @Override
                            public Observable<Void> call(Measurements metrics) {
                                response.getHeaders().set("Access-Control-Allow-Origin", "*");
                                response.getHeaders().set("content-type", "text/event-stream");
                                ServerSentEvent event = null;
                                try {
                                    ByteBuf data = response.getAllocator().buffer().writeBytes((mapper.writeValueAsString(metrics)).getBytes());
                                    event = new ServerSentEvent(data);
                                    //event = new ServerSentEvent(null, "data", mapper.writeValueAsString(metrics));
                                } catch (JsonProcessingException e) {
                                    logger.error("Failed to map metrics to JSON", e);
                                }
                                if (event != null) {
                                    response.write(event);
                                    return response.writeStringAndFlush("\n");
                                }
                                return null;
                            }
                        });
                    }
                },
                PipelineConfigurators.<ByteBuf>serveSseConfigurator()
        ).start();
    }

    public void shutdown() {
        if (server != null) {
            logger.info("Shutting down metrics server on port");
            logger.info("Waiting (2 x push-period) to flush buffers, before shut down.");
            try {
                TimeUnit.SECONDS.sleep(2);
                server.shutdown();
            } catch (InterruptedException e) {
                logger.warn("Failed to shutdown metrics server", e);
            }
        }
    }
}
