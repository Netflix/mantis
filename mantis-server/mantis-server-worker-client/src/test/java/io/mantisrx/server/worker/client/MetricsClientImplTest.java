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

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.metrics.measurement.CounterMeasurement;
import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import io.mantisrx.common.metrics.measurement.Measurements;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.common.network.WorkerEndpoint;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.server.worker.TestSseServerFactory;
import io.reactivex.mantis.remote.observable.EndpointChange;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;


public class MetricsClientImplTest {

    private static final Logger logger = LoggerFactory.getLogger(MetricsClientImplTest.class);
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public String generateMetricJson(final String metricGroup) throws JsonProcessingException {
        return mapper.writeValueAsString(new Measurements(metricGroup, System.currentTimeMillis(),
                Collections.<CounterMeasurement>emptyList(),
                Arrays.asList(new GaugeMeasurement(MetricStringConstants.CPU_PCT_USAGE_CURR, 20),
                        new GaugeMeasurement(MetricStringConstants.TOT_MEM_USAGE_CURR, 123444)),
                Collections.<String, String>emptyMap()));
    }

    @Test
    public void testMetricConnections() throws InterruptedException, UnsupportedEncodingException, JsonProcessingException {
        final String jobId = "test-job-1";
        final String testResUsageMetricData = generateMetricJson(MetricStringConstants.RESOURCE_USAGE_METRIC_GROUP);
        final String testDropDataMetricData = generateMetricJson(MetricStringConstants.DATA_DROP_METRIC_GROUP);
        final int metricsPort = TestSseServerFactory.newServerWithInitialData(testResUsageMetricData);

        final AtomicInteger i = new AtomicInteger(0);
        final Observable<EndpointChange> workerMetricLocationStream = Observable.interval(1, TimeUnit.SECONDS, Schedulers.io()).map(new Func1<Long, EndpointChange>() {
            @Override
            public EndpointChange call(Long aLong) {
                logger.info("emitting endpointChange");
                if (i.getAndIncrement() % 10 == 0) {
                    return new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", 31002));
                } else {
                    return new EndpointChange(EndpointChange.Type.add, new WorkerEndpoint("localhost", 31002, 1, metricsPort, 0, 1));
                }
            }
        });

        MetricsClientImpl<MantisServerSentEvent> metricsClient =
                new MetricsClientImpl<>(jobId,
                        new SseWorkerConnectionFunction(true, new Action1<Throwable>() {
                            @Override
                            public void call(Throwable throwable) {
                                logger.error("Metric connection error: " + throwable.getMessage());
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException ie) {
                                    logger.error("Interrupted waiting for retrying connection");
                                }
                            }
                        }, new SinkParameters.Builder().withParameter("name", MetricStringConstants.RESOURCE_USAGE_METRIC_GROUP).build()),
                        new JobWorkerMetricsLocator() {
                            @Override
                            public Observable<EndpointChange> locateWorkerMetricsForJob(String jobId) {
                                return workerMetricLocationStream;
                            }
                        },
                        Observable.just(1),
                        new Observer<WorkerConnectionsStatus>() {
                            @Override
                            public void onCompleted() {
                                logger.info("got onCompleted in WorkerConnStatus obs");
                            }

                            @Override
                            public void onError(Throwable e) {
                                logger.info("got onError in WorkerConnStatus obs");
                            }

                            @Override
                            public void onNext(WorkerConnectionsStatus workerConnectionsStatus) {
                                logger.info("got WorkerConnStatus {}", workerConnectionsStatus);
                            }
                        },
                        60);

        final CountDownLatch latch = new CountDownLatch(1);

        final Observable<Observable<MantisServerSentEvent>> results = metricsClient.getResults();
        Observable.merge(results)
                .doOnNext(new Action1<MantisServerSentEvent>() {
                    @Override
                    public void call(MantisServerSentEvent event) {
                        logger.info("got event {}", event.getEventAsString());
                        assertEquals(testResUsageMetricData, event.getEventAsString());
                        latch.countDown();
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.error("got error {}", throwable.getMessage(), throwable);
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        logger.info("onComplete");
                    }
                })
                .subscribe();
        latch.await(30, TimeUnit.SECONDS);

        TestSseServerFactory.stopAllRunning();
    }
}
