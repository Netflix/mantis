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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.mantisrx.common.network.WorkerEndpoint;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivx.mantis.operators.DropOperator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subscriptions.Subscriptions;


class MetricsClientImpl<T> implements MetricsClient<T> {

    private static final Logger logger = LoggerFactory.getLogger(MetricsClientImpl.class);
    final String jobId;
    final WorkerConnectionFunc<T> workerConnectionFunc;
    final JobWorkerMetricsLocator jobWorkerMetricsLocator;
    final private AtomicBoolean nowClosed = new AtomicBoolean(false);
    final private WorkerConnections workerConnections = new WorkerConnections();
    private final String workersGuageName = "MetricsConnections";
    private final String expectedWorkersGaugeName = "ExpectedMetricsConnections";
    private final String workerConnReceivingDataGaugeName = "metricsRecvngData";
    private final Gauge workersGauge;
    private final AtomicLong workersGaugeValue = new AtomicLong(0);
    private final Gauge expectedWorkersGauge;
    private final AtomicLong expectedWorkersGaugeValue = new AtomicLong(0);
    private final Gauge workerConnReceivingDataGauge;
    private final AtomicLong workerConnReceivingDataGaugeValue = new AtomicLong(0);
    private final AtomicInteger numWorkers = new AtomicInteger();
    private final Observer<WorkerConnectionsStatus> workerConnectionsStatusObserver;
    private final long dataRecvTimeoutSecs;
    private final MeterRegistry meterRegistry;
    MetricsClientImpl(String jobId, WorkerConnectionFunc<T> workerConnectionFunc, JobWorkerMetricsLocator jobWorkerMetricsLocator,
                      Observable<Integer> numWorkersObservable,
                      Observer<WorkerConnectionsStatus> workerConnectionsStatusObserver, long dataRecvTimeoutSecs,
                      MeterRegistry meterRegistry) {
        this.jobId = jobId;
        this.workerConnectionFunc = workerConnectionFunc;
        this.jobWorkerMetricsLocator = jobWorkerMetricsLocator;
        this.meterRegistry = meterRegistry;
        String groupName = MetricsClientImpl.class.getCanonicalName() + "-" + jobId;
        workersGauge = Gauge.builder(groupName + "_" + workersGuageName, workersGaugeValue::get)
                .register(meterRegistry);
        expectedWorkersGauge = Gauge.builder(groupName + "_" + expectedWorkersGaugeName, expectedWorkersGaugeValue::get)
                .register(meterRegistry);
        workerConnReceivingDataGauge = Gauge.builder(groupName + "_" + workerConnReceivingDataGaugeName, workerConnReceivingDataGaugeValue::get)
                .register(meterRegistry);
        numWorkersObservable
                .doOnNext(new Action1<Integer>() {
                    @Override
                    public void call(Integer integer) {
                        numWorkers.set(integer);
                    }
                })
                .takeWhile(new Func1<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer integer) {
                        return !nowClosed.get();
                    }
                })
                .subscribe();
        this.workerConnectionsStatusObserver = workerConnectionsStatusObserver;
        this.dataRecvTimeoutSecs = dataRecvTimeoutSecs;
    }

    private String toWorkerConnName(String host, int port) {
        return host + "-" + port;
    }

    @Override
    public boolean hasError() {
        return false;
    }

    @Override
    public String getError() {
        return null;
    }

    @Override
    public Observable<Observable<T>> getResults() {
        return Observable
                .create(new Observable.OnSubscribe<Observable<T>>() {
                    @Override
                    public void call(final Subscriber subscriber) {
                        internalGetResults().subscribe(subscriber);
                    }
                })
                .subscribeOn(Schedulers.io());
    }

    private Observable<Observable<T>> internalGetResults() {
        return jobWorkerMetricsLocator
                .locateWorkerMetricsForJob(jobId)
                .map(new Func1<EndpointChange, Observable<T>>() {
                    @Override
                    public Observable<T> call(EndpointChange endpointChange) {
                        if (nowClosed.get())
                            return Observable.empty();
                        if (endpointChange.getType() == EndpointChange.Type.complete) {
                            return handleEndpointClose(endpointChange);
                        } else {
                            return handleEndpointConnect(endpointChange);
                        }
                    }
                })
                .lift(new Observable.Operator<Observable<T>, Observable<T>>() {
                    @Override
                    public Subscriber<? super Observable<T>> call(Subscriber<? super Observable<T>> subscriber) {
                        subscriber.add(Subscriptions.create(new Action0() {
                            @Override
                            public void call() {
                                try {
                                    logger.warn("Closing metrics connections to workers of job " + jobId);
                                    closeAllConnections();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }));
                        return subscriber;
                    }
                })
                .share()
                .lift(new DropOperator<Observable<T>>(meterRegistry, "client_metrics_share"))
                ;
    }

    private Observable<T> handleEndpointConnect(EndpointChange ec) {
        logger.info("Opening connection to metrics sink at " + ec.toString());
        final String unwrappedHost = MasterClientWrapper.getUnwrappedHost(ec.getEndpoint().getHost());
        final int metricsPort;
        if (ec.getEndpoint() instanceof WorkerEndpoint) {
            metricsPort = ((WorkerEndpoint) ec.getEndpoint()).getMetricPort();
        } else {
            logger.error("endpoint received on Endpoint connect is not a WorkerEndpoint {}, no metrics port to connect to", ec.getEndpoint());
            return Observable.empty();
        }

        WorkerConnection<T> workerConnection = workerConnectionFunc.call(unwrappedHost, metricsPort,
                new Action1<Boolean>() {
                    @Override
                    public void call(Boolean flag) {
                        updateWorkerConx(flag);
                    }
                },
                new Action1<Boolean>() {
                    @Override
                    public void call(Boolean flag) {
                        updateWorkerDataReceivingStatus(flag);
                    }
                },
                dataRecvTimeoutSecs
        );
        if (nowClosed.get()) {// check if closed before adding
            try {
                workerConnection.close();
            } catch (Exception e) {
                logger.warn("Error closing worker metrics connection " + workerConnection.getName() + " - " + e.getMessage(), e);
            }
            return Observable.empty();
        }
        workerConnections.put(toWorkerConnName(unwrappedHost, metricsPort), workerConnection);
        if (nowClosed.get()) {
            try {
                workerConnection.close();
                workerConnections.remove(toWorkerConnName(unwrappedHost, metricsPort));
                return Observable.empty();
            } catch (Exception e) {
                logger.warn("Error closing worker metrics connection - " + e.getMessage());
            }
        }
        return workerConnection.call()
                //        		.flatMap(new Func1<Observable<T>, Observable<T>>() {
                //            @Override
                //            public Observable<T> call(Observable<T> tObservable) {
                //                return tObservable;
                //            }
                //        })
                ;
    }

    private void updateWorkerDataReceivingStatus(Boolean flag) {
        if (flag)
            workerConnReceivingDataGaugeValue.incrementAndGet();
        else
            workerConnReceivingDataGaugeValue.decrementAndGet();
        expectedWorkersGaugeValue.set(numWorkers.get());
        if (workerConnectionsStatusObserver != null) {
            synchronized (workerConnectionsStatusObserver) {
                workerConnectionsStatusObserver.onNext(new WorkerConnectionsStatus(workerConnReceivingDataGaugeValue.get(), workersGaugeValue.get(), numWorkers.get()));
            }
        }
    }

    private void updateWorkerConx(Boolean flag) {
        if (flag)
            workersGaugeValue.incrementAndGet();
        else
            workersGaugeValue.decrementAndGet();
        expectedWorkersGaugeValue.set(numWorkers.get());
        if (workerConnectionsStatusObserver != null) {
            synchronized (workerConnectionsStatusObserver) {
                workerConnectionsStatusObserver.onNext(new WorkerConnectionsStatus(workerConnReceivingDataGaugeValue.get(), workersGaugeValue.get(), numWorkers.get()));
            }
        }
    }

    private Observable<T> handleEndpointClose(EndpointChange ec) {
        logger.info("Closed connection to metrics sink at " + ec.toString());
        final String unwrappedHost = MasterClientWrapper.getUnwrappedHost(ec.getEndpoint().getHost());
        final int metricsPort;
        if (ec.getEndpoint() instanceof WorkerEndpoint) {
            metricsPort = ((WorkerEndpoint) ec.getEndpoint()).getMetricPort();
        } else {
            logger.warn("endpoint received on Endpoint close is not a WorkerEndpoint {}, worker endpoint required for metrics port", ec.getEndpoint());
            return Observable.empty();
        }

        final WorkerConnection<T> removed = workerConnections.remove(toWorkerConnName(unwrappedHost, metricsPort));
        if (removed != null) {
            try {
                removed.close();
            } catch (Exception e) {
                // shouldn't happen
                logger.error("Unexpected exception on closing worker metrics connection: " + e.getMessage(), e);
            }
        }
        return Observable.empty();
    }

    private void closeAllConnections() throws Exception {
        nowClosed.set(true);
        workerConnections.closeOut(new Action1<WorkerConnection<T>>() {
            @Override
            public void call(WorkerConnection<T> tWorkerConnection) {
                try {
                    tWorkerConnection.close();
                } catch (Exception e) {
                    logger.warn("Error closing worker metrics connection " + tWorkerConnection.getName() +
                            " - " + e.getMessage(), e);
                }
            }
        });
    }

    class WorkerConnections {

        final private Map<String, WorkerConnection<T>> workerConnections = new HashMap<>();
        private boolean isClosed = false;

        private void put(String key, WorkerConnection<T> val) {
            synchronized (workerConnections) {
                if (isClosed)
                    return;
                workerConnections.put(key, val);
            }
        }

        private WorkerConnection<T> remove(String key) {
            synchronized (workerConnections) {
                return workerConnections.remove(key);
            }
        }

        private void closeOut(Action1<WorkerConnection<T>> onClose) {
            synchronized (workerConnections) {
                isClosed = true;
            }
            for (WorkerConnection<T> workerConnection : workerConnections.values()) {
                logger.info("Closing " + workerConnection.getName());
                onClose.call(workerConnection);
            }
        }
    }
}
