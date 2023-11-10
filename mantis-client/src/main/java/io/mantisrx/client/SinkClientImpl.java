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

import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.reactivex.mantis.remote.observable.EndpointChange;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;


public class SinkClientImpl<T> implements SinkClient<T> {

    private static final Logger logger = LoggerFactory.getLogger(SinkClientImpl.class);
    final String jobId;
    final SinkConnectionFunc<T> sinkConnectionFunc;
    final JobSinkLocator jobSinkLocator;
    final private AtomicBoolean nowClosed = new AtomicBoolean(false);
    final private SinkConnections<T> sinkConnections = new SinkConnections<>();
    private final String sinkGuageName = "SinkConnections";
    private final String expectedSinksGaugeName = "ExpectedSinkConnections";
    private final String sinkReceivingDataGaugeName = "sinkRecvngData";
    private final String clientNotConnectedToAllSourcesGaugeName = "clientNotConnectedToAllSources";
    private final Gauge sinkGauge;
    private final Gauge expectedSinksGauge;
    private final Gauge sinkReceivingDataGauge;
    private final Gauge clientNotConnectedToAllSourcesGauge;
    private final AtomicInteger numSinkWorkers = new AtomicInteger();
    private final AtomicInteger numSinkRunningWorkers = new AtomicInteger();
    private final Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver;
    private final long dataRecvTimeoutSecs;
    private final Metrics metrics;
    private final boolean disablePingFiltering;

    SinkClientImpl(String jobId, SinkConnectionFunc<T> sinkConnectionFunc, JobSinkLocator jobSinkLocator,
                   Observable<MasterClientWrapper.JobSinkNumWorkers> numSinkWorkersObservable,
                   Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver, long dataRecvTimeoutSecs) {
        this(jobId, sinkConnectionFunc, jobSinkLocator, numSinkWorkersObservable, sinkConnectionsStatusObserver,
                dataRecvTimeoutSecs, false);

    }

    SinkClientImpl(String jobId, SinkConnectionFunc<T> sinkConnectionFunc, JobSinkLocator jobSinkLocator,
                   Observable<MasterClientWrapper.JobSinkNumWorkers> numSinkWorkersObservable,
                   Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver, long dataRecvTimeoutSecs,
                   boolean disablePingFiltering) {
        this.jobId = jobId;
        this.sinkConnectionFunc = sinkConnectionFunc;
        this.jobSinkLocator = jobSinkLocator;
        BasicTag jobIdTag = new BasicTag("jobId", Optional.ofNullable(jobId).orElse("NullJobId"));
        MetricGroupId metricGroupId = new MetricGroupId(SinkClientImpl.class.getCanonicalName(), jobIdTag);
        this.metrics = new Metrics.Builder()
                .id(metricGroupId)
                .addGauge(sinkGuageName)
                .addGauge(expectedSinksGaugeName)
                .addGauge(sinkReceivingDataGaugeName)
                .addGauge(clientNotConnectedToAllSourcesGaugeName)
                .build();
        sinkGauge = metrics.getGauge(sinkGuageName);
        expectedSinksGauge = metrics.getGauge(expectedSinksGaugeName);
        sinkReceivingDataGauge = metrics.getGauge(sinkReceivingDataGaugeName);
        clientNotConnectedToAllSourcesGauge = metrics.getGauge(clientNotConnectedToAllSourcesGaugeName);
        numSinkWorkersObservable
                .doOnNext((jobSinkNumWorkers) -> {
                    numSinkWorkers.set(jobSinkNumWorkers.getNumSinkWorkers());
                    numSinkRunningWorkers.set(jobSinkNumWorkers.getNumSinkRunningWorkers());

                })
                .takeWhile((integer) -> !nowClosed.get())
                .subscribe();
        this.sinkConnectionsStatusObserver = sinkConnectionsStatusObserver;
        this.dataRecvTimeoutSecs = dataRecvTimeoutSecs;
        this.disablePingFiltering = disablePingFiltering;
    }

    private String toSinkName(String host, int port) {
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
        return getPartitionedResults(-1, 0);
    }

    @Override
    public Observable<Observable<T>> getPartitionedResults(final int forIndex, final int totalPartitions) {
        return internalGetResults(forIndex, totalPartitions);
        //        return Observable
        //                .create(new Observable.OnSubscribe<Observable<T>>() {
        //                    @Override
        //                    public void call(final Subscriber subscriber) {
        //                        internalGetResults(forIndex, totalPartitions).subscribe(subscriber);
        //                    }
        //                })
        //                .subscribeOn(Schedulers.io());
    }

    private <T> Observable<Observable<T>> internalGetResults(int forIndex, int totalPartitions) {
        return jobSinkLocator
                .locatePartitionedSinkForJob(jobId, forIndex, totalPartitions)
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
                .doOnUnsubscribe(() -> {
                    try {
                        logger.warn("Closing connections to sink of job {}", jobId);
                        closeAllConnections();
                    } catch (Exception e) {
                        logger.warn("Error closing all connections to sink of job {}", jobId, e);
                    }
                })
                .share()
                //                .lift(new Observable.Operator<Observable<T>, Observable<T>>() {
                //                    @Override
                //                    public Subscriber<? super Observable<T>> call(Subscriber<? super Observable<T>> subscriber) {
                //                        subscriber.add(Subscriptions.create(new Action0() {
                //                            @Override
                //                            public void call() {
                //                                try {
                //                                    logger.warn("Closing connections to sink of job " + jobId);
                //                                    closeAllConnections();
                //                                } catch (Exception e) {
                //                                    throw new RuntimeException(e);
                //                                }
                //                            }
                //                        }));
                //                        return subscriber;
                //                    }
                //                })
                //                .share()
                //                .lift(new DropOperator<Observable<T>>("client_partition_share"))
                ;
    }

    private <T> Observable<T> handleEndpointConnect(EndpointChange endpoint) {
        logger.info("Opening connection to sink at " + endpoint.toString());
        final String unwrappedHost = MasterClientWrapper.getUnwrappedHost(endpoint.getEndpoint().getHost());

        SinkConnection sinkConnection = sinkConnectionFunc.call(unwrappedHost, endpoint.getEndpoint().getPort(),
                new Action1<Boolean>() {
                    @Override
                    public void call(Boolean flag) {
                        updateSinkConx(flag);
                    }
                },
                new Action1<Boolean>() {
                    @Override
                    public void call(Boolean flag) {
                        updateSinkDataReceivingStatus(flag);
                    }
                },
                dataRecvTimeoutSecs,
                this.disablePingFiltering
        );
        if (nowClosed.get()) {// check if closed before adding
            try {
                sinkConnection.close();
            } catch (Exception e) {
                logger.warn("Error closing sink connection " + sinkConnection.getName() + " - " + e.getMessage(), e);
            }
            return Observable.empty();
        }
        sinkConnections.put(toSinkName(unwrappedHost, endpoint.getEndpoint().getPort()), sinkConnection);
        if (nowClosed.get()) {
            try {
                sinkConnection.close();
                sinkConnections.remove(toSinkName(unwrappedHost, endpoint.getEndpoint().getPort()));
                return Observable.empty();
            } catch (Exception e) {
                logger.warn("Error closing sink connection - " + e.getMessage());
            }
        }
        return ((SinkConnection<T>) sinkConnection).call()
                .takeWhile(e -> !nowClosed.get())
                ;
    }

    private void updateSinkDataReceivingStatus(Boolean flag) {
        if (flag)
            sinkReceivingDataGauge.increment();
        else
            sinkReceivingDataGauge.decrement();
        expectedSinksGauge.set(numSinkWorkers.get());
        if (expectedSinksGauge.value() != sinkReceivingDataGauge.value()) {
            this.clientNotConnectedToAllSourcesGauge.set(1);
        } else {
            this.clientNotConnectedToAllSourcesGauge.set(0);
        }
        if (sinkConnectionsStatusObserver != null) {
            synchronized (sinkConnectionsStatusObserver) {
                sinkConnectionsStatusObserver.onNext(new SinkConnectionsStatus(sinkReceivingDataGauge.value(), sinkGauge.value(), numSinkWorkers.get(), numSinkRunningWorkers.get()));
            }
        }
    }

    private void updateSinkConx(Boolean flag) {
        if (flag)
            sinkGauge.increment();
        else
            sinkGauge.decrement();
        expectedSinksGauge.set(numSinkWorkers.get());
        if (expectedSinksGauge.value() != sinkReceivingDataGauge.value()) {
            this.clientNotConnectedToAllSourcesGauge.set(1);
        } else {
            this.clientNotConnectedToAllSourcesGauge.set(0);
        }
        if (sinkConnectionsStatusObserver != null) {
            synchronized (sinkConnectionsStatusObserver) {
                sinkConnectionsStatusObserver.onNext(new SinkConnectionsStatus(sinkReceivingDataGauge.value(), sinkGauge.value(), numSinkWorkers.get(), numSinkRunningWorkers.get()));
            }
        }
    }

    private <T> Observable<T> handleEndpointClose(EndpointChange endpoint) {
        logger.info("Closed connection to sink at " + endpoint.toString());
        final String unwrappedHost = MasterClientWrapper.getUnwrappedHost(endpoint.getEndpoint().getHost());
        final SinkConnection<T> removed = (SinkConnection<T>) sinkConnections.remove(toSinkName(unwrappedHost, endpoint.getEndpoint().getPort()));
        if (removed != null) {
            try {
                removed.close();
            } catch (Exception e) {
                // shouldn't happen
                logger.error("Unexpected exception on closing sinkConnection: " + e.getMessage(), e);
            }
        } else {
            logger.error("SinkConnections does not contain endpoint to be removed. host: {}, sinkConnections: {}",
                    unwrappedHost, sinkConnections);
        }
        return Observable.empty();
    }

    private void closeAllConnections() throws Exception {
        nowClosed.set(true);
        sinkConnections.closeOut((SinkConnection<T> tSinkConnection) -> {
            try {
                tSinkConnection.close();
            } catch (Exception e) {
                logger.warn("Error closing sink connection " + tSinkConnection.getName() +
                        " - " + e.getMessage(), e);
            }
        });
    }

    @ToString
    class SinkConnections<T> {

        final private Map<String, SinkConnection<T>> sinkConnections = new HashMap<>();
        private boolean isClosed = false;

        private void put(String key, SinkConnection<T> val) {
            synchronized (sinkConnections) {
                if (isClosed)
                    return;
                sinkConnections.put(key, val);
            }
        }

        private SinkConnection<T> remove(String key) {
            synchronized (sinkConnections) {
                return sinkConnections.remove(key);
            }
        }

        private void closeOut(Action1<SinkConnection<T>> onClose) {
            synchronized (sinkConnections) {
                isClosed = true;
            }
            for (SinkConnection<T> sinkConnection : sinkConnections.values()) {
                logger.info("Closing " + sinkConnection.getName());
                onClose.call(sinkConnection);
            }
        }
    }
}
