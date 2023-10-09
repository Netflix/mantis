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

package io.mantisrx.runtime.executor;

import io.mantisrx.common.metrics.rx.MonitorOperator;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.SinkHolder;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.sink.Sink;
import io.reactivex.mantis.remote.observable.RxMetrics;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

/**
 * Implementation that publishes the results of a stage to a sink such as an SSE port.
 * @param <T> type of the data-item that's getting consumed by the sink.
 */
public class SinkPublisher<T> implements WorkerPublisher<T> {

    private static final Logger logger = LoggerFactory.getLogger(SinkPublisher.class);
    private final SinkHolder<T> sinkHolder;
    private final PortSelector portSelector;
    private final Context context;
    private final Action0 observableTerminatedCallback;
    private final Action0 onSubscribeAction;
    private final Action0 onUnsubscribeAction;
    private final Action0 observableOnCompleteCallback;
    private final Action1<Throwable> observableOnErrorCallback;

    // state created during the lifecycle of the sink.
    private Subscription eagerSubscription;
    private Sink<T> sink;

    public SinkPublisher(SinkHolder<T> sinkHolder,
                         PortSelector portSelector,
                         Context context,
                         Action0 observableTerminatedCallback,
                         Action0 onSubscribeAction,
                         Action0 onUnsubscribeAction,
                         Action0 observableOnCompleteCallback,
                         Action1<Throwable> observableOnErrorCallback) {
        this.sinkHolder = sinkHolder;
        this.portSelector = portSelector;
        this.context = context;
        this.observableTerminatedCallback = observableTerminatedCallback;
        this.onSubscribeAction = onSubscribeAction;
        this.onUnsubscribeAction = onUnsubscribeAction;
        this.observableOnCompleteCallback = observableOnCompleteCallback;
        this.observableOnErrorCallback = observableOnErrorCallback;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void start(StageConfig<?, T> stage,
                      Observable<Observable<T>> observablesToPublish) {
        sink = sinkHolder.getSinkAction();

        int sinkPort = -1;
        if (sinkHolder.isPortRequested()) {
            sinkPort = portSelector.acquirePort();
        }

        // apply transform

        Observable<T> merged = Observable.merge(observablesToPublish);
        final Observable wrappedO = merged.lift(new MonitorOperator("worker_sink"));

        Observable o = Observable
                .create(subscriber -> {
                    logger.info("Got sink subscription with onSubscribeAction={}", onSubscribeAction);
                    wrappedO
                            .doOnCompleted(observableOnCompleteCallback)
                            .doOnError(observableOnErrorCallback)
                            .doOnTerminate(observableTerminatedCallback)
                            .subscribe(subscriber);
                    if (onSubscribeAction != null) {
                        onSubscribeAction.call();
                    }
                })
                .doOnCompleted(() -> logger.info("Sink observable subscription completed."))
                .doOnError(err -> logger.error("Sink observable subscription onError:", err))
                .doOnTerminate(() -> logger.info("Sink observable subscription termindated."))
                .doOnUnsubscribe(() -> {
                    logger.info("Sink subscriptions clean up, action={}", onUnsubscribeAction);
                    if (onUnsubscribeAction != null)
                        onUnsubscribeAction.call();
                })
                .share();
        if (context.getWorkerInfo().getDurationType() == MantisJobDurationType.Perpetual) {
            // eager subscribe, don't allow unsubscribe back
            logger.info("eagerSubscription subscribed for Perpetual job.");
            eagerSubscription = o.subscribe();
        }
        sink.init(context);
        sink.call(context, new PortRequest(sinkPort), o);
    }

    @Override
    public RxMetrics getMetrics() {return null;}

    @Override
    public void close() throws IOException {
        try {
            sink.close();
        } finally {
            sink = null;
            if (eagerSubscription != null) {
                eagerSubscription.unsubscribe();
                eagerSubscription = null;
            }
        }
    }
}
