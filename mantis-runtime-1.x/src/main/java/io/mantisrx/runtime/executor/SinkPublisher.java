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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;


public class SinkPublisher<T, R> implements WorkerPublisher<T, R> {

    private static final Logger logger = LoggerFactory.getLogger(SinkPublisher.class);
    private SinkHolder<R> sinkHolder;
    private PortSelector portSelector;
    private Context context;
    private Action0 observableTerminatedCallback;
    private Action0 onSubscribeAction;
    private Action0 onUnsubscribeAction;
    private Action0 observableOnCompleteCallback;
    private Action1<Throwable> observableOnErrorCallback;

    public SinkPublisher(SinkHolder<R> sinkHolder,
                         PortSelector portSelector,
                         Context context,
                         Action0 observableTerminatedCallback, Action0 onSubscribeAction, Action0 onUnsubscribeAction,
                         Action0 observableOnCompleteCallback, Action1<Throwable> observableOnErrorCallback) {
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
    @SuppressWarnings( {"unchecked", "rawtypes"})
    public void start(StageConfig<T, R> stage,
                      Observable<Observable<R>> observablesToPublish) {
        final Sink<R> sink = sinkHolder.getSinkAction();

        int sinkPort = -1;
        if (sinkHolder.isPortRequested()) {
            sinkPort = portSelector.acquirePort();
        }

        // apply transform

        Observable<R> merged = Observable.merge(observablesToPublish);
        final Observable wrappedO = merged.lift(new MonitorOperator("worker_sink"));

        Observable o = Observable
                .create(new Observable.OnSubscribe<Object>() {
                    @Override
                    public void call(Subscriber subscriber) {
                        logger.info("Got sink subscription, onSubscribe=" + onSubscribeAction);
                        wrappedO
                                .doOnCompleted(observableOnCompleteCallback)
                                .doOnError(observableOnErrorCallback)
                                .doOnTerminate(observableTerminatedCallback)
                                .subscribe(subscriber);
                        if (onSubscribeAction != null) {
                            onSubscribeAction.call();
                        }
                    }
                })
                .doOnUnsubscribe(new Action0() {
                    @Override
                    public void call() {
                        logger.info("Sink subscriptions clean up, action=" + onUnsubscribeAction);
                        if (onUnsubscribeAction != null)
                            onUnsubscribeAction.call();
                    }
                })
                .share();
        if (context.getWorkerInfo().getDurationType() == MantisJobDurationType.Perpetual) {
            // eager subscribe, don't allow unsubscribe back
            o.subscribe();
        }
        sink.init(context);
        sink.call(context, new PortRequest(sinkPort),
                o);
        //o.lift(new DoOnRequestOperator("beforeShare")).share().lift(new DropOperator<>("sink_share")));
    }

    @Override
    public RxMetrics getMetrics() {return null;}

    @Override
    public void stop() {}
}
