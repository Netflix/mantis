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

import rx.functions.Action1;

import io.mantisrx.runtime.api.Context;
import io.mantisrx.runtime.api.PortRequest;
import io.mantisrx.runtime.api.SinkHolder;
import io.mantisrx.runtime.api.StageConfig;
import io.mantisrx.runtime.api.sink.Sink;
import io.mantisrx.runtime.common.MantisJobDurationType;
import io.mantisrx.runtime.rx.MonitorOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.functions.Action0;


public class SinkPublisher<T, R> implements WorkerPublisher<T, R> {

    private static final Logger logger = LoggerFactory.getLogger(SinkPublisher.class);
    private SinkHolder<R> sinkHolder;
    private PortSelector portSelector;
    private Context context;
    private Action0 onTerminatedCallback;
    private Action0 onSubscribeAction;
    private Action0 onUnsubscribeAction;
    private Action0 onCompleteCallback;
    private Action1<Throwable> onErrorCallback;

    public SinkPublisher(SinkHolder<R> sinkHolder,
                         PortSelector portSelector,
                         Context context,
                         Action0 onTerminatedCallback,
                         Action0 onSubscribeAction,
                         Action0 onUnsubscribeAction,
                         Action0 onCompleteCallback,
                         Action1<Throwable> onErrorCallback) {
        this.sinkHolder = sinkHolder;
        this.portSelector = portSelector;
        this.context = context;
        this.onTerminatedCallback = onTerminatedCallback;
        this.onSubscribeAction = onSubscribeAction;
        this.onUnsubscribeAction = onUnsubscribeAction;
        this.onCompleteCallback = onCompleteCallback;
        this.onErrorCallback = onErrorCallback;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
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
                                .doOnCompleted(onCompleteCallback)
                                .doOnError(onErrorCallback)
                                .doOnTerminate(onTerminatedCallback)
                                .subscribe(subscriber);
                        if (onSubscribeAction != null) {
                            onSubscribeAction.call();
                        }
                    }
                })
                .doOnUnsubscribe((Action0) () -> {
                    logger.info("Sink subscriptions clean up, action=" + onUnsubscribeAction);
                    if (onUnsubscribeAction != null)
                        onUnsubscribeAction.call();
                })
                .share();
        if (context.getWorkerInfo().getDurationType() == MantisJobDurationType.Perpetual) {
            // eager subscribe, don't allow unsubscribe back
            o.subscribe();
        }
        sink.init(context);
        sink.apply(context, new PortRequest(sinkPort), RxReactiveStreams.toPublisher(o));
    }

    @Override
    public void stop() { }
}
