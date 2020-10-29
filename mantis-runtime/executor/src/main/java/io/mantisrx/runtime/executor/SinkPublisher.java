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

import java.util.function.Consumer;

import io.mantisrx.runtime.api.Context;
import io.mantisrx.runtime.api.PortRequest;
import io.mantisrx.runtime.api.SinkHolder;
import io.mantisrx.runtime.api.StageConfig;
import io.mantisrx.runtime.api.sink.Sink;
import io.mantisrx.runtime.common.MantisJobDurationType;
import io.mantisrx.runtime.reactor.MantisOperators;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;


public class SinkPublisher<T, R> implements WorkerPublisher<T, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkPublisher.class);
    private SinkHolder<R> sinkHolder;
    private PortSelector portSelector;
    private Context context;
    private Runnable onTerminatedCallback;
    private Runnable onSubscribeAction;
    private Runnable onUnsubscribeAction;
    private Runnable onCompleteCallback;
    private Consumer<Throwable> onErrorCallback;

    public SinkPublisher(SinkHolder<R> sinkHolder,
                         PortSelector portSelector,
                         Context context,
                         Runnable onTerminatedCallback,
                         Runnable onSubscribeAction,
                         Runnable onUnsubscribeAction,
                         Runnable onCompleteCallback,
                         Consumer<Throwable> onErrorCallback) {
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
                      Publisher<Publisher<R>> toPublish) {
        final Sink<R> sink = sinkHolder.getSinkAction();

        int sinkPort = -1;
        if (sinkHolder.isPortRequested()) {
            sinkPort = portSelector.acquirePort();
        }

        Flux<R> mergedFlux = Flux.merge(toPublish)
            .transform(MantisOperators.<R>monitor("worker_sink"))
            .doOnSubscribe(subscription -> {
                LOGGER.info("Got sink subscription, onSubscribe action {}", onSubscribeAction);
                if (onSubscribeAction != null) {
                    onSubscribeAction.run();
                }
            })
            .doOnComplete(onCompleteCallback)
            .doOnError(onErrorCallback)
            .doOnTerminate(onTerminatedCallback)
            .doOnCancel(() -> LOGGER.info("source CANCEL"))
            .doFinally(signalType -> {
                LOGGER.info("Sink subscriptions clean up {}, unsubscribe action {}", signalType, onUnsubscribeAction);
                if (onUnsubscribeAction != null) {
                    onUnsubscribeAction.run();
                }
            })
            .share();

        if (context.getWorkerInfo().getDurationType() == MantisJobDurationType.Perpetual) {
            // eager subscribe, don't allow unsubscribe back
            mergedFlux
                .subscribeOn(Schedulers.newSingle("PerpetualJobEagerSubscribe", true))
                .subscribe(i -> LOGGER.trace("eagerSub {}", i));
        }
        sink.init(context);
        sink.apply(context, new PortRequest(sinkPort), mergedFlux);
    }

    @Override
    public void stop() { }
}
