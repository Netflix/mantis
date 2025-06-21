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
import io.mantisrx.runtime.EagerSubscriptionStrategy;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.SinkHolder;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.sink.Sink;
import io.reactivex.mantis.remote.observable.RxMetrics;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private Observable<T> delayedEagerObservable;
    private Sink<T> sink;
    private ScheduledExecutorService timeoutScheduler;
    private final AtomicBoolean eagerSubscriptionActivated = new AtomicBoolean(false);
    
    // Parameter names for eager subscription strategy (delegate to parameter definitions)
    private static final String EAGER_SUBSCRIPTION_STRATEGY_PARAM = 
        io.mantisrx.runtime.EagerSubscriptionParameters.EAGER_SUBSCRIPTION_STRATEGY_PARAM;
    private static final String SUBSCRIPTION_TIMEOUT_SECS_PARAM = 
        io.mantisrx.runtime.EagerSubscriptionParameters.SUBSCRIPTION_TIMEOUT_SECS_PARAM;

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
            EagerSubscriptionStrategy strategy = getEagerSubscriptionStrategy(context);
            handlePerpetualJobSubscription(o, context, strategy);
        }
        sink.init(context);
        sink.call(context, new PortRequest(sinkPort), o);
    }

    @Override
    public RxMetrics getMetrics() {return null;}

    /**
     * Gets the eager subscription strategy from job parameters.
     * Defaults to IMMEDIATE for backward compatibility.
     */
    private EagerSubscriptionStrategy getEagerSubscriptionStrategy(Context context) {
        String strategyStr = (String) context.getParameters().get(EAGER_SUBSCRIPTION_STRATEGY_PARAM, "IMMEDIATE");
        try {
            return EagerSubscriptionStrategy.valueOf(strategyStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid eager subscription strategy '{}', defaulting to IMMEDIATE", strategyStr);
            return EagerSubscriptionStrategy.IMMEDIATE;
        }
    }

    /**
     * Handles subscription logic for perpetual jobs based on the configured strategy.
     */
    private void handlePerpetualJobSubscription(Observable<T> observable, Context context, EagerSubscriptionStrategy strategy) {
        switch (strategy) {
            case IMMEDIATE:
                logger.info("Perpetual job using IMMEDIATE eager subscription strategy");
                eagerSubscription = observable.subscribe();
                break;
                
            case ON_FIRST_CLIENT:
                logger.info("Perpetual job using ON_FIRST_CLIENT eager subscription strategy");
                delayedEagerObservable = observable;
                context.setEagerSubscriptionActivationCallback(this::activateEagerSubscription);
                break;
                
            case TIMEOUT_BASED:
                logger.info("Perpetual job using TIMEOUT_BASED eager subscription strategy");
                delayedEagerObservable = observable;
                context.setEagerSubscriptionActivationCallback(this::activateEagerSubscription);
                scheduleTimeoutActivation(context);
                break;
                
            default:
                logger.warn("Unknown eager subscription strategy {}, defaulting to IMMEDIATE", strategy);
                eagerSubscription = observable.subscribe();
                break;
        }
    }

    /**
     * Schedules timeout-based activation for TIMEOUT_BASED strategy.
     */
    private void scheduleTimeoutActivation(Context context) {
        int timeoutSecs = (Integer) context.getParameters().get(SUBSCRIPTION_TIMEOUT_SECS_PARAM, 60);
        if (timeoutSecs <= 0) {
            logger.warn("Invalid subscription timeout {} seconds, defaulting to 60 seconds", timeoutSecs);
            timeoutSecs = 60;
        }
        
        timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "EagerSubscriptionTimeout");
            t.setDaemon(true);
            return t;
        });
        
        final int finalTimeoutSecs = timeoutSecs;
        timeoutScheduler.schedule(() -> {
            if (eagerSubscriptionActivated.compareAndSet(false, true)) {
                logger.info("Activating eager subscription due to timeout after {} seconds", finalTimeoutSecs);
                activateEagerSubscription();
            }
        }, timeoutSecs, TimeUnit.SECONDS);
        
        logger.info("Scheduled timeout activation in {} seconds for perpetual job", timeoutSecs);
    }

    /**
     * Activates delayed eager subscription for perpetual jobs when first client connects or timeout occurs.
     * This ensures the job becomes perpetual and won't terminate even after clients disconnect.
     */
    public void activateEagerSubscription() {
        if (eagerSubscriptionActivated.compareAndSet(false, true) && 
            eagerSubscription == null && delayedEagerObservable != null) {
            logger.info("Creating delayed eager subscription for perpetual job");
            eagerSubscription = delayedEagerObservable.subscribe();
            
            // Cancel timeout scheduler if it exists
            if (timeoutScheduler != null && !timeoutScheduler.isShutdown()) {
                timeoutScheduler.shutdown();
            }
        }
    }

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
            if (timeoutScheduler != null && !timeoutScheduler.isShutdown()) {
                timeoutScheduler.shutdown();
                timeoutScheduler = null;
            }
        }
    }
}
