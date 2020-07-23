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

import static io.mantisrx.runtime.parameter.ParameterUtils.STAGE_CONCURRENCY;

import java.util.concurrent.TimeUnit;

import io.mantisrx.common.MantisGroup;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.rx.MonitorOperator;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.GroupToGroup;
import io.mantisrx.runtime.GroupToScalar;
import io.mantisrx.runtime.Groups;
import io.mantisrx.runtime.KeyToKey;
import io.mantisrx.runtime.KeyToScalar;
import io.mantisrx.runtime.ScalarToGroup;
import io.mantisrx.runtime.ScalarToKey;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.SinkHolder;
import io.mantisrx.runtime.SourceHolder;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.computation.Computation;
import io.mantisrx.runtime.markers.MantisMarker;
import io.mantisrx.runtime.scheduler.SingleThreadScheduler;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.server.core.ServiceRegistry;
import io.reactivex.mantis.remote.observable.RxMetrics;
import io.reactivx.mantis.operators.GroupedObservableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.internal.util.RxThreadFactory;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;


public class StageExecutors {

    private static final Logger logger = LoggerFactory.getLogger(StageExecutors.class);

    private static Counter groupsExpiredCounter;
    private static long stageBufferIntervalMs = 100;
    private static int maxItemsInBuffer = 100;

    static {
        Metrics m = new Metrics.Builder()
                .name("StageExecutors")
                .addCounter("groupsExpiredCounter")
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        groupsExpiredCounter = m.getCounter("groupsExpiredCounter");

        String stageBufferIntervalMillisStr = ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.stage.buffer.intervalMs", "100");
        //.info("Read fast property mantis.sse.batchInterval" + flushIntervalMillisStr);
        stageBufferIntervalMs = Integer.parseInt(stageBufferIntervalMillisStr);

        String stageBufferMaxStr = ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.stage.buffer.maxSize", "100");
        //.info("Read fast property mantis.sse.batchInterval" + flushIntervalMillisStr);
        maxItemsInBuffer = Integer.parseInt(stageBufferMaxStr);
    }

    private StageExecutors() {

    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public static void executeSingleStageJob(final SourceHolder source, final StageConfig stage,
                                             final SinkHolder sink, final PortSelector portSelector, RxMetrics rxMetrics,
                                             final Context context, Action0 sinkObservableTerminatedCallback,
                                             final int workerIndex,
                                             final Observable<Integer> totalWorkerAtStageObservable,
                                             final Action0 onSinkSubscribe, final Action0 onSinkUnsubscribe,
                                             Action0 observableOnCompleteCallback, Action1<Throwable> observableOnErrorCallback) {
        // no previous stage for single stage job
        // source consumer
        WorkerConsumer sourceConsumer = new WorkerConsumer() {
            @Override
            public Observable start(StageConfig previousStage) {
                Index index = new Index(workerIndex, totalWorkerAtStageObservable);
                // call init on source
                source.getSourceFunction().init(context, index);

                Observable<Observable<?>> sourceObservable
                        = (Observable) source.getSourceFunction().call(
                        context, index);
                if (stage.getInputStrategy() == StageConfig.INPUT_STRATEGY.CONCURRENT) {
                    return sourceObservable;
                } else {
                    return Observable.just(Observable.merge(sourceObservable));
                }
            }

            @Override
            public void stop() {}
        };
        // sink publisher with metrics
        WorkerPublisher sinkPublisher = new SinkPublisher(sink, portSelector, context,
                sinkObservableTerminatedCallback, onSinkSubscribe, onSinkUnsubscribe,
                observableOnCompleteCallback, observableOnErrorCallback);
        StageExecutors.executeIntermediate(sourceConsumer, stage, sinkPublisher, context);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public static void executeSource(final int workerIndex, final SourceHolder source, final StageConfig stage,
                                     WorkerPublisher publisher, final Context context, final Observable<Integer> totalWorkerAtStageObservable) {
        // create a consumer from passed in source
        WorkerConsumer sourceConsumer = new WorkerConsumer() {
            @Override
            public Observable start(StageConfig stage) {
                Index index = new Index(workerIndex, totalWorkerAtStageObservable);
                // call init on source
                source.getSourceFunction().init(context, index);
                Observable<Observable<?>> sourceObservable
                        = (Observable) source.getSourceFunction().call(context, new Index(workerIndex, totalWorkerAtStageObservable));
                return MantisMarker.sourceOut(sourceObservable);
            }

            @Override
            public void stop() {}
        };
        executeIntermediate(sourceConsumer, stage, publisher, context);
    }


    @SuppressWarnings("unchecked")
    private static <K, T, R> Observable<Observable<R>> executeGroupsInParallel(Observable<GroupedObservable<K, T>> go,
                                                                               final Computation computation, final Context context, final long groupTakeUntil) {
        logger.info("initializing {}", computation.getClass().getCanonicalName());
        computation.init(context);

        // from groups to observable
        final Func2<Context, GroupedObservable<K, T>, Observable<R>> c
                = (Func2<Context, GroupedObservable<K, T>, Observable<R>>) computation;
        return
                go
                        .lift(new MonitorOperator<>("worker_stage_outer"))
                        .map((Func1<GroupedObservable<K, T>, Observable<R>>) group -> c
                                .call(context, GroupedObservableUtils.createGroupedObservable(group.getKey(),
                                        group
                                                // comment out as it induces NPE in merge supposedly fixed in rxJava 1.0
                                                .doOnUnsubscribe(() -> {
                                                    //logger.info("Expiring group in executeGroupsInParallel" + group.getKey());
                                                    if (groupsExpiredCounter != null)
                                                        groupsExpiredCounter.increment();
                                                })
                                                .timeout(groupTakeUntil, TimeUnit.SECONDS, (Observable<? extends T>) Observable.empty())

                                                .subscribeOn(Schedulers.computation())

                                                .lift(new MonitorOperator<T>("worker_stage_inner_input"))))
                                .lift(new MonitorOperator("worker_stage_inner_output")));
    }

    @SuppressWarnings("unchecked")
    private static <K, T, R> Observable<Observable<R>> executeMantisGroups(Observable<Observable<MantisGroup<K, T>>> go,
                                                                           final Computation computation, final Context context, final long groupTakeUntil) {
        logger.info("initializing {}", computation.getClass().getCanonicalName());
        computation.init(context);

        // from groups to observable
        final Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>> c
                = (Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>>) computation;
        return
                go
                        .lift(new MonitorOperator<>("worker_stage_outer"))
                        .map((Func1<Observable<MantisGroup<K, T>>, Observable<R>>) group -> c
                                .call(context, group
                                        .lift(new MonitorOperator<>("worker_stage_inner_input")))
                                .lift(new MonitorOperator("worker_stage_inner_output")));
    }

    /**
     * @param go
     * @param computation
     *
     * @return untyped to support multiple callers return types
     */
    @SuppressWarnings("unchecked")
    private static <K, T, R> Observable<Observable<R>> executeMantisGroupsInParallel(Observable<Observable<MantisGroup<K, T>>> go, Computation computation,
                                                                                     final Context context, final boolean applyTimeoutToInners, final long timeout) {
        logger.info("initializing {}", computation.getClass().getCanonicalName());
        computation.init(context);

        // from groups to observable
        final Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>> c
                = (Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>>) computation;

        return
                go
                        .lift(new MonitorOperator<>("worker_stage_outer"))
                        .map(observable -> c
                                .call(context, observable
                                        .subscribeOn(Schedulers.computation())
                                        .lift(new MonitorOperator<MantisGroup<K, T>>("worker_stage_inner_input")))
                                .lift(new MonitorOperator<R>("worker_stage_inner_output")));
    }

    /**
     * @param oo
     * @param computation
     *
     * @return untyped to support multiple callers return types
     */
    @SuppressWarnings("unchecked")
    private static <T, R> Observable<Observable<R>> executeInners(Observable<Observable<T>> oo, Computation computation,
                                                                  final Context context, final boolean applyTimeoutToInners, final long timeout) {
        logger.info("initializing {}", computation.getClass().getCanonicalName());
        computation.init(context);

        // from groups to observable
        final Func2<Context, Observable<T>, Observable<R>> c
                = (Func2<Context, Observable<T>, Observable<R>>) computation;

        return
                oo
                        .lift(new MonitorOperator<>("worker_stage_outer"))
                        .map(observable -> c
                                .call(context, observable
                                        .lift(new MonitorOperator<T>("worker_stage_inner_input")))
                                .lift(new MonitorOperator<R>("worker_stage_inner_output")));
    }

    /**
     * @param oo
     * @param computation
     *
     * @return untyped to support multiple callers return types
     */
    @SuppressWarnings("unchecked")
    private static <T, R> Observable<Observable<R>> executeInnersInParallel(Observable<Observable<T>> oo, Computation computation,
                                                                            final Context context, final boolean applyTimeoutToInners, final long timeout, final int concurrency) {
        logger.info("initializing {}", computation.getClass().getCanonicalName());
        computation.init(context);

        // from groups to observable
        final Func2<Context, Observable<T>, Observable<R>> c
                = (Func2<Context, Observable<T>, Observable<R>>) computation;

        if (concurrency == StageConfig.DEFAULT_STAGE_CONCURRENCY) {
            return oo
                    .lift(new MonitorOperator<>("worker_stage_outer"))
                    .map(observable -> c
                            .call(context, observable
                                    .observeOn(Schedulers.computation())
                                    .lift(new MonitorOperator<T>("worker_stage_inner_input")))
                            .lift(new MonitorOperator<R>("worker_stage_inner_output")));
        } else {
            final SingleThreadScheduler[] singleThreadSchedulers = new SingleThreadScheduler[concurrency];
            RxThreadFactory rxThreadFactory = new RxThreadFactory("MantisSingleThreadScheduler-");
            logger.info("creating {} Mantis threads", concurrency);
            for (int i = 0; i < concurrency; i++) {
                singleThreadSchedulers[i] = new SingleThreadScheduler(rxThreadFactory);
            }
            return oo
                    .lift(new MonitorOperator<>("worker_stage_outer"))
                    .map(observable -> observable
                            .groupBy(e -> System.nanoTime() % concurrency)
                            .flatMap(go ->
                                    c
                                            .call(context, go
                                                    .observeOn(singleThreadSchedulers[go.getKey().intValue()])
                                                    .lift(new MonitorOperator<>("worker_stage_inner_input")))
                                            .lift(new MonitorOperator<>("worker_stage_inner_output"))));
        }
    }

    /**
     * If stage concurrency is not specified on the stage config check job param and use it if set.
     *
     * @param givenStageConcurrency
     *
     * @return
     */
    private static int resolveStageConcurrency(int givenStageConcurrency) {
        if (givenStageConcurrency == StageConfig.DEFAULT_STAGE_CONCURRENCY) {
            String jobParamPrefix = "JOB_PARAM_";
            String stageConcurrencyParam = jobParamPrefix + STAGE_CONCURRENCY;
            String concurrency = System.getenv(stageConcurrencyParam);
            logger.info("Job param: " + stageConcurrencyParam + " value: " + concurrency);
            // check if env property is set.
            if (concurrency != null && !concurrency.isEmpty()) {
                logger.info("Job param: " + stageConcurrencyParam + " value: " + concurrency);
                try {
                    int jobParamConcurrency = Integer.parseInt(concurrency);
                    if (jobParamConcurrency <= 0) {
                        return givenStageConcurrency;
                    } else {
                        return jobParamConcurrency;
                    }
                } catch (NumberFormatException ignored) {

                }
                // check if System property has been set (useful for local debugging)
            }
        }
        return givenStageConcurrency;
    }


    private static <T, R> Observable<Observable<R>> setupScalarToScalarStage(ScalarToScalar<T, R> stage,
                                                                             Observable<Observable<T>> source, Context context) {

        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up ScalarToScalar stage with input type: " + inputType);
        // check if job overrides the default input strategy
        if (inputType == StageConfig.INPUT_STRATEGY.CONCURRENT) {

            return executeInnersInParallel(source, stage.getComputation(), context, false, Integer.MAX_VALUE, resolveStageConcurrency(stage.getConcurrency()));
        } else if (inputType == StageConfig.INPUT_STRATEGY.SERIAL) {
            Observable<Observable<T>> merged = Observable.just(Observable.merge(source));
            return executeInners(merged, stage.getComputation(), context, false, Integer.MAX_VALUE);
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

    private static <K, T, R> Observable<Observable<GroupedObservable<String, R>>> setupScalarToKeyStage(ScalarToKey<K, T, R> stage,
                                                                                                        Observable<Observable<T>> source, Context context) {
        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up ScalarToKey stage with input type: " + inputType);
        // check if job overrides the default input strategy
        if (inputType == StageConfig.INPUT_STRATEGY.CONCURRENT) {
            return executeInnersInParallel(source, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds(), resolveStageConcurrency(stage.getConcurrency()));
        } else if (inputType == StageConfig.INPUT_STRATEGY.SERIAL) {
            Observable<Observable<T>> merged = Observable.just(Observable.merge(source));
            return executeInners(merged, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds());
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

    // NJ
    private static <K, T, R> Observable<Observable<MantisGroup<String, R>>> setupScalarToGroupStage(ScalarToGroup<K, T, R> stage,
                                                                                                    Observable<Observable<T>> source, Context context) {
        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up ScalarToGroup stage with input type: " + inputType);
        // check if job overrides the default input strategy
        if (inputType == StageConfig.INPUT_STRATEGY.CONCURRENT) {
            return executeInnersInParallel(source, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds(),resolveStageConcurrency(stage.getConcurrency()));
        } else if (inputType == StageConfig.INPUT_STRATEGY.SERIAL) {
            Observable<Observable<T>> merged = Observable.just(Observable.merge(source));
            return executeInners(merged, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds());
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

    private static <K1, T, K2, R> Observable<Observable<GroupedObservable<String, R>>> setupKeyToKeyStage(KeyToKey<K1, T, K2, R> stage,
                                                                                                          Observable<Observable<GroupedObservable<String, T>>> source, Context context) {
        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up KeyToKey stage with input type: " + inputType);
        // check if job overrides the default input strategy
        if (inputType == StageConfig.INPUT_STRATEGY.CONCURRENT) {
            throw new RuntimeException("Concurrency is not a supported input strategy for KeyComputation");
        } else if (inputType == StageConfig.INPUT_STRATEGY.SERIAL) {
            Observable<GroupedObservable<String, T>> shuffled = Groups.flatten(source);
            return executeGroupsInParallel(shuffled, stage.getComputation(), context, stage.getKeyExpireTimeSeconds());
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

    private static <K1, T, K2, R> Observable<Observable<MantisGroup<String, R>>> setupGroupToGroupStage(GroupToGroup<K1, T, K2, R> stage,
                                                                                                        Observable<Observable<MantisGroup<String, T>>> source, Context context) {
        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up GroupToGroup stage with input type: " + inputType);
        // check if job overrides the default input strategy
        if (inputType == StageConfig.INPUT_STRATEGY.CONCURRENT) {
            throw new RuntimeException("Concurrency is not a supported input strategy for KeyComputation");
        } else if (inputType == StageConfig.INPUT_STRATEGY.SERIAL) {
            //Observable<MantisGroup<String,T>> shuffled = Groups.flatten(source);
            Observable<Observable<MantisGroup<String, T>>> merged = Observable.just(Observable.merge(source));
            return executeMantisGroups(merged, stage.getComputation(), context, stage.getKeyExpireTimeSeconds());
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

    // NJ
    private static <K, T, R> Observable<Observable<R>> setupKeyToScalarStage(KeyToScalar<K, T, R> stage,
                                                                             Observable<Observable<MantisGroup<String, T>>> source, Context context) {
        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up KeyToScalar stage with input type: " + inputType);
        // need to 'shuffle' groups across observables into
        // single observable<GroupedObservable>
        Observable<GroupedObservable<String, T>> shuffled = Groups.flattenMantisGroupsToGroupedObservables(source);
        return executeGroupsInParallel(shuffled, stage.getComputation(), context,
                stage.getKeyExpireTimeSeconds());
    }

    // NJ
    private static <K, T, R> Observable<Observable<R>> setupGroupToScalarStage(GroupToScalar<K, T, R> stage,
                                                                               Observable<Observable<MantisGroup<K, T>>> source, Context context) {
        StageConfig.INPUT_STRATEGY inputType = stage.getInputStrategy();
        logger.info("Setting up GroupToScalar stage with input type: " + inputType);
        // check if job overrides the default input strategy

        if (inputType == StageConfig.INPUT_STRATEGY.CONCURRENT) {
            logger.info("Execute Groups in PARALLEL!!!!");
            return executeMantisGroupsInParallel(source, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds());
        } else if (inputType == StageConfig.INPUT_STRATEGY.SERIAL) {

            Observable<Observable<MantisGroup<K, T>>> merged = Observable.just(Observable.merge(source));
            return executeMantisGroups(merged, stage.getComputation(), context,
                    stage.getKeyExpireTimeSeconds());
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public static <T, R> void executeIntermediate(WorkerConsumer consumer,
                                                  final StageConfig<T, R> stage, WorkerPublisher publisher, final Context context) {
        if (consumer == null) {
            throw new IllegalArgumentException("consumer cannot be null");
        }
        if (stage == null) {
            throw new IllegalArgumentException("stage cannot be null");
        }
        if (publisher == null) {
            throw new IllegalArgumentException("producer cannot be null");
        }

        Observable<?> toSink = null;
        if (stage instanceof ScalarToScalar) {
            ScalarToScalar scalarStage = (ScalarToScalar) stage;
            Observable<Observable<T>> source
                    = consumer.start(scalarStage);
            toSink = setupScalarToScalarStage(scalarStage, source, context);

        } else if (stage instanceof ScalarToKey) {
            ScalarToKey scalarStage = (ScalarToKey) stage;
            Observable<Observable<T>> source
                    = consumer.start(scalarStage);
            toSink = setupScalarToKeyStage(scalarStage, source, context);
        }
        // NJ
        else if (stage instanceof ScalarToGroup) {
            ScalarToGroup scalarStage = (ScalarToGroup) stage;
            Observable<Observable<T>> source
                    = consumer.start(scalarStage);
            toSink = setupScalarToGroupStage(scalarStage, source, context);
        } else if (stage instanceof KeyToKey) {
            KeyToKey keyToKey = (KeyToKey) stage;
            Observable<Observable<GroupedObservable<String, T>>> source =
                    consumer.start(keyToKey);
            toSink = setupKeyToKeyStage(keyToKey, source, context);

        } else if (stage instanceof GroupToGroup) {
            GroupToGroup groupToGroup = (GroupToGroup) stage;
            Observable<Observable<MantisGroup<String, T>>> source =
                    consumer.start(groupToGroup);
            toSink = setupGroupToGroupStage(groupToGroup, source, context);

        } else if (stage instanceof KeyToScalar) {

            KeyToScalar scalarToKey = (KeyToScalar) stage;
            Observable<Observable<MantisGroup<String, T>>> source =
                    consumer.start(scalarToKey);
            toSink = setupKeyToScalarStage(scalarToKey, source, context);

        } else if (stage instanceof GroupToScalar) {

            GroupToScalar groupToScalar = (GroupToScalar) stage;
            Observable<Observable<MantisGroup<String, T>>> source =
                    consumer.start(groupToScalar);

            toSink = setupGroupToScalarStage(groupToScalar, source, context);

        }

        publisher.start(stage, toSink);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public static void executeSink(WorkerConsumer consumer, StageConfig stage, SinkHolder sink,
                                   PortSelector portSelector, RxMetrics rxMetrics, Context context,
                                   Action0 sinkObservableCompletedCallback,
                                   final Action0 onSinkSubscribe, final Action0 onSinkUnsubscribe,
                                   Action0 observableOnCompleteCallback, Action1<Throwable> observableOnErrorCallback) {
        WorkerPublisher sinkPublisher = new SinkPublisher(sink, portSelector, context,
                sinkObservableCompletedCallback, onSinkSubscribe, onSinkUnsubscribe,
                observableOnCompleteCallback, observableOnErrorCallback);
        executeIntermediate(consumer, stage, sinkPublisher, context);
    }
}
