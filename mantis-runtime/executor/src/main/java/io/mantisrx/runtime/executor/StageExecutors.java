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

import static io.mantisrx.runtime.api.parameter.ParameterUtils.STAGE_CONCURRENCY;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.api.Context;
import io.mantisrx.runtime.api.ScalarToScalar;
import io.mantisrx.runtime.api.SinkHolder;
import io.mantisrx.runtime.api.SourceHolder;
import io.mantisrx.runtime.api.StageConfig;
import io.mantisrx.runtime.api.computation.Computation;
import io.mantisrx.runtime.api.computation.ScalarComputation;
import io.mantisrx.runtime.api.source.Index;
import io.mantisrx.runtime.reactor.MantisOperators;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;


public final class StageExecutors {

    private static final Logger LOGGER = LoggerFactory.getLogger(StageExecutors.class);

    private static Counter groupsExpiredCounter;

    static {
        Metrics m = new Metrics.Builder()
                .name("StageExecutors")
                .addCounter("groupsExpiredCounter")
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        groupsExpiredCounter = m.getCounter("groupsExpiredCounter");
    }

    private StageExecutors() {

    }

    @SuppressWarnings( {"rawtypes", "unchecked", "checkstyle:ParameterNumber"})
    static void executeSingleStageJob(final SourceHolder source,
                                      final StageConfig stage,
                                      final SinkHolder sink,
                                      final PortSelector portSelector,
                                      final Context context,
                                      Runnable sinkOnTerminatedCallback,
                                      final int workerIndex,
                                      final Publisher<Integer> totalWorkerAtStagePublisher,
                                      final Runnable onSinkSubscribe,
                                      final Runnable onSinkUnsubscribe,
                                      Runnable onCompleteCallback,
                                      Consumer<Throwable> onErrorCallback) {
        // no previous stage for single stage job
        // source consumer
        WorkerConsumer sourceConsumer = new WorkerConsumer() {
            @Override
            public Publisher<Publisher<?>> start(StageConfig previousStage) {
                Index index = new Index(workerIndex, totalWorkerAtStagePublisher);
                // call init on source
                source.getSourceFunction().init(context, index);

                Publisher dataSource = source.getSourceFunction().createDataSource(context, index);
                if (stage.getInputStrategy() == StageConfig.InputStrategy.CONCURRENT) {
                    return dataSource;
                } else {
                    return Flux.just(Flux.merge(dataSource));
                }
            }

            @Override
            public void stop() { }
        };
        // sink publisher with metrics
        WorkerPublisher sinkPublisher = new SinkPublisher(sink, portSelector, context,
                sinkOnTerminatedCallback, onSinkSubscribe, onSinkUnsubscribe,
                onCompleteCallback, onErrorCallback);
        StageExecutors.executeIntermediate(sourceConsumer, stage, sinkPublisher, context);
    }

//    @SuppressWarnings({"rawtypes", "unchecked"})
//    public static void executeSource(final int workerIndex, final SourceHolder source, final StageConfig stage,
//                                     WorkerPublisher publisher, final Context context, final Observable<Integer> totalWorkerAtStageObservable) {
//        // create a consumer from passed in source
//        WorkerConsumer sourceConsumer = new WorkerConsumer() {
//            @Override
//            public Observable start(StageConfig stage) {
//                Index index = new Index(workerIndex, totalWorkerAtStageObservable);
//                // call init on source
//                source.getSourceFunction().init(context, index);
//                Observable<Observable<?>> sourceObservable
//                        = (Observable) source.getSourceFunction().call(context, new Index(workerIndex, totalWorkerAtStageObservable));
//                return MantisMarker.sourceOut(sourceObservable);
//            }
//
//            @Override
//            public void stop() {}
//        };
//        executeIntermediate(sourceConsumer, stage, publisher, context);
//    }
//
//
//    @SuppressWarnings("unchecked")
//    private static <K, T, R> Observable<Observable<R>> executeGroupsInParallel(Observable<GroupedObservable<K, T>> go,
//                                                                               final Computation computation, final Context context, final long groupTakeUntil) {
//        logger.info("initializing {}", computation.getClass().getCanonicalName());
//        computation.init(context);
//
//        // from groups to observable
//        final Func2<Context, GroupedObservable<K, T>, Observable<R>> c
//                = (Func2<Context, GroupedObservable<K, T>, Observable<R>>) computation;
//        return
//                go
//                        .lift(new MonitorOperator<>("worker_stage_outer"))
//                        .map((Func1<GroupedObservable<K, T>, Observable<R>>) group -> c
//                                .call(context, GroupedObservableUtils.createGroupedObservable(group.getKey(),
//                                        group
//                                                // comment out as it induces NPE in merge supposedly fixed in rxJava 1.0
//                                                .doOnUnsubscribe(() -> {
//                                                    //logger.info("Expiring group in executeGroupsInParallel" + group.getKey());
//                                                    if (groupsExpiredCounter != null)
//                                                        groupsExpiredCounter.increment();
//                                                })
//                                                .timeout(groupTakeUntil, TimeUnit.SECONDS, (Observable<? extends T>) Observable.empty())
//
//                                                .subscribeOn(Schedulers.computation())
//
//                                                .lift(new MonitorOperator<T>("worker_stage_inner_input"))))
//                                .lift(new MonitorOperator("worker_stage_inner_output")));
//    }
//
//    @SuppressWarnings("unchecked")
//    private static <K, T, R> Observable<Observable<R>> executeMantisGroups(Observable<Observable<MantisGroup<K, T>>> go,
//                                                                           final Computation computation, final Context context, final long groupTakeUntil) {
//        logger.info("initializing {}", computation.getClass().getCanonicalName());
//        computation.init(context);
//
//        // from groups to observable
//        final Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>> c
//                = (Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>>) computation;
//        return
//                go
//                        .lift(new MonitorOperator<>("worker_stage_outer"))
//                        .map((Func1<Observable<MantisGroup<K, T>>, Observable<R>>) group -> c
//                                .call(context, group
//                                        .lift(new MonitorOperator<>("worker_stage_inner_input")))
//                                .lift(new MonitorOperator("worker_stage_inner_output")));
//    }
//
//    /**
//     * @param go
//     * @param computation
//     *
//     * @return untyped to support multiple callers return types
//     */
//    @SuppressWarnings("unchecked")
//    private static <K, T, R> Observable<Observable<R>> executeMantisGroupsInParallel(Observable<Observable<MantisGroup<K, T>>> go, Computation computation,
//                                                                                     final Context context, final boolean applyTimeoutToInners, final long timeout) {
//        logger.info("initializing {}", computation.getClass().getCanonicalName());
//        computation.init(context);
//
//        // from groups to observable
//        final Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>> c
//                = (Func2<Context, Observable<MantisGroup<K, T>>, Observable<R>>) computation;
//
//        return
//                go
//                        .lift(new MonitorOperator<>("worker_stage_outer"))
//                        .map(observable -> c
//                                .call(context, observable
//                                        .subscribeOn(Schedulers.computation())
//                                        .lift(new MonitorOperator<MantisGroup<K, T>>("worker_stage_inner_input")))
//                                .lift(new MonitorOperator<R>("worker_stage_inner_output")));
//    }

    /**
     * @param pp
     * @param computation
     *
     * @return untyped to support multiple callers return types
     */
    @SuppressWarnings("unchecked")
    private static <T, R> Publisher<Publisher<R>> executeInners(Publisher<Publisher<T>> pp,
                                                                  Computation computation,
                                                                  Context context) {
        final BiFunction<Context, Publisher<T>, Publisher<R>> c
                = (BiFunction<Context, Publisher<T>, Publisher<R>>) computation;

        return
            Flux.from(pp)
                .transform(MantisOperators.<Publisher<T>>monitor("worker_stage_outer"))
                .map(publisher -> Flux.from(c
                    .apply(context, Flux.from(publisher)
                        .transform(MantisOperators.<T>monitor("worker_stage_inner_input"))))
                    .transform(MantisOperators.<R>monitor("worker_stage_inner_output")));
    }

    /**
     * @param pp
     * @param computation
     *
     * @return untyped to support multiple callers return types
     */
    @SuppressWarnings("unchecked")
    private static <T, R> Publisher<Publisher<R>> executeInnersInParallel(Publisher<Publisher<T>> pp,
                                                                            Computation computation,
                                                                            Context context,
                                                                            int concurrency) {
        final BiFunction<Context, Publisher<T>, Publisher<R>> c
                = (BiFunction<Context, Publisher<T>, Publisher<R>>) computation;

        if (concurrency == StageConfig.DEFAULT_STAGE_CONCURRENCY) {
            return Flux.from(pp)
                .transform(MantisOperators.<Publisher<T>>monitor("worker_stage_outer"))
                .map(p -> Flux.from(c.apply(context,
                    Flux.from(p)
                        .publishOn(Schedulers.parallel())
                        .transform(MantisOperators.<T>monitor("worker_stage_inner_input"))))
                    .transform(MantisOperators.<R>monitor("worker_stage_inner_output")));

        } else {
            // TODO review and test
            // Note the newParallel() executor will detect and reject the usage of blocking Reactor APIs
            return Flux.from(pp)
                .transform(MantisOperators.<Publisher<T>>monitor("worker_stage_outer"))
                .map(p -> Flux.from(p)
                        .groupBy(e -> System.nanoTime() % concurrency)
                        .flatMap(gf -> Flux.from(c.apply(context, gf
                            .publishOn(Schedulers.newParallel("MantisParallelExecutor", concurrency, true))
                            .transform(MantisOperators.<T>monitor("worker_stage_inner_input"))))
                        .transform(MantisOperators.<R>monitor("worker_stage_inner_output"))));
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
            LOGGER.info("Job param: " + stageConcurrencyParam + " value: " + concurrency);
            // check if env property is set.
            if (concurrency != null && !concurrency.isEmpty()) {
                LOGGER.info("Job param: " + stageConcurrencyParam + " value: " + concurrency);
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


    private static <T, R> Publisher<Publisher<R>> setupScalarToScalarStage(ScalarToScalar<T, R> stage,
                                                                           Publisher<Publisher<T>> source,
                                                                           Context context) {
        ScalarComputation<T, R> computation = stage.getComputation();
        LOGGER.info("initializing {}", computation.getClass().getCanonicalName());
        computation.init(context);

        StageConfig.InputStrategy inputType = stage.getInputStrategy();
        LOGGER.info("Setting up ScalarToScalar stage with input type: " + inputType);
        // check if job overrides the default input strategy
        if (inputType == StageConfig.InputStrategy.CONCURRENT) {
            return executeInnersInParallel(source, computation, context, resolveStageConcurrency(stage.getConcurrency()));
        } else if (inputType == StageConfig.InputStrategy.SERIAL) {
            Publisher<Publisher<T>> merged = Flux.just(Flux.merge(source));
            return executeInners(merged, computation, context);
        } else {
            throw new RuntimeException("Unsupported input type: " + inputType.name());
        }
    }

//    private static <K, T, R> Observable<Observable<GroupedObservable<String, R>>> setupScalarToKeyStage(ScalarToKey<K, T, R> stage,
//                                                                                                        Observable<Observable<T>> source, Context context) {
//        StageConfig.InputStrategy inputType = stage.getInputStrategy();
//        logger.info("Setting up ScalarToKey stage with input type: " + inputType);
//        // check if job overrides the default input strategy
//        if (inputType == StageConfig.InputStrategy.CONCURRENT) {
//            return executeInnersInParallel(source, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds(), resolveStageConcurrency(stage.getConcurrency()));
//        } else if (inputType == StageConfig.InputStrategy.SERIAL) {
//            Observable<Observable<T>> merged = Observable.just(Observable.merge(source));
//            return executeInners(merged, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds());
//        } else {
//            throw new RuntimeException("Unsupported input type: " + inputType.name());
//        }
//    }
//
//    private static <K, T, R> Observable<Observable<MantisGroup<String, R>>> setupScalarToGroupStage(ScalarToGroup<K, T, R> stage,
//                                                                                                    Observable<Observable<T>> source, Context context) {
//        StageConfig.InputStrategy inputType = stage.getInputStrategy();
//        logger.info("Setting up ScalarToGroup stage with input type: " + inputType);
//        // check if job overrides the default input strategy
//        if (inputType == StageConfig.InputStrategy.CONCURRENT) {
//            return executeInnersInParallel(source, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds(),resolveStageConcurrency(stage.getConcurrency()));
//        } else if (inputType == StageConfig.InputStrategy.SERIAL) {
//            Observable<Observable<T>> merged = Observable.just(Observable.merge(source));
//            return executeInners(merged, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds());
//        } else {
//            throw new RuntimeException("Unsupported input type: " + inputType.name());
//        }
//    }
//
//    private static <K1, T, K2, R> Observable<Observable<GroupedObservable<String, R>>> setupKeyToKeyStage(KeyToKey<K1, T, K2, R> stage,
//                                                                                                          Observable<Observable<GroupedObservable<String, T>>> source, Context context) {
//        StageConfig.InputStrategy inputType = stage.getInputStrategy();
//        logger.info("Setting up KeyToKey stage with input type: " + inputType);
//        // check if job overrides the default input strategy
//        if (inputType == StageConfig.InputStrategy.CONCURRENT) {
//            throw new RuntimeException("Concurrency is not a supported input strategy for KeyComputation");
//        } else if (inputType == StageConfig.InputStrategy.SERIAL) {
//            Observable<GroupedObservable<String, T>> shuffled = Groups.flatten(source);
//            return executeGroupsInParallel(shuffled, stage.getComputation(), context, stage.getKeyExpireTimeSeconds());
//        } else {
//            throw new RuntimeException("Unsupported input type: " + inputType.name());
//        }
//    }
//
//    private static <K1, T, K2, R> Observable<Observable<MantisGroup<String, R>>> setupGroupToGroupStage(GroupToGroup<K1, T, K2, R> stage,
//                                                                                                        Observable<Observable<MantisGroup<String, T>>> source, Context context) {
//        StageConfig.InputStrategy inputType = stage.getInputStrategy();
//        logger.info("Setting up GroupToGroup stage with input type: " + inputType);
//        // check if job overrides the default input strategy
//        if (inputType == StageConfig.InputStrategy.CONCURRENT) {
//            throw new RuntimeException("Concurrency is not a supported input strategy for KeyComputation");
//        } else if (inputType == StageConfig.InputStrategy.SERIAL) {
//            //Observable<MantisGroup<String,T>> shuffled = Groups.flatten(source);
//            Observable<Observable<MantisGroup<String, T>>> merged = Observable.just(Observable.merge(source));
//            return executeMantisGroups(merged, stage.getComputation(), context, stage.getKeyExpireTimeSeconds());
//        } else {
//            throw new RuntimeException("Unsupported input type: " + inputType.name());
//        }
//    }
//
//
//    private static <K, T, R> Observable<Observable<R>> setupKeyToScalarStage(KeyToScalar<K, T, R> stage,
//                                                                             Observable<Observable<MantisGroup<String, T>>> source, Context context) {
//        StageConfig.InputStrategy inputType = stage.getInputStrategy();
//        logger.info("Setting up KeyToScalar stage with input type: " + inputType);
//        // need to 'shuffle' groups across observables into
//        // single observable<GroupedObservable>
//        Observable<GroupedObservable<String, T>> shuffled = Groups.flattenMantisGroupsToGroupedObservables(source);
//        return executeGroupsInParallel(shuffled, stage.getComputation(), context,
//                stage.getKeyExpireTimeSeconds());
//    }
//
//
//    private static <K, T, R> Observable<Observable<R>> setupGroupToScalarStage(GroupToScalar<K, T, R> stage,
//                                                                               Observable<Observable<MantisGroup<K, T>>> source, Context context) {
//        StageConfig.InputStrategy inputType = stage.getInputStrategy();
//        logger.info("Setting up GroupToScalar stage with input type: " + inputType);
//        // check if job overrides the default input strategy
//
//        if (inputType == StageConfig.InputStrategy.CONCURRENT) {
//            logger.info("Execute Groups in PARALLEL!!!!");
//            return executeMantisGroupsInParallel(source, stage.getComputation(), context, true, stage.getKeyExpireTimeSeconds());
//        } else if (inputType == StageConfig.InputStrategy.SERIAL) {
//
//            Observable<Observable<MantisGroup<K, T>>> merged = Observable.just(Observable.merge(source));
//            return executeMantisGroups(merged, stage.getComputation(), context,
//                    stage.getKeyExpireTimeSeconds());
//        } else {
//            throw new RuntimeException("Unsupported input type: " + inputType.name());
//        }
//    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static <T, R> void executeIntermediate(WorkerConsumer consumer,
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

        Publisher<?> toSink = null;
        if (stage instanceof ScalarToScalar) {
            ScalarToScalar scalarStage = (ScalarToScalar) stage;
            Publisher<Publisher<T>> source = consumer.start(scalarStage);
            toSink = setupScalarToScalarStage(scalarStage, source, context);
        }
//        else if (stage instanceof ScalarToKey) {
//            ScalarToKey scalarStage = (ScalarToKey) stage;
//            Observable<Observable<T>> source
//                    = consumer.start(scalarStage);
//            toSink = setupScalarToKeyStage(scalarStage, source, context);
//        }
//        else if (stage instanceof ScalarToGroup) {
//            ScalarToGroup scalarStage = (ScalarToGroup) stage;
//            Observable<Observable<T>> source
//                    = consumer.start(scalarStage);
//            toSink = setupScalarToGroupStage(scalarStage, source, context);
//        } else if (stage instanceof KeyToKey) {
//            KeyToKey keyToKey = (KeyToKey) stage;
//            Observable<Observable<GroupedObservable<String, T>>> source =
//                    consumer.start(keyToKey);
//            toSink = setupKeyToKeyStage(keyToKey, source, context);
//
//        } else if (stage instanceof GroupToGroup) {
//            GroupToGroup groupToGroup = (GroupToGroup) stage;
//            Observable<Observable<MantisGroup<String, T>>> source =
//                    consumer.start(groupToGroup);
//            toSink = setupGroupToGroupStage(groupToGroup, source, context);
//
//        } else if (stage instanceof KeyToScalar) {
//
//            KeyToScalar scalarToKey = (KeyToScalar) stage;
//            Observable<Observable<MantisGroup<String, T>>> source =
//                    consumer.start(scalarToKey);
//            toSink = setupKeyToScalarStage(scalarToKey, source, context);
//
//        } else if (stage instanceof GroupToScalar) {
//
//            GroupToScalar groupToScalar = (GroupToScalar) stage;
//            Observable<Observable<MantisGroup<String, T>>> source =
//                    consumer.start(groupToScalar);
//
//            toSink = setupGroupToScalarStage(groupToScalar, source, context);
//
//        }

        publisher.start(stage, toSink);
    }

//    @SuppressWarnings({"rawtypes", "unchecked"})
//    public static void executeSink(WorkerConsumer consumer, StageConfig stage, SinkHolder sink,
//                                   PortSelector portSelector, RxMetrics rxMetrics, Context context,
//                                   Runnable sinkObservableCompletedCallback,
//                                   final Runnable onSinkSubscribe, final Runnable onSinkUnsubscribe,
//                                   Runnable observableOnCompleteCallback, Action1<Throwable>
    //                                   observableOnErrorCallback) {
//        WorkerPublisher sinkPublisher = new SinkPublisher(sink, portSelector, context,
//                sinkObservableCompletedCallback, onSinkSubscribe, onSinkUnsubscribe,
//                observableOnCompleteCallback, observableOnErrorCallback);
//        executeIntermediate(consumer, stage, sinkPublisher, context);
//    }
}
