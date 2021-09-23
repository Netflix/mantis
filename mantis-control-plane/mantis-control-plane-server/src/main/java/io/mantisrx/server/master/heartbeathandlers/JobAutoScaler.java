/*
 * Copyright 2021 Netflix, Inc.
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
///*
// * Copyright 2019 Netflix, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.mantisrx.server.master.heartbeathandlers;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicReference;
//
//import io.mantisrx.runtime.descriptor.StageScalingPolicy;
//import io.mantisrx.server.core.stats.UsageDataStats;
//import io.mantisrx.server.master.MantisJobMgr;
//import io.mantisrx.server.master.store.InvalidJobException;
//import io.mantisrx.server.master.store.MantisStageMetadata;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.Observable;
//import rx.Observer;
//import rx.Subscriber;
//import rx.Subscription;
//import rx.functions.Action0;
//import rx.functions.Action1;
//import rx.functions.Func1;
//import rx.observables.GroupedObservable;
//import rx.observers.SerializedObserver;
//import rx.subjects.PublishSubject;
//
//
//class JobAutoScaler {
//
//    static class Event {
//
//        private final StageScalingPolicy.ScalingReason type;
//        private final int stage;
//        private final double value;
//        private final String message;
//
//        public Event(StageScalingPolicy.ScalingReason type, int stage, double value, String message) {
//            this.type = type;
//            this.stage = stage;
//            this.value = value;
//            this.message = message;
//        }
//
//        public StageScalingPolicy.ScalingReason getType() {
//            return type;
//        }
//
//        public int getStage() {
//            return stage;
//        }
//
//        public double getValue() {
//            return value;
//        }
//
//        public String getMessage() {
//            return message;
//        }
//
//        @Override
//        public String toString() {
//            return "type=" + type + ", stage=" + stage + ", value=" + value + ", message=" + message;
//        }
//    }
//
//    private class StageScaleOperator<T, R> implements Observable.Operator<Object, Event> {
//
//        private final int stage;
//        private final MantisStageMetadata stageMetadata;
//        private volatile long lastScaledAt = 0L;
//
//        private StageScaleOperator(int stage, MantisStageMetadata stageMetadata) {
//            this.stage = stage;
//            this.stageMetadata = stageMetadata;
//        }
//
//        @Override
//        public Subscriber<? super Event> call(final Subscriber<? super Object> child) {
//
//            return new Subscriber<Event>() {
//                private final Map<StageScalingPolicy.ScalingReason, UsageDataStats> dataStatsMap = new HashMap<>();
//
//                @Override
//                public void onCompleted() {
//                    child.unsubscribe();
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    logger.error("Unexpected error: " + e.getMessage(), e);
//                }
//
//                @Override
//                public void onNext(Event event) {
//                    final StageScalingPolicy scalingPolicy = stageMetadata.getScalingPolicy();
//                    long coolDownSecs = scalingPolicy == null ? Long.MAX_VALUE : scalingPolicy.getCoolDownSecs();
//                    boolean scalable = stageMetadata.getScalable() && scalingPolicy != null && scalingPolicy.isEnabled();
//                    //logger.info("Will check for autoscaling job " + jobId + " stage " + stage + " due to event: " + event);
//                    if (scalable && scalingPolicy != null) {
//                        final StageScalingPolicy.Strategy strategy = scalingPolicy.getStrategies().get(event.getType());
//                        if (strategy != null) {
//                            double effectiveValue = getEffectiveValue(event.getType(), event.getValue());
//                            UsageDataStats stats = dataStatsMap.get(event.getType());
//                            if (stats == null) {
//                                stats = new UsageDataStats(
//                                        strategy.getScaleUpAbovePct(), strategy.getScaleDownBelowPct(), strategy.getRollingCount());
//                                dataStatsMap.put(event.getType(), stats);
//                            }
//                            stats.add(effectiveValue);
//                            if (lastScaledAt < (System.currentTimeMillis() - coolDownSecs * 1000)) {
//                                logger.info(jobId + ", stage " + stage + ": eff=" +
//                                        String.format(PercentNumberFormat, effectiveValue) + ", thresh=" + strategy.getScaleUpAbovePct());
//                                if (stats.getHighThreshTriggered()) {
//                                    logger.info("Attempting to scale up stage " + stage + " of job " + jobId + " by " +
//                                            scalingPolicy.getIncrement() + " workers, because " +
//                                            event.type + " exceeded scaleUpThreshold of " +
//                                            String.format(PercentNumberFormat, strategy.getScaleUpAbovePct()) + " " +
//                                            stats.getCurrentHighCount() + "  times");
//                                    try {
//                                        int scaledUp =
//                                                jobMgr.scaleUpStage(stage, scalingPolicy.getIncrement(), event.getType() + " with value " +
//                                                        String.format(PercentNumberFormat, effectiveValue) +
//                                                        " exceeded scaleUp threshold of " + strategy.getScaleUpAbovePct());
//                                        if (scaledUp > 0)
//                                            lastScaledAt = System.currentTimeMillis();
//                                    } catch (InvalidJobException e) {
//                                        logger.error("Couldn't scale up job " + jobId + " stage " + stage + ": " + e.getMessage());
//                                    }
//                                } else if (stats.getLowThreshTriggered()) {
//                                    logger.info("Attempting to scale down stage " + stage + " of job " + jobId + " by " +
//                                            scalingPolicy.getDecrement() + " workers because " + event.getType() +
//                                            " is below scaleDownThreshold of " + strategy.getScaleDownBelowPct() +
//                                            " " + stats.getCurrentLowCount() + " times");
//                                    try {
//                                        final int scaledDown =
//                                                jobMgr.scaleDownStage(stage, scalingPolicy.getDecrement(), event.getType() + " with value " +
//                                                        String.format(PercentNumberFormat, effectiveValue) +
//                                                        " is below scaleDown threshold of " + strategy.getScaleDownBelowPct());
//                                        if (scaledDown > 0)
//                                            lastScaledAt = System.currentTimeMillis();
//                                    } catch (InvalidJobException e) {
//                                        logger.error("Couldn't scale down job " + jobId + " stage " + stage + ": " + e.getMessage());
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//
//                private double getEffectiveValue(StageScalingPolicy.ScalingReason type, double value) {
//                    switch (type) {
//                    case CPU:
//                        return 100.0 * value / stageMetadata.getMachineDefinition().getCpuCores();
//                    case Memory:
//                        return 100.0 * value / stageMetadata.getMachineDefinition().getMemoryMB();
//                    case DataDrop:
//                        return value;
//                    case Network:
//                        // value is in bytes, multiply by 8, divide by M
//                        return 100.0 * value * 8 / (1024.0 * 1024.0 * stageMetadata.getMachineDefinition().getNetworkMbps());
//                    default:
//                        logger.warn("Unsupported type " + type);
//                        return 0.0;
//                    }
//                }
//            };
//        }
//    }
//
//    private static final Logger logger = LoggerFactory.getLogger(JobAutoScaler.class);
//    private static final String PercentNumberFormat = "%5.2f";
//    private final String jobId;
//    private final MantisJobMgr jobMgr;
//    private final PublishSubject<Event> subject;
//
//    JobAutoScaler(String jobId, MantisJobMgr jobMgr) {
//        this.jobId = jobId;
//        this.jobMgr = jobMgr;
//        subject = PublishSubject.create();
//    }
//
//    Observer<Event> getObserver() {
//        return new SerializedObserver<>(subject);
//    }
//
//    void start() {
//        final AtomicReference<List<Subscription>> ref = new AtomicReference<List<Subscription>>(new ArrayList<Subscription>());
//        subject
//                .groupBy(new Func1<Event, Integer>() {
//                    @Override
//                    public Integer call(Event event) {
//                        return event.getStage();
//                    }
//                })
//                .doOnNext(new Action1<GroupedObservable<Integer, Event>>() {
//                    @Override
//                    public void call(GroupedObservable<Integer, Event> go) {
//                        Integer stage = go.getKey();
//                        final StageScalingPolicy scalingPolicy = jobMgr.getJobMetadata().getStageMetadata(stage).getScalingPolicy();
//                        logger.info("Setting up stage scale operator for job " + jobId + " stage " + stage);
//                        final Subscription s = go
//                                .lift(new StageScaleOperator<>(stage, jobMgr.getJobMetadata().getStageMetadata(stage)))
//                                .subscribe();
//                        ref.get().add(s);
//                    }
//                })
//                .doOnUnsubscribe(new Action0() {
//                    @Override
//                    public void call() {
//                        logger.info("Unsubscribing for autoscaler of job " + jobId);
//                        for (Subscription s : ref.get())
//                            s.unsubscribe();
//                    }
//                })
//                .subscribe();
//    }
//}
