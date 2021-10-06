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
//import java.util.Collection;
//import java.util.List;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//import io.mantisrx.runtime.MantisJobDurationType;
//import io.mantisrx.server.core.Status;
//import io.mantisrx.server.core.StatusPayloads;
//import io.mantisrx.server.master.MantisJobMgr;
//import io.mantisrx.server.master.store.MantisJobMetadata;
//import io.mantisrx.server.master.store.MantisStageMetadata;
//import io.reactivx.mantis.operators.DropOperator;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.Observable;
//import rx.Observer;
//import rx.Subscriber;
//import rx.functions.Action0;
//import rx.functions.Action1;
//import rx.functions.Func1;
//import rx.observables.GroupedObservable;
//import rx.observers.SerializedObserver;
//import rx.schedulers.Schedulers;
//import rx.subjects.PublishSubject;
//
//
//public class HeartbeatPayloadHandler {
//
////    private static final HeartbeatPayloadHandler instance;
////
////    static {
////        instance = new HeartbeatPayloadHandler();
////    }
////
////    public static HeartbeatPayloadHandler getInstance() {
////        return instance;
////    }
////
////    public static class Data {
////
////        private final String jobId;
////        private final MantisJobMgr jobMgr;
////        private final int stage;
////        private final int workerIndex;
////        private final int workerNumber;
////        private final Status.Payload payload;
////
////        public Data(String jobId, MantisJobMgr jobMgr, int stage, int workerIndex, int workerNumber, Status.Payload payload) {
////            this.jobId = jobId;
////            this.jobMgr = jobMgr;
////            this.stage = stage;
////            this.workerIndex = workerIndex;
////            this.workerNumber = workerNumber;
////            this.payload = payload;
////        }
////
////        String getJobId() {
////            return jobId;
////        }
////
////        MantisJobMgr getJobMgr() {
////            return jobMgr;
////        }
////
////        int getStage() {
////            return stage;
////        }
////
////        int getWorkerIndex() {
////            return workerIndex;
////        }
////
////        int getWorkerNumber() {
////            return workerNumber;
////        }
////
////        Status.Payload getPayload() {
////            return payload;
////        }
////
////        @Override
////        public String toString() {
////            return String.format("jobId=%s, stage=%d, worker index=%d, number=%d, payload type=%s", jobId, stage,
////                    workerIndex, workerNumber, (payload == null ? "none" : payload.getType()));
////        }
//    }
//
//    private static class PayloadOperator<T, R> implements Observable.Operator<String, Data> {
//
////        private final String jobId;
////        private final Observer<Data> subscriptionStateObserver;
////        private final Observer<Data> dataDropObserver;
////        private final Observer<Data> resUsageObserver;
////        private final List<Observer<Data>> observers = new ArrayList<>();
////        private final JobAutoScaler jobAutoScaler;
////
////        private PayloadOperator(final String jobId, final MantisJobMgr jobMgr) {
////            logger.info("Setting up payload operator for job " + jobId);
////            this.jobId = jobId;
////            if (jobMgr.hasJobMaster()) {
////                // don't do anything but data drop handler for worker outlier detection, pass null autoscaler
////                subscriptionStateObserver = null;
////                dataDropObserver = new DataDropHandler(jobId, null, jobMgr).call();
////                observers.add(dataDropObserver);
////                resUsageObserver = null;
////                jobAutoScaler = null;
////            } else {
////                if (jobMgr.getJobMetadata() != null &&
////                        jobMgr.getJobMetadata().getSla().getDurationType() == MantisJobDurationType.Transient) {
////                    subscriptionStateObserver = new SubscriptionStateHandler(jobId, jobMgr).call();
////                    observers.add(subscriptionStateObserver);
////                } else
////                    subscriptionStateObserver = null;
////                if (isScalable(jobMgr))
////                    jobAutoScaler = new JobAutoScaler(jobId, jobMgr);
////                else
////                    jobAutoScaler = null;
////                dataDropObserver = new DataDropHandler(jobId,
////                        jobAutoScaler == null ? null : jobAutoScaler.getObserver(), jobMgr).call();
////                observers.add(dataDropObserver);
////                if (jobAutoScaler == null)
////                    resUsageObserver = null;
////                else {
////                    resUsageObserver = new ResUsageHandler(jobId, jobAutoScaler.getObserver(), jobMgr).call();
////                    observers.add(resUsageObserver);
////                }
////                if (jobAutoScaler != null)
////                    jobAutoScaler.start();
////            }
////            logger.info("Done setting up payload operator for job " + jobId);
////        }
////
////        private boolean isScalable(MantisJobMgr jobMgr) {
////            final MantisJobMetadata jobMetadata = jobMgr.getJobMetadata();
////            if (jobMetadata != null) {
////                final Collection<? extends MantisStageMetadata> stageMetadata = jobMetadata.getStageMetadata();
////                for (MantisStageMetadata s : stageMetadata) {
////                    // scalable only if autoscaling config set and instances max > instances min for stage
////                    if (s.getScalable() &&
////                            s.getScalingPolicy() != null &&
////                            s.getScalingPolicy().getMax() > s.getScalingPolicy().getMin()) {
////                        return true;
////                    }
////                }
////            }
////            return false;
////        }
////
////        @Override
////        public Subscriber<? super Data> call(Subscriber<? super String> subscriber) {
////            return new Subscriber<Data>() {
////                @Override
////                public void onCompleted() {
////                    logger.info("Completing payload handler for job " + jobId);
////                    for (Observer<Data> o : observers)
////                        o.onCompleted();
////                }
////
////                @Override
////                public void onError(Throwable e) {
////                    logger.error("Unexpected error: " + e.getMessage(), e);
////                }
////
////                @Override
////                public void onNext(Data data) {
////                    //logger.info("Got data: " + data);
////                    switch (StatusPayloads.Type.valueOf(data.payload.getType())) {
////                    case SubscriptionState:
////                        if (subscriptionStateObserver != null)
////                            subscriptionStateObserver.onNext(data);
////                        break;
////                    case IncomingDataDrop:
////                        dataDropObserver.onNext(data);
////                        break;
////                    case ResourceUsage:
////                        if (resUsageObserver != null)
////                            resUsageObserver.onNext(data);
////                        break;
////                    default:
////                        logger.warn("Unknown status payload " + data.payload.getType() + " in heartbeat for job " +
////                                jobId + " worker index " + data.workerIndex + " number " + data.workerNumber);
////                    }
////                }
////            };
////        }
//    }
//
//    private static final Logger logger = LoggerFactory.getLogger(HeartbeatPayloadHandler.class);
//    private final Observer<Data> observer;
//    private ConcurrentMap<String, Subscriber<? super Integer>> inners = new ConcurrentHashMap<>();
//    private Func1<String, MantisJobMgr> jobMgrGetter = null;
//
//    private HeartbeatPayloadHandler() {
//        PublishSubject<Data> subject = PublishSubject.create();
//        observer = new SerializedObserver<>(subject);
//        subject
//                .lift(new DropOperator<Data>("JobHeartbeatsHandler"))
//                .groupBy(new Func1<Data, MantisJobMgr>() {
//                    @Override
//                    public MantisJobMgr call(Data data) {
//                        return data.getJobMgr();
//                    }
//                })
//                .doOnError(new Action1<Throwable>() {
//                    @Override
//                    public void call(Throwable throwable) {
//                        logger.warn("Heartbeats handler error: " + throwable.getMessage(), throwable);
//                    }
//                })
//                .flatMap(new Func1<GroupedObservable<MantisJobMgr, Data>, Observable<?>>() {
//                    @Override
//                    public Observable<?> call(final GroupedObservable<MantisJobMgr, Data> go) {
//                        final String jobId = go.getKey().getJobId();
//                        logger.info("handling HB for job " + jobId);
//                        final Observable<Integer> iO = Observable.create(new Observable.OnSubscribe<Integer>() {
//                            @Override
//                            public void call(final Subscriber<? super Integer> subscriber) {
//                                inners.put(jobId, subscriber);
//                                logger.info("Subscribed to inner for " + (go.getKey() == null ? "nullJobMgr" : jobId));
//                            }
//                        });
//                        MantisJobMgr jobMgr = jobMgrGetter.call(jobId);
//                        if (jobMgr == null) {
//                            logger.warn("jobMgr NULL when setting up payload operator for job {}", jobId);
//                            return Observable.empty();
//                        } else {
//                            return go
//                                    .takeUntil(iO)
//                                    .observeOn(Schedulers.computation())
//                                    .lift(new PayloadOperator<>(jobId, jobMgr))
//                                    .doOnUnsubscribe(() -> logger.info("Unsubscribe: job " + jobId));
//                        }
//                    }
//                })
//                .onErrorResumeNext(new Func1<Throwable, Observable<?>>() {
//                    @Override
//                    public Observable<?> call(Throwable throwable) {
//                        logger.error("UNEXPECTED error in heartbeats handler: " + throwable.getMessage(), throwable);
//                        return Observable.empty();
//                    }
//                })
//                .doOnUnsubscribe(new Action0() {
//                    @Override
//                    public void call() {
//                        logger.error("Unexpected: main heartbeats handler Observable was unsubscribed");
//                    }
//                })
//                .subscribe();
//    }
//
//    public void handle(Data data) {
//        observer.onNext(data);
//        //logger.info("Done onNext'ing heartbeat for job " + data.getJobId() + " +, worker " + data.getWorkerNumber());
//    }
//
//    public void completeJob(MantisJobMgr jobMgr) {
//        logger.info("Completing observable chain for job " + jobMgr.getJobId());
//        final Subscriber<? super Integer> subscriber = inners.get(jobMgr.getJobId());
//        if (subscriber != null) {
//            subscriber.onNext(1);
//            subscriber.onCompleted();
//            inners.remove(jobMgr.getJobId(), subscriber);
//        }
//    }
//
//    public void setJobMetadataGetter(Func1<String, MantisJobMgr> jobMgrGetter) {
//        this.jobMgrGetter = jobMgrGetter;
//    }
//}
