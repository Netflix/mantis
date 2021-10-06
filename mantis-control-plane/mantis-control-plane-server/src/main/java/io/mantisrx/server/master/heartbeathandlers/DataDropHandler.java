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
//package io.mantisrx.server.master.heartbeathandlers;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import io.mantisrx.runtime.descriptor.StageScalingPolicy;
//import io.mantisrx.server.core.ServiceRegistry;
//import io.mantisrx.server.core.StatusPayloads;
//import io.mantisrx.server.core.WorkerOutlier;
//import io.mantisrx.server.master.store.InvalidJobException;
//import io.mantisrx.server.master.store.InvalidJobStateChangeException;
//import io.mantisrx.server.master.MantisJobMgr;
//import io.mantisrx.server.master.store.MantisWorkerMetadata;
//import io.reactivx.mantis.operators.DropOperator;
//
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
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
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
///* package */ class DataDropHandler implements PayloadExecutor {
//
//    private static volatile boolean resubmitOutlierWorker=true;
//    private static final String resubmitOutlierWorkerProp = "mantis.master.outlier.worker.resubmit";
//    static {
//        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(new Runnable() {
//            @Override
//            public void run() {
//                resubmitOutlierWorker = Boolean.valueOf(
//                        ServiceRegistry.INSTANCE.getPropertiesService().getStringValue(resubmitOutlierWorkerProp,
//                                ""+resubmitOutlierWorker));
//            }
//        }, 0, 60, TimeUnit.SECONDS);
//    }
//
//    private static final Logger logger = LoggerFactory.getLogger(DataDropHandler.class);
//    private final String jobId;
//    private final PublishSubject<HeartbeatPayloadHandler.Data> dataDropSubject = PublishSubject.create();
//    private final ObjectMapper objectMapper = new ObjectMapper();
//    private final Observer<JobAutoScaler.Event> jobAutoScaleObserver;
//    private final MantisJobMgr jobMgr;
//    private ConcurrentMap<Integer, Subscriber<? super Integer>> inners = new ConcurrentHashMap<>();
//
//    DataDropHandler(String jobId, Observer<JobAutoScaler.Event> jobAutoScaleObserver, MantisJobMgr jobMgr) {
//        this.jobId = jobId;
//        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        this.jobAutoScaleObserver = jobAutoScaleObserver;
//        this.jobMgr = jobMgr;
//    }
//
//    private static class DroppedData {
//        private final long when;
//        private final double value;
//        private DroppedData(long when, double value) {
//            this.when = when;
//            this.value = value;
//        }
//        public long getWhen() {
//            return when;
//        }
//        public double getValue() {
//            return value;
//        }
//    }
//
//    private class StageDataDropOperator2<T,R> implements Observable.Operator<Object, HeartbeatPayloadHandler.Data> {
//
//        private final long timeInPastThresholdSecs=60;
//        private final long killCooldownSecs=600;
//        private volatile long lastKilledWorkerAt=System.currentTimeMillis();
//        private final ConcurrentMap<Integer, List<DroppedData>> workersMap = new ConcurrentHashMap<>();
//        //private final Map<Integer, List<Boolean>> isOutlierMap = new HashMap<>();
//        private final int stage;
//        final WorkerOutlier workerOutlier;
//
//        StageDataDropOperator2(final int stage) {
//            this.stage = stage;
//             workerOutlier = new WorkerOutlier(killCooldownSecs, new Action1<Integer>() {
//                @Override
//                public void call(Integer workerIndex) {
//                    try {
//                        final MantisWorkerMetadata worker = jobMgr.getJobMetadata().getWorkerByIndex(stage, workerIndex);
//                        if (resubmitOutlierWorker)
//                            jobMgr.resubmitWorker(worker.getWorkerNumber(),
//                                    "dropping excessive data compared to others in stage");
//                        logger.warn((new Date()) + ": " + (resubmitOutlierWorker ? "" : "(not) ") +
//                                "Killing worker idx " + worker.getWorkerIndex() + " of job " + worker.getJobId() +
//                                ", stg " + stage + ", wrkNmbr " + worker.getWorkerNumber() +
//                                ": it has been dropping excessive data for a while, compared to others");
//                    } catch (InvalidJobException | InvalidJobStateChangeException e) {
//                        logger.warn("Can't resubmit outlier worker: " + e.getMessage(), e);
//                    }
//                }
//            });
//        }
//
//        private void addDataPoint(int workerIndex, double value) {
//            final int numWorkers = jobMgr.getJobMetadata().getStageMetadata(stage).getNumWorkers();
//            workerOutlier.addDataPoint(workerIndex, value, numWorkers);
//            if (jobAutoScaleObserver != null) {
//                addDroppedData(workerIndex, value);
//                // remove any data for workers with index that don't exist anymore (happens when stage scales down)
//                int maxIdx = 0;
//                for (Integer idx : workersMap.keySet())
//                    maxIdx = Math.max(maxIdx, idx);
//                for (int idx = numWorkers; idx < maxIdx; idx++) {
//                    workersMap.remove(idx);
//                }
//            }
//        }
//
//        private void addDroppedData(int workerIndex, double value) {
//            List<DroppedData> droppedDataList = workersMap.get(workerIndex);
//            if(droppedDataList==null)
//                workersMap.put(workerIndex, new ArrayList<DroppedData>());
//            droppedDataList = workersMap.get(workerIndex);
//            while(!droppedDataList.isEmpty()) {
//                if(droppedDataList.get(droppedDataList.size()-1).when < (System.currentTimeMillis()-timeInPastThresholdSecs*1000))
//                    droppedDataList.remove(droppedDataList.size()-1); // remove last
//                else
//                    break;
//            }
//            droppedDataList.add(0, new DroppedData(System.currentTimeMillis(), value)); // add to the beginning of list
//        }
//
//        @Override
//        public Subscriber<? super HeartbeatPayloadHandler.Data> call(final Subscriber<? super Object> child) {
//            if (jobAutoScaleObserver != null) {
//                child.add(Schedulers.computation().createWorker().schedulePeriodically(
//                        new Action0() {
//                            @Override
//                            public void call() {
//                                long timeScope = System.currentTimeMillis() - timeInPastThresholdSecs*1000;
//                                double avgOfAvgs=0.0;
//                                for(Map.Entry<Integer, List<DroppedData>> entry: workersMap.entrySet()) {
//                                    double average=0.0;
//                                    int count=0;
//                                    for(DroppedData dd: entry.getValue()) {
//                                        if(dd.when>timeScope) {
//                                            average += dd.value;
//                                            count++;
//                                        }
//                                    }
//                                    if(count > 0)
//                                        average /= count;
////                        logger.info("Job " + jobId + " stage " + stage + " index " + entry.getKey() + " has " + average + "% average data drop from " +
////                                count + " data points");
//                                    avgOfAvgs += average;
//                                    // ToDo need better math to figure out if stage needs autoscaling
//                                }
//                                if(workersMap.size()>0)
//                                    avgOfAvgs /= workersMap.size();
//                                if(avgOfAvgs>1.0)
//                                    logger.info("Job " + jobId + " stage " + stage + " has " + avgOfAvgs + "% average data drop from " +
//                                            workersMap.size() + " of its workers");
//                                jobAutoScaleObserver.onNext(
//                                        new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.DataDrop, stage, avgOfAvgs, ""));
//                            }
//                        }, 30, 30, TimeUnit.SECONDS // TODO make it configurable
//                ));
//            } else {
//                logger.info("DataDropHandler starting only for worker outlier detection of job " + jobId);
//            }
//            return new Subscriber<HeartbeatPayloadHandler.Data>() {
//                @Override
//                public void onCompleted() {
//                    logger.info("**** onCompleted");
//                    workerOutlier.completed();
//                    child.unsubscribe();
//                }
//                @Override
//                public void onError(Throwable e) {
//                    logger.error("Unexpected error: " + e.getMessage(), e);
//                }
//                @Override
//                public void onNext(HeartbeatPayloadHandler.Data data) {
//                    try {
//                        StatusPayloads.DataDropCounts dataDrop = objectMapper.readValue(data.getPayload().getData(), StatusPayloads.DataDropCounts.class);
//                        double droppedPercentage = (double)dataDrop.getDroppedCount()*100.0 /
//                                (double)(dataDrop.getDroppedCount()+dataDrop.getOnNextCount());
//                        addDataPoint(data.getWorkerIndex(), droppedPercentage);
//                    } catch (IOException e) {
//                        logger.error("Invalid json for dataDrop heartbeat payload for job " + jobId + ", stage " +
//                                data.getStage() + " index " + data.getWorkerIndex() + " number " + data.getWorkerNumber() +
//                                ": " + e.getMessage());
//                    }
//                }
//            };
//        }
//    }
//
//    @Override
//    public Observer<HeartbeatPayloadHandler.Data> call() {
//        start();
//        return new SerializedObserver<>(dataDropSubject);
//    }
//
//    private void start() {
//        dataDropSubject
//                .groupBy(new Func1<HeartbeatPayloadHandler.Data, Integer>() {
//                    @Override
//                    public Integer call(HeartbeatPayloadHandler.Data data) {
//                        return data.getStage();
//                    }
//                })
//                .lift(new DropOperator<GroupedObservable<Integer, HeartbeatPayloadHandler.Data>>(DataDropHandler.class.getName()))
//                .flatMap(new Func1<GroupedObservable<Integer, HeartbeatPayloadHandler.Data>, Observable<?>>() {
//                    @Override
//                    public Observable<?> call(final GroupedObservable<Integer, HeartbeatPayloadHandler.Data> go) {
//                        final Observable<Integer> iO = Observable.create(new Observable.OnSubscribe<Integer>() {
//                            @Override
//                            public void call(Subscriber<? super Integer> subscriber) {
//                                inners.put(go.getKey(), subscriber);
//                                logger.info("Subscribed to inner of stage " + go.getKey() + " of job " + jobId);
//                            }
//                        });
//                        return go
//                                .takeUntil(iO)
//                                .lift(new StageDataDropOperator2<>(go.getKey()))
//                                .doOnUnsubscribe(new Action0() {
//                                    @Override
//                                    public void call() {
//                                        logger.info("Unsubscribed stage " + go.getKey() + " of job " + jobId);
//                                    }
//                                });
//                    }
//                })
//                .onErrorResumeNext(new Func1<Throwable, Observable<?>>() {
//                    @Override
//                    public Observable<?> call(Throwable throwable) {
//                        logger.warn("Unexpected error job " + jobId + ": " + throwable.getMessage(), throwable);
//                        return Observable.empty();
//                    }
//                })
//                .doOnUnsubscribe(new Action0() {
//                    @Override
//                    public void call() {
//                        for(Subscriber s: inners.values()) {
//                            s.onNext(1);
//                            s.onCompleted();
//                        }
//                        inners.clear();
//                    }
//                })
//                .subscribe();
//    }
//}
