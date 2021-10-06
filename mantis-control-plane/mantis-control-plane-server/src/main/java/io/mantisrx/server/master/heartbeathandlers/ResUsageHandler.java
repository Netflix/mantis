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
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.mantisrx.runtime.descriptor.StageScalingPolicy;
//import io.mantisrx.server.core.StatusPayloads;
//import io.mantisrx.server.master.MantisJobMgr;
//import io.reactivx.mantis.operators.DropOperator;
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
//import rx.schedulers.Schedulers;
//import rx.subjects.PublishSubject;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//
///* package */ class ResUsageHandler implements PayloadExecutor {
//    private static final Logger logger = LoggerFactory.getLogger(ResUsageHandler.class);
//    private final String jobId;
//    private final PublishSubject<HeartbeatPayloadHandler.Data> resUsageSubject = PublishSubject.create();
//    private final ObjectMapper objectMapper = new ObjectMapper();
//    private final Observer<JobAutoScaler.Event> jobAutoScaleObserver;
//    private final MantisJobMgr jobMgr;
//
//    public ResUsageHandler(String jobId, Observer<JobAutoScaler.Event> jobAutoScaleObserver, MantisJobMgr jobMgr) {
//        this.jobId = jobId;
//        this.jobAutoScaleObserver = jobAutoScaleObserver;
//        this.jobMgr = jobMgr;
//    }
//
//    private String getStagedMetricName(String metricName, int stageNum) {
//        return metricName + "-stage-" + stageNum;
//    }
//
//    @Override
//    public Observer<HeartbeatPayloadHandler.Data> call() {
//        start();
//        return new SerializedObserver<>(resUsageSubject);
//    }
//
//    private static class ResUsageData {
//        private final long when;
//        private final StatusPayloads.ResourceUsage usage;
//        private ResUsageData(long when, StatusPayloads.ResourceUsage usage) {
//            this.when = when;
//            this.usage = usage;
//        }
//        private long getWhen() {
//            return when;
//        }
//        private StatusPayloads.ResourceUsage getUsage() {
//            return usage;
//        }
//    }
//
//    private ResUsageData getAverages(List<ResUsageData> usages) {
//        double cpuLimit=0.0;
//        double cpuUsageCurrent=0.0;
//        double cpuUsagePeak=0.0;
//        double memLimit=0.0;
//        double memCacheCurrent=0.0;
//        double memCachePeak=0.0;
//        double totMemUsageCurrent=0.0;
//        double totMemUsagePeak=0.0;
//        double nwBytesCurrent=0.0;
//        double nwBytesPeak=0.0;
//        int n=0;
//        for(ResUsageData usageData: usages) {
//            n++;
//            StatusPayloads.ResourceUsage u = usageData.getUsage();
//            cpuLimit = u.getCpuLimit();
//            cpuUsageCurrent = ((cpuUsageCurrent*(n-1)) + u.getCpuUsageCurrent())/(double)n;
//            cpuUsagePeak = ((cpuUsagePeak*(n-1)) + u.getCpuUsagePeak())/(double)n;
//            memLimit = u.getMemLimit();
//            memCacheCurrent = ((memCacheCurrent*(n-1)) + u.getMemCacheCurrent())/(double)n;
//            memCachePeak = ((memCachePeak*(n-1)) + u.getMemCachePeak())/(double)n;
//            totMemUsageCurrent = ((totMemUsageCurrent*(n-1)) + u.getTotMemUsageCurrent())/(double)n;
//            totMemUsagePeak = ((totMemUsagePeak*(n-1)) + u.getTotMemUsagePeak())/(double)n;
//            nwBytesCurrent = ((nwBytesCurrent*(n-1)) + u.getNwBytesCurrent())/(double)n;
//            nwBytesPeak = ((nwBytesPeak*(n-1)) + u.getNwBytesPeak())/(double)n;
//        }
//        return new ResUsageData(System.currentTimeMillis(),
//                new StatusPayloads.ResourceUsage(cpuLimit, cpuUsageCurrent, cpuUsagePeak, memLimit,
//                memCacheCurrent, memCachePeak, totMemUsageCurrent, totMemUsagePeak, nwBytesCurrent, nwBytesPeak));
//    }
//
//    private class StageResUsageOperator<T,R> implements Observable.Operator<Object, HeartbeatPayloadHandler.Data> {
//
//        private final int stage;
//        private final int valuesToKeep=2;
//        private final Map<Integer, List<ResUsageData>> workersMap=new HashMap<>();
//        public StageResUsageOperator(int stage) {
//            logger.info("setting operator for " + jobId + " stage " + stage);
//            this.stage = stage;
//        }
//
//        private void addDataPoint(int workerIndex, StatusPayloads.ResourceUsage usage) {
//            List<ResUsageData> usageDataList = workersMap.get(workerIndex);
//            if(usageDataList==null) {
//                workersMap.put(workerIndex, new ArrayList<ResUsageData>());
//                usageDataList = workersMap.get(workerIndex);
//            }
//            usageDataList.add(new ResUsageData(System.currentTimeMillis(), usage));
//            if(usageDataList.size()>valuesToKeep)
//                usageDataList.remove(0);
//            // remove any data for workers with index that don't exist anymore (happens when stage scales down)
//            int maxIdx=0;
//            for(Integer idx: workersMap.keySet())
//                maxIdx = Math.max(maxIdx, idx);
//            for(int idx=jobMgr.getJobMetadata().getStageMetadata(stage).getNumWorkers(); idx<=maxIdx; idx++)
//                workersMap.remove(idx);
//        }
//
//        @Override
//        public Subscriber<? super HeartbeatPayloadHandler.Data> call(final Subscriber<? super Object> child) {
//            child.add(Schedulers.computation().createWorker().schedulePeriodically(
//                    new Action0() {
//                        @Override
//                        public void call() {
//                            List<ResUsageData> listOfAvgs = new ArrayList<>();
//                            for (Map.Entry<Integer, List<ResUsageData>> entry : workersMap.entrySet()) {
//                                listOfAvgs.add(getAverages(entry.getValue()));
//                            }
//                            ResUsageData avgOfAvgs = getAverages(listOfAvgs);
//                            logger.debug("Job " + jobId + " stage " + stage + " avgResUsage from " +
//                                    workersMap.size() + " workers: " + avgOfAvgs.getUsage());
//                            jobAutoScaleObserver.onNext(
//                                    new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.CPU, stage,
//                                            avgOfAvgs.getUsage().getCpuUsageCurrent(), ""));
//                            jobAutoScaleObserver.onNext(
//                                    new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, stage,
//                                            avgOfAvgs.getUsage().getTotMemUsageCurrent(), ""));
//                            jobAutoScaleObserver.onNext(
//                                    new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Network, stage,
//                                            avgOfAvgs.getUsage().getNwBytesCurrent(), ""));
//                        }
//                    }, 30, 30, TimeUnit.SECONDS // TODO make it configurable
//            ));
//            return new Subscriber<HeartbeatPayloadHandler.Data>() {
//                @Override
//                public void onCompleted() {
//                    child.unsubscribe();
//                }
//                @Override
//                public void onError(Throwable e) {
//                    logger.error("Unexpected error: " + e.getMessage(), e);
//                }
//                @Override
//                public void onNext(HeartbeatPayloadHandler.Data data) {
//                    try {
//                        StatusPayloads.ResourceUsage usage= objectMapper.readValue(data.getPayload().getData(), StatusPayloads.ResourceUsage.class);
////                        logger.info("Got resource usage of job " + jobId + " stage " + stage +
////                                ", worker " + data.getWorkerNumber() + ": " + usage);
//                        addDataPoint(data.getWorkerIndex(), usage);
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
//    private void start() {
//        final AtomicReference<List<Subscription>> ref = new AtomicReference<List<Subscription>>(new ArrayList<Subscription>());
//        resUsageSubject
//                .groupBy(new Func1<HeartbeatPayloadHandler.Data, Integer>() {
//                    @Override
//                    public Integer call(HeartbeatPayloadHandler.Data data) {
//                        return data.getStage();
//                    }
//                })
//                .lift(new DropOperator<GroupedObservable<Integer, HeartbeatPayloadHandler.Data>>(ResUsageHandler.class.getName()))
//                .doOnNext(new Action1<GroupedObservable<Integer, HeartbeatPayloadHandler.Data>>() {
//                    @Override
//                    public void call(GroupedObservable<Integer, HeartbeatPayloadHandler.Data> go) {
//                        final Integer stage = go.getKey();
//                        final Subscription s = go
//                                .lift(new StageResUsageOperator<>(stage))
//                                .subscribe();
//                        ref.get().add(s);
//                    }
//                })
//                .doOnUnsubscribe(new Action0() {
//                    @Override
//                    public void call() {
//                        for(Subscription s: ref.get())
//                            s.unsubscribe();
//                    }
//                })
//                .subscribe();
//    }
//}
