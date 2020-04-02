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

package io.mantisrx.server.worker.jobmaster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.ServiceRegistry;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerOutlier;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.reactivx.mantis.operators.DropOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.observers.SerializedObserver;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import static io.mantisrx.server.core.stats.MetricStringConstants.*;


/* package */ class WorkerMetricHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkerMetricHandler.class);
    private final PublishSubject<MetricData> metricDataSubject = PublishSubject.create();
    private final Observer<JobAutoScaler.Event> jobAutoScaleObserver;
    private final MantisMasterClientApi masterClientApi;
    private final AutoScaleMetricsConfig autoScaleMetricsConfig;
    private final MetricAggregator metricAggregator;
    private final Map<Integer, Integer> numWorkersByStage = new HashMap<>();

    private final String jobId;
    private final Func1<Integer, Integer> lookupNumWorkersByStage = stage -> {
        if (numWorkersByStage.containsKey(stage)) {
            return numWorkersByStage.get(stage);
        } else {
            logger.warn("num workers for stage {} not known", stage);
            return -1;
        }
    };

    public WorkerMetricHandler(final String jobId,
                               final Observer<JobAutoScaler.Event> jobAutoScaleObserver,
                               final MantisMasterClientApi masterClientApi,
                               final AutoScaleMetricsConfig autoScaleMetricsConfig) {
        this.jobId = jobId;
        this.jobAutoScaleObserver = jobAutoScaleObserver;
        this.masterClientApi = masterClientApi;
        this.autoScaleMetricsConfig = autoScaleMetricsConfig;
        this.metricAggregator = new MetricAggregator(autoScaleMetricsConfig);
    }

    public Observer<MetricData> initAndGetMetricDataObserver() {
        start();
        return new SerializedObserver<>(metricDataSubject);
    }

    private Map<String, GaugeData> getAggregates(List<Map<String, GaugeData>> dataPointsList) {

        final Map<String, List<GaugeData>> transformed = new HashMap<>();

        for (Map<String, GaugeData> datapoint : dataPointsList) {
            for (Map.Entry<String, GaugeData> gauge : datapoint.entrySet()) {
                if (!transformed.containsKey(gauge.getKey())) {
                    transformed.put(gauge.getKey(), new ArrayList<>());
                }
                transformed.get(gauge.getKey()).add(gauge.getValue());
            }
        }

        return metricAggregator.getAggregates(transformed);
    }

    private class StageMetricDataOperator implements Observable.Operator<Object, MetricData> {

        private static final int killCooldownSecs = 600;

        private final int stage;
        private final Func1<Integer, Integer> numStageWorkersFn;
        private final int valuesToKeep = 2;
        private final AutoScaleMetricsConfig autoScaleMetricsConfig;
        private final ConcurrentMap<Integer, WorkerMetrics> workersMap = new ConcurrentHashMap<>();
        private final WorkerOutlier workerOutlier;

        private final Map<Integer, Integer> workerNumberByIndex = new HashMap<>();

        public StageMetricDataOperator(final int stage,
                                       final Func1<Integer, Integer> numStageWorkersFn,
                                       final AutoScaleMetricsConfig autoScaleMetricsConfig) {
            logger.debug("setting operator for stage " + stage);
            this.stage = stage;
            this.numStageWorkersFn = numStageWorkersFn;
            this.autoScaleMetricsConfig = autoScaleMetricsConfig;
            this.workerOutlier = new WorkerOutlier(killCooldownSecs, workerIndex -> {
                try {
                    final int workerNumber;
                    if (workerNumberByIndex.containsKey(workerIndex)) {
                        workerNumber = workerNumberByIndex.get(workerIndex);
                    } else {
                        logger.error("outlier resubmit FAILED. worker number not found for worker index {} stage {}", workerIndex, stage);
                        return;
                    }

                    if (resubmitOutlierWorkerEnabled()) {
                        logger.info("resubmitting worker job {} stage {} idx {} workerNum {} (dropping excessive data compared to others)",
                                jobId, stage, workerIndex, workerNumber);
                        masterClientApi.resubmitJobWorker(jobId, "JobMaster", workerNumber, "dropping excessive data compared to others in stage")
                                .onErrorResumeNext(throwable -> {
                                    logger.error("caught error ({}) when resubmitting outlier worker num {}", throwable.getMessage(), workerNumber);
                                    return Observable.empty();
                                })
                                .subscribe();
                    } else {
                        logger.info("resubmitOutlier property is disabled. Not killing worker job {} stage {} idx {} workerNum {} (dropping excessive data compared to others)",
                                jobId, stage, workerIndex, workerNumber);
                    }
                } catch (Exception e) {
                    logger.warn("Can't resubmit outlier worker idx {} error {}", workerIndex, e.getMessage(), e);
                }
            });
        }

        private boolean resubmitOutlierWorkerEnabled() {
            final String resubmitOutlierWorkerProp =
                    "mantis.worker.jobmaster.outlier.worker.resubmit";
            final String enableOutlierWorkerResubmit = "true";

            final boolean resubmitOutlierWorker =
                    Boolean.valueOf(
                            ServiceRegistry.INSTANCE.getPropertiesService()
                                    .getStringValue(resubmitOutlierWorkerProp, enableOutlierWorkerResubmit));
            return resubmitOutlierWorker;
        }

        private void addDataPoint(final MetricData datapoint) {
            final int workerIndex = datapoint.getWorkerIndex();
            logger.debug("adding data point for worker idx={} data={}", workerIndex, datapoint);

            if (!workersMap.containsKey(workerIndex)) {
                workersMap.putIfAbsent(workerIndex, new WorkerMetrics(valuesToKeep));
            }
            WorkerMetrics workerMetrics = workersMap.get(workerIndex);

            final MetricData transformedMetricData = workerMetrics.addDataPoint(datapoint.getMetricGroupName(), datapoint);
            if (transformedMetricData.getMetricGroupName().equals(DATA_DROP_METRIC_GROUP)) {
                final Map<String, Double> dataDropGauges = transformedMetricData.getGaugeData().getGauges();
                if (dataDropGauges.containsKey(DROP_PERCENT)) {
                    workerOutlier.addDataPoint(workerIndex,
                            dataDropGauges.get(DROP_PERCENT), numStageWorkersFn.call(stage));
                }
            }
            workerNumberByIndex.put(workerIndex, datapoint.getWorkerNumber());
            // remove any data for workers with index that don't exist anymore (happens when stage scales down)
            int maxIdx = 0;
            synchronized (workersMap) {
                for (Integer idx : workersMap.keySet()) {
                    maxIdx = Math.max(maxIdx, idx);
                }
            }
            final Integer numWorkers = numStageWorkersFn.call(stage);
            if (numWorkers > -1) {
                for (int idx = numWorkers; idx <= maxIdx; idx++) {
                    workersMap.remove(idx);
                }
            }
        }

        private static final int metricsIntervalSeconds = 30; // TODO make it configurable

        @Override
        public Subscriber<? super MetricData> call(final Subscriber<? super Object> child) {
            child.add(Schedulers.computation().createWorker().schedulePeriodically(
                    new Action0() {
                        @Override
                        public void call() {

                            List<Map<String, GaugeData>> listofAggregates = new ArrayList<>();

                            synchronized (workersMap) {
                                for (Map.Entry<Integer, WorkerMetrics> entry : workersMap.entrySet()) {
                                    // get the aggregate metric values by metric group per worker
                                    listofAggregates.add(metricAggregator.getAggregates(entry.getValue().getGaugesByMetricGrp()));
                                }
                            }
                            final int numWorkers = numStageWorkersFn.call(stage);
                            // get the aggregate metric values by metric group for all workers in stage
                            Map<String, GaugeData> allWorkerAggregates = getAggregates(listofAggregates);
                            logger.info("Job stage " + stage + " avgResUsage from " +
                                    workersMap.size() + " workers: " + allWorkerAggregates.toString());

                            for (Map.Entry<String, Set<String>> userDefinedMetric : autoScaleMetricsConfig.getUserDefinedMetrics().entrySet()) {
                                final String metricGrp = userDefinedMetric.getKey();
                                for (String metric : userDefinedMetric.getValue()) {
                                    if (!allWorkerAggregates.containsKey(metricGrp) || !allWorkerAggregates.get(metricGrp).getGauges().containsKey(metric)) {
                                        logger.debug("no gauge data found for UserDefined (metric={})", userDefinedMetric);
                                    } else {
                                        jobAutoScaleObserver.onNext(
                                                new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.UserDefined, stage,
                                                        allWorkerAggregates.get(metricGrp).getGauges().get(metric), numWorkers, ""));
                                    }
                                }
                            }
                            if (allWorkerAggregates.containsKey(KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP)) {
                                final Map<String, Double> gauges = allWorkerAggregates.get(KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP).getGauges();
                                if (gauges.containsKey(KAFKA_LAG)) {
                                    jobAutoScaleObserver.onNext(
                                            new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.KafkaLag, stage,
                                                    gauges.get(KAFKA_LAG), numWorkers, "")
                                    );
                                }
                                if (gauges.containsKey(KAFKA_PROCESSED)) {
                                    jobAutoScaleObserver.onNext(
                                            new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.KafkaProcessed, stage,
                                                    gauges.get(KAFKA_PROCESSED), numWorkers, ""));
                                }
                            }
                            if (allWorkerAggregates.containsKey(RESOURCE_USAGE_METRIC_GROUP)) {
                                // cpuPctUsageCurr is Published as (cpuUsageCurr * 100.0) from ResourceUsagePayloadSetter, reverse transform to retrieve curr cpu usage
                                double cpuUsageCurr = allWorkerAggregates.get(RESOURCE_USAGE_METRIC_GROUP).getGauges().get(MetricStringConstants.CPU_PCT_USAGE_CURR) / 100.0;
                                jobAutoScaleObserver.onNext(
                                        new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.CPU, stage,
                                                cpuUsageCurr, numWorkers, ""));
                                jobAutoScaleObserver.onNext(
                                        new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, stage,
                                                allWorkerAggregates.get(RESOURCE_USAGE_METRIC_GROUP).getGauges().get(MetricStringConstants.TOT_MEM_USAGE_CURR), numWorkers, ""));
                                jobAutoScaleObserver.onNext(
                                        new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Network, stage,
                                                allWorkerAggregates.get(RESOURCE_USAGE_METRIC_GROUP).getGauges().get(MetricStringConstants.NW_BYTES_USAGE_CURR), numWorkers, ""));
                            }

                            if (allWorkerAggregates.containsKey(DATA_DROP_METRIC_GROUP)) {
                                final GaugeData gaugeData = allWorkerAggregates.get(DATA_DROP_METRIC_GROUP);
                                final Map<String, Double> gauges = gaugeData.getGauges();
                                if (gauges.containsKey(DROP_PERCENT)) {
                                    jobAutoScaleObserver.onNext(
                                            new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.DataDrop, stage,
                                                    gauges.get(DROP_PERCENT), numWorkers, ""));
                                }
                            }

                            if (allWorkerAggregates.containsKey(WORKER_STAGE_INNER_INPUT)) {
                                final GaugeData gaugeData = allWorkerAggregates.get(WORKER_STAGE_INNER_INPUT);
                                final Map<String, Double> gauges = gaugeData.getGauges();
                                if (gauges.containsKey(ON_NEXT_GAUGE)) {
                                    jobAutoScaleObserver.onNext(
                                            new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.RPS, stage,
                                                    gauges.get(ON_NEXT_GAUGE) / 6.0, numWorkers, ""));
                                }
                            }
                        }
                    }, metricsIntervalSeconds, metricsIntervalSeconds, TimeUnit.SECONDS
            ));
            return new Subscriber<MetricData>() {
                @Override
                public void onCompleted() {
                    child.unsubscribe();
                }

                @Override
                public void onError(Throwable e) {
                    logger.error("Unexpected error: " + e.getMessage(), e);
                }

                @Override
                public void onNext(MetricData metricData) {
                    logger.debug("Got metric metricData for job " + jobId + " stage " + stage +
                            ", worker " + metricData.getWorkerNumber() + ": " + metricData);
                    addDataPoint(metricData);
                }
            };
        }
    }

    private void start() {
        final AtomicReference<List<Subscription>> ref = new AtomicReference<>(new ArrayList<>());
        masterClientApi.schedulingChanges(jobId)
                .doOnNext(jobSchedulingInfo -> {
                    final Map<Integer, WorkerAssignments> workerAssignments = jobSchedulingInfo.getWorkerAssignments();
                    for (Map.Entry<Integer, WorkerAssignments> workerAssignmentsEntry : workerAssignments.entrySet()) {
                        final WorkerAssignments workerAssignment = workerAssignmentsEntry.getValue();
                        logger.debug("setting numWorkers={} for stage={}", workerAssignment.getNumWorkers(), workerAssignment.getStage());
                        numWorkersByStage.put(workerAssignment.getStage(), workerAssignment.getNumWorkers());
                    }
                }).subscribe();

        logger.info("Starting worker metric handler with autoscale config {}", autoScaleMetricsConfig);
        metricDataSubject
                .groupBy(metricData -> metricData.getStage())
                .lift(new DropOperator<>(WorkerMetricHandler.class.getName()))
                .doOnNext(go -> {
                    final Integer stage = go.getKey();
                    final Subscription s = go
                            .lift(new StageMetricDataOperator(stage, lookupNumWorkersByStage, autoScaleMetricsConfig))
                            .subscribe();
                    logger.info("adding subscription for stage {} StageMetricDataOperator", stage);
                    ref.get().add(s);
                })
                .doOnUnsubscribe(() -> {
                    for (Subscription s : ref.get())
                        s.unsubscribe();
                })
                .subscribe();
    }
}
