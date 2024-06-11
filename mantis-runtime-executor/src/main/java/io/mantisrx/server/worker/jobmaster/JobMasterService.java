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

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.SystemParameters;
import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import io.mantisrx.common.metrics.measurement.Measurements;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.SourceJobParameters;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

// job master service is the one responsible for autoscaling
// it represents stage 0.
public class JobMasterService implements Service {

    private static final Logger logger = LoggerFactory.getLogger(JobMasterService.class);

    private final static ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final String jobId;
    private final WorkerMetricsClient workerMetricsClient;
    private final AutoScaleMetricsConfig autoScaleMetricsConfig;
    private final Observer<MetricData> metricObserver;
    private final JobAutoScaler jobAutoScaler;
    private final Context context;
    private final Action0 observableOnCompleteCallback;
    private final Action1<Throwable> observableOnErrorCallback;
    private final Action0 observableOnTerminateCallback;
    private final MantisMasterGateway masterClientApi;

    private Subscription subscription = null;

    public JobMasterService(final String jobId,
                            final SchedulingInfo schedInfo,
                            final WorkerMetricsClient workerMetricsClient,
                            final AutoScaleMetricsConfig autoScaleMetricsConfig,
                            final MantisMasterGateway masterClientApi,
                            final Context context,
                            final Action0 observableOnCompleteCallback,
                            final Action1<Throwable> observableOnErrorCallback,
                            final Action0 observableOnTerminateCallback,
                            final JobAutoscalerManager jobAutoscalerManager) {
        this.jobId = jobId;
        this.workerMetricsClient = workerMetricsClient;
        this.autoScaleMetricsConfig = autoScaleMetricsConfig;
        this.masterClientApi = masterClientApi;
        this.jobAutoScaler = new JobAutoScaler(jobId, schedInfo, masterClientApi, context, jobAutoscalerManager);
        this.metricObserver = new WorkerMetricHandler(jobId, jobAutoScaler.getObserver(), masterClientApi, autoScaleMetricsConfig, jobAutoscalerManager).initAndGetMetricDataObserver();
        this.observableOnCompleteCallback = observableOnCompleteCallback;
        this.observableOnErrorCallback = observableOnErrorCallback;
        this.observableOnTerminateCallback = observableOnTerminateCallback;
        this.context = context;
    }

    private Measurements handleMetricEvent(final String ev) {
        try {
            final Measurements measurements = objectMapper.readValue(ev, Measurements.class);

            final String jobId = measurements.getTags().get(MetricStringConstants.MANTIS_JOB_ID);
            final int workerIdx = Integer.parseInt(measurements.getTags().get(MetricStringConstants.MANTIS_WORKER_INDEX));
            int stage = Integer.parseInt(measurements.getTags().get(MetricStringConstants.MANTIS_STAGE_NUM));
            final int workerNum = Integer.parseInt(measurements.getTags().get(MetricStringConstants.MANTIS_WORKER_NUM));
            List<GaugeMeasurement> gauges = (List<GaugeMeasurement>) measurements.getGauges();

            // Metric is not from current job, it is from the source job
            if (jobId != this.jobId) {
                // Funnel source job metric into the 1st stage
                stage = 1;
                if (gauges.isEmpty()) {
                    gauges = measurements.getCounters().stream().map(counter ->
                        new GaugeMeasurement(counter.getEvent(), counter.getCount())).collect(Collectors.toList());
                }
            }
            metricObserver.onNext(new MetricData(jobId, stage, workerIdx, workerNum, measurements.getName(), gauges));
            return measurements;

        } catch (JsonProcessingException e) {
            logger.error("failed to parse json", e);
        } catch (Exception e) {
            logger.error("caught exception", e);
        }
        return null;
    }

    @Override
    public void start() {
        logger.info("Starting JobMasterService");
        logger.info("Starting Job Auto Scaler");
        jobAutoScaler.start();

        final WorkerMetricSubscription workerMetricSubscription = new WorkerMetricSubscription(jobId, workerMetricsClient, autoScaleMetricsConfig.getMetricGroups());

        Observable<Observable<MantisServerSentEvent>> metrics = workerMetricSubscription.getMetricsClient().getResults();

        boolean isSourceJobMetricEnabled = (boolean) context.getParameters().get(
            SystemParameters.JOB_MASTER_AUTOSCALE_SOURCEJOB_METRIC_PARAM, false);
        if (isSourceJobMetricEnabled) {
            metrics = metrics.mergeWith(getSourceJobMetrics());
        }

        subscription = Observable.merge(metrics)
            .map(event -> handleMetricEvent(event.getEventAsString()))
            .doOnTerminate(observableOnTerminateCallback)
            .doOnCompleted(observableOnCompleteCallback)
            .doOnError(observableOnErrorCallback)
            .subscribe();
    }

    protected Observable<Observable<MantisServerSentEvent>> getSourceJobMetrics() {
        List<SourceJobParameters.TargetInfo> targetInfos = SourceJobParameters.parseTargetInfo(
            (String) context.getParameters().get(SystemParameters.JOB_MASTER_AUTOSCALE_SOURCEJOB_TARGET_PARAM, "{}"));
        if (targetInfos.isEmpty()) {
            targetInfos = SourceJobParameters.parseInputParameters(context);
        }
        targetInfos = SourceJobParameters.enforceClientIdConsistency(targetInfos, jobId);

        String additionalDropMetricPatterns =
            (String) context.getParameters().get(SystemParameters.JOB_MASTER_AUTOSCALE_SOURCEJOB_DROP_METRIC_PATTERNS_PARAM, "");
        autoScaleMetricsConfig.addSourceJobDropMetrics(additionalDropMetricPatterns);

        SourceJobWorkerMetricsSubscription sourceSub = new SourceJobWorkerMetricsSubscription(
            targetInfos, masterClientApi, workerMetricsClient, autoScaleMetricsConfig);

        return sourceSub.getResults();
    }

    @Override
    public void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }

    @Override
    public void enterActiveMode() {

    }
}
