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

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import io.mantisrx.common.metrics.measurement.Measurements;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;


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

    private Subscription subscription = null;

    public JobMasterService(final String jobId,
                            final SchedulingInfo schedInfo,
                            final WorkerMetricsClient workerMetricsClient,
                            final AutoScaleMetricsConfig autoScaleMetricsConfig,
                            final MantisMasterClientApi masterClientApi,
                            final Context context,
                            final Action0 observableOnCompleteCallback,
                            final Action1<Throwable> observableOnErrorCallback,
                            final Action0 observableOnTerminateCallback) {
        this.jobId = jobId;
        this.workerMetricsClient = workerMetricsClient;
        this.autoScaleMetricsConfig = autoScaleMetricsConfig;
        this.jobAutoScaler = new JobAutoScaler(jobId, schedInfo, masterClientApi, context);
        this.metricObserver = new WorkerMetricHandler(jobId, jobAutoScaler.getObserver(), masterClientApi, autoScaleMetricsConfig).initAndGetMetricDataObserver();
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
            final int stage = Integer.parseInt(measurements.getTags().get(MetricStringConstants.MANTIS_STAGE_NUM));
            final int workerNum = Integer.parseInt(measurements.getTags().get(MetricStringConstants.MANTIS_WORKER_NUM));

            //            logger.info("got data from idx {} num {} stage {}", workerIdx, workerNum, stage);
            metricObserver.onNext(new MetricData(jobId, stage, workerIdx, workerNum,
                    measurements.getName(), (List<GaugeMeasurement>) measurements.getGauges()));

            return measurements;

        } catch (JsonProcessingException e) {
            logger.error("failed to parse json", e);
        } catch (IOException e) {
            logger.error("failed to process json", e);
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

        final Observable<Observable<MantisServerSentEvent>> metrics = workerMetricSubscription.getMetricsClient().getResults();
        subscription = Observable.merge(metrics)
                .map(event -> handleMetricEvent(event.getEventAsString()))
                .doOnTerminate(observableOnTerminateCallback)
                .doOnCompleted(observableOnCompleteCallback)
                .doOnError(observableOnErrorCallback)
                .subscribe();
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
