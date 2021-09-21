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

import static io.mantisrx.server.core.stats.MetricStringConstants.METRIC_NAME_STR;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.worker.client.MetricsClient;
import io.mantisrx.server.worker.client.SseWorkerConnectionFunction;
import io.mantisrx.server.worker.client.WorkerConnectionsStatus;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.functions.Action1;


public class WorkerMetricSubscription {

    private static final Logger logger = LoggerFactory.getLogger(WorkerMetricSubscription.class);
    final MetricsClient<MantisServerSentEvent> metricsClient;
    // worker metrics to subscribe to
    private final Set<String> metrics;

    public WorkerMetricSubscription(final String jobId, WorkerMetricsClient workerMetricsClient, Set<String> metricGroups) {
        this.metrics = metricGroups;

        SinkParameters metricNamesFilter = null;
        try {
            SinkParameters.Builder sinkParamsBuilder = new SinkParameters.Builder();
            for (String metric : metricGroups) {
                sinkParamsBuilder = sinkParamsBuilder.withParameter(METRIC_NAME_STR, metric);
            }
            metricNamesFilter = sinkParamsBuilder.build();
        } catch (UnsupportedEncodingException e) {
            logger.error("error encoding sink parameters", e);
        }

        metricsClient = workerMetricsClient.getMetricsClientByJobId(jobId,
                new SseWorkerConnectionFunction(true, new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.error("Metric connection error: " + throwable.getMessage());
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ie) {
                            logger.error("Interrupted waiting for retrying connection");
                        }
                    }
                }, metricNamesFilter),
                new Observer<WorkerConnectionsStatus>() {
                    @Override
                    public void onCompleted() {
                        logger.info("got onCompleted in WorkerConnStatus obs");
                    }

                    @Override
                    public void onError(Throwable e) {
                        logger.info("got onError in WorkerConnStatus obs");
                    }

                    @Override
                    public void onNext(WorkerConnectionsStatus workerConnectionsStatus) {
                        logger.info("got WorkerConnStatus {}", workerConnectionsStatus);
                    }
                });
    }

    public Set<String> getMetrics() {
        return metrics;
    }

    public MetricsClient<MantisServerSentEvent> getMetricsClient() {
        return metricsClient;

    }
}
