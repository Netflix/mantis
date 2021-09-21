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

import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import java.util.List;


class MetricData {

    private final String jobId;
    private final int stage;
    private final int workerIndex;
    private final int workerNumber;
    private final String metricGroupName;
    private final GaugeData gauges;

    MetricData(final String jobId, final int stage, final int workerIndex, final int workerNumber,
               final String metricGroupName, final List<GaugeMeasurement> gaugeMeasurements) {
        this(jobId, stage, workerIndex, workerNumber, metricGroupName,
                new GaugeData(System.currentTimeMillis(), gaugeMeasurements));
    }

    MetricData(final String jobId, final int stage, final int workerIndex, final int workerNumber,
               final String metricGroupName, final GaugeData gaugeData) {
        this.jobId = jobId;
        this.stage = stage;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.metricGroupName = metricGroupName;
        this.gauges = gaugeData;
    }

    String getJobId() {
        return jobId;
    }

    int getStage() {
        return stage;
    }

    int getWorkerIndex() {
        return workerIndex;
    }

    int getWorkerNumber() {
        return workerNumber;
    }

    public String getMetricGroupName() {
        return metricGroupName;
    }

    public GaugeData getGaugeData() {
        return gauges;
    }

    @Override
    public String toString() {
        return "MetricData{" +
                "jobId='" + jobId + '\'' +
                ", stage=" + stage +
                ", workerIndex=" + workerIndex +
                ", workerNumber=" + workerNumber +
                ", metricGroupName='" + metricGroupName + '\'' +
                ", gauges=" + gauges +
                '}';
    }

}
