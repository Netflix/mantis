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

import static io.mantisrx.server.core.stats.MetricStringConstants.DATA_DROP_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.DROP_COUNT;
import static io.mantisrx.server.core.stats.MetricStringConstants.DROP_PERCENT;
import static io.mantisrx.server.core.stats.MetricStringConstants.ON_NEXT_COUNT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class WorkerMetrics {

    private final int valuesToKeepPerMetric;
    private final ConcurrentMap<String, List<GaugeData>> gaugesByMetricGrp = new ConcurrentHashMap<>();

    public WorkerMetrics(int valuesToKeepPerMetric) {
        this.valuesToKeepPerMetric = valuesToKeepPerMetric;
    }

    public GaugeData transform(final String metricGroupName, final GaugeData data) {
        if (metricGroupName.equals(DATA_DROP_METRIC_GROUP)) {
            final Map<String, Double> gauges = data.getGauges();
            if (gauges.containsKey(DROP_COUNT) && gauges.containsKey(ON_NEXT_COUNT)) {
                final Double onNextCount = gauges.get(ON_NEXT_COUNT);
                final Double dropCount = gauges.get(DROP_COUNT);
                final double totalCount = dropCount + onNextCount;
                if (totalCount > 0.0) {
                    final double dropPercent = (dropCount * 100.0) / totalCount;

                    Map<String, Double> newGauges = new HashMap<>(2);
                    newGauges.put(DROP_PERCENT, dropPercent);
                    newGauges.put(ON_NEXT_COUNT, gauges.get(ON_NEXT_COUNT));
                    return new GaugeData(data.getWhen(), newGauges);
                }
            } else if (gauges.containsKey(ON_NEXT_COUNT)) {
                return new GaugeData(data.getWhen(), Collections.singletonMap(ON_NEXT_COUNT, gauges.get(ON_NEXT_COUNT)));
            }
            return new GaugeData(data.getWhen(), Collections.emptyMap());
        }
        return data;
    }

    public MetricData addDataPoint(final String metricGroupName, final MetricData metricData) {
        if (!gaugesByMetricGrp.containsKey(metricGroupName)) {
            gaugesByMetricGrp.putIfAbsent(metricGroupName, new CopyOnWriteArrayList<>());
        }
        final GaugeData transformed = transform(metricGroupName, metricData.getGaugeData());

        gaugesByMetricGrp.get(metricGroupName).add(transformed);
        if (gaugesByMetricGrp.get(metricGroupName).size() > valuesToKeepPerMetric) {
            gaugesByMetricGrp.get(metricGroupName).remove(0);
        }
        return new MetricData(metricData.getJobId(), metricData.getStage(), metricData.getWorkerIndex(),
                metricData.getWorkerNumber(), metricData.getMetricGroupName(), transformed);
    }

    public Map<String, List<GaugeData>> getGaugesByMetricGrp() {
        return gaugesByMetricGrp;
    }
}
