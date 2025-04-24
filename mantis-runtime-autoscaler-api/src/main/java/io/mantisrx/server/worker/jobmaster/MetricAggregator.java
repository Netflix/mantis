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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class MetricAggregator {

    private static final Logger logger = LoggerFactory.getLogger(MetricAggregator.class);

    private final AutoScaleMetricsConfig autoScaleMetricsConfig;

    MetricAggregator(final AutoScaleMetricsConfig autoScaleMetricsConfig) {
        this.autoScaleMetricsConfig = autoScaleMetricsConfig;
    }

    public Map<String, GaugeData> getAggregates(final Map<String, List<GaugeData>> dataPointsByMetricGrp) {
        final Map<String, GaugeData> result = new HashMap<>();

        for (Map.Entry<String, List<GaugeData>> metricGrpGauges : dataPointsByMetricGrp.entrySet()) {
            final String metricGrp = metricGrpGauges.getKey();
            final List<GaugeData> gaugeDataList = metricGrpGauges.getValue();

            int n = 0;
            Map<String, Double> gaugeAggregates = new HashMap<>();
            for (GaugeData gaugeData : gaugeDataList) {
                n++;
                final Map<String, Double> gauges = gaugeData.getGauges();

                for (Map.Entry<String, Double> gaugeEntry : gauges.entrySet()) {
                    final String gaugeName = gaugeEntry.getKey();
                    final double currValue = gaugeEntry.getValue();

                    final AutoScaleMetricsConfig.AggregationAlgo aggregationAlgo = autoScaleMetricsConfig.getAggregationAlgo(metricGrp, gaugeName);
                    switch (aggregationAlgo) {
                    case AVERAGE:
                        if (!gaugeAggregates.containsKey(gaugeName)) {
                            gaugeAggregates.put(gaugeName, currValue);
                        } else {
                            final double avg = ((gaugeAggregates.get(gaugeName) * (n - 1)) + currValue) / n;
                            gaugeAggregates.put(gaugeName, avg);
                        }
                        break;
                    case MAX:
                        if (!gaugeAggregates.containsKey(gaugeName)) {
                            gaugeAggregates.put(gaugeName, currValue);
                        } else {
                            final Double prev = gaugeAggregates.get(gaugeName);
                            final double max = (currValue > prev) ? currValue : prev;
                            gaugeAggregates.put(gaugeName, max);
                        }
                        break;
                    default:
                        logger.warn("unsupported aggregation algo {} for {}:{}", aggregationAlgo.name(), metricGrp, gaugeName);
                        break;
                    }
                }
            }
            result.put(metricGrp, new GaugeData(System.currentTimeMillis(), gaugeAggregates));
        }

        return result;
    }

}
