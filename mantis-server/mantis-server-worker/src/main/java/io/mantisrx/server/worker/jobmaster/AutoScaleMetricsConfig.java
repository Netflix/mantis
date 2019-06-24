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
import static io.mantisrx.server.core.stats.MetricStringConstants.KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.KAFKA_LAG;
import static io.mantisrx.server.core.stats.MetricStringConstants.RESOURCE_USAGE_METRIC_GROUP;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class AutoScaleMetricsConfig {

    private static final AggregationAlgo DEFAULT_ALGO = AggregationAlgo.AVERAGE;
    // autoscaling metric groups to subscribe to by default
    private static final Map<String, Map<String, AggregationAlgo>> defaultAutoScaleMetrics = new HashMap<>();

    static {
        defaultAutoScaleMetrics.put(RESOURCE_USAGE_METRIC_GROUP, new HashMap<>());
        defaultAutoScaleMetrics.put(DATA_DROP_METRIC_GROUP, new HashMap<>());
        final Map<String, AggregationAlgo> defaultKafkaConsumerMetric = new HashMap<>();
        defaultKafkaConsumerMetric.put(KAFKA_LAG, AggregationAlgo.MAX);
        defaultAutoScaleMetrics.put(KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP, defaultKafkaConsumerMetric);
    }

    private final Map<String, Map<String, AggregationAlgo>> userDefinedAutoScaleMetrics;

    public AutoScaleMetricsConfig() {
        this(new HashMap<>());
    }

    public AutoScaleMetricsConfig(final Map<String, Map<String, AggregationAlgo>> userDefinedAutoScaleMetrics) {
        this.userDefinedAutoScaleMetrics = userDefinedAutoScaleMetrics;
    }

    public void addUserDefinedMetric(final String metricGroupName,
                                     final String metricName,
                                     final AggregationAlgo algo) {
        userDefinedAutoScaleMetrics.putIfAbsent(metricGroupName, new HashMap<>());
        userDefinedAutoScaleMetrics.get(metricGroupName).put(metricName, algo);
    }

    public AggregationAlgo getAggregationAlgo(final String metricGroupName, final String metricName) {
        if (userDefinedAutoScaleMetrics.containsKey(metricGroupName) && userDefinedAutoScaleMetrics.get(metricGroupName).containsKey(metricName)) {
            return userDefinedAutoScaleMetrics.get(metricGroupName).getOrDefault(metricName, DEFAULT_ALGO);
        }
        if (defaultAutoScaleMetrics.containsKey(metricGroupName) && defaultAutoScaleMetrics.get(metricGroupName).containsKey(metricName)) {
            return defaultAutoScaleMetrics.get(metricGroupName).getOrDefault(metricName, DEFAULT_ALGO);
        }
        return DEFAULT_ALGO;
    }

    public Map<String, Set<String>> getAllMetrics() {
        final Map<String, Set<String>> metrics = new HashMap<>();

        for (Map.Entry<String, Map<String, AggregationAlgo>> entry : defaultAutoScaleMetrics.entrySet()) {
            metrics.put(entry.getKey(),
                    entry.getValue().keySet());
        }

        for (Map.Entry<String, Map<String, AggregationAlgo>> entry : userDefinedAutoScaleMetrics.entrySet()) {
            metrics.put(entry.getKey(),
                    entry.getValue().keySet());
        }
        return metrics;
    }

    public Map<String, Set<String>> getUserDefinedMetrics() {
        final Map<String, Set<String>> metrics = new HashMap<>();

        for (Map.Entry<String, Map<String, AggregationAlgo>> entry : userDefinedAutoScaleMetrics.entrySet()) {
            metrics.put(entry.getKey(),
                    entry.getValue().keySet());
        }

        return metrics;
    }

    public Set<String> getMetricGroups() {
        return getAllMetrics().keySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AutoScaleMetricsConfig that = (AutoScaleMetricsConfig) o;

        return userDefinedAutoScaleMetrics != null ? userDefinedAutoScaleMetrics.equals(that.userDefinedAutoScaleMetrics) : that.userDefinedAutoScaleMetrics == null;

    }

    @Override
    public int hashCode() {
        return userDefinedAutoScaleMetrics != null ? userDefinedAutoScaleMetrics.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "AutoScaleMetricsConfig{" +
                "userDefinedAutoScaleMetrics=" + userDefinedAutoScaleMetrics +
                '}';
    }

    public enum AggregationAlgo {
        AVERAGE,
        MAX
    }
}
