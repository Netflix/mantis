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

import static io.mantisrx.server.core.stats.MetricStringConstants.*;
import static io.reactivex.mantis.network.push.PushServerSse.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


public class AutoScaleMetricsConfig {

    public static final String CLIENT_ID_TOKEN = "_CLIENT_ID_";
    public static final String OUTBOUND_METRIC_GROUP_PATTERN = String.format("%s:%s=%s:*", PUSH_SERVER_METRIC_GROUP_NAME, CLIENT_ID_TAG_NAME, CLIENT_ID_TOKEN);
    public static final String OUTBOUND_LEGACY_METRIC_GROUP_PATTERN = String.format("%s:%s=%s:*", PUSH_SERVER_LEGACY_METRIC_GROUP_NAME, CLIENT_ID_TAG_NAME, CLIENT_ID_TOKEN);

    private static final AggregationAlgo DEFAULT_ALGO = AggregationAlgo.AVERAGE;
    // autoscaling metric groups to subscribe to by default
    private static final Map<String, Map<String, AggregationAlgo>> defaultAutoScaleMetrics = new HashMap<>();

    private final Map<String, Map<String, AggregationAlgo>> sourceJobMetrics = new HashMap<>();
    private final Map<String, Pattern> sourceJobMetricsPatterns = new HashMap<>();

    static {
        defaultAutoScaleMetrics.put(RESOURCE_USAGE_METRIC_GROUP, new HashMap<>());
        defaultAutoScaleMetrics.put(DATA_DROP_METRIC_GROUP, new HashMap<>());
        final Map<String, AggregationAlgo> defaultKafkaConsumerMetric = new HashMap<>();
        defaultKafkaConsumerMetric.put(KAFKA_LAG, AggregationAlgo.MAX);
        defaultAutoScaleMetrics.put(KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP, defaultKafkaConsumerMetric);

        final Map<String, AggregationAlgo> defaultWorkerStageInnerInputMetric = new HashMap<>();
        defaultWorkerStageInnerInputMetric.put(ON_NEXT_GAUGE, AggregationAlgo.AVERAGE);
        defaultAutoScaleMetrics.put(WORKER_STAGE_INNER_INPUT, defaultWorkerStageInnerInputMetric);
    }

    private final Map<String, Map<String, AggregationAlgo>> userDefinedAutoScaleMetrics;

    public AutoScaleMetricsConfig() {
        this(new HashMap<>());
    }

    public AutoScaleMetricsConfig(final Map<String, Map<String, AggregationAlgo>> userDefinedAutoScaleMetrics) {
        this.userDefinedAutoScaleMetrics = userDefinedAutoScaleMetrics;

        final Map<String, AggregationAlgo> defaultOutboundMetric = new HashMap<>();
        defaultOutboundMetric.put(DROPPED_COUNTER_METRIC_NAME, AggregationAlgo.MAX);
        sourceJobMetrics.put(OUTBOUND_METRIC_GROUP_PATTERN, defaultOutboundMetric);
        sourceJobMetrics.put(OUTBOUND_LEGACY_METRIC_GROUP_PATTERN, defaultOutboundMetric);
        sourceJobMetricsPatterns.put(OUTBOUND_METRIC_GROUP_PATTERN, generateSourceJobMetricPattern(OUTBOUND_METRIC_GROUP_PATTERN));
        sourceJobMetricsPatterns.put(OUTBOUND_LEGACY_METRIC_GROUP_PATTERN, generateSourceJobMetricPattern(OUTBOUND_LEGACY_METRIC_GROUP_PATTERN));
    }

    public void addUserDefinedMetric(final String metricGroupName,
                                     final String metricName,
                                     final AggregationAlgo algo) {
        userDefinedAutoScaleMetrics.putIfAbsent(metricGroupName, new HashMap<>());
        userDefinedAutoScaleMetrics.get(metricGroupName).put(metricName, algo);
    }

    /**
     * Add source job drop metric patterns in addition to the default patterns.
     * @param metricsStr comma separated list of metrics in the form of metricGroupName::metricName::algo
     */
    public void addSourceJobDropMetrics(String metricsStr) {
        if (metricsStr == null) {
            return;
        }
        for (String metric : metricsStr.split(",")) {
            metric = metric.trim();
            if (metric.isEmpty()) {
                continue;
            }
            try {
                String[] parts = metric.split("::");
                String metricGroupName = parts[0];
                String metricName = parts[1];
                AggregationAlgo algo = AggregationAlgo.valueOf(parts[2]);

                Map<String, AggregationAlgo> metricGroup = sourceJobMetrics.get(metricGroupName);
                if (metricGroup == null) {
                    metricGroup = new HashMap<>();
                    sourceJobMetrics.put(metricGroupName, metricGroup);
                    sourceJobMetricsPatterns.put(metricGroupName, generateSourceJobMetricPattern(metricGroupName));
                }
                metricGroup.put(metricName, algo);
            } catch (Exception ex) {
                String errMsg = String.format("Invalid format for source job metric: %s", metricsStr);
                throw new RuntimeException(errMsg, ex);
            }
        }
    }

    public AggregationAlgo getAggregationAlgo(final String metricGroupName, final String metricName) {
        if (userDefinedAutoScaleMetrics.containsKey(metricGroupName) && userDefinedAutoScaleMetrics.get(metricGroupName).containsKey(metricName)) {
            return userDefinedAutoScaleMetrics.get(metricGroupName).getOrDefault(metricName, DEFAULT_ALGO);
        }
        if (defaultAutoScaleMetrics.containsKey(metricGroupName) && defaultAutoScaleMetrics.get(metricGroupName).containsKey(metricName)) {
            return defaultAutoScaleMetrics.get(metricGroupName).getOrDefault(metricName, DEFAULT_ALGO);
        }
        for (Map.Entry<String, Pattern> entry : sourceJobMetricsPatterns.entrySet()) {
            if (entry.getValue().matcher(metricGroupName).matches()) {
                return sourceJobMetrics.get(entry.getKey()).getOrDefault(metricName, DEFAULT_ALGO);
            }
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
            metrics.put(entry.getKey(), entry.getValue().keySet());
        }

        return metrics;
    }

    public Set<String> getMetricGroups() {
        return getAllMetrics().keySet();
    }

    public Set<String> generateSourceJobMetricGroups(Set<String> clientIds) {
        Set<String> results = new HashSet<>();
        for (String clientId : clientIds) {
            for (String metricPattern : sourceJobMetrics.keySet()) {
                results.add(metricPattern.replaceAll(CLIENT_ID_TOKEN, clientId));
            }
        }
        return results;
    }

    public boolean isSourceJobDropMetric(String metricGroupName, String metricName) {
        for (Map.Entry<String, Pattern> entry : sourceJobMetricsPatterns.entrySet()) {
            if (entry.getValue().matcher(metricGroupName).matches()) {
                return sourceJobMetrics.get(entry.getKey()).keySet().contains(metricName);
            }
        }
        return false;
    }

    private static Pattern generateSourceJobMetricPattern(String metricGroupName) {
        return Pattern.compile(metricGroupName.replace("*", ".*").replaceAll(CLIENT_ID_TOKEN, ".*"));
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
