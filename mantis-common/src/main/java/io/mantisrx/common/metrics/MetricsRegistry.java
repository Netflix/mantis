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

package io.mantisrx.common.metrics;

import io.mantisrx.common.metrics.spectator.MetricGroupId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class MetricsRegistry {

    private static final MetricsRegistry instance = new MetricsRegistry();
    private ConcurrentMap<String, Metrics> metricsRegistered = new ConcurrentHashMap<>();

    private MetricsRegistry() {}

    public static MetricsRegistry getInstance() {
        return instance;
    }

    /**
     * Register metrics if not already registered with the metrics' name.
     *
     * @param metrics
     *
     * @return the already registered metrics object associated with metrics' name, or null.
     */
    public Metrics registerAndGet(Metrics metrics) {
        final Metrics old = metricsRegistered.putIfAbsent(metrics.getMetricGroupId().id(), metrics);
        if (old == null) {
            return metrics;
        }
        return old;
    }

    Collection<Metrics> metrics() {
        return metricsRegistered.values();
    }

    public Collection<Metrics> getMetrics(String prefix) {
        List<Metrics> result = new ArrayList<>();
        for (Metrics m : metricsRegistered.values()) {
            if (m.getMetricGroupId().id().startsWith(prefix))
                result.add(m);
        }
        return result;
    }

    @Deprecated
    public boolean remove(String metricGroupId) {
        return metricsRegistered.remove(metricGroupId) != null;
    }

    public boolean remove(final MetricGroupId metricGroupId) {
        return metricsRegistered.remove(metricGroupId.id()) != null;
    }

    @Deprecated
    public Metrics getMetric(String metricGroupId) {
        return metricsRegistered.get(metricGroupId);
    }

    public Metrics getMetric(final MetricGroupId metricGroupId) {
        return metricsRegistered.get(metricGroupId.id());
    }
}
