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

package io.mantisrx.master.utils;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import java.util.concurrent.TimeUnit;

public final class CaffeineMetrics implements StatsCounter {
    private final Counter hitCount;
    private final Counter missCount;
    private final Counter loadSuccessCount;
    private final Counter loadFailureCount;
    // TODO make totalLoadTime a Timer
    private final Gauge totalLoadTime;
    private final Counter evictionCount;
    private final Counter evictionWeight;

    /**
     * Constructs an instance for use by a single cache.
     */
    public CaffeineMetrics(String metricGroup) {
        requireNonNull(metricGroup);
        Metrics m = new Metrics.Builder()
            .id("CaffeineMetrics_" + metricGroup)
            .addCounter("hits")
            .addCounter("misses")
            .addGauge("loadTimeMillis")
            .addCounter("loadsSuccess")
            .addCounter("loadsFailure")
            .addCounter("evictions")
            .addCounter("evictionsWeight")
            .build();

        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        hitCount = metrics.getCounter("hits");
        missCount = metrics.getCounter("misses");
        totalLoadTime = metrics.getGauge("loadTimeMillis");
        loadSuccessCount = metrics.getCounter("loadsSuccess");
        loadFailureCount = metrics.getCounter("loadsFailure");
        evictionCount = metrics.getCounter("evictions");
        evictionWeight = metrics.getCounter("evictionsWeight");
    }

    @Override
    public void recordHits(int count) {
        hitCount.increment(count);
    }

    @Override
    public void recordMisses(int count) {
        missCount.increment(count);
    }

    @Override
    public void recordLoadSuccess(long loadTime) {
        loadSuccessCount.increment();
        totalLoadTime.set(TimeUnit.MILLISECONDS.convert(loadTime, TimeUnit.NANOSECONDS));
    }

    @Override
    public void recordLoadFailure(long loadTime) {
        loadFailureCount.increment();
        totalLoadTime.set(TimeUnit.MILLISECONDS.convert(loadTime, TimeUnit.NANOSECONDS));
    }

    @Override
    @SuppressWarnings("deprecation")
    public void recordEviction() {
        // This method is scheduled for removal in version 3.0 in favor of recordEviction(weight)
        recordEviction(1);
    }

    @Override
    public void recordEviction(int weight) {
        evictionCount.increment();
        evictionWeight.increment(weight);
    }

    @Override
    public CacheStats snapshot() {
        return new CacheStats(
            hitCount.value(),
            missCount.value(),
            loadSuccessCount.value(),
            loadFailureCount.value(),
            totalLoadTime.value(),
            evictionCount.value(),
            evictionWeight.value());
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }
}
