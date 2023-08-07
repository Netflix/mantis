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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class CaffeineMetrics implements StatsCounter {
    private final Counter hitCount;
    private final Counter missCount;
    private final Counter loadSuccessCount;
    private final Counter loadFailureCount;
    // TODO make totalLoadTime a Timer
    private final Timer totalLoadTime;
    private final Counter evictionCount;
    private final Counter evictionWeight;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs an instance for use by a single cache.
     */
    public CaffeineMetrics(String metricGroup, MeterRegistry meterRegistry) {
        requireNonNull(metricGroup);
        this.meterRegistry = meterRegistry;
        hitCount = addCounter(metricGroup + "_hits");
        missCount = addCounter(metricGroup + "_misses");
        loadSuccessCount = addCounter(metricGroup + "_loadsSuccess");
        loadFailureCount = addCounter(metricGroup + "_loadsFailure");
        evictionCount = addCounter(metricGroup + "_evictions");
        evictionWeight = addCounter(metricGroup + "_evictionsWeight");
        totalLoadTime = meterRegistry.timer(metricGroup + "_loadTimeMillis");
    }

    private Counter addCounter(String name) {
        Counter counter = Counter.builder("CaffeineMetrics_" + name)
            .register(meterRegistry);
        return counter;
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
        totalLoadTime.record(Duration.ofDays(TimeUnit.MILLISECONDS.convert(loadTime, TimeUnit.NANOSECONDS)));
    }

    @Override
    public void recordLoadFailure(long loadTime) {
        loadFailureCount.increment();
        totalLoadTime.record(Duration.ofDays(TimeUnit.MILLISECONDS.convert(loadTime, TimeUnit.NANOSECONDS)));
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
            (long)hitCount.count(),
            (long) missCount.count(),
            (long) loadSuccessCount.count(),
            (long) loadFailureCount.count(),
            totalLoadTime.count(),
            (long)evictionCount.count(),
            (long)evictionWeight.count());
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }
}
