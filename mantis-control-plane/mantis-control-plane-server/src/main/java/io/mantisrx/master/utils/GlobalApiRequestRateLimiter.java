/*
 * Copyright 2024 Netflix, Inc.
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

import akka.http.javadsl.model.HttpRequest;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.shaded.com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

/**
 * One bucket, drawn from by every client alike, sized by a {@link LongDynamicProperty} so it can be
 * re-tuned at runtime without a deploy. "Global" refers to the set of clients, not the set of
 * endpoints: this limiter is unkeyed — {@link #tryAcquire(HttpRequest)} discards the request — so it
 * cannot tell one caller from another. Which endpoints it covers is decided entirely by where the
 * instance is wired in, not by anything here.
 *
 * <p>This is the simplest useful form of admission control: it caps total load on whatever it guards
 * and so stops one client from storming it. What it cannot do is isolate a well-behaved caller from a
 * noisy one — once the bucket is drained, everyone is shed alike. Keying the limit per caller needs a
 * different {@link ApiRequestRateLimiter} implementation, one holding a bucket per client.
 *
 * <p>The bucket is per node, but since every API route sits behind
 * {@code LeaderRedirectionFilter.redirectIfNotLeader} only the leader ever reaches a limiter — standbys
 * 302 the caller away first. So the configured rate is in practice the cluster-wide ceiling, not a
 * per-node share of one.
 *
 * <p>Sheds are counted by this class rather than by the route layer, so that the {@code route} tag
 * always names the bucket that actually shed the request instead of whatever the call site claimed.
 */
@Slf4j
public class GlobalApiRequestRateLimiter implements ApiRequestRateLimiter {

    private static final String METRIC_GROUP = "ApiRequestRateLimiter";
    private static final String THROTTLED_REQUEST_COUNT = "throttledRequestCount";
    private static final String ROUTE_TAG = "route";

    private final String route;
    private final LongDynamicProperty permitsPerSecondDp;
    private final RateLimiter rateLimiter;
    private final Counter throttledRequestCount;

    /**
     * Guards {@link #syncRate()} so concurrent callers don't race on {@code RateLimiter.setRate}.
     * Reads are unsynchronized: a stale read only costs one redundant {@code setRate} to the same
     * value.
     */
    private volatile long currentRate;

    /**
     * @param route names what this bucket guards, and is the {@code route} tag its sheds are counted
     *     under. Two limiters built with the same label report into the same time series, so give each
     *     bucket its own label.
     */
    public GlobalApiRequestRateLimiter(String route, LongDynamicProperty permitsPerSecondDp) {
        this.route = route;
        this.permitsPerSecondDp = permitsPerSecondDp;
        this.currentRate = permitsPerSecondDp.getValue();
        this.rateLimiter = RateLimiter.create(this.currentRate);
        final Metrics metrics = MetricsRegistry.getInstance().registerAndGet(
            new Metrics.Builder()
                .id(METRIC_GROUP, new BasicTag(ROUTE_TAG, route))
                .addCounter(THROTTLED_REQUEST_COUNT)
                .build());
        this.throttledRequestCount = metrics.getCounter(THROTTLED_REQUEST_COUNT);
        log.info("Created rate limiter for route {} from {} at {} permits/sec",
            route, permitsPerSecondDp, this.currentRate);
    }

    /**
     * @return true if a permit was available and consumed, false if the caller should be throttled.
     *     Never blocks. {@code request} is deliberately unused — every caller draws from the same
     *     bucket regardless of who they are or what they asked for.
     */
    @Override
    public boolean tryAcquire(HttpRequest request) {
        syncRate();
        if (rateLimiter.tryAcquire()) {
            return true;
        }
        throttledRequestCount.increment();
        return false;
    }

    /**
     * {@link LongDynamicProperty#getValue()} already self-refreshes on its own interval
     * ({@code mantis.config.dynamic.refreshSecs}, default 30s), so the rate is re-read lazily on the
     * calling thread rather than from a dedicated scheduler thread. This runs on every request, so it
     * relies on {@code getValue()} being safe to call concurrently and on its steady-state path not
     * touching the underlying properties loader — see {@code DynamicProperty}.
     */
    private void syncRate() {
        long newRate = permitsPerSecondDp.getValue();
        if (newRate == currentRate) {
            return;
        }
        synchronized (this) {
            if (newRate == currentRate) {
                return;
            }
            log.info("Setting the rate limiter rate for route {} to {} (was {})", route, newRate, currentRate);
            rateLimiter.setRate(newRate);
            currentRate = newRate;
        }
    }

    /**
     * Visible for testing: the rate the underlying bucket is actually set to, read from it rather than
     * from this class's bookkeeping copy.
     */
    double getPermitsPerSecond() {
        return rateLimiter.getRate();
    }

    @Override
    public String toString() {
        return "GlobalApiRequestRateLimiter(route=" + route + ", permitsPerSecond=" + currentRate + ")";
    }
}
