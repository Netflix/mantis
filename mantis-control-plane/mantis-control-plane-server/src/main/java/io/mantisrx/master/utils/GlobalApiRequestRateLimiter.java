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
 * <p>Nothing here is counted. Sheds are counted by the route layer, which is the only place they can be
 * counted once the mechanism is a deployment's to choose — see {@link ApiRequestRateLimiterProvider}.
 *
 * <p>{@link ApiRequestRateLimiter#getRetryAfter(HttpRequest)} is left at the inherited one second, which is
 * the honest answer for every rate this limiter can be given: the bucket refills a permit every
 * {@code 1/rate} seconds and the rate is a whole number of permits per second, so the next permit is always
 * within a second — and {@code Retry-After} cannot express less than that anyway. Its request parameter is
 * of no use here for the same reason {@link #tryAcquire(HttpRequest)} discards one. Two caveats. The hint
 * does not say the permit will be <em>this</em> caller's: the bucket is unkeyed, so every client shed in the
 * same second gets the same number and they contend again when it elapses, which is why the route layer's
 * message asks for backoff and jitter on top. And if the rate ever becomes fractional, one second turns into
 * an underestimate and this needs an override.
 */
@Slf4j
public class GlobalApiRequestRateLimiter implements ApiRequestRateLimiter {

    private final String route;
    private final LongDynamicProperty permitsPerSecondDp;
    private final RateLimiter rateLimiter;

    /**
     * Guards {@link #syncRate()} so concurrent callers don't race on {@code RateLimiter.setRate}.
     * Reads are unsynchronized: a stale read only costs one redundant {@code setRate} to the same
     * value.
     */
    private volatile long currentRate;

    /**
     * @param route names what this bucket guards, for the startup log line and the rate-change one. It is
     *     not a metric tag — sheds are counted at the route layer under the endpoint that shed them, which
     *     is finer than this label: one bucket can guard several endpoints.
     */
    public GlobalApiRequestRateLimiter(String route, LongDynamicProperty permitsPerSecondDp) {
        this.route = route;
        this.permitsPerSecondDp = permitsPerSecondDp;
        this.currentRate = permitsPerSecondDp.getValue();
        this.rateLimiter = RateLimiter.create(this.currentRate);
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
        return rateLimiter.tryAcquire();
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
