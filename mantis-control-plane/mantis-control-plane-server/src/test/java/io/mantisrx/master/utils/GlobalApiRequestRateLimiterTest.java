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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import akka.http.javadsl.model.HttpRequest;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class GlobalApiRequestRateLimiterTest {

    private static final HttpRequest REQUEST = HttpRequest.POST("/api/v1/jobs");
    private static final String PROPERTY = "test.permitsPerSecond";

    /** A loader whose value an operator can change mid-test, standing in for a config push. */
    private static class MutableLoader implements MantisPropertiesLoader {
        private final Map<String, String> overrides = new HashMap<>();

        void set(String key, String value) {
            overrides.put(key, value);
        }

        @Override
        public String getStringValue(String name, String defaultVal) {
            return overrides.getOrDefault(name, defaultVal);
        }
    }

    /** Lets a test jump past the dynamic property's refresh interval without sleeping through it. */
    private static class MutableClock extends Clock {
        private Instant now = Instant.parse("2024-01-01T00:00:00Z");

        void advance(Duration amount) {
            now = now.plus(amount);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private static LongDynamicProperty property(
        MantisPropertiesLoader loader, long defaultValue, Clock clock) {
        return new LongDynamicProperty(loader, PROPERTY, defaultValue, clock);
    }

    /**
     * Guava's bucket starts empty and refills one permit per stable interval, so it does not hand out a
     * second's worth of permits up front — at one permit per second the very next request has nothing to
     * draw on. Worth knowing when picking a rate: a burst arriving back-to-back is shed even when it is
     * far smaller than the per-second limit, so a caller that paces itself gets the full rate while one
     * that fires everything at once does not. (A bucket left idle for a second does accumulate up to a
     * second of permits, which is where burst allowance comes from.)
     *
     * <p>The rate is 1 rather than something production-sized on purpose: it puts the next permit a full
     * second away, which is the only spacing this can assert without racing a JIT pause.
     */
    @Test
    public void requestsBeyondTheRateAreShed() {
        ApiRequestRateLimiter limiter = new GlobalApiRequestRateLimiter(
            "shed", property(new MutableLoader(), 1L, Clock.systemUTC()));

        assertTrue(limiter.tryAcquire(REQUEST));
        assertFalse(limiter.tryAcquire(REQUEST));
    }

    /**
     * The point of sizing the bucket with a {@link LongDynamicProperty}: an operator who finds the limit
     * wrong mid-incident can re-tune it by pushing config, with no deploy and no restart. The property
     * only re-reads itself once per refresh interval, hence the clock jump.
     */
    @Test
    public void rateFollowsTheDynamicProperty() {
        MutableLoader loader = new MutableLoader();
        MutableClock clock = new MutableClock();
        GlobalApiRequestRateLimiter limiter =
            new GlobalApiRequestRateLimiter("retuned", property(loader, 10_000L, clock));

        assertEquals(10_000d, limiter.getPermitsPerSecond(), 0d);

        loader.set(PROPERTY, "25");
        clock.advance(Duration.ofSeconds(31));
        // The re-read is lazy, on the request path: nothing changes until a request arrives.
        limiter.tryAcquire(REQUEST);

        assertEquals(25d, limiter.getPermitsPerSecond(), 0d);
    }

    /**
     * A stale read of the rate must not be re-applied on every subsequent request either — the property
     * is only re-read once per refresh interval, so between refreshes the rate has to hold steady.
     */
    @Test
    public void rateHoldsSteadyBetweenRefreshes() {
        MutableLoader loader = new MutableLoader();
        MutableClock clock = new MutableClock();
        GlobalApiRequestRateLimiter limiter =
            new GlobalApiRequestRateLimiter("steady", property(loader, 500L, clock));

        loader.set(PROPERTY, "25");
        clock.advance(Duration.ofSeconds(5));
        limiter.tryAcquire(REQUEST);

        assertEquals(500d, limiter.getPermitsPerSecond(), 0d);
    }

    /**
     * Two limiters registering under the same {@code route} tag must not fail. The metrics registry
     * de-duplicates by group id, so this is a no-op there — but if it ever threw instead, it would throw
     * during startup wiring and take the master down rather than degrade a metric.
     */
    @Test
    public void twoLimitersMayShareARouteLabel() {
        ApiRequestRateLimiter first = new GlobalApiRequestRateLimiter(
            "duplicate", property(new MutableLoader(), 1L, Clock.systemUTC()));
        ApiRequestRateLimiter second = new GlobalApiRequestRateLimiter(
            "duplicate", property(new MutableLoader(), 1L, Clock.systemUTC()));

        // Each still owns its own bucket; only the counter they report into is shared.
        assertTrue(first.tryAcquire(REQUEST));
        assertFalse(first.tryAcquire(REQUEST));
        assertTrue(second.tryAcquire(REQUEST));
    }

    /**
     * The shed path increments a counter looked up at construction time. That path only runs when the
     * system is already under stress, so an unregistered counter would surface as a 500 exactly when the
     * throttle was supposed to be protecting the master.
     */
    @Test
    public void sheddingDoesNotThrow() {
        ApiRequestRateLimiter limiter = new GlobalApiRequestRateLimiter(
            "counted", property(new MutableLoader(), 1L, Clock.systemUTC()));

        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire(REQUEST);
        }
    }
}
