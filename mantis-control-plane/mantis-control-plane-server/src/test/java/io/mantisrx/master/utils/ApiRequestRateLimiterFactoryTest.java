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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.http.javadsl.model.HttpRequest;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.server.master.config.MasterConfiguration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class ApiRequestRateLimiterFactoryTest {

    /** The key {@code @Config} puts on the accessor {@link ApiRequestRateLimiterFactory#v1Jobs()} reads. */
    private static final String PERMITS_PROPERTY = "mantis.master.api.v1.jobs.throttle.permitsPerSecond";

    private static final HttpRequest REQUEST = HttpRequest.POST("/api/v1/jobs");

    /** Records what was looked up, so a test can assert a disabled route resolves no property. */
    private static class RecordingLoader implements MantisPropertiesLoader {
        private final Map<String, String> overrides = new HashMap<>();
        private final Set<String> lookups = new HashSet<>();

        @Override
        public String getStringValue(String name, String defaultVal) {
            lookups.add(name);
            return overrides.getOrDefault(name, defaultVal);
        }
    }

    private static MasterConfiguration configWithV1JobsThrottle(boolean enabled, int permitsPerSecond) {
        MasterConfiguration config = mock(MasterConfiguration.class);
        when(config.isApiV1JobsThrottleEnabled()).thenReturn(enabled);
        when(config.getApiV1JobsThrottlePermitsPerSecond()).thenReturn(permitsPerSecond);
        return config;
    }

    /**
     * A route is one budget. Building it twice would hand out two independent limiters — doubling the
     * effective rate — that report their sheds into a single time series, so the split would not even be
     * visible. Every other guarantee here rests on this one: the accessor is the identity of the budget.
     */
    @Test
    public void sameRouteYieldsTheSameLimiter() {
        ApiRequestRateLimiterFactory factory = new ApiRequestRateLimiterFactory(
            configWithV1JobsThrottle(true, 1000), new RecordingLoader());

        assertSame(
            factory.v1Jobs(),
            factory.v1Jobs());
    }

    /**
     * The throttle is opt-in, so a route left switched off must build nothing at all: no limiter, and no
     * dynamic property quietly polling config on the request path.
     */
    @Test
    public void disabledRouteYieldsUnlimitedAndResolvesNoProperty() {
        RecordingLoader loader = new RecordingLoader();
        ApiRequestRateLimiterFactory factory =
            new ApiRequestRateLimiterFactory(configWithV1JobsThrottle(false, 1000), loader);

        assertSame(ApiRequestRateLimiter.UNLIMITED, factory.v1Jobs());
        assertFalse(loader.lookups.contains(PERMITS_PROPERTY));
    }

    /**
     * The static config value sizes the limiter when nothing overrides it. One permit per second means it
     * is empty immediately after the first request, which is what makes the rate observable without
     * waiting on a clock.
     */
    @Test
    public void limiterIsSizedFromTheConfiguredRate() {
        ApiRequestRateLimiterFactory factory = new ApiRequestRateLimiterFactory(
            configWithV1JobsThrottle(true, 1), new RecordingLoader());

        ApiRequestRateLimiter limiter = factory.v1Jobs();

        assertTrue(limiter.tryAcquire(REQUEST));
        assertFalse(limiter.tryAcquire(REQUEST));
    }

    /**
     * The accessor names a config method, and the property key is resolved by reflecting on that method's
     * {@code @Config} annotation. This pins that wiring end to end: an override pushed under the
     * annotated key has to reach the limiter, or re-tuning a limit mid-incident would silently do
     * nothing. Overriding down to 1 permit is what distinguishes it from the configured 1000.
     */
    @Test
    public void overriddenPropertyReachesTheLimiter() {
        RecordingLoader loader = new RecordingLoader();
        loader.overrides.put(PERMITS_PROPERTY, "1");
        ApiRequestRateLimiterFactory factory =
            new ApiRequestRateLimiterFactory(configWithV1JobsThrottle(true, 1000), loader);

        ApiRequestRateLimiter limiter = factory.v1Jobs();

        assertTrue(limiter.tryAcquire(REQUEST));
        assertFalse(limiter.tryAcquire(REQUEST));
    }
}
