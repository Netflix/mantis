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

import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.server.core.utils.ConfigUtils;
import io.mantisrx.server.master.config.MasterConfiguration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Hands out the rate limiter guarding a route, one accessor per throttled route, building it on first ask
 * and returning that same instance thereafter.
 *
 * <p>A route is one budget. Building it twice would produce two independent limiters — each admitting the
 * configured rate, so the effective limit is doubled — that both register their shed counter under the
 * same {@code route} tag. The metrics registry de-duplicates by group id, so the two would report as one
 * series and the split would be invisible in exactly the state where the numbers matter. Caching here is
 * what makes that unrepresentable, so the accessors are the only supported way to reach a limiter.
 *
 * <p>Each accessor holds the entire binding for its route: the flag that switches the throttle on, the
 * config method whose {@code @Config} key sizes it, and the label its sheds are tagged with. Throttling
 * another route means adding an accessor here — deliberately more friction than a config key, because a
 * new limiter is a new budget somebody has to size and watch.
 *
 * <p>A route whose flag is off yields {@link ApiRequestRateLimiter#UNLIMITED}: no dynamic property is
 * resolved and no limiter is constructed, so the feature stays inert until an operator opts in, and the
 * route layer has no disabled case to special-case.
 *
 * <p>What kind of limiter backs a budget is not decided here — that is the
 * {@link ApiRequestRateLimiterProvider}'s job, so that a deployment can swap the mechanism (per-caller
 * buckets, say) while this class keeps deciding which routes have budgets at all. Everything above still
 * holds whatever the provider returns: the flag, the caching, and the one-limiter-per-route guarantee are
 * applied to it, not delegated.
 */
@Slf4j
public class ApiRequestRateLimiterFactory {

    private final MasterConfiguration config;
    private final MantisPropertiesLoader propertiesLoader;
    private final ApiRequestRateLimiterProvider provider;
    private final ConcurrentMap<String, ApiRequestRateLimiter> limiters = new ConcurrentHashMap<>();

    /**
     * Builds limiters with the node-wide single-bucket mechanism. Equivalent to passing
     * {@link ApiRequestRateLimiterProvider#GLOBAL}.
     */
    public ApiRequestRateLimiterFactory(
        MasterConfiguration config, MantisPropertiesLoader propertiesLoader) {
        this(config, propertiesLoader, ApiRequestRateLimiterProvider.GLOBAL);
    }

    public ApiRequestRateLimiterFactory(
        MasterConfiguration config,
        MantisPropertiesLoader propertiesLoader,
        ApiRequestRateLimiterProvider provider) {
        this.config = config;
        this.propertiesLoader = propertiesLoader;
        this.provider = provider;
    }

    /**
     * @return the limiter shared by the throttled endpoints of the v1 jobs route, or
     *     {@link ApiRequestRateLimiter#UNLIMITED} while the throttle is switched off
     */
    public ApiRequestRateLimiter v1Jobs() {
        if (!config.isApiV1JobsThrottleEnabled()) {
            log.info("throttle for route v1Jobs is disabled");
            return ApiRequestRateLimiter.UNLIMITED;
        }
        return limiters.computeIfAbsent("v1Jobs", route -> create(
            route,
            // The same accessor twice over: by name so its @Config key can be resolved for overrides, and
            // called for the value that sizes the limiter until the first override arrives.
            "getApiV1JobsThrottlePermitsPerSecond",
            config.getApiV1JobsThrottlePermitsPerSecond()));
    }

    private ApiRequestRateLimiter create(
        String route, String permitsPerSecondConfigMethod, long defaultPermitsPerSecond) {
        final LongDynamicProperty permitsPerSecondDp = ConfigUtils.getDynamicPropertyLong(
            permitsPerSecondConfigMethod,
            MasterConfiguration.class,
            defaultPermitsPerSecond,
            propertiesLoader);
        return provider.create(route, permitsPerSecondDp, propertiesLoader);
    }
}
