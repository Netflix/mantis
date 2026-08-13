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

/**
 * Builds the {@link ApiRequestRateLimiter} backing one route's budget, so that a deployment can swap the
 * admission-control <em>mechanism</em> without owning the route map.
 *
 * <p>The split is deliberate. {@link ApiRequestRateLimiterFactory} decides which routes get a budget, what
 * each is called, and that each is built exactly once; this decides only how a bucket behaves once asked
 * for. A deployment wanting per-caller isolation supplies a provider and inherits every route the factory
 * already throttles, and a route added to the factory later is covered without that deployment changing
 * anything. Were this an interface over the factory instead, each new throttled route would break every
 * implementation of it, and the route-to-budget mapping the factory documents at length would have to be
 * re-decided downstream.
 *
 * <p>Implementations are constructed once per route at startup, not per request, so they may do work a
 * request path could not afford. What they return is on the request path, however, and must honour
 * {@link ApiRequestRateLimiter}'s contract: cheap, non-blocking, never throwing.
 *
 * <p>Providers are supplied by dependency injection — {@code MasterMain} takes one and passes it down —
 * so an implementation is free to close over whatever services it needs. {@code propertiesLoader} is
 * handed to {@link #create} anyway so a provider stays constructible without a container, which the
 * {@code MasterMain.main()} bootstrap and the tests both rely on.
 */
@FunctionalInterface
public interface ApiRequestRateLimiterProvider {

    /**
     * The node-wide single-bucket mechanism: what a deployment gets unless it supplies its own. Every
     * caller draws from one bucket per route, which caps total load but cannot isolate a well-behaved
     * caller from a noisy one — see {@link GlobalApiRequestRateLimiter}.
     */
    ApiRequestRateLimiterProvider GLOBAL =
        (route, permitsPerSecondDp, propertiesLoader) ->
            new GlobalApiRequestRateLimiter(route, permitsPerSecondDp);

    /**
     * @param route names what the returned limiter guards, and is the label its sheds are reported under.
     *     Unique per budget; the factory guarantees it asks at most once per route.
     * @param permitsPerSecondDp the route's configured ceiling, re-readable at runtime. An implementation
     *     that keys per caller should treat this as the ceiling it enforces <em>underneath</em> rather
     *     than as a per-caller allowance, so that the meaning of the operator-facing config key does not
     *     change with the mechanism.
     * @param propertiesLoader for resolving whatever further keys the mechanism needs, so that its own
     *     knobs can be dynamic in the same way the rate above is.
     * @return the limiter for this route; never null
     */
    ApiRequestRateLimiter create(
        String route, LongDynamicProperty permitsPerSecondDp, MantisPropertiesLoader propertiesLoader);
}
