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

/**
 * Admission control for a set of API endpoints: decides whether an inbound request may proceed or
 * should be shed (typically as a 429).
 *
 * <p>Implementations are called on the request thread before the entity is unmarshalled, so they
 * must be cheap and must never block.
 *
 * <p>An instance owns one bucket, and which endpoints draw from it is decided entirely by where the
 * instance is wired in — the granularity is a wiring decision, not a property of this interface.
 * Endpoints wired to one instance share its permits, so group them by the resource they contend for
 * (an actor mailbox, a store) rather than by convenience; giving an unrelated endpoint a limiter
 * sized for a different workload silently splits one budget between two.
 *
 * @see GlobalApiRequestRateLimiter the node-wide single-bucket implementation
 */
@FunctionalInterface
public interface ApiRequestRateLimiter {

    /**
     * An {@link ApiRequestRateLimiter} that admits everything. Used for tests and for routes whose
     * throttle is switched off, so the route layer has no disabled case to special-case.
     */
    ApiRequestRateLimiter UNLIMITED = request -> true;

    /**
     * @param request the inbound request. Passed so that implementations can key on caller identity
     *     (headers, remote address, ...) without every call site having to extract it first; the
     *     node-wide implementation ignores it.
     * @return true if a permit was available and the request may proceed, false if the caller
     *     should be throttled. Never blocks.
     */
    boolean tryAcquire(HttpRequest request);
}
