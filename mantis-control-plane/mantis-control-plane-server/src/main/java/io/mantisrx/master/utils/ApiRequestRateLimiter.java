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
import java.time.Duration;

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

    /**
     * How long a shed caller should wait before trying again, reported to it as the {@code Retry-After}
     * header of the 429. This is a hint, not a reservation: nothing holds a permit for the caller, so a
     * client that waits exactly this long can still be shed again if others drained the bucket first.
     *
     * <p>The default of one second is the smallest value {@code Retry-After} can express in seconds, and
     * suits any bucket refilling at a permit per second or faster. An implementation whose bucket refills
     * more slowly than that should override this — telling a caller to come back in a second when the next
     * permit is a minute away just converts one shed request into sixty.
     *
     * <p>The request is passed so that a keyed implementation can answer per caller: the client that
     * drained its own bucket should be told to wait longer than one shed by a shared ceiling, and the
     * two are indistinguishable without something to key on. It is called only on the shed path, and
     * separately from {@link #tryAcquire}, so a keyed implementation pays a second lookup and may
     * observe its bucket a moment later than the decision did. Both are deliberate: {@code Retry-After}
     * has whole-second resolution, which is coarser than either effect. An implementation with no
     * per-caller answer to give — no bucket for this client, because the shared ceiling shed it —
     * should delegate to this default rather than invent one.
     *
     * @param request the request being shed, to key on
     * @return a positive duration. The route layer clamps it into a range {@code Retry-After} can carry
     *     sensibly, so an implementation need not round: see {@code BaseRoute.throttledResponse}.
     */
    default Duration getRetryAfter(HttpRequest request) {
        return Duration.ofSeconds(1);
    }
}
