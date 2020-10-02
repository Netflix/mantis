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

package io.mantisrx.master.akka;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;


/**
 * A holder class for metrics associated with an Actor.
 */
public final class ActorSystemMetrics {
    private final Counter actorKilledCount;
    private final Counter actorInitExceptionCount;
    private final Counter actorDeathPactExcCount;
    private final Counter actorResumeCount;

    private static final ActorSystemMetrics INSTANCE = new ActorSystemMetrics();

    private ActorSystemMetrics() {
        Metrics m = new Metrics.Builder()
            .id("ActorSystemMetrics")
            .addCounter("actorKilledCount")
            .addCounter("actorInitExceptionCount")
            .addCounter("actorDeathPactExcCount")
            .addCounter("actorResumeCount")
            .build();
        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.actorKilledCount = metrics.getCounter("actorKilledCount");
        this.actorInitExceptionCount = metrics.getCounter("actorInitExceptionCount");
        this.actorDeathPactExcCount = metrics.getCounter("actorDeathPactExcCount");
        this.actorResumeCount = metrics.getCounter("actorResumeCount");
    }

    public static ActorSystemMetrics getInstance() {
        return INSTANCE;
    }

    /**
     * Increments Actor kill count.
     */
    public void incrementActorKilledCount() {
        actorKilledCount.increment();
    }

    /**
     * Tracks how many times an actor failed to initialize.
     */
    public void incrementActorInitExceptionCount() {
        actorInitExceptionCount.increment();
    }

    /**
     * Tracks how many times an actor was killed due to a death pack.
     */
    public void incrementActorDeathPactExcCount() {
        actorDeathPactExcCount.increment();
    }

    /**
     * Tracks how many times an actor has been resumed.
     */
    public void incrementActorResumeCount() {
        actorResumeCount.increment();
    }
}
