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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;


/**
 * A holder class for metrics associated with an Actor.
 */
public final class ActorSystemMetrics {
    private final Counter actorKilledCount;
    private final Counter actorInitExceptionCount;
    private final Counter actorDeathPactExcCount;
    private final Counter actorResumeCount;
    private final MeterRegistry meterRegistry;
    private static ActorSystemMetrics INSTANCE = null;

    public ActorSystemMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.actorKilledCount = meterRegistry.counter("ActorSystemMetrics_actorKilledCount");
        this.actorInitExceptionCount = meterRegistry.counter("ActorSystemMetrics_actorInitExceptionCount");
        this.actorDeathPactExcCount = meterRegistry.counter("ActorSystemMetrics_actorDeathPactExcCount");
        this.actorResumeCount = meterRegistry.counter("ActorSystemMetrics_actorResumeCount");
    }

    public static synchronized ActorSystemMetrics getInstance(MeterRegistry meterRegistry) {
        if(INSTANCE == null) {
            INSTANCE = new ActorSystemMetrics(meterRegistry);
        }
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
