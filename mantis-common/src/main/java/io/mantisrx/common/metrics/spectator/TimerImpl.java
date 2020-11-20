/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.common.metrics.spectator;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Measurement;
import com.netflix.spectator.api.Registry;
import io.mantisrx.common.metrics.Timer;


public class TimerImpl implements Timer {

    private final MetricId id;
    private final com.netflix.spectator.api.Timer spectatorTimer;

    public TimerImpl(final MetricId id,
                     final Registry registry) {
        this.id = id;
        this.spectatorTimer = registry.timer(id.getSpectatorId(registry));
    }

    @Override
    public void record(long amount, TimeUnit unit) {
        spectatorTimer.record(amount, unit);
    }

    @Override
    public <T> T record(Callable<T> f) throws Exception {
        return spectatorTimer.record(f);
    }

    @Override
    public void record(Runnable f) {
        spectatorTimer.record(f);
    }

    @Override
    public long count() {
        return spectatorTimer.count();
    }

    @Override
    public long totalTime() {
        return spectatorTimer.totalTime();
    }

    @Override
    public MetricId id() {
        return id;
    }

    @Override
    public Iterable<Measurement> measure() {
        return spectatorTimer.measure();
    }

    @Override
    public boolean hasExpired() {
        return spectatorTimer.hasExpired();
    }
}
