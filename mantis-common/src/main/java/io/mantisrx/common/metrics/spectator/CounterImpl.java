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

package io.mantisrx.common.metrics.spectator;

import com.netflix.spectator.api.Registry;
import io.mantisrx.common.metrics.Counter;


public class CounterImpl implements Counter {

    private final MetricId id;
    private final com.netflix.spectator.api.Counter spectatorCounter;

    public CounterImpl(final MetricId id,
                       final Registry registry) {
        this.id = id;
        this.spectatorCounter = registry.counter(id.getSpectatorId(registry));
    }

    @Override
    public void increment() {
        spectatorCounter.increment();
    }

    @Override
    public void increment(long x) {
        if (x < 0)
            throw new IllegalArgumentException("Can't add negative numbers");
        spectatorCounter.increment(x);
    }

    @Override
    public long value() {
        return spectatorCounter.count();
    }

    @Override
    public long rateValue() {
        return -1;
    }

    @Override
    public long rateTimeInMilliseconds() {
        return -1;
    }

    @Override
    public String event() {
        return spectatorCounter.id().toString();
    }

    @Override
    public MetricId id() {
        return id;
    }
}
