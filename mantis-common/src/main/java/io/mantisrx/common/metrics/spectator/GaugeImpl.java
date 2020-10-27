/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.common.metrics.spectator;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.common.metrics.Gauge;


public class GaugeImpl implements Gauge {

    private final MetricId metricId;
    private String event;
    private AtomicDouble value;

    public GaugeImpl(final MetricId metricId,
                     final Registry registry) {
        this.metricId = metricId;
        final Id spectatorId = metricId.getSpectatorId(registry);
        this.value = PolledMeter.using(registry)
                .withId(spectatorId)
                .monitorValue(new AtomicDouble());
        this.event = spectatorId.toString();
    }

    @Override
    public String event() {
        return event;
    }

    @Override
    public MetricId id() {
        return metricId;
    }

    @Override
    public long value() {
        return this.value.longValue();
    }

    @Override
    public void set(double value) {
        this.value.set(value);
    }

    @Override
    public void increment() {
        value.addAndGet(1.0);

    }

    @Override
    public void increment(double delta) {
        value.addAndGet(delta);
    }

    @Override
    public void decrement() {
        value.getAndAdd(-1.0);

    }

    @Override
    public void decrement(double delta) {
        value.getAndAdd((-1) * delta);
    }

    @Override
    public double doubleValue() {
        return this.value.get();
    }

    @Override
    public void set(long x) {
        this.value.set(x);
    }

    @Override
    public void increment(long x) {
        value.getAndAdd(x);
    }

    @Override
    public void decrement(long x) {
        value.getAndAdd(-1.0 * x);
    }
}
