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

import java.util.function.Supplier;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.mantisrx.common.metrics.Gauge;

public class GaugeCallback implements Gauge {

    private final MetricId metricId;
    private final Id spectatorId;
    private Supplier<Double> valueCallback;

    public GaugeCallback(final MetricId metricId,
                         final Supplier<Double> valueCallback,
                         final Registry registry) {
        this.metricId = metricId;
        this.spectatorId = metricId.getSpectatorId(registry);
        PolledMeter.using(registry).withId(spectatorId).monitorValue(this, GaugeCallback::doubleValue);
        this.valueCallback = valueCallback;
    }

    public GaugeCallback(final String metricGroup,
                         final String metricName,
                         final Supplier<Double> valueCallback,
                         final Registry registry,
                         final Iterable<Tag> tags) {
        this(new MetricId(metricGroup, metricName, tags), valueCallback, registry);
    }

    public GaugeCallback(final String metricGroup,
                         final String metricName,
                         final Supplier<Double> valueCallback,
                         final Registry registry,
                         final Tag... tags) {
        this(new MetricId(metricGroup, metricName, tags), valueCallback, registry);
    }

    public GaugeCallback(final String metricGroup,
                         final String metricName,
                         final Supplier<Double> valueCallback,
                         final Tag... tags) {
        this(new MetricId(metricGroup, metricName, tags), valueCallback, SpectatorRegistryFactory.getRegistry());
    }

    public GaugeCallback(final MetricGroupId metricGroup,
                         final String metricName,
                         final Supplier<Double> valueCallback) {
        this(new MetricId(metricGroup.name(), metricName, metricGroup.tags()), valueCallback, SpectatorRegistryFactory.getRegistry());
    }

    @Override
    public MetricId id() {
        return metricId;
    }

    @Override
    public String event() {
        return spectatorId.toString();
    }

    @Override
    public long value() {
        return valueCallback.get().longValue();
    }

    @Override
    public double doubleValue() {
        return valueCallback.get();
    }

    @Override
    public void increment() { }

    @Override
    public void decrement() { }

    @Override
    public void set(double value) { }

    @Override
    public void increment(double value) { }

    @Override
    public void decrement(double value) { }

    @Override
    public void set(long value) { }

    @Override
    public void increment(long value) { }

    @Override
    public void decrement(long value) { }
}
