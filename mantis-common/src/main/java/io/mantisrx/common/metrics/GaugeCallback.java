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

package io.mantisrx.common.metrics;


import java.util.function.Supplier;

import io.mantisrx.common.metrics.spectator.MetricId;

public class GaugeCallback implements Gauge {

    public static final String LEGACY_GAUGE_CALLBACK_METRICGROUP = "legacyGaugeCallbackMetricGroup";

    private String event;
    private Supplier<Long> valueCallback;
    private MetricId metricId;

    /**
     * @deprecated use {@link io.mantisrx.common.metrics.spectator.GaugeCallback} instead
     */
    @Deprecated
    public GaugeCallback(String event, Supplier<Long> valueCallback) {
        this.event = event;
        this.valueCallback = valueCallback;
        this.metricId = new MetricId(LEGACY_GAUGE_CALLBACK_METRICGROUP, event);
    }

    @Override
    public String event() {
        return event;
    }

    @Override
    public long value() {
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

    @Override
    public MetricId id() { 
        return metricId;
    }
}