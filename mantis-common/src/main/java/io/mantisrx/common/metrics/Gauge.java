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

package io.mantisrx.common.metrics;


import io.mantisrx.common.metrics.spectator.MetricId;


public interface Gauge {

    String event();

    MetricId id();

    void set(double value);

    void increment();

    void increment(double value);

    void decrement();

    void decrement(double value);

    void set(long value);

    void increment(long value);

    void decrement(long value);

    long value();

    default double doubleValue() {
        return value() * 1.0;
    }
}
