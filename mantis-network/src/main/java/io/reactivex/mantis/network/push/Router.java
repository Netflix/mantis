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

package io.reactivex.mantis.network.push;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import rx.functions.Func1;


public abstract class Router<T> {

    protected Func1<T, byte[]> encoder;
    protected Counter numEventsRouted;
    protected Counter numEventsProcessed;
    private MeterRegistry meterRegistry;

    public Router(String name, Func1<T, byte[]> encoder, MeterRegistry meterRegistry) {
        this.encoder = encoder;
        this.meterRegistry = meterRegistry;
        numEventsRouted = meterRegistry.counter("Router_" + name +"_numEventsRouted");
        numEventsProcessed = meterRegistry.counter("Router_" + name +"_numEventsProcessed");
    }

    public abstract void route(Set<AsyncConnection<T>> connections, List<T> chunks);

    public List<Meter> getMetrics() {
        List<Meter> routerMeters = new LinkedList<>();
        routerMeters.add(numEventsRouted);
        routerMeters.add(numEventsProcessed);
        return routerMeters;
    }
}
