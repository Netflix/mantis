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

import java.util.List;
import java.util.Set;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import rx.functions.Func1;


public abstract class Router<T> {

    protected Func1<T, byte[]> encoder;
    protected Counter numEventsRouted;
    protected Counter numEventsProcessed;
    private Metrics metrics;

    public Router(String name, Func1<T, byte[]> encoder) {
        this.encoder = encoder;
        metrics = new Metrics.Builder()
                .name("Router_" + name)
                .addCounter("numEventsRouted")
                .addCounter("numEventsProcessed")
                .build();
        numEventsRouted = metrics.getCounter("numEventsRouted");
        numEventsProcessed = metrics.getCounter("numEventsProcessed");
    }

    public abstract void route(Set<AsyncConnection<T>> connections, List<T> chunks);

    public Metrics getMetrics() {
        return metrics;
    }
}
