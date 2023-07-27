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

package io.mantisrx.common.network;

import com.netflix.spectator.api.CompositeRegistry;
import io.micrometer.core.instrument.Meter;
import io.mantisrx.common.metrics.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class RoundRobinRouter<T> {

    private Map<String, WritableEndpoint<T>> endpoints = new HashMap<>();
    private List<String> idList = new ArrayList<String>();
    private int currentListSize;
    private int count;
    private Metrics metrics;
    private MeterRegistry meterRegistry;
    private Gauge activeConnections;
    private AtomicLong activeConnectionsValue = new AtomicLong(0);

    public RoundRobinRouter() {
        meterRegistry = new SimpleMeterRegistry();
        activeConnections = Gauge.builder("activeConnections", activeConnectionsValue, AtomicLong::get)
            .register(meterRegistry);

        metrics = new Metrics.Builder()
            .name("RoundRobin")
            .addGauge("activeConnections")
            .build();
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public Meter getMeter() {
        return meterRegistry.getMeters().iterator().next();
    }

    public synchronized boolean add(WritableEndpoint<T> endpoint) {
        String id = Endpoint.uniqueHost(endpoint.getHost(),
                endpoint.getPort(), endpoint.getSlotId());
        boolean added = false;
        if (!endpoints.containsKey(id)) {
            endpoints.put(id, endpoint);
            idList.add(id);
            currentListSize++;
            added = true;
        }
        activeConnectionsValue.set(endpoints.size());
        return added;
    }

    public synchronized boolean remove(WritableEndpoint<T> endpoint) {
        String id = Endpoint.uniqueHost(endpoint.getHost(),
                endpoint.getPort(), endpoint.getSlotId());
        boolean removed = false;
        if (endpoints.containsKey(id)) {
            endpoints.remove(id);
            idList.remove(id);
            currentListSize--;
            removed = true;
        }
        activeConnectionsValue.set(endpoints.size());
        return removed;
    }

    public synchronized WritableEndpoint<T> nextSlot() {
        return endpoints.get(idList.get((count++ & Integer.MAX_VALUE) % currentListSize));
    }

    // TODO should completeAll and errorAll de-register?
    public synchronized void completeAllEndpoints() {
        for (WritableEndpoint<T> endpoint : endpoints.values()) {
            endpoint.complete();
        }
    }

    public synchronized boolean isEmpty() {
        return endpoints.isEmpty();
    }

    public synchronized void errorAllEndpoints(Throwable e) {
        for (WritableEndpoint<T> endpoint : endpoints.values()) {
            endpoint.error(e);
        }
    }
}
