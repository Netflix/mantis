/*
 * Copyright 2025 Netflix, Inc.
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

import com.netflix.spectator.api.Tag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import rx.functions.Func1;

import java.util.*;

public class ProactiveRoundRobinRouter<T> implements ProactiveRouter<T> {
    private final List<AsyncConnection<T>> connections = new ArrayList<>();
    private int currentIndex = 0;

    protected Func1<T, byte[]> encoder;
    protected final Counter numEventsRouted;
    protected final Counter numEventsProcessed;
    protected final Counter numConnectionUpdates;
    protected final Metrics metrics;

    public ProactiveRoundRobinRouter(String name, Func1<T, byte[]> encoder) {
        this.encoder = encoder;
        metrics = new Metrics.Builder()
            .id("Router_" + name, Tag.of("router_type", "proactive_round_robin"))
            .addCounter("numEventsRouted")
            .addCounter("numEventsProcessed")
            .addCounter("numConnectionUpdates")
            .build();
        numEventsRouted = metrics.getCounter("numEventsRouted");
        numEventsProcessed = metrics.getCounter("numEventsProcessed");
        numConnectionUpdates = metrics.getCounter("numConnectionUpdates");
    }

    @Override
    public synchronized void addConnection(AsyncConnection<T> connection) {
        // We do not need to shuffle because we are constantly looping through
        numConnectionUpdates.increment();
        connections.add(connection);
    }

    @Override
    public synchronized void removeConnection(AsyncConnection<T> connection) {
        numConnectionUpdates.increment();
        connections.remove(connection);
    }

    @Override
    public synchronized void route(List<T> chunks) {
        if (connections.isEmpty() || chunks == null || chunks.isEmpty()) {
            return;
        }
        numEventsProcessed.increment(chunks.size());
        Map<AsyncConnection<T>, List<byte[]>> writes = new HashMap<>();
        int arrayListSize = chunks.size() / connections.size() + 1; // assume even distribution
        // process chunks
        for (T chunk : chunks) {
            currentIndex = currentIndex % connections.size();
            AsyncConnection<T> connection = connections.get(currentIndex);
            Func1<T, Boolean> predicate = connection.getPredicate();
            if (predicate == null || predicate.call(chunk)) {
                List<byte[]> buffer = writes.computeIfAbsent(connection, k -> new ArrayList<>(arrayListSize));
                buffer.add(encoder.call(chunk));
                currentIndex++;
            }
        }
        for (Map.Entry<AsyncConnection<T>, List<byte[]>> entry : writes.entrySet()) {
            AsyncConnection<T> connection = entry.getKey();
            List<byte[]> toWrite = entry.getValue();
            connection.write(toWrite);
            numEventsRouted.increment(toWrite.size());
        }
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }
}
