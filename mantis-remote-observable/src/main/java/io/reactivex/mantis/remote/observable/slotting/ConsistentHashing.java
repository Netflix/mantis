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

package io.reactivex.mantis.remote.observable.slotting;

import java.util.LinkedList;
import java.util.List;

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.network.HashFunction;
import io.mantisrx.common.network.HashFunctions;
import io.mantisrx.common.network.ServerSlotManager.SlotAssignmentManager;
import io.mantisrx.common.network.WritableEndpoint;


public class ConsistentHashing<T> extends SlottingStrategy<T> {

    private SlotAssignmentManager<T> manager;
    private List<WritableEndpoint<T>> connections = new LinkedList<>();
    private Gauge activeConnections;
    private Metrics metrics;

    public ConsistentHashing(String ringName, HashFunction function) {
        manager = new SlotAssignmentManager<T>(function, ringName);
        metrics = new Metrics.Builder()
                .name("ConsistentHashing")
                .addGauge("activeConnections")
                .build();

        activeConnections = metrics.getGauge("activeConnections");
    }

    public ConsistentHashing() {
        this("default-ring", HashFunctions.ketama());
    }

    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public synchronized boolean addConnection(WritableEndpoint<T> endpoint) {
        boolean wasEmpty = manager.isEmpty();
        // force adding slot to manager
        boolean isRegistered = manager.forceRegisterServer(endpoint);
        if (wasEmpty && isRegistered) {
            doAfterFirstConnectionAdded.call();
        }
        if (isRegistered) {
            connections.add(endpoint);
            doOnEachConnectionAdded.call();
        }
        activeConnections.set(connections.size());
        return isRegistered;
    }

    @Override
    public synchronized boolean removeConnection(WritableEndpoint<T> endpoint) {
        // remove connection, do not remove slot
        boolean isDeregistered = connections.remove(endpoint);
        if (isDeregistered && connections.isEmpty()) {
            doAfterLastConnectionRemoved.call();
        }
        if (isDeregistered) {
            doOnEachConnectionRemoved.call();
        }
        activeConnections.set(connections.size());
        return isDeregistered;
    }

    @Override
    public void writeOnSlot(byte[] keyBytes, T data) {
        manager.lookup(keyBytes).write(data);
    }

    @Override
    // TODO should completeAll and errorAll de-register?
    public synchronized void completeAllConnections() {
        for (WritableEndpoint<T> endpoint : manager.endpoints()) {
            endpoint.complete();
        }
    }

    @Override
    public synchronized void errorAllConnections(Throwable e) {
        for (WritableEndpoint<T> endpoint : manager.endpoints()) {
            endpoint.error(e);
        }
    }
}
