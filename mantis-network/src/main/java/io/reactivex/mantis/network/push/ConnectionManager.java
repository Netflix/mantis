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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action0;


public class ConnectionManager<T> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private Map<String, ConnectionGroup<T>> managedConnections
            = new LinkedHashMap<String, ConnectionGroup<T>>();
    private MetricsRegistry metricsRegistry;
    private AtomicReference<Gauge> activeConnectionsRef = new AtomicReference<>(null);
    private Action0 doOnFirstConnection;
    private Action0 doOnZeroConnections;
    private Lock connectionState = new ReentrantLock();
    private AtomicBoolean subscribed = new AtomicBoolean();

    public ConnectionManager(MetricsRegistry metricsRegistry,
                             Action0 doOnFirstConnection, Action0 doOnZeroConnections) {
        this.doOnFirstConnection = doOnFirstConnection;
        this.doOnZeroConnections = doOnZeroConnections;
        this.metricsRegistry = metricsRegistry;
    }

    private int activeConnections() {
        connectionState.lock();
        try {
            int connections = 0;
            for (ConnectionGroup<T> group : managedConnections.values()) {
                connections += group.getConnections().size();
            }
            return connections;
        } finally {
            connectionState.unlock();
        }
    }

    protected Gauge getActiveConnections(final MetricGroupId metricsGroup) {
        activeConnectionsRef.compareAndSet(null,
                new GaugeCallback(metricsGroup, "activeConnections", () -> (double) activeConnections()));
        return activeConnectionsRef.get();
    }

    public Set<AsyncConnection<T>> connections(String groupId) {
        connectionState.lock();
        try {
            Set<AsyncConnection<T>> connections = new HashSet<>();
            ConnectionGroup<T> group = managedConnections.get(groupId);
            if (group != null) {
                connections.addAll(group.getConnections());
            }
            return connections;
        } finally {
            connectionState.unlock();
        }
    }

    protected void successfulWrites(AsyncConnection<T> connection, Integer numWrites) {
        connectionState.lock();
        try {
            String groupId = connection.getGroupId();
            ConnectionGroup<T> current = managedConnections.get(groupId);
            if (current != null) {
                current.incrementSuccessfulWrites(numWrites);
            }
        } finally {
            connectionState.unlock();
        }
    }

    protected void failedWrites(AsyncConnection<T> connection, Integer numWrites) {
        connectionState.lock();
        try {
            String groupId = connection.getGroupId();
            ConnectionGroup<T> current = managedConnections.get(groupId);
            if (current != null) {
                current.incrementFailedWrites(numWrites);
            }
        } finally {
            connectionState.unlock();
        }
    }

    protected void add(AsyncConnection<T> connection) {
        connectionState.lock();
        try {
            String groupId = connection.getGroupId();
            ConnectionGroup<T> current = managedConnections.get(groupId);
            if (current == null) {
                ConnectionGroup<T> newGroup = new ConnectionGroup<T>(groupId);
                current = managedConnections.putIfAbsent(groupId, newGroup);
                if (current == null) {
                    current = newGroup;
                    metricsRegistry.registerAndGet(current.getMetrics());
                }
            }
            current.addConnection(connection);
            logger.info("Connection added to group: " + groupId + ", connection: " + connection + ", group: " + current);
        } finally {
            connectionState.unlock();
        }

        if (subscribed.compareAndSet(false, true)) {
            logger.info("Calling callback when active connections is one");
            doOnFirstConnection.call();
            logger.info("Completed callback when active connections is one");
        }
    }

    protected void remove(AsyncConnection<T> connection) {
        connectionState.lock();
        try {
            String groupId = connection.getGroupId();
            ConnectionGroup<T> current = managedConnections.get(groupId);
            if (current != null) {
                current.removeConnection(connection);
                logger.info("Connection removed from group: " + groupId + ", connection: " + connection + ", group: " + current);
                if (current.isEmpty()) {
                    logger.info("Removing group: " + groupId + ", zero connections");
                    // deregister metrics
                    metricsRegistry.remove(current.getMetricsGroup());
                    // remove group
                    managedConnections.remove(groupId);
                }
            }
        } finally {
            connectionState.unlock();
        }
        if (activeConnections() == 0 && subscribed.compareAndSet(true, false)) {

            logger.info("Connection Manager Calling callback when active connections is zero");
            doOnZeroConnections.call();
            logger.info("Completed callback when active connections is zero");

        }
    }

    public Set<AsyncConnection<T>> connections() {
        connectionState.lock();
        try {
            Set<AsyncConnection<T>> connections = new HashSet<>();
            for (ConnectionGroup<T> group : managedConnections.values()) {
                connections.addAll(group.getConnections());
            }
            return connections;
        } finally {
            connectionState.unlock();
        }
    }

    public Map<String, ConnectionGroup<T>> groups() {
        connectionState.lock();
        try {
            return new HashMap<String, ConnectionGroup<T>>(managedConnections);
        } finally {
            connectionState.unlock();
        }
    }
}
