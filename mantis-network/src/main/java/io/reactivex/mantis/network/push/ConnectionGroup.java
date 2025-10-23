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

import com.mantisrx.common.utils.MantisMetricStringConstants;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func0;


public class ConnectionGroup<T> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionGroup.class);
    private String groupId;
    private Map<String, AsyncConnection<T>> connections;
    private Metrics metrics;
    private MetricGroupId metricsGroup;
    private Counter successfulWrites;
    private Counter numSlotSwaps;
    private Counter failedWrites;
    private final Optional<ProactiveRouter<T>> router;

    public ConnectionGroup(String groupId, Optional<ProactiveRouter<T>> router) {
        this.groupId = groupId;
        this.connections = new HashMap<>();
        this.router = router;

        final String grpId = Optional.ofNullable(groupId).orElse("none");
        final BasicTag groupIdTag = new BasicTag(MantisMetricStringConstants.GROUP_ID_TAG, grpId);
        this.metricsGroup = new MetricGroupId("ConnectionGroup", groupIdTag);

        Gauge activeConnections
                = new GaugeCallback(metricsGroup, "activeConnections", new Func0<Double>() {
            @Override
            public Double call() {
                synchronized (this) {
                    return (double) connections.size();
                }
            }
        });
        this.metrics = new Metrics.Builder()
                .id(metricsGroup)
                .addCounter("numSlotSwaps")
                .addCounter("numSuccessfulWrites")
                .addCounter("numFailedWrites")
                .addGauge(activeConnections)
                .build();

        this.successfulWrites = metrics.getCounter("numSuccessfulWrites");
        this.failedWrites = metrics.getCounter("numFailedWrites");
        this.numSlotSwaps = metrics.getCounter("numSlotSwaps");
    }

    public synchronized void incrementSuccessfulWrites(int count) {
        successfulWrites.increment(count);
    }

    public synchronized void incrementFailedWrites(int count) {
        failedWrites.increment(count);
    }

    public synchronized void removeConnection(AsyncConnection<T> connection) {
        AsyncConnection<T> existingConn = connections.get(connection.getSlotId());
        logger.info("Attempt to remove connection: " + connection + " existing connection entry " + existingConn);

        if (existingConn != null && existingConn.getId() == connection.getId()) {
            connections.remove(connection.getSlotId());
        } else {
            logger.warn("Attempt to remove connection ignored. Either there is no such connection or a"
                    + " a new connection has already been swapped in the place of the old connection");

        }
        this.router.ifPresent(router -> router.removeConnection(connection));
    }

    public synchronized void addConnection(AsyncConnection<T> connection) {
        // check if connection previously existed with slotId
        String slotId = connection.getSlotId();
        AsyncConnection<T> previousConnection = connections.get(slotId);
        // add new connection
        connections.put(slotId, connection);
        // close previous
        if (previousConnection != null) {
            logger.info("Swapping connection: " + previousConnection + " with new connection: " + connection);
            previousConnection.close();
            numSlotSwaps.increment();
        }
        this.router.ifPresent(router -> router.addConnection(connection));
    }

    public synchronized boolean isEmpty() {
        return connections.isEmpty();
    }

    public synchronized Set<AsyncConnection<T>> getConnections() {
        Set<AsyncConnection<T>> copy = new HashSet<>();
        copy.addAll(connections.values());
        return copy;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public MetricGroupId getMetricsGroup() {
        return metricsGroup;
    }

    @Override
    public String toString() {
        return "ConnectionGroup [groupId=" + groupId + ", connections="
                + connections + "]";
    }

    public void route(List<T> chunks, Router<T> fallbackRouter) {
        this.router.ifPresentOrElse(
            router -> router.route(chunks),
            () -> fallbackRouter.route(this.getConnections(), chunks)
        );
    }
}
