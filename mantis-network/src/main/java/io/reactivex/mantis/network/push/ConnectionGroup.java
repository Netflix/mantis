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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func0;


public class ConnectionGroup<T> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionGroup.class);
    private String groupId;
    private Map<String, AsyncConnection<T>> connections;
    private Counter successfulWrites;
    private Counter numSlotSwaps;
    private Counter failedWrites;
    private final Gauge activeConnections;
    private MeterRegistry meterRegistry;

    public ConnectionGroup(String groupId, MeterRegistry meterRegistry) {
        this.groupId = groupId;
        this.connections = new HashMap<>();
        this.meterRegistry = meterRegistry;

        final String grpId = Optional.ofNullable(groupId).orElse("none");
        final Tags tags = Tags.of(MantisMetricStringConstants.GROUP_ID_TAG, grpId);

        activeConnections = Gauge.builder("ConnectionGroup_activeConnections", () -> {
                synchronized (this) {
                    return connections.size();
                }
            })
            .tags(tags)
            .register(meterRegistry);

        this.successfulWrites = meterRegistry.counter("ConnectionGroup_numSuccessfulWrites", tags);
        this.failedWrites = meterRegistry.counter("ConnectionGroup_numFailedWrites", tags);
        this.numSlotSwaps = meterRegistry.counter("ConnectionGroup_numSlotSwaps", tags);
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
    }

    public synchronized boolean isEmpty() {
        return connections.isEmpty();
    }

    public synchronized Set<AsyncConnection<T>> getConnections() {
        Set<AsyncConnection<T>> copy = new HashSet<>();
        copy.addAll(connections.values());
        return copy;
    }

    public List<Meter> getMetrics() {
        List<Meter> meters = new ArrayList<>();
        meters.add(successfulWrites);
        meters.add(failedWrites);
        meters.add(numSlotSwaps);
        meters.add(activeConnections);
        return meters;
    }

    @Override
    public String toString() {
        return "ConnectionGroup [groupId=" + groupId + ", connections="
                + connections + "]";
    }
}
