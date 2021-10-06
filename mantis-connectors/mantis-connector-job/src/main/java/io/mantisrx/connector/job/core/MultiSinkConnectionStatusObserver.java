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

package io.mantisrx.connector.job.core;

import io.mantisrx.client.SinkConnectionsStatus;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiSinkConnectionStatusObserver implements SinkConnectionStatusObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiSinkConnectionStatusObserver.class);
    public static final MultiSinkConnectionStatusObserver INSTANCE = new MultiSinkConnectionStatusObserver();
    private final ConcurrentHashMap<String, SinkConnectionStatusObserver> sinkObserverMap = new ConcurrentHashMap<>();

    public void addSinkConnectionObserver(String name, SinkConnectionStatusObserver obs) {
        sinkObserverMap.put(name, obs);
    }

    public void removeSinkConnectionObserver(String name) {
        sinkObserverMap.remove(name);
    }

    public SinkConnectionStatusObserver getSinkConnectionObserver(String name) {
        return sinkObserverMap.get(name);
    }

    // for testing
    void removeAllSinkConnectionObservers() {
        sinkObserverMap.clear();
    }

    /**
     * Iterate through all member connectionObservers and sum up the connectedServer counts.
     */

    @Override
    public long getConnectedServerCount() {
        if (sinkObserverMap.isEmpty()) {
            LOGGER.warn("No connection observers registered!");
        }
        Iterator<SinkConnectionStatusObserver> it = sinkObserverMap.values().iterator();
        int count = 0;
        while (it.hasNext()) {
            SinkConnectionStatusObserver ob = it.next();
            count += ob.getConnectedServerCount();
        }
        LOGGER.info("Total connected server count" + count);
        return count;
    }

    /**
     * Iterate through all member connectionObservers and sum up the totalServer counts.
     */

    @Override
    public long getTotalServerCount() {
        if (sinkObserverMap.isEmpty()) {
            LOGGER.warn("No connection observers registered!");
        }

        Iterator<SinkConnectionStatusObserver> it = sinkObserverMap.values().iterator();
        int count = 0;
        while (it.hasNext()) {
            SinkConnectionStatusObserver ob = it.next();
            count += ob.getTotalServerCount();
        }
        LOGGER.info("Total  server count" + count);

        return count;
    }

    /**
     * Iterate through all member connectionObservers and sum up the receiving data counts.
     */

    @Override
    public long getReceivingDataCount() {
        if (sinkObserverMap.isEmpty()) {
            LOGGER.warn("No connection observers registered!");
        }
        Iterator<SinkConnectionStatusObserver> it = sinkObserverMap.values().iterator();
        int count = 0;
        while (it.hasNext()) {
            SinkConnectionStatusObserver ob = it.next();
            count += ob.getConnectedServerCount();
        }
        LOGGER.info("Total receiving server count" + count);
        return count;
    }

    /**
     * Iterate through all member connectionObservers and return false if any of the constituent client connections
     * are not complete.
     */
    @Override
    public boolean isConnectedToAllSinks() {
        if (sinkObserverMap.isEmpty()) {
            LOGGER.warn("No connection observers registered!");
        }
        Iterator<SinkConnectionStatusObserver> it = sinkObserverMap.values().iterator();
        boolean connectedToAll = false;
        while (it.hasNext()) {
            SinkConnectionStatusObserver ob = it.next();
            connectedToAll = ob.isConnectedToAllSinks();
            if (!connectedToAll) {
                LOGGER.warn("Not connected to sinks of all jobs");
                break;
            }
        }

        return connectedToAll;
    }

    @Override
    public void onCompleted() {
        // NO OP
    }

    @Override
    public void onError(Throwable e) {
        // NO OP
    }

    @Override
    public void onNext(SinkConnectionsStatus t) {
        // NO OP
    }
}
