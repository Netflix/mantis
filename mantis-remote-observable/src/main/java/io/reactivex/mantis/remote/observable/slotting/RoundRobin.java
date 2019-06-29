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

import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.network.RoundRobinRouter;
import io.mantisrx.common.network.WritableEndpoint;


public class RoundRobin<T> extends SlottingStrategy<T> {

    private RoundRobinRouter<T> router = new RoundRobinRouter<>();

    public Metrics getMetrics() {
        return router.getMetrics();
    }

    @Override
    public synchronized boolean addConnection(WritableEndpoint<T> endpoint) {
        boolean wasEmpty = router.isEmpty();
        boolean isRegistered = router.add(endpoint);
        if (wasEmpty && isRegistered) {
            doAfterFirstConnectionAdded.call();
        }
        return isRegistered;
    }

    @Override
    public synchronized boolean removeConnection(WritableEndpoint<T> endpoint) {
        boolean isDeregistered = router.remove(endpoint);
        if (isDeregistered && router.isEmpty()) {
            doAfterLastConnectionRemoved.call();
        }
        return isDeregistered;
    }

    @Override
    public void writeOnSlot(byte[] keyBytes, T data) {
        router.nextSlot().write(data);
    }

    @Override
    // TODO should completeAll and errorAll de-register?
    public void completeAllConnections() {
        router.completeAllEndpoints();
    }

    @Override
    public void errorAllConnections(Throwable e) {
        router.errorAllEndpoints(e);
    }
}
