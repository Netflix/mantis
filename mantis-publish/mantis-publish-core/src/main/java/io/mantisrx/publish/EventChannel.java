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

package io.mantisrx.publish;

import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.publish.api.Event;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/**
 * A transport that lets users publish events into Mantis.
 * <p>
 * All I/O operations are asynchronous. This means calls to {@link EventChannel#send(MantisWorker, Event)} will
 * return immediately without guarantee that the operation has completed. It is up to the caller to react to the
 * resulting {@link Future<Void>}.
 */
public interface EventChannel {

    /**
     * Asynchronous API to send an event to a given destination.
     *
     * @param worker a {@link MantisWorker} which represents a destination for the event.
     * @param event  a {@link Event} which represents a set of key-value pairs with
     *               additional context.
     *
     * @return a {@link Future<Void>} which represents the action of sending an event.
     * The caller is responsible for reacting to this future.
     */
    CompletableFuture<Void> send(MantisWorker worker, Event event);

    /**
     * Returns the buffer size as a percentage utilization of the channel's internal transport.
     * <p>
     * An {@link EventChannel} may have many underlying connections which implement the same transport,
     * each of which could have their own buffers.
     *
     * @param worker a {@link MantisWorker} which is used to query for the buffer size of a specific internal transport.
     */
    double bufferSize(MantisWorker worker);

    /**
     * Asynchronous API to close the underlying channel for a given {@link MantisWorker}.
     *
     * @param worker a {@link MantisWorker} which is used to query for the buffer size of a specific internal transport.
     */
    void close(MantisWorker worker);
}
