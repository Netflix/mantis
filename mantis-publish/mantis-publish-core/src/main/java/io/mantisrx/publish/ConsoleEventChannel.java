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
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;


/**
 * An {@link EventChannel} that prints its output to stdout.
 */
public class ConsoleEventChannel implements EventChannel {

    private final PrintStream printStream;

    /**
     * Creates a new instance.
     *
     * @param printStream the output stream of where to send events,
     *                    for example, {@code System.out}.
     */
    public ConsoleEventChannel(PrintStream printStream) {

        this.printStream = printStream;
    }

    /**
     * Writes the event to {@code stdout} on localhost.
     *
     * @param worker  a {@link MantisWorker} representing localhost for this class.
     * @param event the output to write to console.
     */
    @Override
    public CompletableFuture<Void> send(MantisWorker worker, Event event) {
        printStream.println(event.toJsonString());
        return CompletableFuture.completedFuture(null);
    }

    /**
     * This {@link EventChannel} doesn't use a buffer, so default the size to 0 to represent 0% utilization.
     */
    @Override
    public double bufferSize(MantisWorker worker) {
        return 0;
    }

    @Override
    public void close(MantisWorker worker) {
        // NOOP
    }
}
