/*
 * Copyright 2022 Netflix, Inc.
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
package com.mantisrx.common.utils;

import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.Listener;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.State;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Services {

    /**
     * Equivalent of service.startAsync().awaitRunning() except that this provides a future that
     * completes when the service reaches the RUNNING state. if the service on the other hand fails
     * to start, then the future is completed exceptionally with the Throwable cause.
     *
     * @param service  service that needs to be started
     * @param executor executor to be used for notifying the caller.
     * @return a future
     */
    public static CompletableFuture<Void> startAsync(Service service, Executor executor) {
        Preconditions.checkArgument(service.state() == State.NEW,
            "Assumes the service has not been started yet");
        final CompletableFuture<Void> result = new CompletableFuture<>();
        service.addListener(new Listener() {
            @Override
            public void running() {
                result.complete(null);
            }

            @Override
            public void failed(State from, Throwable failure) {
                if (from.ordinal() < State.RUNNING.ordinal()) {
                    result.completeExceptionally(failure);
                }
            }
        }, executor);

        service.startAsync();
        return result;
    }

    /**
     * Equivalent service.stopAsync().awaitTerminated() except that this method returns a future
     * that gets completed when the service terminated successfully.
     *
     * @param service  service to be stopped
     * @param executor executor on which the caller needs to be notified.
     * @return future
     */
    public static CompletableFuture<Void> awaitAsync(Service service, Executor executor) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        service.addListener(new Listener() {
            @Override
            public void terminated(State from) {
                result.complete(null);
            }

            @Override
            public void failed(State from, Throwable failure) {
                result.completeExceptionally(failure);
            }
        }, executor);

        if (service.state() == State.FAILED) {
            result.completeExceptionally(service.failureCause());
        } else if (service.state() == State.TERMINATED) {
            result.complete(null);
        }
        return result;
    }

    public static CompletableFuture<Void> stopAsync(Service service, Executor executor) {
        CompletableFuture<Void> result = awaitAsync(service, executor);
        service.stopAsync();
        return result;
    }

    /**
     * Attempts to start the service and waits for it to reach the RUNNING state. If the service has
     * already started, then this method returns immediately.
     *
     * @param service
     */
    public static void startAndWait(Service service) {
        try {
            service.startAsync();
        } catch (IllegalStateException e) {
            log.warn("Service already started: {}", service);
        }
        service.awaitRunning();
    }
}
