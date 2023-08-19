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

package io.mantisrx.master.resourcecluster;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import io.mantisrx.common.Ack;
import io.mantisrx.server.master.resourcecluster.RequestThrottledException;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.ResourceClusterTaskExecutorMapper;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.shaded.com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class ResourceClusterGatewayAkkaImpl implements ResourceClusterGateway {
    protected final ActorRef resourceClusterManagerActor;
    protected final Duration askTimeout;
    private final ResourceClusterTaskExecutorMapper mapper;
    protected final RateLimiter rateLimiter;

    ResourceClusterGatewayAkkaImpl(
            ActorRef resourceClusterManagerActor,
            Duration askTimeout,
            ResourceClusterTaskExecutorMapper mapper,
            int maxConcurrentRequestCount) {
        this.resourceClusterManagerActor = resourceClusterManagerActor;
        this.askTimeout = askTimeout;
        this.mapper = mapper;

        this.rateLimiter = RateLimiter.create(maxConcurrentRequestCount);
    }

    protected <In, Out> Out withThrottle(Function<In, Out> func, In req) throws RequestThrottledException {
        if (this.rateLimiter.tryAcquire(2, TimeUnit.SECONDS)) {
            return func.apply(req);
        } else {
            throw new RequestThrottledException("Throttled req: " + req.getClass().getSimpleName());
        }
    }

    @Override
    public CompletableFuture<Ack> registerTaskExecutor(TaskExecutorRegistration registration)
            throws RequestThrottledException {
        return withThrottle(this::registerTaskExecutorImpl, registration);
    }

    private CompletableFuture<Ack> registerTaskExecutorImpl(TaskExecutorRegistration registration) {
        return Patterns
                .ask(resourceClusterManagerActor, registration, askTimeout)
                .thenApply(Ack.class::cast)
                .toCompletableFuture()
                .whenComplete((dontCare, throwable) ->
                        mapper.onTaskExecutorDiscovered(
                                registration.getClusterID(),
                                registration.getTaskExecutorID()));
    }

    @Override
    public CompletableFuture<Ack> heartBeatFromTaskExecutor(TaskExecutorHeartbeat heartbeat)
            throws RequestThrottledException {
        return withThrottle(this::heartBeatFromTaskExecutorImpl, heartbeat);
    }

    private CompletableFuture<Ack> heartBeatFromTaskExecutorImpl(TaskExecutorHeartbeat heartbeat) {
        return
            Patterns
                .ask(resourceClusterManagerActor, heartbeat, askTimeout)
                .thenApply(Ack.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Ack> notifyTaskExecutorStatusChange(TaskExecutorStatusChange statusChange) {
        return
            Patterns
                .ask(resourceClusterManagerActor, statusChange, askTimeout)
                .thenApply(Ack.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Ack> disconnectTaskExecutor(
        TaskExecutorDisconnection taskExecutorDisconnection) throws RequestThrottledException {
        return withThrottle(this::disconnectTaskExecutorImpl, taskExecutorDisconnection);
    }

    CompletableFuture<Ack> disconnectTaskExecutorImpl(
            TaskExecutorDisconnection taskExecutorDisconnection) {
        return
                Patterns.ask(resourceClusterManagerActor, taskExecutorDisconnection, askTimeout)
                        .thenApply(Ack.class::cast)
                        .toCompletableFuture();
    }
}
