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
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.ResourceClusterTaskExecutorMapper;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class ResourceClusterGatewayAkkaImpl implements ResourceClusterGateway {
    protected final ActorRef resourceClusterManagerActor;
    protected final Duration askTimeout;
    private final ResourceClusterTaskExecutorMapper mapper;
    private final Counter registrationCounter;
    private final Counter heartbeatCounter;
    private final Counter disconnectionCounter;


    ResourceClusterGatewayAkkaImpl(
            ActorRef resourceClusterManagerActor,
            Duration askTimeout,
            ResourceClusterTaskExecutorMapper mapper) {
        this.resourceClusterManagerActor = resourceClusterManagerActor;
        this.askTimeout = askTimeout;
        this.mapper = mapper;

        Metrics m = new Metrics.Builder()
                .id("ResourceClusterGatewayAkkaImpl")
                .addCounter("registrationCounter")
                .addCounter("heartbeatCounter")
                .addCounter("disconnectionCounter")
                .addCounter("throttledCounter")
                .build();
        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.registrationCounter = metrics.getCounter("registrationCounter");
        this.heartbeatCounter = metrics.getCounter("heartbeatCounter");
        this.disconnectionCounter = metrics.getCounter("disconnectionCounter");
    }

    @Override
    public CompletableFuture<Ack> registerTaskExecutor(TaskExecutorRegistration registration) {
        this.registrationCounter.increment();
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
    public CompletableFuture<Ack> heartBeatFromTaskExecutor(TaskExecutorHeartbeat heartbeat) {
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
        TaskExecutorDisconnection taskExecutorDisconnection) {
            this.disconnectionCounter.increment();
            return
                Patterns.ask(resourceClusterManagerActor, taskExecutorDisconnection, askTimeout)
                    .thenApply(Ack.class::cast)
                    .toCompletableFuture();
    }
}
