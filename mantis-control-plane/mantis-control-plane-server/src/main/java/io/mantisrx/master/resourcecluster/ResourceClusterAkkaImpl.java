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
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetAvailableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetBusyTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetRegisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorStatusRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorWorkerMappingRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetUnregisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.InitializeTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ResourceOverviewRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorInfoRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorsList;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.TriggerClusterRuleRefreshRequest;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusterTaskExecutorMapper;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class ResourceClusterAkkaImpl extends ResourceClusterGatewayAkkaImpl implements ResourceCluster {

    private final ClusterID clusterID;
    private final ResourceClusterTaskExecutorMapper mapper;

    public ResourceClusterAkkaImpl(
        ActorRef resourceClusterManagerActor,
        Duration askTimeout,
        ClusterID clusterID,
        ResourceClusterTaskExecutorMapper mapper) {
        super(resourceClusterManagerActor, askTimeout, mapper);
        this.clusterID = clusterID;
        this.mapper = mapper;
    }

    @Override
    public String getName() {
        return clusterID.getResourceID();
    }

    @Override
    public CompletableFuture<Ack> initializeTaskExecutor(TaskExecutorID taskExecutorID, WorkerId workerId) {
        return Patterns.ask(
                resourceClusterManagerActor,
                new InitializeTaskExecutorRequest(taskExecutorID, workerId),
                askTimeout)
            .thenApply(Ack.class::cast)
            .toCompletableFuture()
            .whenComplete((ack, dontCare) ->
                mapper.onTaskExecutorDiscovered(clusterID, taskExecutorID));
    }

    @Override
    public CompletableFuture<List<TaskExecutorID>> getRegisteredTaskExecutors() {
        return Patterns.ask(
                resourceClusterManagerActor,
                new GetRegisteredTaskExecutorsRequest(clusterID), askTimeout)
            .thenApply(TaskExecutorsList.class::cast)
            .toCompletableFuture()
            .thenApply(l -> l.getTaskExecutors());
    }

    @Override
    public CompletableFuture<List<TaskExecutorID>> getAvailableTaskExecutors() {
        return Patterns.ask(
                resourceClusterManagerActor,
                new GetAvailableTaskExecutorsRequest(clusterID), askTimeout)
            .thenApply(TaskExecutorsList.class::cast)
            .toCompletableFuture()
            .thenApply(l -> l.getTaskExecutors());
    }

    @Override
    public CompletableFuture<List<TaskExecutorID>> getBusyTaskExecutors() {
        return Patterns.ask(
                resourceClusterManagerActor,
                new GetBusyTaskExecutorsRequest(clusterID), askTimeout)
            .thenApply(TaskExecutorsList.class::cast)
            .toCompletableFuture()
            .thenApply(l -> l.getTaskExecutors());
    }

    @Override
    public CompletableFuture<List<TaskExecutorID>> getUnregisteredTaskExecutors() {
        return Patterns.ask(
                resourceClusterManagerActor,
                new GetUnregisteredTaskExecutorsRequest(clusterID), askTimeout)
            .thenApply(TaskExecutorsList.class::cast)
            .toCompletableFuture()
            .thenApply(l -> l.getTaskExecutors());
    }

    @Override
    public CompletableFuture<ResourceOverview> resourceOverview() {
        return
            Patterns
                .ask(resourceClusterManagerActor, new ResourceOverviewRequest(clusterID), askTimeout)
                .thenApply(ResourceOverview.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<TaskExecutorID> getTaskExecutorFor(MachineDefinition machineDefinition,
                                                                WorkerId workerId) {
        return
            Patterns
                .ask(resourceClusterManagerActor, new TaskExecutorAssignmentRequest(machineDefinition, workerId, clusterID), askTimeout)
                .thenApply(TaskExecutorID.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<TaskExecutorGateway> getTaskExecutorGateway(
        TaskExecutorID taskExecutorID) {
        return
            Patterns
                .ask(resourceClusterManagerActor, new TaskExecutorGatewayRequest(taskExecutorID, clusterID), askTimeout)
                .thenApply(TaskExecutorGateway.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<TaskExecutorRegistration> getTaskExecutorInfo(String hostName) {
        return
            Patterns
                .ask(resourceClusterManagerActor, new TaskExecutorInfoRequest(null, hostName, clusterID), askTimeout)
                .thenApply(TaskExecutorRegistration.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<TaskExecutorRegistration> getTaskExecutorInfo(
        TaskExecutorID taskExecutorID) {
        return
            Patterns
                .ask(resourceClusterManagerActor, new TaskExecutorInfoRequest(taskExecutorID, null, clusterID), askTimeout)
                .thenApply(TaskExecutorRegistration.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<TaskExecutorStatus> getTaskExecutorState(TaskExecutorID taskExecutorID) {
        return
            Patterns
                .ask(resourceClusterManagerActor, new GetTaskExecutorStatusRequest(taskExecutorID, clusterID), askTimeout)
                .thenApply(TaskExecutorStatus.class::cast)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Ack> refreshClusterScalerRuleSet() {
        return Patterns
            .ask(
                resourceClusterManagerActor,
                TriggerClusterRuleRefreshRequest.builder().clusterID(this.clusterID).build(),
                askTimeout)
            .thenApply(Ack.class::cast)
            .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> disableTaskExecutorsFor(Map<String, String> attributes, Instant expiry) {
        return CompletableFuture.supplyAsync(() -> {
            resourceClusterManagerActor.tell(
                new DisableTaskExecutorsRequest(attributes, clusterID, expiry), ActorRef.noSender());
            return null;
        });
    }

    @Override
    public CompletableFuture<Map<TaskExecutorID, WorkerId>> getTaskExecutorWorkerMapping() {
        return
            Patterns
                .ask(resourceClusterManagerActor, new GetTaskExecutorWorkerMappingRequest(ImmutableMap.of()), askTimeout)
                .thenApply(obj -> (Map<TaskExecutorID, WorkerId>) obj)
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Map<TaskExecutorID, WorkerId>> getTaskExecutorWorkerMapping(Map<String, String> attributes) {
        return
            Patterns
                .ask(resourceClusterManagerActor, new GetTaskExecutorWorkerMappingRequest(attributes), askTimeout)
                .thenApply(obj -> (Map<TaskExecutorID, WorkerId>) obj)
                .toCompletableFuture();
    }
}
