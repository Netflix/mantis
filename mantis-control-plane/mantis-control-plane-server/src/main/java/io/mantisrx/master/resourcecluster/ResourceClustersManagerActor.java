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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.master.akka.MantisActorSupervisorStrategy;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.AddNewJobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetActiveJobsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetAssignedTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetAvailableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetBusyTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetDisabledTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetJobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetRegisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorStatusRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetUnregisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.MarkExecutorTaskCancelledRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.RemoveJobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ResourceOverviewRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorInfoRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.QueueClusterRuleRefreshRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.TriggerClusterRuleRefreshRequest;
import io.mantisrx.master.resourcecluster.proto.SetResourceClusterScalerStatusRequest;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * Supervisor actor responsible for creating/deleting/listing all resource clusters in the system.
 */
@Slf4j
class ResourceClustersManagerActor extends AbstractActor {

    private final MasterConfiguration masterConfiguration;
    private final Clock clock;
    private final RpcService rpcService;
    private final MantisJobStore mantisJobStore;

    // Cluster Id to <ResourceClusterActor, ResourceClusterScalerActor> map.
    private final Map<ClusterID, ActorHolder> resourceClusterActorMap;

    private final ActorRef resourceClusterHostActor;
    private final IMantisPersistenceProvider mantisPersistenceProvider;
    private final JobMessageRouter jobMessageRouter;

    public static Props props(
        MasterConfiguration masterConfiguration,
        Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore,
        ActorRef resourceClusterHostActorRef,
        IMantisPersistenceProvider mantisPersistenceProvider,
        JobMessageRouter jobMessageRouter) {
        return Props.create(
            ResourceClustersManagerActor.class,
            masterConfiguration,
            clock,
            rpcService,
            mantisJobStore,
            resourceClusterHostActorRef,
            mantisPersistenceProvider,
            jobMessageRouter);
    }

    public ResourceClustersManagerActor(
        MasterConfiguration masterConfiguration, Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore,
        ActorRef resourceClusterHostActorRef,
        IMantisPersistenceProvider mantisPersistenceProvider,
        JobMessageRouter jobMessageRouter) {
        this.masterConfiguration = masterConfiguration;
        this.clock = clock;
        this.rpcService = rpcService;
        this.mantisJobStore = mantisJobStore;
        this.resourceClusterHostActor = resourceClusterHostActorRef;
        this.mantisPersistenceProvider = mantisPersistenceProvider;
        this.jobMessageRouter = jobMessageRouter;

        this.resourceClusterActorMap = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(ListActiveClusters.class, req -> sender().tell(getActiveClusters(), self()))

                .match(GetRegisteredTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetBusyTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetDisabledTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetAvailableTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetUnregisteredTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetTaskExecutorStatusRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetActiveJobsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetAssignedTaskExecutorRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
                .match(MarkExecutorTaskCancelledRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))

                .match(TaskExecutorRegistration.class, registration ->
                    getRCActor(registration.getClusterID()).forward(registration, context()))
                .match(TaskExecutorHeartbeat.class, heartbeat ->
                    getRCActor(heartbeat.getClusterID()).forward(heartbeat, context()))
                .match(TaskExecutorStatusChange.class, statusChange ->
                    getRCActor(statusChange.getClusterID()).forward(statusChange, context()))
                .match(TaskExecutorDisconnection.class, disconnection ->
                    getRCActor(disconnection.getClusterID()).forward(disconnection, context()))
                .match(TaskExecutorBatchAssignmentRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(ResourceOverviewRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(TaskExecutorInfoRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(TaskExecutorGatewayRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(DisableTaskExecutorsRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(AddNewJobArtifactsToCacheRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(RemoveJobArtifactsToCacheRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(GetJobArtifactsToCacheRequest.class, req ->
                    getRCActor(req.getClusterID()).forward(req, context()))
                .match(TriggerClusterRuleRefreshRequest.class, req ->
                    getRCScalerActor(req.getClusterID()).forward(req, context()))
                .match(QueueClusterRuleRefreshRequest.class, req ->
                    getRCScalerActor(req.getClusterID()).forward(req, context()))
                .match(SetResourceClusterScalerStatusRequest.class, req ->
                    getRCScalerActor(req.getClusterID()).forward(req, context()))
                .build();
    }

    private ActorRef createResourceClusterActorFor(ClusterID clusterID) {
        log.info("Creating resource cluster actor for {}", clusterID);
        ActorRef clusterActor =
            getContext().actorOf(
                ResourceClusterActor.props(
                    clusterID,
                    Duration.ofMillis(masterConfiguration.getHeartbeatIntervalInMs()),
                    Duration.ofMillis(masterConfiguration.getAssignmentIntervalInMs()),
                    Duration.ofMillis(masterConfiguration.getAssignmentIntervalInMs()),
                    Duration.ofMillis(masterConfiguration.getSchedulerLeaseExpirationDurationInMs()),
                    clock,
                    rpcService,
                    mantisJobStore,
                    jobMessageRouter,
                    masterConfiguration.getMaxJobArtifactsToCache(),
                    masterConfiguration.getJobClustersWithArtifactCachingEnabled(),
                    masterConfiguration.isJobArtifactCachingEnabled(),
                    masterConfiguration.getSchedulingConstraints(),
                    masterConfiguration.getFitnessCalculator(),
                    masterConfiguration.getAvailableTaskExecutorMutatorHook()),
                "ResourceClusterActor-" + clusterID.getResourceID());
        log.info("Created resource cluster actor for {}", clusterID);
        return clusterActor;
    }

    private ActorRef createResourceClusterScalerActorFor(ClusterID clusterID, ActorRef rcActor) {
        log.info("Creating resource cluster scaler actor for {}", clusterID);
        ActorRef clusterScalerActor =
            getContext().actorOf(
                ResourceClusterScalerActor.props(
                    clusterID,
                    clock,
                    Duration.ofSeconds(masterConfiguration.getScalerTriggerThresholdInSecs()),
                    Duration.ofSeconds(masterConfiguration.getScalerRuleSetRefreshThresholdInSecs()),
                    this.mantisPersistenceProvider,
                    this.resourceClusterHostActor,
                    rcActor
                ),
                "ResourceClusterScalerActor-" + clusterID.getResourceID());
        log.info("Created resource cluster scaler actor for {}", clusterID);
        return clusterScalerActor;
    }

    private ActorRef getRCActor(ClusterID clusterID) {
        return getOrCreateRCActors(clusterID).getResourceClusterActor();
    }

    private ActorRef getRCScalerActor(ClusterID clusterID) {
        return getOrCreateRCActors(clusterID).getResourceClusterScalerActor();
    }

    private ActorHolder getOrCreateRCActors(ClusterID clusterID) {
        if (resourceClusterActorMap.get(clusterID) != null) {
            return resourceClusterActorMap.get(clusterID);
        } else {
            return resourceClusterActorMap.computeIfAbsent(clusterID, (dontCare) -> {
                ActorRef rcActorRef = createResourceClusterActorFor(clusterID);
                getContext().watch(rcActorRef);
                ActorRef scalerActorRef = createResourceClusterScalerActorFor(clusterID, rcActorRef);
                getContext().watch(scalerActorRef);

                return ActorHolder.builder()
                    .resourceClusterActor(rcActorRef)
                    .resourceClusterScalerActor(scalerActorRef)
                    .build();
            });
        }
    }

    private ClusterIdSet getActiveClusters() {
        return new ClusterIdSet(resourceClusterActorMap.keySet());
    }

    @Value
    static class ListActiveClusters {
    }

    @Value
    static class ClusterIdSet {
        Set<ClusterID> clusterIDS;
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return MantisActorSupervisorStrategy.getInstance().create();
    }

    @Value
    @Builder
    static class ActorHolder {
        ActorRef resourceClusterActor;
        ActorRef resourceClusterScalerActor;
    }
}
