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
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetAvailableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetBusyTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetRegisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorStatusRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetUnregisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ResourceOverviewRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorInfoRequest;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * Supervisor actor responsible for creating/deleting/listing all resource clusters in the system.
 */
@Slf4j
public class ResourceClustersManagerActor extends AbstractActor {

  private final MasterConfiguration masterConfiguration;
  private final Clock clock;
  private final RpcService rpcService;

  private final Map<ClusterID, ActorRef> resourceClusterActorMap;

  public static Props props(MasterConfiguration masterConfiguration, Clock clock, RpcService rpcService) {
    return Props.create(ResourceClustersManagerActor.class, masterConfiguration, clock, rpcService);
  }

  public ResourceClustersManagerActor(
      MasterConfiguration masterConfiguration, Clock clock,
      RpcService rpcService) {
    this.masterConfiguration = masterConfiguration;
    this.clock = clock;
    this.rpcService = rpcService;

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
            .match(GetAvailableTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
            .match(GetUnregisteredTaskExecutorsRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))
            .match(GetTaskExecutorStatusRequest.class, req -> getRCActor(req.getClusterID()).forward(req, context()))

            .match(TaskExecutorRegistration.class, registration ->
                getRCActor(registration.getClusterID()).forward(registration, context()))
            .match(TaskExecutorHeartbeat.class, heartbeat ->
                getRCActor(heartbeat.getClusterID()).forward(heartbeat, context()))
            .match(TaskExecutorStatusChange.class, statusChange ->
                getRCActor(statusChange.getClusterID()).forward(statusChange, context()))
            .match(TaskExecutorDisconnection.class, disconnection ->
                getRCActor(disconnection.getClusterID()).forward(disconnection, context()))
            .match(TaskExecutorAssignmentRequest.class, req ->
                getRCActor(req.getClusterID()).forward(req, context()))
            .match(ResourceOverviewRequest.class, req ->
                getRCActor(req.getClusterID()).forward(req, context()))
            .match(TaskExecutorInfoRequest.class, req ->
                getRCActor(req.getClusterID()).forward(req, context()))
            .match(TaskExecutorGatewayRequest.class, req ->
                getRCActor(req.getClusterID()).forward(req, context()))
            .build();
  }

  private ActorRef createResourceClusterActorFor(ClusterID clusterID) {
    log.info("Creating resource cluster actor for {}", clusterID);
    ActorRef clusterActor = getContext().actorOf(ResourceClusterActor.props(clusterID,
        Duration.ofMillis(masterConfiguration.getHeartbeatIntervalInMs()), clock, rpcService), "ResourceClusterActor-" + clusterID.getResourceID());
    log.info("Created resource cluster actor for {}", clusterID);
    return clusterActor;
  }

  private ActorRef getRCActor(ClusterID clusterID) {
    if (resourceClusterActorMap.get(clusterID) != null) {
      return resourceClusterActorMap.get(clusterID);
    } else {
      return resourceClusterActorMap.computeIfAbsent(clusterID, (dontCare) -> createResourceClusterActorFor(clusterID));
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
}
