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
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ResourceOverviewRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorInfoRequest;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.worker.TaskExecutorGateway;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ResourceClusterAkkaImpl extends ResourceClusterGatewayAkkaImpl implements ResourceCluster {

  private final ClusterID clusterID;

  public ResourceClusterAkkaImpl(ActorRef resourceClusterActor, Duration askTimeout,
      ClusterID clusterID) {
    super(resourceClusterActor, askTimeout);
    this.clusterID = clusterID;
  }

  @Override
  public CompletableFuture<List<TaskExecutorID>> getRegisteredTaskExecutors() {
    return null;
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
}
