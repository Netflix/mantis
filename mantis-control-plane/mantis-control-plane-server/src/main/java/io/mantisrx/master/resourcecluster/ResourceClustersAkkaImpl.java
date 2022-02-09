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
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ResourceClustersAkkaImpl implements ResourceClusters {

  private final ActorRef resourceClusterManagerActor;
  private final Duration askTimeout;

  @Override
  public ResourceCluster getClusterFor(ClusterID clusterID) {
    return new ResourceClusterAkkaImpl(resourceClusterManagerActor, askTimeout, clusterID);
  }

  @Override
  public CompletableFuture<Set<ClusterID>> listActiveClusters() {
    return
        Patterns.ask(resourceClusterManagerActor,
                new ResourceClusterManagerActor.ListActiveClusters(), askTimeout)
            .toCompletableFuture()
            .thenApply(ResourceClusterManagerActor.ClusterIdSet.class::cast)
            .thenApply(clusterIdSet -> clusterIdSet.getClusterIDS());
  }
}
