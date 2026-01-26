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

package io.mantisrx.server.master.resourcecluster;

import io.mantisrx.common.Ack;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * ResourceClusters is a factory class for getting and managing individual resource clusters.
 */
public interface ResourceClusters {
  ResourceCluster getClusterFor(ClusterID clusterID);

  CompletableFuture<Set<ClusterID>> listActiveClusters();

  /**
   * Mark all reservation registries as ready to process reservations.
   * This should be called after master initialization is complete and all
   * existing jobs have been recovered.
   *
   * @return Future that completes when all clusters are marked ready
   */
  CompletableFuture<Ack> markAllRegistriesReady();
}
