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

package io.mantisrx.master.api.akka.route.v1;

import static akka.http.javadsl.server.PathMatchers.segment;

import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resource Cluster Route
 * Defines the following end points:
 *    /api/v1/resourceClusters/list                                      (GET)
 *
 *    /api/v1/resourceClusters/{}/getResourceOverview                    (GET)
 *    /api/v1/resourceClusters/{}/getRegisteredTaskExecutors             (GET)
 *    /api/v1/resourceClusters/{}/getBusyTaskExecutors                   (GET)
 *    /api/v1/resourceClusters/{}/getAvailableTaskExecutors              (GET)
 *    /api/v1/resourceClusters/{}/getUnregisteredTaskExecutors           (GET)
 *
 *    /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState  (GET)
 */
@Slf4j
@RequiredArgsConstructor
public class ResourceClustersReadRoute extends BaseRoute {

  private static final PathMatcher0 RESOURCECLUSTERS_API_PREFIX =
      segment("api").slash("v1").slash("resourceClusters");

  private final ResourceClusters gateway;

  @Override
  protected Route constructRoutes() {
    return pathPrefix(
        RESOURCECLUSTERS_API_PREFIX,
        () -> concat(
            // /list
            pathPrefix(
                "list",
                () -> concat(
                    // GET
                    get(this::listClusters))
            ),
            // /{}/getResourceOverview
            path(
                PathMatchers.segment().slash("getResourceOverview"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(get(() -> getResourceOverview(getClusterID(clusterName)))))
            ),
            // /{}/getRegisteredTaskExecutors
            path(
                PathMatchers.segment().slash("getRegisteredTaskExecutors"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(get(() -> withFuture(gateway.getClusterFor(getClusterID(clusterName)).getRegisteredTaskExecutors()))))
            ),
            // /{}/getBusyTaskExecutors
            path(
                PathMatchers.segment().slash("getBusyTaskExecutors"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(get(() -> withFuture(gateway.getClusterFor(getClusterID(clusterName)).getBusyTaskExecutors()))))
            ),
            // /{}/getAvailableTaskExecutors
            path(
                PathMatchers.segment().slash("getAvailableTaskExecutors"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(get(() -> withFuture(gateway.getClusterFor(getClusterID(clusterName)).getAvailableTaskExecutors()))))
            ),
            // /{}/getUnregisteredTaskExecutors
            path(
                PathMatchers.segment().slash("getUnregisteredTaskExecutors"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(get(() -> withFuture(gateway.getClusterFor(getClusterID(clusterName)).getUnregisteredTaskExecutors()))))
            ),

            // /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState
            path(
                PathMatchers.segment().slash("taskExecutors"),
                (clusterName) -> concat(
                    path(
                        PathMatchers.segment().slash("getTaskExecutorState"),
                        (taskExecutorId) -> pathEndOrSingleSlash(() -> concat(
                            // GET
                            get(() -> getTaskExecutorState(getClusterID(clusterName), getTaskExecutorID(taskExecutorId)))
                        ))
                ))
            )
        ));
  }

  private Route listClusters() {
      return withFuture(gateway.listActiveClusters());
  }

  private Route getResourceOverview(ClusterID clusterID) {
    CompletableFuture<ResourceCluster.ResourceOverview> resourceOverview =
        gateway.getClusterFor(clusterID).resourceOverview();
    return withFuture(resourceOverview);
  }

  private Route getTaskExecutorState(ClusterID clusterID, TaskExecutorID taskExecutorID) {
    log.info("GET /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState called", clusterID, taskExecutorID);
    CompletableFuture<ResourceCluster.TaskExecutorStatus> statusOverview =
        gateway.getClusterFor(clusterID).getTaskExecutorState(taskExecutorID);
    return withFuture(statusOverview);
  }

  private ClusterID getClusterID(String clusterName) {
    return ClusterID.of(clusterName);
  }

  private TaskExecutorID getTaskExecutorID(String resourceName) {
    return TaskExecutorID.of(resourceName);
  }
}
