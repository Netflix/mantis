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

import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/***
 * Resource Cluster Route
 * Defines the following end points:
 *    /api/v1/resourceClusters/list                                      (GET)
 *
 *    /api/v1/resourceClusters/{}/getResourceOverview                    (GET)
 *    /api/v1/resourceClusters/{}/getRegisteredTaskExecutors             (GET)
 *    /api/v1/resourceClusters/{}/getBusyTaskExecutors                   (GET)
 *    /api/v1/resourceClusters/{}/getAvailableTaskExecutors              (GET)
 *    /api/v1/resourceClusters/{}/getUnregisteredTaskExecutors           (GET)
 *    /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState  (GET)
 *
 *    /api/v1/resourceClusters/{}/actions/registerTaskExecutor           (POST)
 *    /api/v1/resourceClusters/{}/actions/heartBeatFromTaskExecutor      (POST)
 *    /api/v1/resourceClusters/{}/actions/notifyTaskExecutorStatusChange (POST)
 *    /api/v1/resourceClusters/{}/actions/disconnectTaskExecutor         (POST)
 */
@Slf4j
@RequiredArgsConstructor
public class ResourceClustersRoute extends BaseRoute {

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

            // /api/v1/resourceClusters/{}/actions/registerTaskExecutor
            path(
                PathMatchers.segment().slash("actions").slash("registerTaskExecutor"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> registerTaskExecutor(getClusterID(clusterName)))
                ))
            ),

            // /api/v1/resourceClusters/{}/actions/heartBeatFromTaskExecutor
            path(
                PathMatchers.segment().slash("actions").slash("heartBeatFromTaskExecutor"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> heartbeatFromTaskExecutor(getClusterID(clusterName)))
                ))
            ),

            // /api/v1/resourceClusters/{}/actions/notifyTaskExecutorStatusChange
            path(
                PathMatchers.segment().slash("actions").slash("notifyTaskExecutorStatusChange"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> notifyTaskExecutorStatusChange(getClusterID(clusterName)))
                ))
            ),

            // /api/v1/resourceClusters/{}/actions/disconnectTaskExecutor
            path(
                PathMatchers.segment().slash("actions").slash("disconnectTaskExecutor"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> disconnectTaskExecutor(getClusterID(clusterName)))
                ))
            ),

            // /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState
            path(
                PathMatchers.segment().slash(segment().slash("getTaskExecutorState")),
                (clusterName, taskExecutorId) -> pathEndOrSingleSlash(() -> concat(
                    // GET
                    get(() -> getTaskExecutorState(getClusterID(clusterName), getTaskExecutorID(taskExecutorId)))
                ))
            )
        ));
  }

  @Override
  public Route createRoute(Function<Route, Route> routeFilter) {
    log.info("creating /api/v1/resourceClusters routes");
    return super.createRoute(routeFilter);
  }

  private Route listClusters() {
      return withFuture(gateway.listActiveClusters());
  }

  private Route getResourceOverview(ClusterID clusterID) {
    CompletableFuture<ResourceOverview> resourceOverview =
        gateway.getClusterFor(clusterID).resourceOverview();
    return withFuture(resourceOverview);
  }

  private Route registerTaskExecutor(ClusterID clusterID) {
    return entity(Jackson.unmarshaller(TaskExecutorRegistration.class), request -> {
      log.info(
          "POST /api/v1/resourceClusters/{}/actions/registerTaskExecutor called {}",
          clusterID,
          request);

      return withFuture(gateway.getClusterFor(clusterID).registerTaskExecutor(request));
    });
  }


  private Route heartbeatFromTaskExecutor(ClusterID clusterID) {
    return entity(Jackson.unmarshaller(TaskExecutorHeartbeat.class), request -> {
      log.info(
          "POST /api/v1/resourceClusters/{}/actions/heartbeatFromTaskExecutor called {}",
          clusterID.getResourceID(),
          request);

      return withFuture(gateway.getClusterFor(clusterID).heartBeatFromTaskExecutor(request));
    });
  }

  private Route getTaskExecutorState(ClusterID clusterID, TaskExecutorID taskExecutorID) {
    CompletableFuture<TaskExecutorStatus> statusOverview =
        gateway.getClusterFor(clusterID).getTaskExecutorState(taskExecutorID);
    return withFuture(statusOverview);
  }

  private Route disconnectTaskExecutor(ClusterID clusterID) {
    return entity(Jackson.unmarshaller(TaskExecutorDisconnection.class), request -> {
      log.info(
          "POST /api/v1/resourceClusters/{}/actions/disconnectTaskExecutor called {}",
          clusterID.getResourceID(),
          request);

      return withFuture(gateway.getClusterFor(clusterID).disconnectTaskExecutor(request));
    });
  }

  private Route notifyTaskExecutorStatusChange(ClusterID clusterID) {
    return entity(Jackson.unmarshaller(TaskExecutorStatusChange.class), request -> {
      log.info(
          "POST /api/v1/resourceClusters/{}/actions/notifyTaskExecutorStatusChange called {}",
          clusterID.getResourceID(),
          request);

      return withFuture(gateway.getClusterFor(clusterID).notifyTaskExecutorStatusChange(request));
    });
  }

  private ClusterID getClusterID(String clusterName) {
    return ClusterID.of(clusterName);
  }

  private TaskExecutorID getTaskExecutorID(String resourceName) {
    return TaskExecutorID.of(resourceName);
  }

  private <T> Route withFuture(CompletableFuture<T> tFuture) {
    return onComplete(tFuture,
        t -> t.fold(
            throwable -> complete(StatusCodes.INTERNAL_SERVER_ERROR, throwable, Jackson.marshaller()),
            r -> complete(StatusCodes.OK, r, Jackson.marshaller())));
  }
}
