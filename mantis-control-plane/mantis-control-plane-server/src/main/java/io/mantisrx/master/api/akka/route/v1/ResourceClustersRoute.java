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
import io.mantisrx.common.Ack;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/***
 * Resource Cluster Route
 * Defines the following end points:
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

  private final ResourceClusterGateway gateway;

  @Override
  protected Route constructRoutes() {
    return pathPrefix(
        RESOURCECLUSTERS_API_PREFIX,
        () -> concat(
            // /api/v1/resourceClusters/{}/actions/registerTaskExecutor
            path(
                PathMatchers.segment().slash("actions").slash("registerTaskExecutor"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> registerTaskExecutor(clusterName))
                ))
            ),

            // /api/v1/resourceClusters/{}/actions/heartBeatFromTaskExecutor
            path(
                PathMatchers.segment().slash("actions").slash("heartBeatFromTaskExecutor"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> heartbeatFromTaskExecutor(clusterName))
                ))
            ),

            // /api/v1/resourceClusters/{}/actions/notifyTaskExecutorStatusChange
            path(
                PathMatchers.segment().slash("actions").slash("notifyTaskExecutorStatusChange"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> notifyTaskExecutorStatusChange(clusterName))
                ))
            ),

            // /api/v1/resourceClusters/{}/actions/disconnectTaskExecutor
            path(
                PathMatchers.segment().slash("actions").slash("disconnectTaskExecutor"),
                (clusterName) -> pathEndOrSingleSlash(() -> concat(
                    // POST
                    post(() -> disconnectTaskExecutor(clusterName))
                ))
            )
        ));
  }

  @Override
  public Route createRoute(Function<Route, Route> routeFilter) {
    log.info("creating /api/v1/resourceClusters routes");
    return super.createRoute(routeFilter);
  }

  private Route registerTaskExecutor(String clusterName) {
    return entity(Jackson.unmarshaller(TaskExecutorRegistration.class), request -> {
      log.info(
          "POST /api/v1/jobClusters/{}/actions/registerTaskExecutor called {}",
          clusterName,
          request);

      CompletionStage<Ack> resp = gateway.registerTaskExecutor(request);
      return onComplete(
          resp,
          t -> t
              .fold(
                  throwable -> complete(StatusCodes.INTERNAL_SERVER_ERROR, throwable,
                      Jackson.marshaller()),
                  ack -> complete(StatusCodes.OK, ack, Jackson.marshaller())));
    });
  }


  private Route heartbeatFromTaskExecutor(String clusterName) {
    return entity(Jackson.unmarshaller(TaskExecutorHeartbeat.class), request -> {
      log.info(
          "POST /api/v1/jobClusters/{}/actions/heartbeatFromTaskExecutor called {}",
          clusterName,
          request);

      CompletionStage<Ack> resp = gateway.heartBeatFromTaskExecutor(request);
      return onComplete(
          resp,
          t -> t
              .fold(
                  throwable -> complete(StatusCodes.INTERNAL_SERVER_ERROR, throwable,
                      Jackson.marshaller()),
                  ack -> complete(StatusCodes.OK, ack, Jackson.marshaller())));
    });
  }

  private Route disconnectTaskExecutor(String clusterName) {
    return entity(Jackson.unmarshaller(TaskExecutorDisconnection.class), request -> {
      log.info(
          "POST /api/v1/jobClusters/{}/actions/disconnectTaskExecutor called {}",
          clusterName,
          request);

      CompletionStage<Ack> resp = gateway.disconnectTaskExecutor(request);
      return onComplete(
          resp,
          t -> t
              .fold(
                  throwable -> complete(StatusCodes.INTERNAL_SERVER_ERROR, throwable,
                      Jackson.marshaller()),
                  ack -> complete(StatusCodes.OK, ack, Jackson.marshaller())));
    });
  }

  private Route notifyTaskExecutorStatusChange(String clusterName) {
    return entity(Jackson.unmarshaller(TaskExecutorStatusChange.class), request -> {
      log.info(
          "POST /api/v1/jobClusters/{}/actions/notifyTaskExecutorStatusChange called {}",
          clusterName,
          request);

      CompletionStage<Ack> resp = gateway.notifyTaskExecutorStatusChange(request);
      return onComplete(
          resp,
          t -> t
              .fold(
                  throwable -> complete(StatusCodes.INTERNAL_SERVER_ERROR, throwable,
                      Jackson.marshaller()),
                  ack -> complete(StatusCodes.OK, ack, Jackson.marshaller())));
    });
  }
}
