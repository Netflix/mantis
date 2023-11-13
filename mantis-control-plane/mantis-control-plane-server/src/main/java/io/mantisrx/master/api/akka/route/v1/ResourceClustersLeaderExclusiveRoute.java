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

import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Rejection;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.directives.LogEntry;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resource Cluster Route
 * Defines the following end points:
 *    /api/v1/resourceClusters/{}/actions/registerTaskExecutor           (POST)
 *    /api/v1/resourceClusters/{}/actions/heartBeatFromTaskExecutor      (POST)
 *    /api/v1/resourceClusters/{}/actions/notifyTaskExecutorStatusChange (POST)
 *    /api/v1/resourceClusters/{}/actions/disconnectTaskExecutor         (POST)
 */
@Slf4j
@RequiredArgsConstructor
public class ResourceClustersLeaderExclusiveRoute extends BaseRoute {

    private static final PathMatcher0 RESOURCECLUSTERS_API_PREFIX =
        segment("api").slash("v1").slash("resourceClusters");

    private final ResourceClusters gateway;

    private Optional<LogEntry> onRequestCompletion(HttpRequest request, HttpResponse response) {
        log.debug("ResourceClustersLeaderExclusiveRoute: {} {}", request, response);
        return Optional.empty();
    }

    private Optional<LogEntry> onRequestRejection(HttpRequest request, List<Rejection> rejections) {
        return Optional.empty();
    }

    @Override
    protected Route constructRoutes() {
        return pathPrefix(
            RESOURCECLUSTERS_API_PREFIX,
            () -> logRequestResultOptional(this::onRequestCompletion, this::onRequestRejection, () -> concat(
                // /{}/actions/registerTaskExecutor
                path(
                    PathMatchers.segment().slash("actions").slash("registerTaskExecutor"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        // POST
                        post(() -> registerTaskExecutor(getClusterID(clusterName)))
                    ))
                ),

                // /{}/actions/heartBeatFromTaskExecutor
                path(
                    PathMatchers.segment().slash("actions").slash("heartBeatFromTaskExecutor"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        // POST
                        post(() -> heartbeatFromTaskExecutor(getClusterID(clusterName)))
                    ))
                ),

                // /{}/actions/notifyTaskExecutorStatusChange
                path(
                    PathMatchers.segment().slash("actions").slash("notifyTaskExecutorStatusChange"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        // POST
                        post(() -> notifyTaskExecutorStatusChange(getClusterID(clusterName)))
                    ))
                ),

                // /{}/actions/disconnectTaskExecutor
                path(
                    PathMatchers.segment().slash("actions").slash("disconnectTaskExecutor"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        // POST
                        post(() -> disconnectTaskExecutor(getClusterID(clusterName)))
                    ))
                )
            )));
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
            log.debug(
                "POST /api/v1/resourceClusters/{}/actions/heartbeatFromTaskExecutor called {}",
                clusterID.getResourceID(),
                request);
            return withFuture(gateway.getClusterFor(clusterID).heartBeatFromTaskExecutor(request));
        });
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

            return withFuture(
                gateway.getClusterFor(clusterID).notifyTaskExecutorStatusChange(request));
        });
    }

    private ClusterID getClusterID(String clusterName) {
        return ClusterID.of(clusterName);
    }
}
