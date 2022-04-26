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
import static akka.http.javadsl.server.directives.CachingDirectives.alwaysCache;

import akka.actor.ActorSystem;
import akka.http.caching.javadsl.Cache;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.v1.HttpRequestMetrics.Endpoints;
import io.mantisrx.master.api.akka.route.v1.HttpRequestMetrics.HttpVerb;
import io.mantisrx.master.resourcecluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.master.resourcecluster.proto.ListResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceResponse;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

/***
 * ResourceClustersRoute
 *  Defines the following end points:
 *  api/v1/resourceClusters                               (GET, POST)
 *  api/v1/resourceClusters/{}                            (GET, DELETE)
 *  api/v1/resourceClusters/{}/actions/scaleSku           (POST)
 *
 *  TODO:
 *      - Support update cluster with version support.
 *      - Full delete/un-provision cluster API.
 *      - Integrate with {@link ResourceClustersActor) to disable/enable cluster.
 */
@Slf4j
public class ResourceClustersHostRoute extends BaseRoute {
    private static final PathMatcher0 RESOURCE_CLUSTERS_API_PREFIX =
            segment("api").slash("v1").slash("resourceClusters");

    private final ResourceClusterRouteHandler resourceClusterRouteHandler;
    private final Cache<Uri, RouteResult> routeResultCache;

    public ResourceClustersHostRoute(final ResourceClusterRouteHandler resourceClusterRouteHandler,
                                     final ActorSystem actorSystem) {
        this.resourceClusterRouteHandler = resourceClusterRouteHandler;
        MasterConfiguration config = ConfigurationProvider.getConfig();
        this.routeResultCache = createCache(actorSystem, config.getApiCacheMinSize(), config.getApiCacheMaxSize(),
                config.getApiCacheTtlMilliseconds());
    }

    @Override
    protected Route constructRoutes() {
        return pathPrefix(
                RESOURCE_CLUSTERS_API_PREFIX,
                () -> concat(
                        // api/v1/resourceClusters
                        pathEndOrSingleSlash(() -> concat(

                                // GET
                                get(this::getResourceClustersRoute),

                                // POST
                                post(this::postResourceClustersRoute)
                                )
                        ),

                        // api/v1/resourceClusters/{}
                        path(
                                PathMatchers.segment(),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // GET
                                        get(() -> getResourceClusterInstanceRoute(clusterName)),

                                        // Delete
                                        delete(() -> deleteResourceClusterInstanceRoute(clusterName))
                                ))
                        ),

                        // api/v1/resourceClusters/{}/actions/scaleSku
                        path(
                                PathMatchers.segment().slash("actions").slash("scaleSku"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> scaleClusterSku(clusterName))
                                ))
                        )
                ));
    }

    private Route deleteResourceClusterInstanceRoute(String clusterId) {
        log.info("DELETE api/v1/resourceClusters/{}", clusterId);
        return completeAsync(
            this.resourceClusterRouteHandler.delete(clusterId),
            resp -> completeOK(
                resp,
                Jackson.marshaller()),
            Endpoints.RESOURCE_CLUSTERS,
            HttpVerb.DELETE);
    }

    private Route scaleClusterSku(String clusterName) {
        return entity(Jackson.unmarshaller(ScaleResourceRequest.class), skuScaleRequest -> {

            log.info("POST api/v1/resourceClusters/{}/actions/scaleSku {}", skuScaleRequest);
            final CompletionStage<ScaleResourceResponse> response =
                    this.resourceClusterRouteHandler.scale(skuScaleRequest);

            return completeAsync(
                    response,
                    resp -> complete(
                            StatusCodes.ACCEPTED,
                            resp,
                            Jackson.marshaller()),
                    Endpoints.RESOURCE_CLUSTERS,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route getResourceClusterInstanceRoute(String clusterId) {
        log.info("GET /api/v1/resourceClusters/{} called", clusterId);
        return parameterMap(param ->
                alwaysCache(routeResultCache, getRequestUriKeyer, () -> extractUri(
                        uri -> completeAsync(
                                this.resourceClusterRouteHandler.get(
                                        GetResourceClusterSpecRequest.builder().id(clusterId).build()),
                                resp -> completeOK(
                                        resp,
                                        Jackson.marshaller()),
                                Endpoints.RESOURCE_CLUSTERS,
                                HttpRequestMetrics.HttpVerb.GET))));
    }

    private Route postResourceClustersRoute() {
        return entity(Jackson.unmarshaller(ProvisionResourceClusterRequest.class), resClusterSpec -> {
            log.info("POST /api/v1/resourceClusters called: {}", resClusterSpec);
            final CompletionStage<GetResourceClusterResponse> response =
                    this.resourceClusterRouteHandler.create(resClusterSpec);

            return completeAsync(
                    response,
                    resp -> complete(
                            StatusCodes.ACCEPTED,
                            resp.getClusterSpec(),
                            Jackson.marshaller()),
                    Endpoints.RESOURCE_CLUSTERS,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route getResourceClustersRoute() {
        log.trace("GET /api/v1/resourceClusters called");
        return parameterMap(param ->
                alwaysCache(routeResultCache, getRequestUriKeyer, () -> extractUri(
                        uri -> {
                            return completeAsync(
                                    this.resourceClusterRouteHandler.get(ListResourceClusterRequest.builder().build()),
                                    resp -> completeOK(
                                            resp,
                                            Jackson.marshaller()),
                                    Endpoints.RESOURCE_CLUSTERS,
                                    HttpRequestMetrics.HttpVerb.GET);
                        })));
    }
}
