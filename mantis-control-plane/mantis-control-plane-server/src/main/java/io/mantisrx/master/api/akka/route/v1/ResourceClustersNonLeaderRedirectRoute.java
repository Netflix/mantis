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
import io.mantisrx.common.Ack;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.v1.HttpRequestMetrics.Endpoints;
import io.mantisrx.master.api.akka.route.v1.HttpRequestMetrics.HttpVerb;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.resourcecluster.proto.DisableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.master.resourcecluster.proto.GetTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.proto.ListResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateAllResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateResourceClusterScaleRuleRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.JobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceResponse;
import io.mantisrx.master.resourcecluster.proto.SetResourceClusterScalerStatusRequest;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersRequest;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.PagedActiveJobOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * Resource Cluster Route
 * Defines the following end points:
 * /api/v1/resourceClusters                                           (GET, POST)
 * /api/v1/resourceClusters/list                                      (GET)
 * <p>
 * /api/v1/resourceClusters/{}                                        (GET, DELETE)
 * <p>
 * <p>
 * /api/v1/resourceClusters/{}/getResourceOverview                    (GET)
 * /api/v1/resourceClusters/{}/getRegisteredTaskExecutors             (GET)
 * /api/v1/resourceClusters/{}/getBusyTaskExecutors                   (GET)
 * /api/v1/resourceClusters/{}/getDisabledTaskExecutors               (GET)
 * /api/v1/resourceClusters/{}/getAvailableTaskExecutors              (GET)
 * /api/v1/resourceClusters/{}/getUnregisteredTaskExecutors           (GET)
 * /api/v1/resourceClusters/{}/scaleSku                               (POST)
 * /api/v1/resourceClusters/{}/upgrade                                (POST)
 * /api/v1/resourceClusters/{}/disableTaskExecutors                   (POST)
 * /api/v1/resourceClusters/{}/setScalerStatus                        (POST)
 * <p>
 * <p>
 * /api/v1/resourceClusters/{}/scaleRule                              (POST)
 * /api/v1/resourceClusters/{}/scaleRules                             (GET, POST)
 * <p>
 * /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState  (GET)
 * <p>
 * /api/v1/resourceClusters/{}/cacheJobArtifacts                       (GET)
 * /api/v1/resourceClusters/{}/cacheJobArtifacts                       (POST)
 * /api/v1/resourceClusters/{}/cacheJobArtifacts                       (DELETE)
 *
 * [Notes]
 * To upgrade cluster containers: each container running task executor is using docker image tag based image version.
 * In regular case the upgrade is to refresh the container to re-deploy with latest digest associated with the image
 * tag (e.g. latest).
 * If multiple image digest versions need to be ran/hosted at the same time, it is recommended to create a separate
 * sku id in addition to the existing sku(s).
 */
@Slf4j
public class ResourceClustersNonLeaderRedirectRoute extends BaseRoute {

    private static final PathMatcher0 RESOURCECLUSTERS_API_PREFIX =
        segment("api").slash("v1").slash("resourceClusters");

    private final ResourceClusters gateway;

    private final ResourceClusterRouteHandler resourceClusterRouteHandler;
    private final Cache<Uri, RouteResult> routeResultCache;

    public ResourceClustersNonLeaderRedirectRoute(
        final ResourceClusters gateway,
        final ResourceClusterRouteHandler resourceClusterRouteHandler,
        final ActorSystem actorSystem) {
        this.gateway = gateway;
        this.resourceClusterRouteHandler = resourceClusterRouteHandler;
        MasterConfiguration config = ConfigurationProvider.getConfig();
        this.routeResultCache = createCache(actorSystem, config.getApiCacheMinSize(),
            config.getApiCacheMaxSize(),
            config.getApiCacheTtlMilliseconds());
    }

    @Override
    protected Route constructRoutes() {
        Route result = pathPrefix(
            RESOURCECLUSTERS_API_PREFIX,
            () -> concat(
                // /
                pathEndOrSingleSlash(() -> concat(

                        // GET
                        get(this::getRegisteredResourceClustersRoute),

                        // POST
                        post(this::provisionResourceClustersRoute)
                    )
                ),
                // /list
                pathPrefix(
                    "list",
                    () -> concat(
                        // GET
                        get(this::listClusters))
                ),
                // /{}
                path(
                    PathMatchers.segment(),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(

                        // GET
                        get(() -> getResourceClusterInstanceRoute(clusterName)),

                        // Delete
                        delete(() -> deleteResourceClusterInstanceRoute(clusterName))
                    ))
                ),
                // /{}/scaleSku
                path(
                    PathMatchers.segment().slash("scaleSku"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(

                        // POST
                        post(() -> scaleClusterSku(clusterName))
                    ))
                ),
                // /{}/disableTaskExecutors
                path(
                    PathMatchers.segment().slash("disableTaskExecutors"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        post(() -> disableTaskExecutors(getClusterID(clusterName)))))
                ),
                // /{}/setScalerStatus
                path(
                    PathMatchers.segment().slash("setScalerStatus"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        post(() -> setScalerStatus(clusterName))))
                ),
                // /{}/upgrade
                path(
                    PathMatchers.segment().slash("upgrade"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(

                        // POST
                        post(() -> upgradeCluster(clusterName))
                    ))
                ),
                // /{}/getResourceOverview
                path(
                    PathMatchers.segment().slash("getResourceOverview"),
                    (clusterName) -> pathEndOrSingleSlash(
                        () -> concat(get(() -> getResourceOverview(getClusterID(clusterName)))))
                ),
                // /{}/activeJobOverview?pageSize={}&startingIndex={}
                path(
                    PathMatchers.segment().slash("activeJobOverview"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(get(() ->
                        parameterOptional("startingIndex", startingIndex ->
                            parameterOptional("pageSize", pageSize ->
                                getActiveJobOverview(getClusterID(clusterName), startingIndex,
                                    pageSize))))))
                ),
                // /{}/getRegisteredTaskExecutors
                path(
                    PathMatchers.segment().slash("getRegisteredTaskExecutors"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        get(() -> mkTaskExecutorsRoute(getClusterID(clusterName), (rc, req) -> rc.getRegisteredTaskExecutors(req.getAttributes())))))
                ),
                // /{}/getBusyTaskExecutors
                path(
                    PathMatchers.segment().slash("getBusyTaskExecutors"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        get(() -> mkTaskExecutorsRoute(getClusterID(clusterName), (rc, req) -> rc.getBusyTaskExecutors(req.getAttributes())))))
                ),
                // /{}/getDisabledTaskExecutors
                path(
                    PathMatchers.segment().slash("getDisabledTaskExecutors"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        get(() -> mkTaskExecutorsRoute(getClusterID(clusterName), (rc, req) -> rc.getDisabledTaskExecutors(req.getAttributes())))))
                ),
                // /{}/getAvailableTaskExecutors
                path(
                    PathMatchers.segment().slash("getAvailableTaskExecutors"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        get(() -> mkTaskExecutorsRoute(getClusterID(clusterName), (rc, req) -> rc.getAvailableTaskExecutors(req.getAttributes())))))
                ),
                // /{}/getUnregisteredTaskExecutors
                path(
                    PathMatchers.segment().slash("getUnregisteredTaskExecutors"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        get(() -> mkTaskExecutorsRoute(getClusterID(clusterName), (rc, req) -> rc.getUnregisteredTaskExecutors(req.getAttributes())))))
                ),
                // /{}/scaleRule
                path(
                    PathMatchers.segment().slash("scaleRule"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(

                        // POST
                        post(() -> createSingleScaleRule(clusterName))
                    ))
                ),
                // /{}/scaleRules
                path(
                    PathMatchers.segment().slash("scaleRules"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        // GET
                        get(() -> getScaleRules(clusterName)),

                        // POST
                        post(() -> createAllScaleRules(clusterName))
                    ))
                ),
                // /{}/cacheJobArtifacts
                path(
                    PathMatchers.segment().slash("cacheJobArtifacts"),
                    (clusterName) -> pathEndOrSingleSlash(() -> concat(
                        // GET
                        get(() -> withFuture(gateway.getClusterFor(getClusterID(clusterName))
                            .getJobArtifactsToCache())),

                        // POST
                        post(() -> cacheJobArtifacts(clusterName)),

                        // DELETE
                        delete(() -> removeJobArtifactsToCache(clusterName))
                    ))
                ),

                // /api/v1/resourceClusters/{}/taskExecutors/{}/getTaskExecutorState
                pathPrefix(
                    PathMatchers.segment().slash("taskExecutors"),
                    (clusterName) -> concat(
                        path(
                            PathMatchers.segment().slash("getTaskExecutorState"),
                            (taskExecutorId) ->
                                pathEndOrSingleSlash(() -> concat(
                                    get(() -> getTaskExecutorState(getClusterID(clusterName),
                                        getTaskExecutorID(taskExecutorId))))))
                    )
                )
            ));

        return result;
    }

    private Route listClusters() {
        return withFuture(gateway.listActiveClusters());
    }

    private Route getActiveJobOverview(ClusterID clusterID, Optional<String> startingIndex,
        Optional<String> pageSize) {
        CompletableFuture<PagedActiveJobOverview> jobsOverview =
            gateway.getClusterFor(clusterID).getActiveJobOverview(
                startingIndex.map(Integer::parseInt),
                pageSize.map(Integer::parseInt));
        return withFuture(jobsOverview);
    }

    private Route getResourceOverview(ClusterID clusterID) {
        CompletableFuture<ResourceCluster.ResourceOverview> resourceOverview =
            gateway.getClusterFor(clusterID).resourceOverview();
        return withFuture(resourceOverview);
    }

    private Route mkTaskExecutorsRoute(
        ClusterID clusterId,
        BiFunction<ResourceCluster, GetTaskExecutorsRequest, CompletableFuture<List<TaskExecutorID>>> taskExecutors) {
        final GetTaskExecutorsRequest empty = new GetTaskExecutorsRequest(ImmutableMap.of());
        return entity(
            Jackson.optionalEntityUnmarshaller(GetTaskExecutorsRequest.class),
            request -> {
                if (request == null) {
                    request = empty;
                }
                return withFuture(taskExecutors.apply(gateway.getClusterFor(clusterId), request));
            });
    }

    private Route getTaskExecutorState(ClusterID clusterID, TaskExecutorID taskExecutorID) {
        CompletableFuture<ResourceCluster.TaskExecutorStatus> statusOverview =
            gateway.getClusterFor(clusterID).getTaskExecutorState(taskExecutorID);
        return withFuture(statusOverview);
    }

    private Route disableTaskExecutors(ClusterID clusterID) {
        return entity(Jackson.unmarshaller(DisableTaskExecutorsRequest.class), request -> {
            log.info("POST /api/v1/resourceClusters/{}/disableTaskExecutors called with body {}",
                clusterID, request);
            return withFuture(gateway.getClusterFor(clusterID).disableTaskExecutorsFor(
                request.getAttributes(),
                Instant.now().plus(Duration.ofHours(request.getExpirationDurationInHours())),
                request.getTaskExecutorID()));
        });
    }

    private Route setScalerStatus(String clusterID) {
        return entity(Jackson.unmarshaller(SetResourceClusterScalerStatusRequest.class),
            request -> {
                log.info("POST /api/v1/resourceClusters/{}/setScalerStatus called with body {}",
                    clusterID, request);
                return withFuture(gateway.getClusterFor(request.getClusterID())
                    .setScalerStatus(request.getClusterID(), request.getSkuId(),
                        request.getEnabled(), request.getExpirationDurationInSeconds()));
            });
    }

    private ClusterID getClusterID(String clusterName) {
        return ClusterID.of(clusterName);
    }

    private TaskExecutorID getTaskExecutorID(String resourceName) {
        return TaskExecutorID.of(resourceName);
    }

    /*
    Host route section.
     */

    private Route getResourceClusterInstanceRoute(String clusterId) {
        log.info("GET /api/v1/resourceClusters/{} called", clusterId);
        return parameterMap(param ->
            alwaysCache(routeResultCache, getRequestUriKeyer, () -> extractUri(
                uri -> completeAsync(
                    this.resourceClusterRouteHandler.get(
                        GetResourceClusterSpecRequest.builder().id(ClusterID.of(clusterId))
                            .build()),
                    resp -> completeOK(
                        resp,
                        Jackson.marshaller()),
                    Endpoints.RESOURCE_CLUSTERS,
                    HttpRequestMetrics.HttpVerb.GET))));
    }

    private Route provisionResourceClustersRoute() {
        return entity(Jackson.unmarshaller(ProvisionResourceClusterRequest.class),
            resClusterSpec -> {
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

    private Route getRegisteredResourceClustersRoute() {
        log.info("GET /api/v1/resourceClusters called");
        return parameterMap(param ->
            alwaysCache(routeResultCache, getRequestUriKeyer, () -> extractUri(
                uri -> {
                    return completeAsync(
                        this.resourceClusterRouteHandler.get(
                            ListResourceClusterRequest.builder().build()),
                        resp -> completeOK(
                            resp,
                            Jackson.marshaller()),
                        Endpoints.RESOURCE_CLUSTERS,
                        HttpRequestMetrics.HttpVerb.GET);
                })));
    }

    private Route deleteResourceClusterInstanceRoute(String clusterId) {
        log.info("DELETE api/v1/resourceClusters/{}", clusterId);
        return completeAsync(
            this.resourceClusterRouteHandler.delete(ClusterID.of(clusterId)),
            resp -> completeOK(
                resp,
                Jackson.marshaller()),
            Endpoints.RESOURCE_CLUSTERS,
            HttpVerb.DELETE);
    }

    private Route scaleClusterSku(String clusterId) {
        return entity(Jackson.unmarshaller(ScaleResourceRequest.class), skuScaleRequest -> {
            log.info("POST api/v1/resourceClusters/{}/scaleSku {}", clusterId, skuScaleRequest);
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


    private Route upgradeCluster(String clusterId) {
        return entity(Jackson.unmarshaller(UpgradeClusterContainersRequest.class),
            upgradeRequest -> {
                log.info("POST api/v1/resourceClusters/{}/upgrade {}", clusterId, upgradeRequest);
                final CompletionStage<UpgradeClusterContainersResponse> response =
                    this.resourceClusterRouteHandler.upgrade(upgradeRequest);

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

    private Route createSingleScaleRule(String clusterId) {
        return entity(Jackson.unmarshaller(CreateResourceClusterScaleRuleRequest.class),
            scaleRuleReq -> {
                log.info("POST api/v1/resourceClusters/{}/scaleRule {}", clusterId, scaleRuleReq);
                final CompletionStage<GetResourceClusterScaleRulesResponse> response =
                    this.resourceClusterRouteHandler.createSingleScaleRule(scaleRuleReq);

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

    private Route createAllScaleRules(String clusterId) {
        return entity(Jackson.unmarshaller(CreateAllResourceClusterScaleRulesRequest.class),
            scaleRuleReq -> {
                log.info("POST api/v1/resourceClusters/{}/scaleRules {}", clusterId, scaleRuleReq);
                final CompletionStage<GetResourceClusterScaleRulesResponse> response =
                    this.resourceClusterRouteHandler.createAllScaleRule(scaleRuleReq);

                return completeAsync(
                    response.thenCombineAsync(
                        this.gateway.getClusterFor(getClusterID(clusterId))
                            .refreshClusterScalerRuleSet(),
                        (createResp, dontCare) -> createResp),
                    resp -> complete(
                        StatusCodes.ACCEPTED,
                        resp,
                        Jackson.marshaller()),
                    Endpoints.RESOURCE_CLUSTERS,
                    HttpRequestMetrics.HttpVerb.POST
                );
            });
    }

    private Route getScaleRules(String clusterId) {
        log.info("GET /api/v1/resourceClusters/{}/scaleRules called", clusterId);
        return parameterMap(param ->
            alwaysCache(routeResultCache, getRequestUriKeyer, () -> extractUri(
                uri -> completeAsync(
                    this.resourceClusterRouteHandler.getClusterScaleRules(
                        GetResourceClusterScaleRulesRequest.builder()
                            .clusterId(getClusterID(clusterId)).build()),
                    resp -> completeOK(
                        resp,
                        Jackson.marshaller()),
                    Endpoints.RESOURCE_CLUSTERS,
                    HttpVerb.GET))));
    }

    private Route cacheJobArtifacts(String clusterId) {
        return entity(Jackson.unmarshaller(JobArtifactsToCacheRequest.class), request -> {
            log.info("POST /api/v1/resourceClusters/{}/cacheJobArtifacts {}", clusterId, request);
            final CompletionStage<Ack> response =
                gateway.getClusterFor(getClusterID(clusterId))
                    .addNewJobArtifactsToCache(request.getClusterID(), request.getArtifacts());

            return completeAsync(
                response.thenApply(dontCare -> new BaseResponse(request.requestId,
                    BaseResponse.ResponseCode.SUCCESS, "job artifacts stored successfully")),
                resp -> complete(
                    StatusCodes.CREATED,
                    request.getArtifacts(),
                    Jackson.marshaller()),
                Endpoints.RESOURCE_CLUSTERS,
                HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route removeJobArtifactsToCache(String clusterId) {
        return entity(Jackson.unmarshaller(JobArtifactsToCacheRequest.class), request -> {
            log.info("DELETE /api/v1/resourceClusters/{}/cacheJobArtifacts {}", clusterId, request);

            final CompletionStage<Ack> response =
                gateway.getClusterFor(getClusterID(clusterId))
                    .removeJobArtifactsToCache(request.getArtifacts());

            return completeAsync(
                response.thenApply(dontCare -> new BaseResponse(request.requestId,
                    BaseResponse.ResponseCode.SUCCESS, "job artifacts removed successfully")),
                resp -> complete(
                    StatusCodes.OK,
                    request.getArtifacts(),
                    Jackson.marshaller()),
                Endpoints.RESOURCE_CLUSTERS,
                HttpRequestMetrics.HttpVerb.DELETE
            );
        });
    }
}
