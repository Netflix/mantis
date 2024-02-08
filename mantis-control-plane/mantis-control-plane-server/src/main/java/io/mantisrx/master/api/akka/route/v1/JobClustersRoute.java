/*
 * Copyright 2019 Netflix, Inc.
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
import static akka.http.javadsl.server.directives.CachingDirectives.cache;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobClustersRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLAResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoResponse;

import akka.actor.ActorSystem;
import akka.http.caching.javadsl.Cache;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/***
 * JobClustersRoute
 *  Defines the following end points:
 *  api/v1/jobsClusters                               (GET, POST)
 *  api/v1/jobClusters/{}/latestJobDiscoveryInfo      (GET)
 *  api/v1/jobClusters/{}                             (GET, POST, PUT, DELETE)
 *  api/v1/jobClusters/{}/actions/updateArtifact      (POST)
 *  api/v1/jobClusters/{}/actions/updateSla           (POST)
 *  api/v1/jobClusters/{}/actions/updateMigrationStrategy       (POST)
 *  api/v1/jobClusters/{}/actions/updateLabel                   (POST)
 *  api/v1/jobClusters/{}/actions/enableCluster                 (POST)
 *  api/v1/jobClusters/{}/actions/disableCluster                (POST)
 *  api/v1/jobClusters/{}/actions/updateSchedulingInfo          (POST)
 */
public class JobClustersRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobClustersRoute.class);
    private static final PathMatcher0 JOBCLUSTERS_API_PREFIX =
            segment("api").slash("v1").slash("jobClusters");

    private final JobClusterRouteHandler jobClusterRouteHandler;
    private final Cache<Uri, RouteResult> routeResultCache;

    public JobClustersRoute(final JobClusterRouteHandler jobClusterRouteHandler,
                            final ActorSystem actorSystem) {
        this.jobClusterRouteHandler = jobClusterRouteHandler;
        MasterConfiguration config = ConfigurationProvider.getConfig();
        this.routeResultCache = createCache(actorSystem, config.getApiCacheMinSize(), config.getApiCacheMaxSize(),
                config.getApiCacheTtlMilliseconds());
    }

    public Route constructRoutes() {
        return pathPrefix(
                JOBCLUSTERS_API_PREFIX,
                () -> concat(
                        // api/v1/jobClusters
                        pathEndOrSingleSlash(() -> concat(

                                // GET
                                get(this::getJobClustersRoute),

                                // POST
                                post(this::postJobClustersRoute))
                        ),

                        // api/v1/jobClusters/{}
                        path(
                                PathMatchers.segment(),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // GET
                                        get(() -> getJobClusterInstanceRoute(clusterName)),

                                        // PUT
                                        put(() -> putJobClusterInstanceRoute(clusterName)),

                                        // DELETE
                                        delete(() -> deleteJobClusterInstanceRoute(clusterName)))
                                )
                        ),

                        // api/v1/jobClusters/{}/latestJobDiscoveryInfo
                        path(
                            PathMatchers.segment().slash("latestJobDiscoveryInfo"),
                            (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                // GET
                                get(() -> getLatestJobDiscoveryInfo(clusterName))
                            ))
                         ),
                        // api/v1/jobClusters/{}/actions/updateArtifact
                        path(
                                PathMatchers.segment().slash("actions").slash("updateArtifact"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> updateClusterArtifactRoute(clusterName))
                                ))
                        ),
                        // api/v1/jobClusters/{}/actions/updateSchedulingInfo
                        path(
                            PathMatchers.segment().slash("actions").slash("updateSchedulingInfo"),
                            (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                // POST
                                post(() -> updateClusterSchedulingInfo(clusterName))
                            ))
                        ),

                        // api/v1/jobClusters/{}/actions/updateSla
                        pathPrefix(
                                PathMatchers.segment().slash("actions").slash("updateSla"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> updateClusterSlaRoute(clusterName))
                                ))
                        ),

                        // api/v1/jobClusters/{}/actions/updateMigrationStrategy
                        pathPrefix(
                                PathMatchers.segment()
                                            .slash("actions")
                                            .slash("updateMigrationStrategy"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> updateMigrationStrategyRoute(clusterName))
                                ))
                        ),

                        // api/v1/jobClusters/{}/actions/updateLabel
                        pathPrefix(
                                PathMatchers.segment().slash("actions").slash("updateLabel"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> updateJobClusterLabelRoute(clusterName))
                                ))
                        ),

                        // api/v1/jobClusters/{}/actions/enableCluster
                        pathPrefix(
                                PathMatchers.segment().slash("actions").slash("enableCluster"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> updateJobClusterStateEnableRoute(clusterName))
                                ))
                        ),

                        // api/v1/jobClusters/{}/actions/disableCluster
                        pathPrefix(
                                PathMatchers.segment().slash("actions").slash("disableCluster"),
                                (clusterName) -> pathEndOrSingleSlash(() -> concat(

                                        // POST
                                        post(() -> updateJobClusterStateDisableRoute(clusterName))
                                ))
                        )
                )
        );
    }


    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/jobClusters routes");
        return super.createRoute(routeFilter);
    }

    private Route getJobClustersRoute() {
        logger.trace("GET /api/v1/jobClusters called");
        return parameterMap(param ->
                alwaysCache(routeResultCache, getRequestUriKeyer, () -> extractUri(
                uri -> {
                    logger.debug("GET all job clusters");
                    return completeAsync(
                        jobClusterRouteHandler.getAllJobClusters(
                            new ListJobClustersRequest()),
                        resp -> completeOK(
                            resp.getJobClusters(
                                param.getOrDefault(
                                    ParamName.JOBCLUSTER_FILTER_MATCH,
                                    null),
                                this.parseInteger(param.getOrDefault(
                                    ParamName.PAGINATION_LIMIT,
                                    null)),
                                this.parseInteger(param.getOrDefault(
                                    ParamName.PAGINATION_OFFSET,
                                    null)),
                                param.getOrDefault(ParamName.SORT_BY, null),
                                this.parseBoolean(param.getOrDefault(
                                    ParamName.SORT_ASCENDING,
                                    null)),
                                uri),
                            Jackson.marshaller(super.parseFilter(
                                param.getOrDefault(ParamName.PROJECTION_FIELDS, null),
                                null))),
                        HttpRequestMetrics.Endpoints.JOB_CLUSTERS,
                        HttpRequestMetrics.HttpVerb.GET);
                })));
    }

    private Route postJobClustersRoute() {
        return entity(Jackson.unmarshaller(NamedJobDefinition.class), jobClusterDefn -> {

            logger.info("POST /api/v1/jobClusters called {}", jobClusterDefn);

            CompletionStage<GetJobClusterResponse> response;
            final CreateJobClusterRequest createJobClusterRequest;
            try {
                createJobClusterRequest =
                    JobClusterProtoAdapter.toCreateJobClusterRequest(jobClusterDefn);
                // sequentially chaining the createJobClusterRequest and getJobClusterRequest
                // when previous is successful
                response =
                    jobClusterRouteHandler
                        .create(createJobClusterRequest)
                        .thenCompose(t -> {
                            if (t.responseCode.getValue() >= 200 &&
                                t.responseCode.getValue() < 300) {
                                final GetJobClusterRequest request = new GetJobClusterRequest(
                                    t.getJobClusterName());
                                return jobClusterRouteHandler.getJobClusterDetails(request);
                            } else {
                                CompletableFuture<GetJobClusterResponse> responseCompletableFuture =
                                    new CompletableFuture<>();
                                responseCompletableFuture.complete(
                                    new JobClusterManagerProto.GetJobClusterResponse(
                                        t.requestId,
                                        t.responseCode,
                                        t.message,
                                        Optional.empty()));
                                return responseCompletableFuture;
                            }
                        });
            } catch (IllegalArgumentException ex) {
                CompletableFuture<GetJobClusterResponse> resp = new CompletableFuture<>();
                resp.complete(
                    new GetJobClusterResponse(
                        0L,
                        BaseResponse.ResponseCode.CLIENT_ERROR,
                        "Invalid request payload: " + ex.getMessage(),
                        Optional.empty())
                    );

                response = resp;
            }

            return completeAsync(
                response,
                resp -> {
                    HttpResponse httpResponse = this.toDefaultHttpResponse(resp);
                    return complete(
                            httpResponse.status().equals(StatusCodes.OK) ?
                                StatusCodes.CREATED : httpResponse.status(), // override GET response back to CREATED
                            resp.getJobCluster(),
                            Jackson.marshaller());
                },
                HttpRequestMetrics.Endpoints.JOB_CLUSTERS,
                HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route getLatestJobDiscoveryInfo(String clusterName) {
        logger.trace("GET /api/v1/jobClusters/{}/latestJobDiscoveryInfo called", clusterName);

        return parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_FIELDS, (fields) ->
            cache(routeResultCache, getRequestUriKeyer, () ->
                extractUri(uri -> {
                    logger.debug("GET latest job discovery info for {}", clusterName);
                    return completeAsync(
                        jobClusterRouteHandler.getLatestJobDiscoveryInfo(new GetLatestJobDiscoveryInfoRequest(clusterName)),
                        resp -> {
                            HttpResponse httpResponse = this.toDefaultHttpResponse(resp);
                            return complete(
                                httpResponse.status(),
                                resp.getDiscoveryInfo().orElse(null),
                                Jackson.marshaller(super.parseFilter(fields.orElse(null),
                                                                     null)));
                        },
                        HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_LATEST_JOB_DISCOVERY_INFO,
                        HttpRequestMetrics.HttpVerb.GET);
                })));
    }

    private Route getJobClusterInstanceRoute(String clusterName) {
        logger.info("GET /api/v1/jobClusters/{} called", clusterName);

        return parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_FIELDS, (fields) ->
                completeAsync(
                        jobClusterRouteHandler.getJobClusterDetails(new GetJobClusterRequest(
                                clusterName)),
                        resp -> {
                            HttpResponse httpResponse = this.toDefaultHttpResponse(resp);
                            return complete(
                                    httpResponse.status(),
                                    resp.getJobCluster(),
                                    Jackson.marshaller(super.parseFilter(fields.orElse(null),
                                            null)));
                        },
                        HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE,
                        HttpRequestMetrics.HttpVerb.GET));
    }


    private Route putJobClusterInstanceRoute(String clusterName) {

        return entity(Jackson.unmarshaller(NamedJobDefinition.class), jobClusterDefn -> {
            logger.info("PUT /api/v1/jobClusters/{} called {}", clusterName, jobClusterDefn);

            CompletionStage<UpdateJobClusterResponse> updateResponse;

            try {
                CompletableFuture<UpdateJobClusterResponse> resp = new CompletableFuture<>();
                final UpdateJobClusterRequest request = JobClusterProtoAdapter
                    .toUpdateJobClusterRequest(jobClusterDefn);
                if (jobClusterDefn.getJobDefinition() == null) {
                    // if request payload is invalid
                    resp.complete(
                        new UpdateJobClusterResponse(
                            request.requestId,
                            BaseResponse.ResponseCode.CLIENT_ERROR,
                            "Invalid request payload."));

                    updateResponse = resp;
                } else if (!clusterName.equals(jobClusterDefn.getJobDefinition().getName())) {
                    // if cluster name specified in request payload does not match with what specified in
                    // the endpoint path segment
                    resp.complete(
                        new UpdateJobClusterResponse(
                            request.requestId,
                            BaseResponse.ResponseCode.CLIENT_ERROR,
                            String.format(
                                "Cluster name specified in request payload %s " +
                                    "does not match with what specified in resource path %s",
                                jobClusterDefn.getJobDefinition().getName(),
                                clusterName)));

                    updateResponse = resp;
                } else {
                    // everything look ok so far, process the request!
                    updateResponse = jobClusterRouteHandler.update(
                        JobClusterProtoAdapter.toUpdateJobClusterRequest(jobClusterDefn));
                }
            } catch (IllegalArgumentException ex) {
                CompletableFuture<UpdateJobClusterResponse> resp = new CompletableFuture<>();
                resp.complete(
                    new UpdateJobClusterResponse(
                        0L,
                        BaseResponse.ResponseCode.CLIENT_ERROR,
                        "Invalid request payload: " + ex.getMessage()));

                updateResponse = resp;
            }

            CompletionStage<GetJobClusterResponse> response = updateResponse
                    .thenCompose(t -> {
                        if (t.responseCode.getValue() >= 200 &&
                            t.responseCode.getValue() < 300) {
                            return jobClusterRouteHandler.getJobClusterDetails(
                                    new GetJobClusterRequest(clusterName));
                        } else {
                            CompletableFuture<GetJobClusterResponse> responseCompletableFuture = new CompletableFuture<>();
                            responseCompletableFuture.complete(
                                    new JobClusterManagerProto.GetJobClusterResponse(
                                            t.requestId,
                                            t.responseCode,
                                            t.message,
                                            Optional.empty()));
                            return responseCompletableFuture;

                        }
                    });

            return completeAsync(
                    response,
                    resp -> {
                        HttpResponse httpResponse = this.toDefaultHttpResponse(resp);
                        return complete(
                                httpResponse.status(),
                                resp.getJobCluster(),
                                Jackson.marshaller());
                    },
                    HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE,
                    HttpRequestMetrics.HttpVerb.PUT);
        });
    }

    private Route deleteJobClusterInstanceRoute(String clusterName) {

        return parameterOptional("user", user -> {
            logger.info("DELETE /api/v1/jobClusters/{} called", clusterName);

            String userStr = user.orElse(null);
            if (Strings.isNullOrEmpty(userStr)) {
                return complete(StatusCodes.BAD_REQUEST, "Missing required parameter 'user'");
            } else {
                return completeAsync(
                        jobClusterRouteHandler.delete(new DeleteJobClusterRequest(userStr, clusterName)),
                        resp -> complete(StatusCodes.ACCEPTED, ""),
                        HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE,
                        HttpRequestMetrics.HttpVerb.DELETE
                );
            }
        });
    }


    private Route updateClusterArtifactRoute(String clusterName) {
        return entity(Jackson.unmarshaller(UpdateJobClusterArtifactRequest.class), request -> {
            logger.info(
                    "POST /api/v1/jobClusters/{}/actions/updateArtifact called {}",
                    clusterName,
                    request);

            CompletionStage<UpdateJobClusterArtifactResponse> updateResponse;

            if (!clusterName.equals(request.getClusterName())) {
                // if cluster name specified in request payload does not match with what specified in
                // the endpoint path segment
                CompletableFuture<UpdateJobClusterArtifactResponse> resp = new CompletableFuture<>();
                resp.complete(
                        new UpdateJobClusterArtifactResponse(
                                request.requestId,
                                BaseResponse.ResponseCode.CLIENT_ERROR,
                                String.format(
                                        "Cluster name specified in request payload %s " +
                                        "does not match with what specified in resource path %s",
                                        request.getClusterName(),
                                        clusterName)));

                updateResponse = resp;
            } else {
                // everything look ok so far, process the request!
                updateResponse = jobClusterRouteHandler.updateArtifact(request);
            }

            return completeAsync(
                    updateResponse,
                    resp -> complete(StatusCodes.NO_CONTENT, ""),
                    HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_UPDATE_ARTIFACT,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route updateClusterSchedulingInfo(String clusterName) {
        return entity(Jackson.unmarshaller(UpdateSchedulingInfoRequest.class), request -> {
            logger.info(
                "POST /api/v1/jobClusters/{}/actions/updateSchedulingInfo called {}",
                clusterName,
                request);

            CompletionStage<UpdateSchedulingInfoResponse> updateResponse =
                jobClusterRouteHandler.updateSchedulingInfo(clusterName, request);

            return completeAsync(
                updateResponse,
                resp -> complete(StatusCodes.NO_CONTENT, ""),
                HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_UPDATE_ARTIFACT,
                HttpRequestMetrics.HttpVerb.POST
            );
        });
    }


    private Route updateClusterSlaRoute(String clusterName) {
        return entity(Jackson.unmarshaller(UpdateJobClusterSLARequest.class), request -> {
            logger.info(
                    "POST /api/v1/jobClusters/{}/actions/updateSla called {}",
                    clusterName,
                    request);

            CompletionStage<UpdateJobClusterSLAResponse> updateResponse;

            if (!clusterName.equals(request.getClusterName())) {
                // if cluster name specified in request payload does not match with what specified in
                // the endpoint path segment
                CompletableFuture<UpdateJobClusterSLAResponse> resp = new CompletableFuture<>();
                resp.complete(
                        new UpdateJobClusterSLAResponse(
                                request.requestId,
                                BaseResponse.ResponseCode.CLIENT_ERROR,
                                String.format(
                                        "Cluster name specified in request payload %s " +
                                        "does not match with what specified in resource path %s",
                                        request.getClusterName(),
                                        clusterName)));

                updateResponse = resp;
            } else {
                // everything look ok so far, process the request!
                updateResponse = jobClusterRouteHandler.updateSLA(request);
            }

            return completeAsync(
                    updateResponse,
                    resp -> complete(StatusCodes.NO_CONTENT, ""),
                    HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_UPDATE_SLA,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route updateMigrationStrategyRoute(String clusterName) {
        return entity(
                Jackson.unmarshaller(UpdateJobClusterWorkerMigrationStrategyRequest.class),
                request -> {
                    logger.info(
                            "POST /api/v1/jobClusters/{}/actions/updateMigrationStrategy called {}",
                            clusterName,
                            request);

                    CompletionStage<UpdateJobClusterWorkerMigrationStrategyResponse> updateResponse;

                    if (!clusterName.equals(request.getClusterName())) {
                        // if cluster name specified in request payload does not match with what specified in
                        // the endpoint path segment
                        CompletableFuture<UpdateJobClusterWorkerMigrationStrategyResponse> resp = new CompletableFuture<>();
                        resp.complete(
                                new UpdateJobClusterWorkerMigrationStrategyResponse(
                                        request.requestId,
                                        BaseResponse.ResponseCode.CLIENT_ERROR,
                                        String.format(
                                                "Cluster name specified in request payload %s " +
                                                "does not match with what specified in resource path %s",
                                                request.getClusterName(),
                                                clusterName)));

                        updateResponse = resp;
                    } else {
                        // everything look ok so far, process the request!
                        updateResponse = jobClusterRouteHandler.updateWorkerMigrateStrategy(request);
                    }

                    return completeAsync(
                            updateResponse,
                            resp -> complete(StatusCodes.NO_CONTENT, ""),
                            HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_UPDATE_MIGRATION_STRATEGY,
                            HttpRequestMetrics.HttpVerb.POST
                    );
                });
    }

    private Route updateJobClusterLabelRoute(String clusterName) {
        return entity(Jackson.unmarshaller(UpdateJobClusterLabelsRequest.class), request -> {
            logger.info(
                    "POST /api/v1/jobClusters/{}/actions/updateLabel called {}",
                    clusterName,
                    request);

            CompletionStage<UpdateJobClusterLabelsResponse> updateResponse;

            if (!clusterName.equals(request.getClusterName())) {
                // if cluster name specified in request payload does not match with what specified in
                // the endpoint path segment
                CompletableFuture<UpdateJobClusterLabelsResponse> resp = new CompletableFuture<>();
                resp.complete(
                        new UpdateJobClusterLabelsResponse(
                                request.requestId,
                                BaseResponse.ResponseCode.CLIENT_ERROR,
                                String.format(
                                        "Cluster name specified in request payload %s " +
                                        "does not match with what specified in resource path %s",
                                        request.getClusterName(),
                                        clusterName)));

                updateResponse = resp;
            } else {
                // everything look ok so far, process the request!
                updateResponse = jobClusterRouteHandler.updateLabels(request);
            }

            return completeAsync(
                    updateResponse,
                    resp -> complete(StatusCodes.NO_CONTENT, ""),
                    HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_UPDATE_LABEL,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route updateJobClusterStateEnableRoute(String clusterName) {
        return entity(Jackson.unmarshaller(EnableJobClusterRequest.class), request -> {
            logger.info(
                    "POST /api/v1/jobClusters/{}/actions/enableCluster called {}",
                    clusterName,
                    request);

            CompletionStage<EnableJobClusterResponse> updateResponse;

            if (!clusterName.equals(request.getClusterName())) {
                // if cluster name specified in request payload does not match with what specified in
                // the endpoint path segment
                CompletableFuture<EnableJobClusterResponse> resp = new CompletableFuture<>();
                resp.complete(
                        new EnableJobClusterResponse(
                                request.requestId,
                                BaseResponse.ResponseCode.CLIENT_ERROR,
                                String.format(
                                        "Cluster name specified in request payload %s " +
                                        "does not match with what specified in resource path %s",
                                        request.getClusterName(),
                                        clusterName)));

                updateResponse = resp;
            } else {
                // everything look ok so far, process the request!
                updateResponse = jobClusterRouteHandler.enable(request);
            }

            return completeAsync(
                    updateResponse,
                    resp -> complete(StatusCodes.NO_CONTENT, ""),
                    HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_ENABLE_CLUSTER,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }

    private Route updateJobClusterStateDisableRoute(String clusterName) {
        return entity(Jackson.unmarshaller(DisableJobClusterRequest.class), request -> {
            logger.info(
                    "POST /api/v1/jobClusters/{}/actions/disableCluster called {}",
                    clusterName,
                    request);

            CompletionStage<DisableJobClusterResponse> updateResponse;

            if (!clusterName.equals(request.getClusterName())) {
                // if cluster name specified in request payload does not match with what specified in
                // the endpoint path segment
                CompletableFuture<DisableJobClusterResponse> resp = new CompletableFuture<>();
                resp.complete(
                        new DisableJobClusterResponse(
                                request.requestId,
                                BaseResponse.ResponseCode.CLIENT_ERROR,
                                String.format(
                                        "Cluster name specified in request payload %s " +
                                        "does not match with what specified in resource path %s",
                                        request.getClusterName(),
                                        clusterName)));

                updateResponse = resp;
            } else {
                // everything look ok so far, process the request!
                updateResponse = jobClusterRouteHandler.disable(request);
            }

            return completeAsync(
                    updateResponse,
                    resp -> complete(StatusCodes.NO_CONTENT, ""),
                    HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_ACTION_DISABLE_CLUSTER,
                    HttpRequestMetrics.HttpVerb.POST
            );
        });
    }
}
