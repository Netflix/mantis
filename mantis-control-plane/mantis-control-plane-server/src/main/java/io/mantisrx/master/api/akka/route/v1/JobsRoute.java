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
import static io.mantisrx.master.api.akka.route.utils.JobRouteUtils.createListJobsRequest;
import static io.mantisrx.master.api.akka.route.utils.JobRouteUtils.createWorkerStatusRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest.DEFAULT_LIST_ARCHIVED_WORKERS_LIMIT;

import akka.actor.ActorSystem;
import akka.http.caching.javadsl.Cache;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.*;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.japi.Pair;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.PostJobStatusRequest;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.http.api.CompactJobInfo;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * JobsRoute
 *  Defines the following end points:
 *  api/v1/jobs                        (GET, POST)
 *  api/v1/jobClusters/{}/jobs         (GET, POST)
 *  api/v1/jobs/{}                     (GET, DELETE)
 *  api/v1/jobClusters/{}/jobs/{}      (GET)
 *  api/v1/jobs/{}/archivedWorkers     (GET)
 *  api/v1/jobs/actions/quickSubmit    (POST)
 *  api/v1/jobs/actions/postJobStatus  (POST)
 *  api/v1/jobs/{}/actions/scaleStage  (POST)
 *  api/v1/jobs/{}/actions/resubmitWorker (POST)
 */
public class JobsRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobsRoute.class);
    private static final PathMatcher0 JOBS_API_PREFIX = segment("api").slash("v1").slash("jobs");

    private static final PathMatcher1<String> CLUSTER_JOBS_API_PREFIX =
            segment("api").slash("v1")
                          .slash("jobClusters")
                          .slash(PathMatchers.segment())
                          .slash("jobs");

    private final JobRouteHandler jobRouteHandler;
    private final JobClusterRouteHandler clusterRouteHandler;
    private final MasterConfiguration config;
    private final Cache<Uri, RouteResult> routeResultCache;

    public JobsRoute(
            final JobClusterRouteHandler clusterRouteHandler,
            final JobRouteHandler jobRouteHandler,
            final ActorSystem actorSystem) {
        this.jobRouteHandler = jobRouteHandler;
        this.clusterRouteHandler = clusterRouteHandler;
        this.config = ConfigurationProvider.getConfig();
        this.routeResultCache = createCache(actorSystem, config.getApiCacheMinSize(), config.getApiCacheMaxSize(), config.getApiCacheTtlMilliseconds());
    }


    public Route constructRoutes() {
        return concat(
                pathPrefix(JOBS_API_PREFIX, () -> concat(
                        // api/v1/jobs
                        pathEndOrSingleSlash(() -> concat(
                                // GET - list jobs
                                get(this::getJobsRoute),

                                // POST - submit a job
                                post(this::postJobsRoute)
                        )),

                        // api/v1/jobs/{jobId}
                        path(
                                PathMatchers.segment(),
                                (jobId) -> pathEndOrSingleSlash(() -> concat(

                                        // GET - retrieve job detail by job ID
                                        get(() -> getJobInstanceRoute(jobId)),

                                        // DELETE - permanently kill a job.
                                        delete(() -> deleteJobInstanceRoute(jobId)),

                                        // reject post
                                        post(() -> complete(StatusCodes.METHOD_NOT_ALLOWED))
                                ))
                        ),

                        path(PathMatchers.segment().slash("archivedWorkers"),
                             (jobId) -> pathEndOrSingleSlash(() -> concat(
                                     get(()-> getArchivedWorkers(jobId))
                             ))
                        ),

                        // api/v1/jobs/actions/quickSubmit
                        path(
                                PathMatchers.segment("actions").slash("quickSubmit"),
                                () -> pathEndOrSingleSlash(
                                        () ->
                                                // POST - quick submit a job
                                                post(this::postJobInstanceQuickSubmitRoute)
                                )
                        ),

                        // api/v1/jobs/actions/postJobStatus
                        path(
                                PathMatchers.segment("actions").slash("postJobStatus"),
                                () -> pathEndOrSingleSlash(
                                        () ->
                                                // POST Job Status
                                                post(this::postJobStatusRoute)
                                )
                        ),

                        // api/v1/jobs/{jobId}/actions/scaleStage
                        path(
                                PathMatchers.segment().slash("actions").slash("scaleStage"),
                                (jobId) -> pathEndOrSingleSlash(
                                        // POST - scale stage
                                        () -> post(() -> postJobInstanceScaleStageRoute(jobId))
                                )
                        ),

                        // api/v1/jobs/{jobId}/actions/resubmitWorker
                        path(
                                PathMatchers.segment().slash("actions").slash("resubmitWorker"),
                                (jobId) -> pathEndOrSingleSlash(
                                        () ->
                                                // POST - resubmit worker
                                                post(() -> postJobInstanceResubmitWorkerRoute(jobId))
                                )
                        ))
                ),
                pathPrefix(CLUSTER_JOBS_API_PREFIX, (cluster) -> concat(

                        // api/v1/jobClusters/{clusterName}/jobs
                        pathEndOrSingleSlash(() -> concat(

                                // GET - list jobs
                                get(() -> getJobsRoute(Optional.of(cluster))),

                                // POST - submit a job
                                post(() -> postJobsRoute(Optional.of(cluster))))
                        ),

                        // api/v1/jobClusters/{clusterName}/jobs/{jobId}
                        path(
                                PathMatchers.segment(),
                                (jobId) -> pathEndOrSingleSlash(() -> concat(

                                        // GET - retrieve job detail by cluster & job ID
                                        get(() -> getJobInstanceRoute(Optional.of(cluster), jobId)),

                                        // reject post
                                        post(() -> complete(StatusCodes.METHOD_NOT_ALLOWED))

                                )))
                           )
                )
        );
    }


    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/jobs routes");
        return super.createRoute(routeFilter);
    }

    private Route getJobsRoute() {
        return getJobsRoute(Optional.empty());
    }

    private Route getJobsRoute(Optional<String> clusterName) {
        return parameterOptional(StringUnmarshallers.INTEGER, ParamName.PAGINATION_LIMIT, (pageSize) ->
                parameterOptional(StringUnmarshallers.INTEGER, ParamName.PAGINATION_OFFSET, (offset) ->
                 parameterOptional(StringUnmarshallers.BOOLEAN, ParamName.SORT_ASCENDING, (ascending) ->
                  parameterOptional(StringUnmarshallers.STRING, ParamName.SORT_BY, (sortField) ->
                   parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_FIELDS, (fields) ->
                    parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_TARGET, (target) ->
                     parameterOptional(StringUnmarshallers.BOOLEAN, ParamName.JOB_COMPACT, (isCompact) ->
                      parameterOptional(StringUnmarshallers.STRING, ParamName.JOB_FILTER_MATCH, (matching) ->
                       parameterMultiMap(params ->
                               alwaysCache(routeResultCache, getRequestUriKeyer , () -> extractUri(uri -> {
                            String endpoint;
                            if (clusterName.isPresent()) {
                                logger.debug("GET /api/v1/jobClusters/{}/jobs called", clusterName);
                                endpoint = HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_JOBS;
                            } else {
                                logger.debug("GET /api/v1/jobs called");
                                endpoint = HttpRequestMetrics.Endpoints.JOBS;
                            }

                            JobClusterManagerProto.ListJobsRequest listJobsRequest = createListJobsRequest(
                                    params,
                                    clusterName.map(s -> Optional.of("^" + s + "$")).orElse(matching),
                                    true);


                              return completeAsync(
                                    jobRouteHandler.listJobs(listJobsRequest),
                                    resp -> completeOK(
                                            (isCompact.isPresent() && isCompact.get()) ?
                                                    resp.getJobList(
                                                            JobClusterProtoAdapter::toCompactJobInfo,
                                                            CompactJobInfo.class,
                                                            pageSize.orElse(null),
                                                            offset.orElse(null),
                                                            sortField.orElse(null),
                                                            ascending.orElse(null),
                                                            uri)
                                                    : resp.getJobList(pageSize.orElse(null),
                                                                      offset.orElse(null),
                                                                      sortField.orElse(null),
                                                                      ascending.orElse(null),
                                                                      uri),
                                            Jackson.marshaller(
                                                    super.parseFilter(fields.orElse(null),
                                                            target.orElse(null)))
                                    ),
                                    endpoint,
                                    HttpRequestMetrics.HttpVerb.GET
                            );
                        })))))))))));

    }


    private Route postJobsRoute() {
        return postJobsRoute(Optional.empty());
    }

    private Route postJobsRoute(Optional<String> clusterName) {

        return decodeRequest(() -> entity(
                Jackson.unmarshaller(MantisJobDefinition.class),
                submitJobRequest -> {
                    String endpoint;
                    if (clusterName.isPresent()) {
                        logger.info(
                                "POST /api/v1/jobClusters/{}/jobs called {}",
                                clusterName);
                        endpoint = HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_JOBS;
                    } else {
                        logger.info(
                                "POST /api/v1/jobs called {}",
                                submitJobRequest);
                        endpoint = HttpRequestMetrics.Endpoints.JOBS;
                    }
                    CompletionStage<JobClusterManagerProto.SubmitJobResponse> response = null;
                    try {

                        // validate request
                        submitJobRequest.validate(true);
                        Pair<Boolean, String> validationResult = validateSubmitJobRequest(
                                submitJobRequest,
                                clusterName);
                        if (!validationResult.first()) {
                            CompletableFuture<JobClusterManagerProto.SubmitJobResponse> resp = new CompletableFuture<>();
                            resp.complete(
                                    new JobClusterManagerProto.SubmitJobResponse(
                                            -1,
                                            BaseResponse.ResponseCode.CLIENT_ERROR,
                                            validationResult.second(),
                                            Optional.empty()));

                            response = resp;
                        } else {
                            response = clusterRouteHandler.submit(
                                    JobClusterProtoAdapter.toSubmitJobClusterRequest(
                                            submitJobRequest));
                        }
                    } catch (Exception e) {
                        logger.warn("exception in submit job request {}", submitJobRequest, e);

                        CompletableFuture<JobClusterManagerProto.SubmitJobResponse> resp = new CompletableFuture<>();
                        resp.complete(
                                new JobClusterManagerProto.SubmitJobResponse(
                                        -1,
                                        BaseResponse.ResponseCode.SERVER_ERROR,
                                        e.getMessage(),
                                        Optional.empty()));

                        response = resp;
                    }

                    CompletionStage<JobClusterManagerProto.GetJobDetailsResponse> r = response.thenCompose(
                            t -> {
                                if (t.responseCode.getValue() >= 200 &&
                                    t.responseCode.getValue() < 300) {
                                    final JobClusterManagerProto.GetJobDetailsRequest request =
                                            new JobClusterManagerProto.GetJobDetailsRequest(
                                                    submitJobRequest.getUser(),
                                                    t.getJobId().get());
                                    return jobRouteHandler.getJobDetails(request);
                                } else {
                                    CompletableFuture<JobClusterManagerProto.GetJobDetailsResponse> responseCompletableFuture =
                                            new CompletableFuture<>();
                                    responseCompletableFuture.complete(
                                            new JobClusterManagerProto.GetJobDetailsResponse(
                                                    t.requestId,
                                                    t.responseCode,
                                                    t.message,
                                                    Optional.empty()));
                                    return responseCompletableFuture;

                                }
                            });

                    return completeAsync(
                            r,
                            resp -> complete(
                                    StatusCodes.CREATED,
                                    resp.getJobMetadata().map(metaData -> new MantisJobMetadataView(metaData, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false)),
                                    Jackson.marshaller()),
                            endpoint,
                            HttpRequestMetrics.HttpVerb.POST);

                })
        );
    }

    private Route getJobInstanceRoute(String jobId) {
        return getJobInstanceRoute(Optional.empty(), jobId);
    }

    private Route getJobInstanceRoute(Optional<String> clusterName, String jobId) {
        String endpoint;
        if (clusterName.isPresent()) {
            logger.debug("GET /api/v1/jobClusters/{}/jobs/{} called", clusterName.get(), jobId);
            endpoint = HttpRequestMetrics.Endpoints.JOB_CLUSTER_INSTANCE_JOBS;
        } else {
            logger.debug("GET /api/v1/jobs/{} called", jobId);
            endpoint = HttpRequestMetrics.Endpoints.JOBS;
        }

        return parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_FIELDS, (fields) ->
                parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_TARGET, (target) ->
                completeAsync(
                        jobRouteHandler.getJobDetails(
                                new JobClusterManagerProto.GetJobDetailsRequest("masterAPI", jobId))
                                       .thenCompose(r -> {
                                           CompletableFuture<JobClusterManagerProto.GetJobDetailsResponse> resp =
                                                   new CompletableFuture<>();


                                           if (r.responseCode.getValue() >= 200 &&
                                               r.responseCode.getValue() < 300 &&
                                               clusterName.isPresent() &&
                                               r.getJobMetadata().isPresent()) {

                                               if (!clusterName.get().equals(
                                                       r.getJobMetadata().get().getClusterName())) {
                                                   String msg = String.format(
                                                           "JobId [%s] exists but does not belong to specified cluster [%s]",
                                                           jobId,
                                                           clusterName.get());
                                                   resp.complete(
                                                           new JobClusterManagerProto.GetJobDetailsResponse(
                                                                   r.requestId,
                                                                   BaseResponse.ResponseCode.CLIENT_ERROR_NOT_FOUND,
                                                                   msg,
                                                                   Optional.empty()));
                                               } else {
                                                   resp.complete(r);
                                               }

                                           } else {
                                               resp.complete(r);
                                           }
                                           return resp;
                                       }),
                        resp -> complete(
                                StatusCodes.OK,
                                resp.getJobMetadata().map(metaData -> new MantisJobMetadataView(metaData, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false)),
                                Jackson.marshaller(super.parseFilter(fields.orElse(null), target.orElse(null)))),
                        endpoint,
                        HttpRequestMetrics.HttpVerb.GET)
                  ));
    }

    private Route getArchivedWorkers(String jobId) {
        logger.info("GET /api/v1/jobs/{}/archivedWorkers called", jobId);

        Optional<JobId> parsedJobId = JobId.fromId(jobId);
        if (!parsedJobId.isPresent()){
            return complete(StatusCodes.BAD_REQUEST, super.generateFailureResponsePayload("Invalid jobId in URI", -1));
        } else {
            return parameterOptional(StringUnmarshallers.INTEGER, ParamName.PAGINATION_LIMIT, (pageSize) ->
                    parameterOptional(StringUnmarshallers.INTEGER, ParamName.PAGINATION_OFFSET, (offset) ->
                     parameterOptional(StringUnmarshallers.BOOLEAN, ParamName.SORT_ASCENDING, (ascending) ->
                      parameterOptional(StringUnmarshallers.STRING, ParamName.SORT_BY, (sortField) ->
                       parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_FIELDS, (fields) ->
                        parameterOptional(StringUnmarshallers.STRING, ParamName.PROJECTION_TARGET, (target) ->
                        parameterOptional(StringUnmarshallers.INTEGER, ParamName.SERVER_FILTER_LIMIT, (limit) ->
                         parameterMultiMap(params -> extractUri(uri -> {
                            JobClusterManagerProto.ListArchivedWorkersRequest req =
                                    new JobClusterManagerProto.ListArchivedWorkersRequest(
                                            parsedJobId.get(),
                                            limit.orElse(DEFAULT_LIST_ARCHIVED_WORKERS_LIMIT));

                            return completeAsync(
                                    jobRouteHandler.listArchivedWorkers(req),
                                    resp -> completeOK(
                                            resp.getWorkerMetadata(DataFormatAdapter::convertMantisWorkerMetadataToMantisWorkerMetadataWritable,
                                                                   MantisWorkerMetadataWritable.class,
                                                                   pageSize.orElse(null),
                                                                   offset.orElse(null),
                                                                   sortField.orElse(null),
                                                                   ascending.orElse(null),
                                                                   uri),
                                            Jackson.marshaller(super.parseFilter(fields.orElse(null), target.orElse(null)))),
                                    HttpRequestMetrics.Endpoints.JOB_INSTANCE_ARCHIVED_WORKERS,
                                    HttpRequestMetrics.HttpVerb.GET);
                        })))))))));
        }
    }

    private Route deleteJobInstanceRoute(String jobId) {

        logger.info("DELETE /api/v1/jobs/{} called", jobId);

        return parameterOptional(StringUnmarshallers.STRING, ParamName.USER, (user) ->
                parameterOptional(StringUnmarshallers.STRING, ParamName.REASON, (reason) -> {

                            String userStr = user.orElse(null);
                            String reasonStr = reason.orElse(null);
                            if (Strings.isNullOrEmpty(userStr)) {
                                return complete(StatusCodes.BAD_REQUEST, "Missing required parameter 'user'");
                            } else if (Strings.isNullOrEmpty(reasonStr)) {
                                return complete(StatusCodes.BAD_REQUEST, "Missing required parameter 'reason'");
                            } else {
                                return completeAsync(
                                        jobRouteHandler.kill(new JobClusterManagerProto.KillJobRequest(
                                                jobId,
                                                reasonStr,
                                                userStr)),
                                        resp -> complete(
                                                StatusCodes.ACCEPTED,
                                                ""),
                                        HttpRequestMetrics.Endpoints.JOB_INSTANCE,
                                        HttpRequestMetrics.HttpVerb.DELETE);
                            }
                        }
                )

        );
    }


    private Route postJobInstanceQuickSubmitRoute() {
        return entity(
                Jackson.unmarshaller(JobClusterManagerProto.SubmitJobRequest.class),
                request -> {
                    logger.info("POST /api/v1/jobs/actions/quickSubmit called");


                    final CompletionStage<JobClusterManagerProto.GetJobDetailsResponse> response =
                            clusterRouteHandler.submit(request)
                                               .thenCompose(t -> {
                                                   if (t.responseCode.getValue() >= 200 &&
                                                       t.responseCode.getValue() < 300) {


                                                       return jobRouteHandler.getJobDetails(new JobClusterManagerProto.GetJobDetailsRequest(
                                                               request.getSubmitter(),
                                                               t.getJobId().get()));
                                                   } else {
                                                       CompletableFuture<JobClusterManagerProto.GetJobDetailsResponse> responseCompletableFuture = new CompletableFuture<>();
                                                       responseCompletableFuture.complete(
                                                               new JobClusterManagerProto.GetJobDetailsResponse(
                                                                       t.requestId,
                                                                       t.responseCode,
                                                                       t.message,
                                                                       Optional.empty()));
                                                       return responseCompletableFuture;
                                                   }
                                               });

                    return completeAsync(
                            response,
                            resp -> complete(
                                    StatusCodes.CREATED,
                                    resp.getJobMetadata().map(metaData -> new MantisJobMetadataView(metaData, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false)),
                                    Jackson.marshaller()
                            ),
                            HttpRequestMetrics.Endpoints.JOBS_ACTION_QUICKSUBMIT,
                            HttpRequestMetrics.HttpVerb.POST
                    );
                });
    }


    private Route postJobStatusRoute() {
        return entity(
                Jackson.unmarshaller(PostJobStatusRequest.class), request -> {
                    logger.info("POST /api/v1/jobs/actions/postJobStatus called");

                    return completeAsync(
                            jobRouteHandler.workerStatus(createWorkerStatusRequest(request)),
                            resp -> complete(
                                    StatusCodes.NO_CONTENT,
                                    ""),
                            HttpRequestMetrics.Endpoints.JOBS_ACTION_POST_JOB_STATUS,
                            HttpRequestMetrics.HttpVerb.POST
                            );

                });
    }

    private Route postJobInstanceScaleStageRoute(String jobId) {
        return entity(
                Jackson.unmarshaller(JobClusterManagerProto.ScaleStageRequest.class),
                request -> {
                    logger.info("POST /api/v1/jobs/{}/actions/scaleStage called", jobId);

                    CompletionStage<JobClusterManagerProto.ScaleStageResponse> response = null;
                    int numWorkers = request.getNumWorkers();
                    int maxWorkersPerStage = ConfigurationProvider.getConfig().getMaxWorkersPerStage();
                    if (numWorkers > maxWorkersPerStage) {
                        CompletableFuture<JobClusterManagerProto.ScaleStageResponse> responseCompletableFuture = new CompletableFuture<>();
                        responseCompletableFuture.complete(
                                new JobClusterManagerProto.ScaleStageResponse(
                                        request.requestId,
                                        BaseResponse.ResponseCode.CLIENT_ERROR,
                                        "num workers must be less than " + maxWorkersPerStage,
                                        -1));
                        response = responseCompletableFuture;
                    } else if (jobId.equals(request.getJobId().getId())) {
                        response = jobRouteHandler.scaleStage(request);
                    } else {
                        CompletableFuture<JobClusterManagerProto.ScaleStageResponse> responseCompletableFuture = new CompletableFuture<>();
                        responseCompletableFuture.complete(
                                new JobClusterManagerProto.ScaleStageResponse(
                                        request.requestId,
                                        BaseResponse.ResponseCode.CLIENT_ERROR,
                                        String.format("JobId specified in request payload [%s] does not match with resource uri [%s]",
                                                      request.getJobId().getId(),
                                                      jobId),
                                        -1));
                        response = responseCompletableFuture;
                    }

                    return completeAsync(
                            response,
                            resp -> complete(
                                    StatusCodes.NO_CONTENT,
                                    ""),
                            HttpRequestMetrics.Endpoints.JOB_INSTANCE_ACTION_SCALE_STAGE,
                            HttpRequestMetrics.HttpVerb.POST
                    );
                });
    }

    private Route postJobInstanceResubmitWorkerRoute(String jobId) {
        return entity(
                Jackson.unmarshaller(JobClusterManagerProto.V1ResubmitWorkerRequest.class),
                request -> {
                    logger.info("POST /api/v1/jobs/{}/actions/resubmitWorker called", jobId);
                    CompletionStage<JobClusterManagerProto.ResubmitWorkerResponse> response;

                    response = jobRouteHandler.resubmitWorker(
                                new JobClusterManagerProto.ResubmitWorkerRequest(jobId,
                                        request.getWorkerNum(),
                                        request.getUser(),
                                        request.getReason()));

                    return completeAsync(
                            response,
                            resp -> complete(
                                    StatusCodes.NO_CONTENT,
                                    ""),
                            HttpRequestMetrics.Endpoints.JOB_INSTANCE_ACTION_RESUBMIT_WORKER,
                            HttpRequestMetrics.HttpVerb.POST
                    );
                });
    }

    /**
     * @return true to indicate valid, false otherwise. The String holds the error message when the request is invalid
     */
    private Pair<Boolean, String> validateSubmitJobRequest(
            MantisJobDefinition mjd,
            Optional<String> clusterNameInResource) {
        if (null == mjd) {
            logger.error("rejecting job submit request, job definition is malformed {}", mjd);
            return Pair.apply(false, "Malformed job definition.");
        }

        // must include job cluster name
        if (mjd.getName() == null || mjd.getName().length() == 0) {
            logger.info("rejecting job submit request, must include name {}", mjd);
            return Pair.apply(false, "Job definition must include name");
        }

        // validate specified job cluster name matches with what specified in REST resource endpoint
        if (clusterNameInResource.isPresent()) {
            if (!clusterNameInResource.get().equals(mjd.getName())) {
                String msg = String.format("Cluster name specified in request payload [%s] " +
                                           "does not match with what specified in resource endpoint [%s]",
                                           mjd.getName(), clusterNameInResource.get());
                logger.info("rejecting job submit request, {} {}", msg, mjd);
                return Pair.apply(false, msg);
            }
        }

        // validate scheduling info
        SchedulingInfo schedulingInfo = mjd.getSchedulingInfo();
        if (schedulingInfo != null) {
            Map<Integer, StageSchedulingInfo> stages = schedulingInfo.getStages();
            if (stages != null) {
                for (StageSchedulingInfo stageSchedInfo : stages.values()) {
                    double cpuCores = stageSchedInfo.getMachineDefinition().getCpuCores();
                    int maxCpuCores = ConfigurationProvider.getConfig()
                                                           .getWorkerMachineDefinitionMaxCpuCores();
                    if (cpuCores > maxCpuCores) {
                        logger.info(
                                "rejecting job submit request, requested CPU {} > max for {} (user: {}) (stage: {})",
                                cpuCores,
                                mjd.getName(),
                                mjd.getUser(),
                                stages);
                        return Pair.apply(
                                false,
                                "requested CPU cannot be more than max CPU per worker " +
                                maxCpuCores);
                    }
                    double memoryMB = stageSchedInfo.getMachineDefinition().getMemoryMB();
                    int maxMemoryMB = ConfigurationProvider.getConfig()
                                                           .getWorkerMachineDefinitionMaxMemoryMB();
                    if (memoryMB > maxMemoryMB) {
                        logger.info(
                                "rejecting job submit request, requested memory {} > max for {} (user: {}) (stage: {})",
                                memoryMB,
                                mjd.getName(),
                                mjd.getUser(),
                                stages);
                        return Pair.apply(
                                false,
                                "requested memory cannot be more than max memoryMB per worker " +
                                maxMemoryMB);
                    }
                    double networkMbps = stageSchedInfo.getMachineDefinition().getNetworkMbps();
                    int maxNetworkMbps = ConfigurationProvider.getConfig()
                                                              .getWorkerMachineDefinitionMaxNetworkMbps();
                    if (networkMbps > maxNetworkMbps) {
                        logger.info(
                                "rejecting job submit request, requested network {} > max for {} (user: {}) (stage: {})",
                                networkMbps,
                                mjd.getName(),
                                mjd.getUser(),
                                stages);
                        return Pair.apply(
                                false,
                                "requested network cannot be more than max networkMbps per worker " +
                                maxNetworkMbps);
                    }
                    int numberOfInstances = stageSchedInfo.getNumberOfInstances();
                    int maxWorkersPerStage = ConfigurationProvider.getConfig()
                                                                  .getMaxWorkersPerStage();
                    if (numberOfInstances > maxWorkersPerStage) {
                        logger.info(
                                "rejecting job submit request, requested num instances {} > max for {} (user: {}) (stage: {})",
                                numberOfInstances,
                                mjd.getName(),
                                mjd.getUser(),
                                stages);
                        return Pair.apply(
                                false,
                                "requested number of instances per stage cannot be more than " +
                                maxWorkersPerStage);
                    }

                    StageScalingPolicy scalingPolicy = stageSchedInfo.getScalingPolicy();
                    if (scalingPolicy != null) {
                        if (scalingPolicy.getMax() > maxWorkersPerStage) {
                            logger.info(
                                    "rejecting job submit request, requested num instances in scaling policy {} > max for {} (user: {}) (stage: {})",
                                    numberOfInstances,
                                    mjd.getName(),
                                    mjd.getUser(),
                                    stages);
                            return Pair.apply(
                                    false,
                                    "requested number of instances per stage in scaling policy cannot be more than " +
                                    maxWorkersPerStage);
                        }
                    }
                }
            }
        }
        return Pair.apply(true, "");
    }

}
