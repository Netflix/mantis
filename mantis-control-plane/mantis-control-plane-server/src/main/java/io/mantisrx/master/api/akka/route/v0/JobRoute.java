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

package io.mantisrx.master.api.akka.route.v0;

import akka.actor.ActorSystem;
import akka.http.caching.javadsl.Cache;
import akka.http.caching.javadsl.CachingSettings;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.RequestContext;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.japi.JavaPartialFunction;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.KillJobRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import io.mantisrx.server.core.PostJobStatusRequest;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static akka.http.javadsl.server.PathMatchers.segment;
import static akka.http.javadsl.server.directives.CachingDirectives.alwaysCache;
import static akka.http.javadsl.server.directives.CachingDirectives.routeCache;
import static io.mantisrx.master.api.akka.route.utils.JobRouteUtils.createListJobIdsRequest;
import static io.mantisrx.master.api.akka.route.utils.JobRouteUtils.createListJobsRequest;
import static io.mantisrx.master.api.akka.route.utils.JobRouteUtils.createWorkerStatusRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest.DEFAULT_LIST_ARCHIVED_WORKERS_LIMIT;

public class JobRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobRoute.class);
    private final JobRouteHandler jobRouteHandler;
    private final Metrics metrics;

    private final Counter jobListGET;
    private final Counter jobListJobIdGET;
    private final Counter jobListRegexGET;
    private final Counter jobListLabelMatchGET;
    private final Counter jobArchivedWorkersGET;
    private final Counter jobArchivedWorkersGETInvalid;
    private final Counter workerHeartbeatStatusPOST;
    private final Counter workerHeartbeatSkipped;
    private final Cache<Uri, RouteResult> cache;
    private final JavaPartialFunction<RequestContext, Uri> requestUriKeyer = new JavaPartialFunction<RequestContext, Uri>() {
        public Uri apply(RequestContext in, boolean isCheck) {
            final HttpRequest request = in.getRequest();
            final boolean isGet = request.method() == HttpMethods.GET;
            if (isGet) {
                return request.getUri();
            } else {
                throw noMatch();
            }
        }
    };

    public JobRoute(final JobRouteHandler jobRouteHandler, final ActorSystem actorSystem) {
        this.jobRouteHandler = jobRouteHandler;
        MasterConfiguration config = ConfigurationProvider.getConfig();
        this.cache = createCache(actorSystem, config.getApiCacheMinSize(), config.getApiCacheMaxSize(),
                config.getApiCacheTtlMilliseconds());

        Metrics m = new Metrics.Builder()
            .id("V0JobRoute")
            .addCounter("jobListGET")
            .addCounter("jobListJobIdGET")
            .addCounter("jobListRegexGET")
            .addCounter("jobListLabelMatchGET")
            .addCounter("jobArchivedWorkersGET")
            .addCounter("jobArchivedWorkersGETInvalid")
            .addCounter("workerHeartbeatStatusPOST")
            .addCounter("workerHeartbeatSkipped")
            .build();
        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.jobListGET = metrics.getCounter("jobListGET");
        this.jobListJobIdGET = metrics.getCounter("jobListJobIdGET");
        this.jobListRegexGET = metrics.getCounter("jobListRegexGET");
        this.jobListLabelMatchGET = metrics.getCounter("jobListLabelMatchGET");
        this.jobArchivedWorkersGET = metrics.getCounter("jobArchivedWorkersGET");
        this.jobArchivedWorkersGETInvalid = metrics.getCounter("jobArchivedWorkersGETInvalid");
        this.workerHeartbeatStatusPOST = metrics.getCounter("workerHeartbeatStatusPOST");
        this.workerHeartbeatSkipped = metrics.getCounter("workerHeartbeatSkipped");
    }

    private static final PathMatcher0 API_JOBS = segment("api").slash("jobs");

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
        HttpHeader.parse("Access-Control-Allow-Origin", "*");
    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS = Arrays.asList(
        ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    public static final String KILL_ENDPOINT = "kill";
    public static final String RESUBMIT_WORKER_ENDPOINT = "resubmitWorker";
    public static final String SCALE_STAGE_ENDPOINT = "scaleStage";
    public static final PathMatcher0 STATUS_ENDPOINT = segment("api").slash("postjobstatus");

    /**
     * Route that returns
     * - a list of Job Ids only if 'jobIdsOnly' query param is set
     * - a list of compact Job Infos if 'compact' query param is set
     * - a list of Job metadatas otherwise
     * The above lists are filtered and returned based on other criteria specified in the List request
     * like stageNumber, workerIndex, workerNumber, matchingLabels, regex, activeOnly, jobState, workerState, limit
     *
     * @param regex the regex to match against Job IDs to return in response
     * @return Route job list route
     */
    private Route jobListRoute(final Optional<String> regex) {
        return parameterOptional(StringUnmarshallers.BOOLEAN, "jobIdsOnly", (jobIdsOnly) ->
            parameterOptional(StringUnmarshallers.BOOLEAN, "compact", (isCompact) ->
                parameterMultiMap(params -> {
                    if (jobIdsOnly.isPresent() && jobIdsOnly.get()) {
                        logger.debug("/api/jobs/list jobIdsOnly called");
                        return alwaysCache(cache, requestUriKeyer, () ->
                            extractUri(uri -> completeAsync(
                                jobRouteHandler.listJobIds(createListJobIdsRequest(params, regex, true)),
                                resp -> completeOK(
                                        resp.getJobIds().stream()
                                        .map(jobId -> jobId.getJobId())
                                        .collect(Collectors.toList()),
                                        Jackson.marshaller()))));
                    }
                    if (isCompact.isPresent() && isCompact.get()) {
                        logger.debug("/api/jobs/list compact called");
                        return alwaysCache(cache, requestUriKeyer, () ->
                            extractUri(uri -> completeAsync(
                            jobRouteHandler.listJobs(createListJobsRequest(params, regex, true)),
                            resp -> completeOK(
                                resp.getJobList()
                                    .stream()
                                    .map(jobMetadataView -> JobClusterProtoAdapter.toCompactJobInfo(jobMetadataView))
                                    .collect(Collectors.toList()),
                                Jackson.marshaller()))));
                    } else {
                        logger.debug("/api/jobs/list called");
                        return alwaysCache(cache, requestUriKeyer, () ->
                            extractUri(uri -> completeAsync(
                            jobRouteHandler.listJobs(createListJobsRequest(params, regex, true)),
                            resp -> completeOK(
                                resp.getJobList(),
                                Jackson.marshaller()))));
                    }
                })
            )
        );
    }

    private Route getJobRoutes() {
        return route(
            path(STATUS_ENDPOINT, () ->
                post(() ->
                    decodeRequest(() ->
                        entity(Unmarshaller.entityToString(), req -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("/api/postjobstatus called {}", req);
                            }
                            try {
                                workerHeartbeatStatusPOST.increment();
                                PostJobStatusRequest postJobStatusRequest = Jackson.fromJSON(req, PostJobStatusRequest.class);
                                WorkerEvent workerStatusRequest = createWorkerStatusRequest(postJobStatusRequest);
                                if (workerStatusRequest instanceof WorkerHeartbeat) {
                                    if (!ConfigurationProvider.getConfig().isHeartbeatProcessingEnabled()) {
                                        // skip heartbeat processing
                                        if (logger.isTraceEnabled()) {
                                            logger.trace("skipped heartbeat event {}", workerStatusRequest);
                                        }
                                        workerHeartbeatSkipped.increment();
                                        return complete(StatusCodes.OK);
                                    }
                                }
                                return completeWithFuture(
                                    jobRouteHandler.workerStatus(workerStatusRequest)
                                        .thenApply(this::toHttpResponse));
                            } catch (IOException e) {
                                logger.warn("Error handling job status {}", req, e);
                                return complete(StatusCodes.BAD_REQUEST, "{\"error\": \"invalid JSON payload to post job status\"}");
                            }
                    })
                ))),
            pathPrefix(API_JOBS, () -> route(
                post(() -> route(
                    path(KILL_ENDPOINT, () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), req -> {
                                logger.debug("/api/jobs/kill called {}", req);
                                try {
                                    final KillJobRequest killJobRequest = Jackson.fromJSON(req, KillJobRequest.class);
                                    return completeWithFuture(
                                        jobRouteHandler.kill(killJobRequest)
                                            .thenApply(resp -> {
                                                if (resp.responseCode == BaseResponse.ResponseCode.SUCCESS) {
                                                    return new JobClusterManagerProto.KillJobResponse(resp.requestId, resp.responseCode,
                                                        resp.getState(), "[\""+ resp.getJobId().getId() +" Killed\"]", resp.getJobId(), resp.getUser());
                                                } else if (resp.responseCode == BaseResponse.ResponseCode.CLIENT_ERROR) {
                                                    // for backwards compatibility with old master
                                                    return new JobClusterManagerProto.KillJobResponse(resp.requestId, BaseResponse.ResponseCode.SUCCESS,
                                                        resp.getState(), "[\""+ resp.message +" \"]", resp.getJobId(), resp.getUser());
                                                }
                                                return resp;
                                            })
                                            .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error on job kill {}", req, e);
                                    return complete(StatusCodes.BAD_REQUEST, "{\"error\": \"invalid json payload to kill job\"}");
                                }
                            })
                    )),
                    path(RESUBMIT_WORKER_ENDPOINT, () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), req -> {
                                logger.debug("/api/jobs/resubmitWorker called {}", req);
                                try {
                                    final ResubmitWorkerRequest resubmitWorkerRequest = Jackson.fromJSON(req, ResubmitWorkerRequest.class);
                                    return completeWithFuture(
                                        jobRouteHandler.resubmitWorker(resubmitWorkerRequest)
                                            .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error on worker resubmit {}", req, e);
                                    return complete(StatusCodes.BAD_REQUEST, "{\"error\": \"invalid json payload to resubmit worker\"}");
                                }
                            })
                    )),
                    path(SCALE_STAGE_ENDPOINT, () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), req -> {
                            logger.debug("/api/jobs/scaleStage called {}", req);
                            try {
                                ScaleStageRequest scaleStageRequest = Jackson.fromJSON(req, ScaleStageRequest.class);
                                int numWorkers = scaleStageRequest.getNumWorkers();
                                int maxWorkersPerStage = ConfigurationProvider.getConfig().getMaxWorkersPerStage();
                                if (numWorkers > maxWorkersPerStage) {
                                    logger.warn("rejecting ScaleStageRequest {} with invalid num workers", scaleStageRequest);
                                    return complete(StatusCodes.BAD_REQUEST, "{\"error\": \"num workers must be less than " + maxWorkersPerStage + "\"}");
                                }
                                return completeWithFuture(
                                    jobRouteHandler.scaleStage(scaleStageRequest)
                                        .thenApply(this::toHttpResponse));
                            } catch (IOException e) {
                                logger.warn("Error scaling stage {}", req, e);
                                return complete(StatusCodes.BAD_REQUEST,
                                    "{\"error\": \"invalid json payload to scale stage " + e.getMessage() +"\"}");
                            }
                        })
                    ))
// TODO                   path("updateScalingPolicy", () ->
//                        entity(Jackson.unmarshaller(UpdateJobClusterRequest.class), req -> {
//                            logger.info("/api/jobs/kill called {}", req);
//                            return completeWithFuture(
//                                jobRouteHandler.kill(req)
//                                    .thenApply(this::toHttpResponse));
//                        })
//                    )
                )),
                get(() -> route(
                    // Context from old mantis master:
                    // list all jobs activeOnly = true
                    // optional boolean 'compact' query param to return compact job infos if set
                    // For compact,
                    //  - optional 'limit' query param
                    // - optional 'jobState' query param
                    // For non compact,
                    // - optional boolean 'jobIdsOnly' query param to return only the job Ids if set
                    // - optional int 'stageNumber' query param to filter for stage number
                    // - optional int 'workerIndex' query param to filter for worker index
                    // - optional int 'workerNumber' query param to filter for worker number
                    // - optional int 'workerState' query param to filter for worker state


                    // list/all - list all jobs activeOnly=false with above query parameters
                    // list/matching/<regex> - if optional regex param specified, propagate regex
                    //                          else list all jobs activeOnly=false with above query parameters
                    // list/matchinglabels
                    // - optional labels query param
                    // - optional labels.op query param - default value is 'or' if not specified (other possible value is 'and'
                    path(segment("list"), () -> {
                        jobListGET.increment();
                        return jobListRoute(Optional.empty());
                    }),
                    path(segment("list").slash("matchinglabels"), () -> {
                        jobListLabelMatchGET.increment();
                        return jobListRoute(Optional.empty());
                    }),
                    path(segment("list").slash(PathMatchers.segment()), (jobId) -> {
                        logger.debug("/api/jobs/list/{} called", jobId);
                        jobListJobIdGET.increment();
                        return completeAsync(
                            jobRouteHandler.getJobDetails(new JobClusterManagerProto.GetJobDetailsRequest("masterAPI", jobId)),
                            resp -> {
                                Optional<MantisJobMetadataView> mantisJobMetadataView = resp.getJobMetadata()
                                    .map(metaData -> new MantisJobMetadataView(metaData, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false));
                                return completeOK(mantisJobMetadataView,
                                    Jackson.marshaller());
                            });
                    }),
                    path(segment("list").slash("matching").slash(PathMatchers.segment()), (regex) -> {
                        jobListRegexGET.increment();
                        return jobListRoute(Optional.ofNullable(regex)
                            .filter(r -> !r.isEmpty()));
                    }),
                    path(segment("archived").slash(PathMatchers.segment()), (jobId) ->
                        parameterOptional(StringUnmarshallers.INTEGER, "limit", (limit) -> {
                            jobArchivedWorkersGET.increment();
                            Optional<JobId> jobIdO = JobId.fromId(jobId);
                            if (jobIdO.isPresent()) {
                                ListArchivedWorkersRequest req = new ListArchivedWorkersRequest(jobIdO.get(),
                                    limit.orElse(DEFAULT_LIST_ARCHIVED_WORKERS_LIMIT));
                                return alwaysCache(cache, requestUriKeyer, () ->
                                    extractUri(uri -> completeAsync(
                                    jobRouteHandler.listArchivedWorkers(req),
                                    resp -> {
                                        List<MantisWorkerMetadataWritable> workers = resp.getWorkerMetadata().stream()
                                            .map(wm -> DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(wm))
                                            .collect(Collectors.toList());
                                        return completeOK(workers,
                                            Jackson.marshaller());
                                    })));
                            } else {
                                return complete(StatusCodes.BAD_REQUEST,
                                    "error: 'archived/<jobId>' request must include a valid jobId");
                            }
                        })
                    ),
                    path(segment("archived"), () -> {
                        jobArchivedWorkersGETInvalid.increment();
                        return complete(StatusCodes.BAD_REQUEST,
                        "error: 'archived' Request must include jobId");
                    })
                )))
            ));
    }

    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating routes");
        final ExceptionHandler genericExceptionHandler = ExceptionHandler.newBuilder()
            .match(Exception.class, x -> {
                logger.error("got exception", x);
                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "{\"error\": \"" + x.getMessage() + "\"}");
            })
            .build();

        return respondWithHeaders(DEFAULT_RESPONSE_HEADERS, () -> handleExceptions(genericExceptionHandler, () -> routeFilter.apply(getJobRoutes())));
    }

}
