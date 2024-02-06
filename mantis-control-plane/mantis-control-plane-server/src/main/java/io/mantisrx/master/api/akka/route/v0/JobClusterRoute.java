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

import static akka.http.javadsl.server.PathMatchers.segment;
import static akka.http.javadsl.server.directives.CachingDirectives.alwaysCache;
import static io.mantisrx.master.api.akka.route.utils.JobRouteUtils.createListJobIdsRequest;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR_CONFLICT;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobClustersRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;

import akka.actor.ActorSystem;
import akka.http.caching.LfuCache;
import akka.http.caching.javadsl.Cache;
import akka.http.caching.javadsl.CachingSettings;
import akka.http.caching.javadsl.LfuCacheSettings;
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
import akka.japi.Pair;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

public class JobClusterRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobClusterRoute.class);
    private final JobClusterRouteHandler jobClusterRouteHandler;
    private final JobRouteHandler jobRouteHandler;
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
    private final Metrics metrics;

    private final Counter jobClusterSubmit;
    private final Counter jobClusterSubmitError;
    private final Counter jobClusterCreate;
    private final Counter jobClusterCreateError;
    private final Counter jobClusterCreateUpdate;
    private final Counter jobClusterCreateUpdateError;
    private final Counter jobClusterDelete;
    private final Counter jobClusterDeleteError;
    private final Counter jobClusterDisable;
    private final Counter jobClusterDisableError;
    private final Counter jobClusterEnable;
    private final Counter jobClusterEnableError;
    private final Counter jobClusterQuickupdate;
    private final Counter jobClusterQuickupdateError;
    private final Counter jobClusterUpdateLabel;
    private final Counter jobClusterUpdateSla;
    private final Counter jobClusterUpdateSlaError;
    private final Counter jobClusterUpdateLabelError;
    private final Counter jobClusterListGET;
    private final Counter jobClusterListJobIdGET;
    private final Counter jobClusterListClusterGET;

    public JobClusterRoute(final JobClusterRouteHandler jobClusterRouteHandler,
                             final JobRouteHandler jobRouteHandler,
                             final ActorSystem actorSystem) {
        this.jobClusterRouteHandler = jobClusterRouteHandler;
        this.jobRouteHandler = jobRouteHandler;
        MasterConfiguration config = ConfigurationProvider.getConfig();
        this.cache = createCache(actorSystem, config.getApiCacheMinSize(), config.getApiCacheMaxSize(),
                config.getApiCacheTtlMilliseconds());

        Metrics m = new Metrics.Builder()
                .id("V0JobClusterRoute")
                .addCounter("jobClusterSubmit")
                .addCounter("jobClusterSubmitError")
                .addCounter("jobClusterCreate")
                .addCounter("jobClusterCreateError")
                .addCounter("jobClusterCreateUpdate")
                .addCounter("jobClusterCreateUpdateError")
                .addCounter("jobClusterDelete")
                .addCounter("jobClusterDeleteError")
                .addCounter("jobClusterDisable")
                .addCounter("jobClusterDisableError")
                .addCounter("jobClusterEnable")
                .addCounter("jobClusterEnableError")
                .addCounter("jobClusterQuickupdate")
                .addCounter("jobClusterQuickupdateError")
                .addCounter("jobClusterUpdateLabel")
                .addCounter("jobClusterUpdateLabelError")
                .addCounter("jobClusterListGET")
                .addCounter("jobClusterListJobIdGET")
                .addCounter("jobClusterListClusterGET")
                .addCounter("jobClusterUpdateSla")
                .addCounter("jobClusterUpdateSlaError")
                .build();
        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.jobClusterSubmit = metrics.getCounter("jobClusterSubmit");
        this.jobClusterSubmitError = metrics.getCounter("jobClusterSubmitError");
        this.jobClusterCreate = metrics.getCounter("jobClusterCreate");
        this.jobClusterCreateError = metrics.getCounter("jobClusterCreateError");
        this.jobClusterCreateUpdate = metrics.getCounter("jobClusterCreateUpdate");
        this.jobClusterCreateUpdateError = metrics.getCounter("jobClusterCreateUpdateError");
        this.jobClusterDelete = metrics.getCounter("jobClusterDelete");
        this.jobClusterDeleteError = metrics.getCounter("jobClusterDeleteError");
        this.jobClusterDisable = metrics.getCounter("jobClusterDisable");
        this.jobClusterDisableError = metrics.getCounter("jobClusterDisableError");
        this.jobClusterEnable = metrics.getCounter("jobClusterEnable");
        this.jobClusterEnableError = metrics.getCounter("jobClusterEnableError");
        this.jobClusterQuickupdate = metrics.getCounter("jobClusterQuickupdate");
        this.jobClusterQuickupdateError = metrics.getCounter("jobClusterQuickupdateError");
        this.jobClusterUpdateLabel = metrics.getCounter("jobClusterUpdateLabel");
        this.jobClusterUpdateLabelError = metrics.getCounter("jobClusterUpdateLabelError");
        this.jobClusterListGET = metrics.getCounter("jobClusterListGET");
        this.jobClusterListJobIdGET = metrics.getCounter("jobClusterListJobIdGET");
        this.jobClusterListClusterGET = metrics.getCounter("jobClusterListClusterGET");
        this.jobClusterUpdateSla = metrics.getCounter("jobClusterUpdateSla");
        this.jobClusterUpdateSlaError = metrics.getCounter("jobClusterUpdateSlaError");
    }

    private Cache<Uri, RouteResult> createCache(ActorSystem actorSystem) {
        final CachingSettings defaultCachingSettings = CachingSettings.create(actorSystem);
        final LfuCacheSettings lfuCacheSettings = defaultCachingSettings.lfuCacheSettings()
            .withInitialCapacity(5)
            .withMaxCapacity(50)
            .withTimeToLive(Duration.create(1, TimeUnit.SECONDS));
        final CachingSettings cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings);
        // Created outside the route to potentially allow using
        // the same cache across multiple calls
        final Cache<Uri, RouteResult> jobClustersListCache = LfuCache.create(cachingSettings);
        return jobClustersListCache;
    }

    private static final PathMatcher0 API_V0_JOBCLUSTER = segment("api").slash("namedjob");

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
        HttpHeader.parse("Access-Control-Allow-Origin", "*");
    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS = Arrays.asList(
        ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    /**
     * Route that returns
     * - a list of Job Ids only if 'jobIdsOnly' query param is set
     * - a list of JobIdInfo objects otherwise
     * The above lists are filtered and returned based on other criteria specified in the List request
     * like stageNumber, workerIndex, workerNumber, matchingLabels, regex, activeOnly, jobState, workerState, limit
     *
     * @param jobCluster the regex to match against Job IDs to return in response
     * @return Route job list route
     */
    private Route jobClusterListRoute(final String jobCluster) {
        return parameterOptional(StringUnmarshallers.BOOLEAN, "jobIdsOnly", (jobIdsOnly) ->
            parameterMultiMap(params -> {
                if (jobIdsOnly.isPresent() && jobIdsOnly.get()) {
                    logger.debug("/api/namedjob/listJobIds jobIdsOnly called");
                    return alwaysCache(cache, requestUriKeyer, () ->
                        extractUri(uri -> completeAsync(
                            jobRouteHandler.listJobIds(createListJobIdsRequest(params,
                                    (Strings.isNullOrEmpty(jobCluster)) ? Optional.empty() : Optional.of("^" + jobCluster + "$"),
                                    true)),
                            resp -> completeOK(
                                    resp.getJobIds().stream()
                                    .map(jobId -> jobId.getJobId())
                                    .collect(Collectors.toList()),
                                    Jackson.marshaller())
                            )
                        )
                    );
                }

                logger.debug("/api/namedjob/listJobIds/{} called", jobCluster);
                return alwaysCache(cache, requestUriKeyer, () ->
                    extractUri(uri -> {
                        return completeAsync(
                            jobRouteHandler.listJobIds(createListJobIdsRequest(params,
                                    (Strings.isNullOrEmpty(jobCluster)) ? Optional.empty() : Optional.of("^" + jobCluster + "$"),
                                    false)),
                            resp -> completeOK(
                                resp.getJobIds(),
                                Jackson.marshaller()),
                            resp -> completeOK(Collections.emptyList(), Jackson.marshaller())
                        );
                    })
                );
            })
        );
    }

    /**
     * @return true to indicate valid, false otherwise. The String holds the error message when the request is invalid
     */
    private Pair<Boolean, String> validateSubmitJobRequest(MantisJobDefinition mjd) {
        if (mjd.getName() == null ||
            mjd.getName().length() == 0) {
            logger.info("rejecting job submit request, must include name {}", mjd);
            return Pair.apply(false, "Job definition must include name");
        }
        SchedulingInfo schedulingInfo = mjd.getSchedulingInfo();
        if (schedulingInfo != null) {
            Map<Integer, StageSchedulingInfo> stages = schedulingInfo.getStages();
            if (stages == null) {
                return Pair.apply(true, "");
            }
            for (StageSchedulingInfo stageSchedInfo : stages.values()) {
                double cpuCores = stageSchedInfo.getMachineDefinition().getCpuCores();
                int maxCpuCores = ConfigurationProvider.getConfig().getWorkerMachineDefinitionMaxCpuCores();
                if (cpuCores > maxCpuCores) {
                    logger.info("rejecting job submit request, requested CPU {} > max for {} (user: {}) (stage: {})",
                        cpuCores, mjd.getName(), mjd.getUser(), stages);
                    return Pair.apply(false, "requested CPU cannot be more than max CPU per worker "+maxCpuCores);
                }
                double memoryMB = stageSchedInfo.getMachineDefinition().getMemoryMB();
                int maxMemoryMB = ConfigurationProvider.getConfig().getWorkerMachineDefinitionMaxMemoryMB();
                if (memoryMB > maxMemoryMB) {
                    logger.info("rejecting job submit request, requested memory {} > max for {} (user: {}) (stage: {})",
                        memoryMB, mjd.getName(), mjd.getUser(), stages);
                    return Pair.apply(false, "requested memory cannot be more than max memoryMB per worker "+maxMemoryMB);
                }
                double networkMbps = stageSchedInfo.getMachineDefinition().getNetworkMbps();
                int maxNetworkMbps = ConfigurationProvider.getConfig().getWorkerMachineDefinitionMaxNetworkMbps();
                if (networkMbps > maxNetworkMbps) {
                    logger.info("rejecting job submit request, requested network {} > max for {} (user: {}) (stage: {})",
                        networkMbps, mjd.getName(), mjd.getUser(), stages);
                    return Pair.apply(false, "requested network cannot be more than max networkMbps per worker "+maxNetworkMbps);
                }
                int numberOfInstances = stageSchedInfo.getNumberOfInstances();
                int maxWorkersPerStage = ConfigurationProvider.getConfig().getMaxWorkersPerStage();
                if (numberOfInstances > maxWorkersPerStage) {
                    logger.info("rejecting job submit request, requested num instances {} > max for {} (user: {}) (stage: {})",
                        numberOfInstances, mjd.getName(), mjd.getUser(), stages);
                    return Pair.apply(false, "requested number of instances per stage cannot be more than " + maxWorkersPerStage);
                }

                StageScalingPolicy scalingPolicy = stageSchedInfo.getScalingPolicy();
                if (scalingPolicy != null) {
                    if (scalingPolicy.getMax() > maxWorkersPerStage) {
                        logger.info("rejecting job submit request, requested num instances in scaling policy {} > max for {} (user: {}) (stage: {})",
                            numberOfInstances, mjd.getName(), mjd.getUser(), stages);
                        return Pair.apply(false, "requested number of instances per stage in scaling policy cannot be more than " + maxWorkersPerStage);
                    }
                }
            }
        }
        return Pair.apply(true, "");
    }

    private Route getJobClusterRoutes() {
        return route(
            path(segment("api").slash("submit"), () ->
                decodeRequest(() ->
                    entity(Unmarshaller.entityToString(), request -> {
                        logger.debug("/api/submit called {}", request);
                        try {
                            MantisJobDefinition mjd = Jackson.fromJSON(request, MantisJobDefinition.class);
                            logger.debug("job submit request {}", mjd);
                            mjd.validate(true);

                            Pair<Boolean, String> validationResult = validateSubmitJobRequest(mjd);
                            if (!validationResult.first()) {
                                jobClusterSubmitError.increment();
                                return complete(StatusCodes.BAD_REQUEST,
                                    "{\"error\": \"" + validationResult.second() + "\"}");
                            }
                            jobClusterSubmit.increment();
                            return completeWithFuture(
                                jobClusterRouteHandler.submit(JobClusterProtoAdapter.toSubmitJobClusterRequest(mjd))
                                    .thenApply(this::toHttpResponse));
                        } catch (Exception e) {
                            logger.warn("exception in submit job request {}", request, e);
                            jobClusterSubmitError.increment();
                            return complete(StatusCodes.INTERNAL_SERVER_ERROR,
                                "{\"error\": \""+e.getMessage()+ "\"}");
                        }
                    })
                )
            ),
            pathPrefix(API_V0_JOBCLUSTER, () -> route(
                post(() -> route(
                    path("create", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), jobClusterDefn -> {
                                logger.debug("/api/namedjob/create called {}", jobClusterDefn);
                                try {
                                    final NamedJobDefinition namedJobDefinition = Jackson.fromJSON(jobClusterDefn, NamedJobDefinition.class);
                                    if (namedJobDefinition == null ||
                                        namedJobDefinition.getJobDefinition() == null ||
                                        namedJobDefinition.getJobDefinition().getJobJarFileLocation() == null ||
                                        namedJobDefinition.getJobDefinition().getName() == null ||
                                        namedJobDefinition.getJobDefinition().getName().isEmpty()) {
                                        logger.warn("JobCluster create request must include name and URL {}", jobClusterDefn);
                                        return complete(StatusCodes.BAD_REQUEST, "{\"error\": \"Job definition must include name and URL\"}");
                                    }
                                    final CompletionStage<CreateJobClusterResponse> response =
                                        jobClusterRouteHandler.create(
                                            JobClusterProtoAdapter.toCreateJobClusterRequest(namedJobDefinition));
                                    jobClusterCreate.increment();
                                    return completeWithFuture(response
                                        .thenApply(r -> {
                                            if ((r.responseCode == CLIENT_ERROR || r.responseCode == CLIENT_ERROR_CONFLICT)
                                                && r.message.contains("already exists")) {
                                                return new CreateJobClusterResponse(r.requestId, SERVER_ERROR, r.message, r.getJobClusterName());
                                            }
                                            return r;
                                        })
                                        .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error creating JobCluster {}", jobClusterDefn, e);
                                    jobClusterCreateError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't read valid json in request: "+e.getMessage());
                                } catch (Exception e) {
                                    logger.warn("Error creating JobCluster {}", jobClusterDefn, e);
                                    jobClusterCreateError.increment();
                                    return complete(StatusCodes.INTERNAL_SERVER_ERROR, "{\"error\": "+e.getMessage()+"}");
                                }
                            })
                        )
                    ),
                    path("update", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), jobClusterDefn -> {
                                logger.debug("/api/namedjob/update called {}", jobClusterDefn);
                                try {
                                    final NamedJobDefinition namedJobDefinition = Jackson.fromJSON(jobClusterDefn, NamedJobDefinition.class);
                                    if (namedJobDefinition == null ||
                                        namedJobDefinition.getJobDefinition() == null ||
                                        namedJobDefinition.getJobDefinition().getJobJarFileLocation() == null ||
                                        namedJobDefinition.getJobDefinition().getName() == null ||
                                        namedJobDefinition.getJobDefinition().getName().isEmpty()) {
                                        logger.warn("JobCluster update request must include name and URL {}", jobClusterDefn);
                                        jobClusterCreateUpdateError.increment();
                                        return complete(StatusCodes.BAD_REQUEST, "{\"error\": \"Job definition must include name and URL\"}");
                                    }
                                    final CompletionStage<UpdateJobClusterResponse> response =
                                        jobClusterRouteHandler.update(
                                            JobClusterProtoAdapter.toUpdateJobClusterRequest(namedJobDefinition));
                                    jobClusterCreateUpdate.increment();
                                    return completeWithFuture(response.thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error updating JobCluster {}", jobClusterDefn, e);
                                    jobClusterCreateUpdateError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't read valid json in request: "+e.getMessage());
                                } catch (Exception e) {
                                    logger.warn("Error updating JobCluster {}", jobClusterDefn, e);
                                    jobClusterCreateUpdateError.increment();
                                    return complete(StatusCodes.INTERNAL_SERVER_ERROR, "{\"error\": "+e.getMessage()+"}");
                                }
                            })
                        )
                    ),
                    path("delete", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), deleteReq -> {
                                logger.debug("/api/namedjob/delete called {}", deleteReq);
                                try {
                                    final DeleteJobClusterRequest deleteJobClusterRequest = Jackson.fromJSON(deleteReq, DeleteJobClusterRequest.class);
                                    final CompletionStage<DeleteJobClusterResponse> response =
                                        jobClusterRouteHandler.delete(deleteJobClusterRequest);
                                    jobClusterDelete.increment();
                                    return completeWithFuture(response.thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error deleting JobCluster {}", deleteReq, e);
                                    jobClusterDeleteError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("disable", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request -> {
                                logger.debug("/api/namedjob/disable called {}", request);
                                try {
                                    final DisableJobClusterRequest disableJobClusterRequest = Jackson.fromJSON(request, DisableJobClusterRequest.class);
                                    final CompletionStage<DisableJobClusterResponse> response =
                                        jobClusterRouteHandler.disable(disableJobClusterRequest);
                                    jobClusterDisable.increment();
                                    return completeWithFuture(response.thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error disabling JobCluster {}", request, e);
                                    jobClusterDisableError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("enable", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request -> {
                                logger.debug("/api/namedjob/enable called {}", request);
                                try {
                                    final EnableJobClusterRequest enableJobClusterRequest = Jackson.fromJSON(request, EnableJobClusterRequest.class);
                                    final CompletionStage<EnableJobClusterResponse> response =
                                        jobClusterRouteHandler.enable(enableJobClusterRequest);
                                    jobClusterEnable.increment();
                                    return completeWithFuture(response.thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error enabling JobCluster {}", request, e);
                                    jobClusterEnableError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("quickupdate", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request -> {
                                logger.debug("/api/namedjob/quickupdate called {}", request);
                                try {
                                    final UpdateJobClusterArtifactRequest updateJobClusterArtifactRequest = Jackson.fromJSON(request, UpdateJobClusterArtifactRequest.class);
                                    final CompletionStage<UpdateJobClusterArtifactResponse> response =
                                        jobClusterRouteHandler.updateArtifact(updateJobClusterArtifactRequest);
                                    jobClusterQuickupdate.increment();
                                    return completeWithFuture(response.thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error on quickupdate for JobCluster {}", request, e);
                                    jobClusterQuickupdateError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("updatelabels", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request ->  {
                                logger.debug("/api/namedjob/updatelabels called {}", request);
                                try {
                                    final UpdateJobClusterLabelsRequest updateJobClusterLabelsRequest = Jackson.fromJSON(request, UpdateJobClusterLabelsRequest.class);
                                    jobClusterUpdateLabel.increment();
                                    return completeWithFuture(jobClusterRouteHandler.updateLabels(updateJobClusterLabelsRequest)
                                        .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error updating labels for JobCluster {}", request, e);
                                    jobClusterUpdateLabelError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("updatesla", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request -> {
                                logger.debug("/api/namedjob/updatesla called {}", request);
                                jobClusterUpdateSla.increment();
                                try {
                                    final UpdateJobClusterSLARequest updateJobClusterSLARequest = Jackson.fromJSON(request, UpdateJobClusterSLARequest.class);
                                    return completeWithFuture(jobClusterRouteHandler.updateSLA(updateJobClusterSLARequest)
                                        .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error updating SLA for JobCluster {}", request, e);
                                    jobClusterUpdateSlaError.increment();
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("migratestrategy", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request -> {
                                logger.debug("/api/namedjob/migratestrategy called {}", request);
                                try {
                                    final UpdateJobClusterWorkerMigrationStrategyRequest updateMigrateStrategyReq =
                                        Jackson.fromJSON(request, UpdateJobClusterWorkerMigrationStrategyRequest.class);
                                    return completeWithFuture(jobClusterRouteHandler.updateWorkerMigrateStrategy(updateMigrateStrategyReq)
                                        .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error updating migrate strategy for JobCluster {}", request, e);
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    ),
                    path("quicksubmit", () ->
                        decodeRequest(() ->
                            entity(Unmarshaller.entityToString(), request -> {
                                logger.debug("/api/namedjob/quicksubmit called {}", request);
                                try {
                                    final JobClusterManagerProto.SubmitJobRequest submitJobRequest = Jackson.fromJSON(request, JobClusterManagerProto.SubmitJobRequest.class);
                                    return completeWithFuture(jobClusterRouteHandler.submit(submitJobRequest)
                                        .thenApply(this::toHttpResponse));
                                } catch (IOException e) {
                                    logger.warn("Error on quick submit for JobCluster {}", request, e);
                                    return complete(StatusCodes.BAD_REQUEST, "Can't find valid json in request: " + e.getMessage());
                                }
                            })
                        )
                    )
                )),
                get(() -> route(
                    pathPrefix("list", () -> route(
                        pathEndOrSingleSlash(() -> {
                            logger.debug("/api/namedjob/list called");
                            jobClusterListGET.increment();
                            return alwaysCache(cache, requestUriKeyer, () ->
                                extractUri(uri -> completeAsync(
                                    jobClusterRouteHandler.getAllJobClusters(new ListJobClustersRequest()),
                                    resp -> completeOK(
                                        resp.getJobClusters()
                                            .stream()
                                            .map(jobClusterMetadataView -> JobClusterProtoAdapter.toJobClusterInfo(jobClusterMetadataView))
                                            .collect(Collectors.toList())
                                            ,
                                        Jackson.marshaller()),
                                    resp -> completeOK(Collections.emptyList(), Jackson.marshaller()))));
                        }),
                        path(PathMatchers.segment(), (jobCluster) -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("/api/namedjob/list/{} called", jobCluster);
                            }
                            jobClusterListClusterGET.increment();
                            return completeAsync(
                                jobClusterRouteHandler.getJobClusterDetails(new JobClusterManagerProto.GetJobClusterRequest(jobCluster)),
                                resp -> completeOK(
                                    resp.getJobCluster().map(jc -> Arrays.asList(jc)).orElse(Collections.emptyList()),
                                    Jackson.marshaller()),
                                resp -> completeOK(Collections.emptyList(), Jackson.marshaller())
                            );
                        })
                    )),
                    path(segment("listJobIds").slash(PathMatchers.segment()), (jobCluster) -> {
                        logger.debug("/api/namedjob/listJobIds/{} called", jobCluster);
                        jobClusterListJobIdGET.increment();
                        return jobClusterListRoute(jobCluster);
                    }),
                    path("listJobIds", () ->
                    {
                        logger.debug("/api/namedjob/listJobIds called");
                        return complete(StatusCodes.BAD_REQUEST,
                            "Specify the Job cluster name '/api/namedjob/listJobIds/<JobClusterName>' to list the job Ids");
                    })
                )))
            ));
    }
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating routes");
        final ExceptionHandler genericExceptionHandler = ExceptionHandler.newBuilder()
            .match(Exception.class, e -> {
                logger.error("got exception", e);
                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "{\"error\": \"" + e.getMessage() + "\"}");
            })
            .build();


        return respondWithHeaders(DEFAULT_RESPONSE_HEADERS, () -> handleExceptions(genericExceptionHandler, () -> routeFilter.apply(getJobClusterRoutes())));
    }
}
