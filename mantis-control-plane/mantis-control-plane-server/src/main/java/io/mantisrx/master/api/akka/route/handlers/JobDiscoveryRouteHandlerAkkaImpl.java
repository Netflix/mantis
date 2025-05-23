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

package io.mantisrx.master.api.akka.route.handlers;

import static akka.pattern.PatternsCS.ask;
import static io.mantisrx.master.api.akka.route.utils.JobDiscoveryHeartbeats.*;

import akka.actor.ActorRef;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.master.api.akka.route.proto.JobClusterInfo;
import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.domain.JobId;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;

public class JobDiscoveryRouteHandlerAkkaImpl implements JobDiscoveryRouteHandler {

    private static final Logger logger = LoggerFactory.getLogger(JobDiscoveryRouteHandlerAkkaImpl.class);
    private final ActorRef jobClustersManagerActor;
    private final Duration askTimeout;
    // We want to heartbeat at least once before the idle conn timeout to keep the SSE stream conn alive
    private final Duration serverIdleConnectionTimeout;

    private final Counter schedInfoStreamErrors;
    private final Counter lastSubmittedJobIdStreamErrors;
    private final Counter jobScalerRuleInfoStreamErrors;

    private final AsyncLoadingCache<GetJobSchedInfoRequest, GetJobSchedInfoResponse> schedInfoCache;
    private final AsyncLoadingCache<GetLastSubmittedJobIdStreamRequest, GetLastSubmittedJobIdStreamResponse> lastSubmittedJobIdStreamRespCache;
    private final AsyncLoadingCache<JobClusterScalerRuleProto.GetJobScalerRuleStreamRequest, JobClusterScalerRuleProto.GetJobScalerRuleStreamSubjectResponse> jobScalerRuleStreamRespCache;

    public JobDiscoveryRouteHandlerAkkaImpl(ActorRef jobClustersManagerActor, Duration serverIdleTimeout) {
        this.jobClustersManagerActor = jobClustersManagerActor;
        long timeoutMs = Optional.ofNullable(ConfigurationProvider.getConfig().getMasterApiAskTimeoutMs()).orElse(1000L);
        this.askTimeout = Duration.ofMillis(timeoutMs);
        this.serverIdleConnectionTimeout = serverIdleTimeout;
        schedInfoCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(500)
            .buildAsync(this::jobSchedInfo);

        lastSubmittedJobIdStreamRespCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(500)
            .buildAsync(this::lastSubmittedJobId);

        jobScalerRuleStreamRespCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(500)
            .buildAsync(this::jobScalerRuleSubject);

        Metrics m = new Metrics.Builder()
            .id("JobDiscoveryRouteHandlerAkkaImpl")
            .addCounter("schedInfoStreamErrors")
            .addCounter("lastSubmittedJobIdStreamErrors")
            .addCounter("jobScalerRuleInfoStreamErrors")
            .build();
        this.schedInfoStreamErrors = m.getCounter("schedInfoStreamErrors");
        this.lastSubmittedJobIdStreamErrors = m.getCounter("lastSubmittedJobIdStreamErrors");
        this.jobScalerRuleInfoStreamErrors = m.getCounter("jobScalerRuleInfoStreamErrors");
    }


    private CompletableFuture<GetJobSchedInfoResponse> jobSchedInfo(final GetJobSchedInfoRequest request, Executor executor) {
        return ask(jobClustersManagerActor, request, askTimeout)
            .thenApply(GetJobSchedInfoResponse.class::cast)
            .toCompletableFuture();
    }

    @Override
    public CompletionStage<JobDiscoveryRouteProto.SchedInfoResponse> schedulingInfoStream(final GetJobSchedInfoRequest request,
                                                                                          final boolean sendHeartbeats) {

        CompletionStage<GetJobSchedInfoResponse> response = schedInfoCache.get(request);
        try {
            AtomicBoolean isJobCompleted = new AtomicBoolean(false);
            final String jobId = request.getJobId().getId();
            final JobSchedulingInfo completedJobSchedulingInfo = new JobSchedulingInfo(jobId, new HashMap<>());
            CompletionStage<JobDiscoveryRouteProto.SchedInfoResponse> jobSchedInfoObsCS = response
                .thenApply(getJobSchedInfoResp -> {
                    Optional<BehaviorSubject<JobSchedulingInfo>> jobStatusSubjectO = getJobSchedInfoResp.getJobSchedInfoSubject();
                    if (getJobSchedInfoResp.responseCode.equals(BaseResponse.ResponseCode.SUCCESS) && jobStatusSubjectO.isPresent()) {
                        BehaviorSubject<JobSchedulingInfo> jobSchedulingInfoObs = jobStatusSubjectO.get();

                        Observable<JobSchedulingInfo> heartbeats =
                            Observable.interval(5, serverIdleConnectionTimeout.getSeconds() - 1, TimeUnit.SECONDS)
                                .map(x -> {
                                    if(!isJobCompleted.get()) {
                                        return SCHED_INFO_HB_INSTANCE;
                                    } else {
                                        return completedJobSchedulingInfo;
                                    }

                                })
                                .takeWhile(x -> sendHeartbeats == true);

                        // Job SchedulingInfo obs completes on job shutdown. Use the do On completed as a signal to inform the user that there are no workers to connect to.
                        // TODO For future a more explicit key in the payload saying the job is completed.
                        Observable<JobSchedulingInfo> jobSchedulingInfoWithHBObs = Observable.merge(jobSchedulingInfoObs.doOnCompleted(() -> isJobCompleted.set(true)), heartbeats);
                        return new JobDiscoveryRouteProto.SchedInfoResponse(
                            getJobSchedInfoResp.requestId,
                            getJobSchedInfoResp.responseCode,
                            getJobSchedInfoResp.message,
                            jobSchedulingInfoWithHBObs

                        );
                    } else {
                        logger.info("Failed to get Sched info stream for {}", request.getJobId().getId());
                        schedInfoStreamErrors.increment();
                        return new JobDiscoveryRouteProto.SchedInfoResponse(
                            getJobSchedInfoResp.requestId,
                            getJobSchedInfoResp.responseCode,
                            getJobSchedInfoResp.message
                        );
                    }
                });
            return jobSchedInfoObsCS;
        } catch (Exception e) {
            logger.error("caught exception fetching sched info stream for {}", request.getJobId().getId(), e);
            schedInfoStreamErrors.increment();
            return CompletableFuture.completedFuture(new JobDiscoveryRouteProto.SchedInfoResponse(
                0,
                BaseResponse.ResponseCode.SERVER_ERROR,
                "Failed to get SchedulingInfo stream for jobId " + request.getJobId().getId() + " error: " + e.getMessage()
            ));
        }
    }

    private CompletableFuture<GetLastSubmittedJobIdStreamResponse> lastSubmittedJobId(final GetLastSubmittedJobIdStreamRequest request, Executor executor) {
        return ask(jobClustersManagerActor, request, askTimeout)
            .thenApply(GetLastSubmittedJobIdStreamResponse.class::cast)
            .toCompletableFuture();
    }

    private CompletableFuture<JobClusterScalerRuleProto.GetJobScalerRuleStreamSubjectResponse> jobScalerRuleSubject(final JobClusterScalerRuleProto.GetJobScalerRuleStreamRequest request, Executor executor) {
        return ask(jobClustersManagerActor, request, askTimeout)
            .thenApply(JobClusterScalerRuleProto.GetJobScalerRuleStreamSubjectResponse.class::cast)
            .toCompletableFuture();
    }

    @Override
    public CompletionStage<JobDiscoveryRouteProto.JobClusterInfoResponse> lastSubmittedJobIdStream(final GetLastSubmittedJobIdStreamRequest request,
                                                                                                   final boolean sendHeartbeats) {
        CompletionStage<GetLastSubmittedJobIdStreamResponse> response = lastSubmittedJobIdStreamRespCache.get(request);
        try {
            return response
                .thenApply(lastSubmittedJobIdResp -> {
                    Optional<BehaviorSubject<JobId>> jobIdSubjectO = lastSubmittedJobIdResp.getjobIdBehaviorSubject();
                    if (lastSubmittedJobIdResp.responseCode.equals(BaseResponse.ResponseCode.SUCCESS) && jobIdSubjectO.isPresent()) {
                        Observable<JobClusterInfo> jobClusterInfoObs = jobIdSubjectO.get().map(jobId -> new JobClusterInfo(jobId.getCluster(), jobId.getId()));

                        Observable<JobClusterInfo> heartbeats =
                            Observable.interval(5, serverIdleConnectionTimeout.getSeconds() - 1, TimeUnit.SECONDS)
                                .map(x -> JOB_CLUSTER_INFO_HB_INSTANCE)
                                .takeWhile(x -> sendHeartbeats == true);

                        Observable<JobClusterInfo> jobClusterInfoWithHB = Observable.merge(jobClusterInfoObs, heartbeats);
                        return new JobDiscoveryRouteProto.JobClusterInfoResponse(
                            lastSubmittedJobIdResp.requestId,
                            lastSubmittedJobIdResp.responseCode,
                            lastSubmittedJobIdResp.message,
                            jobClusterInfoWithHB
                        );
                    } else {
                        logger.info("Failed to get lastSubmittedJobId stream for job cluster {}", request.getClusterName());
                        lastSubmittedJobIdStreamErrors.increment();
                        return new JobDiscoveryRouteProto.JobClusterInfoResponse(
                            lastSubmittedJobIdResp.requestId,
                            lastSubmittedJobIdResp.responseCode,
                            lastSubmittedJobIdResp.message
                        );
                    }
                });

        } catch (Exception e) {
            logger.error("caught exception fetching lastSubmittedJobId stream for {}", request.getClusterName(), e);
            lastSubmittedJobIdStreamErrors.increment();
            return CompletableFuture.completedFuture(new JobDiscoveryRouteProto.JobClusterInfoResponse(
                0,
                BaseResponse.ResponseCode.SERVER_ERROR,
                "Failed to get last submitted jobId stream for " + request.getClusterName() + " error: " + e.getMessage()
            ));
        }
    }

    @Override
    public CompletionStage<JobClusterScalerRuleProto.GetJobScalerRuleStreamResponse> jobScalerRuleStream(JobClusterScalerRuleProto.GetJobScalerRuleStreamRequest request, boolean sendHeartbeats) {
        CompletionStage<JobClusterScalerRuleProto.GetJobScalerRuleStreamSubjectResponse> response =
            jobScalerRuleStreamRespCache.get(request);
        try {
            AtomicBoolean isJobCompleted = new AtomicBoolean(false);
            final String jobId = request.getJobId().getId();
            final JobScalerRuleInfo completedJobScalerRuleInfo = new JobScalerRuleInfo(jobId, true, null);
            return response.thenApply(getJobScalerRuleStreamSubjectResponse -> {
                    BehaviorSubject<JobScalerRuleInfo> jobScalerRuleSubject =
                        getJobScalerRuleStreamSubjectResponse.getJobScalerRuleStreamBehaviorSubject();
                    if (getJobScalerRuleStreamSubjectResponse.responseCode.equals(BaseResponse.ResponseCode.SUCCESS) && jobScalerRuleSubject != null) {
                        Observable<JobScalerRuleInfo> heartbeats =
                            Observable.interval(5, serverIdleConnectionTimeout.getSeconds() - 1, TimeUnit.SECONDS)
                                .map(x -> {
                                    if(!isJobCompleted.get()) {
                                        return JOB_SCALER_RULES_INFO_HB_INSTANCE;
                                    } else {
                                        return completedJobScalerRuleInfo;
                                    }

                                })
                                .takeWhile(x -> sendHeartbeats);

                        Observable<JobScalerRuleInfo> jobScalerRuleInfoWithHBObs = Observable.merge(
                            jobScalerRuleSubject.doOnCompleted(() -> isJobCompleted.set(true)),
                            heartbeats);
                        return JobClusterScalerRuleProto.GetJobScalerRuleStreamResponse.builder()
                            .requestId(getJobScalerRuleStreamSubjectResponse.requestId)
                            .responseCode(getJobScalerRuleStreamSubjectResponse.responseCode)
                            .message(getJobScalerRuleStreamSubjectResponse.message)
                            .scalerRuleObs(jobScalerRuleInfoWithHBObs)
                            .build();
                    } else {
                        logger.info("Failed to get job scaler rule info stream for {}", request.getJobId());
                        jobScalerRuleInfoStreamErrors.increment();
                        return JobClusterScalerRuleProto.GetJobScalerRuleStreamResponse.builder()
                            .requestId(getJobScalerRuleStreamSubjectResponse.requestId)
                            .responseCode(getJobScalerRuleStreamSubjectResponse.responseCode)
                            .message(getJobScalerRuleStreamSubjectResponse.message)
                            .build();
                    }
                });
        } catch (Exception e) {
            logger.error("caught exception fetching job scaler rule info stream for {}", request.getJobId(), e);
            jobScalerRuleInfoStreamErrors.increment();
            return CompletableFuture.completedFuture(
                JobClusterScalerRuleProto.GetJobScalerRuleStreamResponse.builder()
                    .requestId(0)
                    .responseCode(BaseResponse.ResponseCode.SERVER_ERROR)
                    .message("Failed to get scaler rule info stream for " + request.getJobId() + " error: " + e.getMessage())
                    .build()
            );
        }
    }
}
