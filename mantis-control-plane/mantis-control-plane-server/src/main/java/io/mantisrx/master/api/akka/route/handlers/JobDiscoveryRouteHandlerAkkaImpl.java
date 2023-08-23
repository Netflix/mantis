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
import static io.mantisrx.master.api.akka.route.utils.JobDiscoveryHeartbeats.JOB_CLUSTER_INFO_HB_INSTANCE;
import static io.mantisrx.master.api.akka.route.utils.JobDiscoveryHeartbeats.SCHED_INFO_HB_INSTANCE;

import akka.actor.ActorRef;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.master.api.akka.route.proto.JobClusterInfo;
import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto;
import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto.JobClusterInfoResponse;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastLaunchedJobIdStreamRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastLaunchedJobIdStreamResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse;
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
    private final Counter lastLaunchedJobIdStreamErrors;
    private final AsyncLoadingCache<GetJobSchedInfoRequest, GetJobSchedInfoResponse> schedInfoCache;
    private final AsyncLoadingCache<GetLastSubmittedJobIdStreamRequest, GetLastSubmittedJobIdStreamResponse> lastSubmittedJobIdStreamRespCache;
    private final AsyncLoadingCache<GetLastLaunchedJobIdStreamRequest, GetLastLaunchedJobIdStreamResponse> lastLaunchedJobIdStreamRespCache;

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

        lastLaunchedJobIdStreamRespCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(500)
            .buildAsync(this::lastLaunchedJobId);

        Metrics m = new Metrics.Builder()
            .id("JobDiscoveryRouteHandlerAkkaImpl")
            .addCounter("schedInfoStreamErrors")
            .addCounter("lastSubmittedJobIdStreamErrors")
            .addCounter("lastLaunchedJobIdStreamErrors")
            .build();
        this.schedInfoStreamErrors = m.getCounter("schedInfoStreamErrors");
        this.lastSubmittedJobIdStreamErrors = m.getCounter("lastSubmittedJobIdStreamErrors");
        this.lastLaunchedJobIdStreamErrors = m.getCounter("lastLaunchedJobIdStreamErrors");
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
                                    if(isJobCompleted.get()) {
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

    @Override
    public CompletionStage<JobDiscoveryRouteProto.JobClusterInfoResponse> lastSubmittedJobIdStream(
        final GetLastSubmittedJobIdStreamRequest request, final boolean sendHeartbeats) {

        try {
            CompletionStage<GetLastSubmittedJobIdStreamResponse> response = lastSubmittedJobIdStreamRespCache.get(request);
            return response.thenApply(r -> streamJobIdBehaviorSubject(r, r.getjobIdBehaviorSubject(), sendHeartbeats, lastSubmittedJobIdStreamErrors));
        } catch (Exception e) {
            logger.error("caught exception fetching lastSubmittedJobId stream for {}", request.getClusterName(), e);
            lastSubmittedJobIdStreamErrors.increment();
            return CompletableFuture.completedFuture(new JobClusterInfoResponse(
                0,
                BaseResponse.ResponseCode.SERVER_ERROR,
                "Failed to get last submitted jobId stream for " + request.getClusterName() + " error: " + e.getMessage()
            ));
        }
    }

    private CompletableFuture<GetLastLaunchedJobIdStreamResponse> lastLaunchedJobId(final GetLastLaunchedJobIdStreamRequest request, Executor executor) {
        return ask(jobClustersManagerActor, request, askTimeout)
            .thenApply(GetLastLaunchedJobIdStreamResponse.class::cast)
            .toCompletableFuture();
    }

    @Override
    public CompletionStage<JobDiscoveryRouteProto.JobClusterInfoResponse> lastLaunchedJobIdStream(
        final GetLastLaunchedJobIdStreamRequest request, final boolean sendHeartbeats) {

        try {
            CompletionStage<GetLastLaunchedJobIdStreamResponse> response = lastLaunchedJobIdStreamRespCache.get(request);
            return response.thenApply(r -> streamJobIdBehaviorSubject(r, r.getjobIdBehaviorSubject(), sendHeartbeats, lastLaunchedJobIdStreamErrors));
        } catch (Exception e) {
            logger.error("caught exception fetching lastSubmittedJobId stream for {}", request.getClusterName(), e);
            lastLaunchedJobIdStreamErrors.increment();
            return CompletableFuture.completedFuture(new JobClusterInfoResponse(
                0,
                BaseResponse.ResponseCode.SERVER_ERROR,
                "Failed to get last submitted jobId stream for " + request.getClusterName() + " error: " + e.getMessage()
            ));
        }
    }

    /**
     *
     * @param response response from actor
     * @param jobIdSubjectO BehaviorSubject that exposes latest jobId for a jobCluster in Accepted/Launched state depending on endpoint
     */
    private JobClusterInfoResponse streamJobIdBehaviorSubject(
        BaseResponse response, Optional<BehaviorSubject<JobId>> jobIdSubjectO,
        boolean sendHeartbeats, Counter counter) {
        if (response.responseCode.equals(BaseResponse.ResponseCode.SUCCESS) && jobIdSubjectO.isPresent()) {
            Observable<JobClusterInfo> jobClusterInfoObs = jobIdSubjectO.get().map(jobId -> new JobClusterInfo(jobId.getCluster(), jobId.getId()));

            Observable<JobClusterInfo> heartbeats =
                Observable.interval(5, serverIdleConnectionTimeout.getSeconds() - 1, TimeUnit.SECONDS)
                    .map(x -> JOB_CLUSTER_INFO_HB_INSTANCE)
                    .takeWhile(x -> sendHeartbeats);

            Observable<JobClusterInfo> jobClusterInfoWithHB = Observable.merge(jobClusterInfoObs, heartbeats);
            return new JobClusterInfoResponse(
                response.requestId,
                response.responseCode,
                response.message,
                jobClusterInfoWithHB
            );
        } else {
            counter.increment();
            return new JobClusterInfoResponse(
                response.requestId,
                response.responseCode,
                response.message
            );
        }
    }
}
