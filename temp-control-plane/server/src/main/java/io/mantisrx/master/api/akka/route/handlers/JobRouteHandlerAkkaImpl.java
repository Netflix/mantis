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

import akka.actor.ActorRef;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.pattern.PatternsCS.ask;

public class JobRouteHandlerAkkaImpl implements JobRouteHandler {
    private static final Logger logger = LoggerFactory.getLogger(JobRouteHandlerAkkaImpl.class);
    private final ActorRef jobClustersManagerActor;
    private final Counter listAllJobs;
    private final Counter listJobIds;
    private final Counter listArchivedWorkers;
    private final Duration timeout;

    public JobRouteHandlerAkkaImpl(ActorRef jobClusterManagerActor) {
        this.jobClustersManagerActor = jobClusterManagerActor;
        long timeoutMs = Optional.ofNullable(ConfigurationProvider.getConfig().getMasterApiAskTimeoutMs()).orElse(1000L);
        this.timeout = Duration.ofMillis(timeoutMs);
        Metrics m = new Metrics.Builder()
            .id("JobRouteHandler")
            .addCounter("listAllJobs")
            .addCounter("listJobIds")
            .addCounter("listArchivedWorkers")
            .build();
        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.listAllJobs = metrics.getCounter("listAllJobs");
        this.listJobIds = metrics.getCounter("listJobIds");
        this.listArchivedWorkers = metrics.getCounter("listArchivedWorkers");
    }

    @Override
    public CompletionStage<JobClusterManagerProto.KillJobResponse> kill(JobClusterManagerProto.KillJobRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.KillJobResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.ResubmitWorkerResponse> resubmitWorker(JobClusterManagerProto.ResubmitWorkerRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.ResubmitWorkerResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.ScaleStageResponse> scaleStage(JobClusterManagerProto.ScaleStageRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.ScaleStageResponse.class::cast);
    }

    @Override
    public CompletionStage<BaseResponse> workerStatus(final WorkerEvent request) {
        jobClustersManagerActor.tell(request, ActorRef.noSender());
        return CompletableFuture.completedFuture(new BaseResponse(0L, BaseResponse.ResponseCode.SUCCESS, "forwarded worker status"));
    }

    @Override
    public CompletionStage<JobClusterManagerProto.GetJobDetailsResponse> getJobDetails(final JobClusterManagerProto.GetJobDetailsRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.GetJobDetailsResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.ListJobsResponse> listJobs(JobClusterManagerProto.ListJobsRequest request) {
        logger.debug("request {}", request);
        listAllJobs.increment();
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.ListJobsResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.ListJobIdsResponse> listJobIds(JobClusterManagerProto.ListJobIdsRequest request) {
        logger.debug("request {}", request);
        listJobIds.increment();
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.ListJobIdsResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.ListArchivedWorkersResponse> listArchivedWorkers(JobClusterManagerProto.ListArchivedWorkersRequest request) {
        listArchivedWorkers.increment();
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.ListArchivedWorkersResponse.class::cast);
    }
}
