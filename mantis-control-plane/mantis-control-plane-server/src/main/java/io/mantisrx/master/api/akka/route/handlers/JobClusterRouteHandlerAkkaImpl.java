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

import akka.actor.ActorRef;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.JobClustersManagerActor.UpdateSchedulingInfo;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoResponse;
import io.mantisrx.server.master.config.ConfigurationProvider;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobClusterRouteHandlerAkkaImpl implements JobClusterRouteHandler {
    private static final Logger logger = LoggerFactory.getLogger(JobClusterRouteHandlerAkkaImpl.class);

    private final ActorRef jobClustersManagerActor;
    private final Counter allJobClustersGET;
    private final Duration timeout;

    public JobClusterRouteHandlerAkkaImpl(ActorRef jobClusterManagerActor) {
        this.jobClustersManagerActor = jobClusterManagerActor;
        long timeoutMs = Optional.ofNullable(ConfigurationProvider.getConfig().getMasterApiAskTimeoutMs()).orElse(1000L);
        this.timeout = Duration.ofMillis(timeoutMs);
        Metrics m = new Metrics.Builder()
            .id("JobClusterRouteHandler")
            .addCounter("allJobClustersGET")
            .build();
        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        allJobClustersGET = metrics.getCounter("allJobClustersGET");
    }

    @Override
    public CompletionStage<JobClusterManagerProto.CreateJobClusterResponse> create(final JobClusterManagerProto.CreateJobClusterRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
                .thenApply(JobClusterManagerProto.CreateJobClusterResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.UpdateJobClusterResponse> update(JobClusterManagerProto.UpdateJobClusterRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
                .thenApply(JobClusterManagerProto.UpdateJobClusterResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.DeleteJobClusterResponse> delete(JobClusterManagerProto.DeleteJobClusterRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.DeleteJobClusterResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.DisableJobClusterResponse> disable(JobClusterManagerProto.DisableJobClusterRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.DisableJobClusterResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.EnableJobClusterResponse> enable(JobClusterManagerProto.EnableJobClusterRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.EnableJobClusterResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.UpdateJobClusterArtifactResponse> updateArtifact(JobClusterManagerProto.UpdateJobClusterArtifactRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.UpdateJobClusterArtifactResponse.class::cast);
    }

    @Override
    public CompletionStage<UpdateSchedulingInfoResponse> updateSchedulingInfo(String clusterName, UpdateSchedulingInfoRequest request) {
        return ask(
            jobClustersManagerActor,
            new UpdateSchedulingInfo(request.requestId, clusterName, request.getSchedulingInfo(),
                request.getVersion()),
            timeout)
        .thenApply(UpdateSchedulingInfoResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.UpdateJobClusterSLAResponse> updateSLA(JobClusterManagerProto.UpdateJobClusterSLARequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.UpdateJobClusterSLAResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse> updateWorkerMigrateStrategy(JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.UpdateJobClusterLabelsResponse> updateLabels(JobClusterManagerProto.UpdateJobClusterLabelsRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.UpdateJobClusterLabelsResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.SubmitJobResponse> submit(JobClusterManagerProto.SubmitJobRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.SubmitJobResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.GetJobClusterResponse> getJobClusterDetails(JobClusterManagerProto.GetJobClusterRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.GetJobClusterResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.ListJobClustersResponse> getAllJobClusters(JobClusterManagerProto.ListJobClustersRequest request) {
        allJobClustersGET.increment();
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.ListJobClustersResponse.class::cast);
    }

    @Override
    public CompletionStage<JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse> getLatestJobDiscoveryInfo(JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest request) {
        return ask(jobClustersManagerActor, request, timeout)
            .thenApply(JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse.class::cast);
    }
}
