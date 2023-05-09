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

import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoResponse;
import java.util.concurrent.CompletionStage;

public interface JobClusterRouteHandler {
    CompletionStage<JobClusterManagerProto.CreateJobClusterResponse> create(final JobClusterManagerProto.CreateJobClusterRequest request);

    CompletionStage<JobClusterManagerProto.UpdateJobClusterResponse> update(final JobClusterManagerProto.UpdateJobClusterRequest request);

    CompletionStage<JobClusterManagerProto.DeleteJobClusterResponse> delete(final JobClusterManagerProto.DeleteJobClusterRequest request);

    CompletionStage<JobClusterManagerProto.DisableJobClusterResponse> disable(final JobClusterManagerProto.DisableJobClusterRequest request);

    CompletionStage<JobClusterManagerProto.EnableJobClusterResponse> enable(final JobClusterManagerProto.EnableJobClusterRequest request);

    CompletionStage<JobClusterManagerProto.UpdateJobClusterArtifactResponse> updateArtifact(final JobClusterManagerProto.UpdateJobClusterArtifactRequest request);

    CompletionStage<UpdateSchedulingInfoResponse> updateSchedulingInfo(
            String clusterName,
            final UpdateSchedulingInfoRequest request);

    CompletionStage<JobClusterManagerProto.UpdateJobClusterSLAResponse> updateSLA(final JobClusterManagerProto.UpdateJobClusterSLARequest request);

    CompletionStage<JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse> updateWorkerMigrateStrategy(final JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest request);

    CompletionStage<JobClusterManagerProto.UpdateJobClusterLabelsResponse> updateLabels(final JobClusterManagerProto.UpdateJobClusterLabelsRequest request);

    CompletionStage<JobClusterManagerProto.SubmitJobResponse> submit(final JobClusterManagerProto.SubmitJobRequest request);

    CompletionStage<JobClusterManagerProto.GetJobClusterResponse> getJobClusterDetails(final JobClusterManagerProto.GetJobClusterRequest request);

    CompletionStage<JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse> getLatestJobDiscoveryInfo(final JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest request);

    CompletionStage<JobClusterManagerProto.ListJobClustersResponse> getAllJobClusters(final JobClusterManagerProto.ListJobClustersRequest request);
}
