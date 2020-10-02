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

import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.master.scheduler.WorkerEvent;

import java.util.concurrent.CompletionStage;

public interface JobRouteHandler {
    CompletionStage<JobClusterManagerProto.KillJobResponse> kill(final JobClusterManagerProto.KillJobRequest request);

    CompletionStage<JobClusterManagerProto.ResubmitWorkerResponse> resubmitWorker(final JobClusterManagerProto.ResubmitWorkerRequest request);

    CompletionStage<JobClusterManagerProto.ScaleStageResponse> scaleStage(final JobClusterManagerProto.ScaleStageRequest request);

    CompletionStage<BaseResponse> workerStatus(final WorkerEvent event);

//TODO    CompletionStage<JobClusterManagerProto.ScaleStageResponse> updateScalingPolicy(final JobClusterManagerProto.Update request);

    CompletionStage<JobClusterManagerProto.GetJobDetailsResponse> getJobDetails(final JobClusterManagerProto.GetJobDetailsRequest request);

    CompletionStage<JobClusterManagerProto.ListJobsResponse> listJobs(final JobClusterManagerProto.ListJobsRequest request);

    CompletionStage<JobClusterManagerProto.ListJobIdsResponse> listJobIds(final JobClusterManagerProto.ListJobIdsRequest request);

    CompletionStage<JobClusterManagerProto.ListArchivedWorkersResponse> listArchivedWorkers(final JobClusterManagerProto.ListArchivedWorkersRequest request);
}
