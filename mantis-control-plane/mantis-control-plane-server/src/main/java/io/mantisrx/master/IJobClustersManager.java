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

package io.mantisrx.master;


import io.mantisrx.master.JobClustersManagerActor.UpdateSchedulingInfo;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.KillJobRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobClustersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.DeleteJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.InitializeJobClusterResponse;
import io.mantisrx.server.master.scheduler.WorkerEvent;

public interface IJobClustersManager {
    // cluster related messages

    void onJobClusterCreate(CreateJobClusterRequest request);

    void onJobClusterInitializeResponse(InitializeJobClusterResponse createResp);

    void onJobClusterDelete(DeleteJobClusterRequest request);

    void onJobClusterDeleteResponse(DeleteJobClusterResponse resp);

    void onJobClusterUpdate(UpdateJobClusterRequest request);

    void onJobClusterUpdateSLA(JobClusterManagerProto.UpdateJobClusterSLARequest r);

    void onJobClusterUpdateArtifact(JobClusterManagerProto.UpdateJobClusterArtifactRequest r);

    void onJobClusterUpdateSchedulingInfo(UpdateSchedulingInfo r);

    void onJobClusterUpdateLabels(JobClusterManagerProto.UpdateJobClusterLabelsRequest r);

    void onJobClusterUpdateWorkerMigrationConfig(JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest r);

    void onJobClustersList(ListJobClustersRequest request);

    void onJobClusterGet(JobClusterManagerProto.GetJobClusterRequest r);

    void onJobClusterEnable(JobClusterManagerProto.EnableJobClusterRequest r);

    void onJobClusterDisable(JobClusterManagerProto.DisableJobClusterRequest r);

    void onGetJobStatusSubject(JobClusterManagerProto.GetJobSchedInfoRequest request);

    void onGetLatestJobDiscoveryInfo(JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest request);

    void onJobListCompleted(JobClusterManagerProto.ListCompletedJobsInClusterRequest r);

    void onJobList(ListJobsRequest request);

    void onJobIdList(ListJobIdsRequest request);


    // worker related messages

    void onGetLastSubmittedJobIdSubject(JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest request);

    void onWorkerEvent(WorkerEvent r);

    // Job related messages

    void onJobSubmit(JobClusterManagerProto.SubmitJobRequest request);

    void onJobKillRequest(KillJobRequest request);

    void onGetJobDetailsRequest(JobClusterManagerProto.GetJobDetailsRequest request);

    void onScaleStage(ScaleStageRequest scaleStage);

    void onResubmitWorker(ResubmitWorkerRequest r);

    void onListArchivedWorkers(ListArchivedWorkersRequest request);

    void onListActiveWorkers(JobClusterManagerProto.ListWorkersRequest request);

    void onReconcileJobClusters(JobClusterManagerProto.ReconcileJobCluster p);
}
