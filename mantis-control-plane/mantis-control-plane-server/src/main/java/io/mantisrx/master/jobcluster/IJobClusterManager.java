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

package io.mantisrx.master.jobcluster;

import io.mantisrx.master.JobClustersManagerActor.UpdateSchedulingInfo;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListCompletedJobsInClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.DeleteJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.EnforceSLARequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.InitializeJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.JobStartedEvent;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.KillJobRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.KillJobResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.TriggerCronRequest;
import io.mantisrx.master.jobcluster.proto.JobProto.JobInitialized;
import io.mantisrx.server.master.scheduler.WorkerEvent;


/**
 * Declares the behavior for Job Cluster Manager.
 */
public interface IJobClusterManager {

    void onJobClusterInitialize(InitializeJobClusterRequest initReq);

    void onJobClusterUpdate(UpdateJobClusterRequest request);

    void onJobClusterDelete(DeleteJobClusterRequest request);

    void onJobList(ListJobsRequest request);

    void onJobListCompleted(ListCompletedJobsInClusterRequest request);

    void onJobClusterDisable(DisableJobClusterRequest req);

    void onJobClusterEnable(EnableJobClusterRequest req);

    void onJobClusterGet(GetJobClusterRequest request);

    void onJobSubmit(SubmitJobRequest request);

    void onJobInitialized(JobInitialized jobInited);

    void onJobStarted(JobStartedEvent startedEvent);

    void onWorkerEvent(WorkerEvent r);

    void onJobKillRequest(KillJobRequest req);

    void onResubmitWorkerRequest(ResubmitWorkerRequest req);

    void onKillJobResponse(KillJobResponse killJobResponse);

    void onGetJobDetailsRequest(GetJobDetailsRequest req);

    void onGetLatestJobDiscoveryInfo(JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest request);

    void onGetJobStatusSubject(JobClusterManagerProto.GetJobSchedInfoRequest request);

    void onGetLastSubmittedJobIdSubject(JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest request);

    void onEnforceSLARequest(EnforceSLARequest request);

    void onBookkeepingRequest(JobClusterProto.BookkeepingRequest request);

    void onJobClusterUpdateSLA(UpdateJobClusterSLARequest slaRequest);

    void onJobClusterUpdateLabels(UpdateJobClusterLabelsRequest labelRequest);

    void onJobClusterUpdateArtifact(UpdateJobClusterArtifactRequest artifactReq);

    void onJobClusterUpdateSchedulingInfo(UpdateSchedulingInfo request);

    void onJobClusterUpdateWorkerMigrationConfig(UpdateJobClusterWorkerMigrationStrategyRequest req);

    void onScaleStage(ScaleStageRequest scaleStage);

    void onResubmitWorker(ResubmitWorkerRequest r);

    void onJobIdList(ListJobIdsRequest request);

    void onListArchivedWorkers(ListArchivedWorkersRequest request);

    void onListActiveWorkers(JobClusterManagerProto.ListWorkersRequest request);

    void onTriggerCron(TriggerCronRequest request);


}
