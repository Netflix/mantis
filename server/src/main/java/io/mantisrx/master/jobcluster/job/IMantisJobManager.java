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


package io.mantisrx.master.jobcluster.job;

import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.KillJobRequest;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.master.jobcluster.proto.JobProto.CheckHeartBeat;
import io.mantisrx.master.jobcluster.proto.JobProto.InitJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.scheduler.WorkerEvent;


/**
 * An interface that declares the behavior of the Mantis Job Manager Actor.
 */
public interface IMantisJobManager {

    /**
     * Returns metadata associated with this job.
     *
     * @return
     */
    IMantisJobMetadata getJobDetails();

    /**
     * Process the scheduling info request from a client by responding with
     * {@link JobClusterManagerProto.GetJobSchedInfoResponse} which will stream details about the workers
     * for this job.
     *
     * @param r
     */
    void onGetJobStatusSubject(JobClusterManagerProto.GetJobSchedInfoRequest r);

    /**
     * Process the discovery info request from a client by responding with
     *
     * @param r {@link JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse} with latest discovery info for this job
     */
    void onGetLatestJobDiscoveryInfo(JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest r);

    /**
     * Process worker related events. Update worker state and transition worker to new states.
     *
     * @param e
     */
    void processWorkerEvent(WorkerEvent e);

    /**
     * Returns the {@link JobId} of this job.
     *
     * @return
     */
    JobId getJobId();

    /**
     * Returns the current {@link JobState} of this job.
     *
     * @return
     */
    JobState getJobState();

    /**
     * Invoked when all workers of this job have entered the Started state for the first time. This will
     * transition the Job into Launched state.
     *
     * @return
     */
    boolean onAllWorkersStarted();

    /**
     * Invoked when all workers of this job have been terminated. This will trigger final clean up of this job.
     */
    void onAllWorkersCompleted();

    /**
     * Invoked when the number of automatic worker resubmits exceeds configured threshold. This will trigger
     * a job shutdown.
     *
     * @return
     */
    boolean onTooManyWorkerResubmits();

    /**
     * Invoked when a job termination request is received. This should tear down the job.
     *
     * @param state
     * @param reason
     */
    void shutdown(JobState state, String reason);

    /**
     * If the job had been launched with a runtime limit then this method gets invoked after that limit has
     * been reached. The Job should then begin termination process.
     *
     * @param r
     */
    void onRuntimeLimitReached(JobProto.RuntimeLimitReached r);

    /**
     * Invoked by the Job Cluster Actor to commence job initialization.
     *
     * @param i
     */
    void onJobInitialize(InitJob i);

    /**
     * Returns Job details using {@link JobClusterManagerProto.GetJobDetailsResponse}.
     *
     * @param r
     */
    void onGetJobDetails(GetJobDetailsRequest r);

    /**
     * Invoked at a periodic basis to make sure all workers of this job have sent heart beats within a
     * preconfigured interval.
     *
     * @param r
     */
    void onCheckHeartBeats(CheckHeartBeat r);

    /**
     * Invoked during Agent fleet deployment to move workers onto the new agent fleet.
     *
     * @param r
     */
    void onMigrateWorkers(JobProto.MigrateDisabledVmWorkersRequest r);

    /**
     * Invoked to trigger job termination.
     *
     * @param req
     */
    void onJobKill(KillJobRequest req);

    /**
     * Invoked by either Job Master or a user to change the number of workers of this job.
     *
     * @param scaleStage
     */
    void onScaleStage(ScaleStageRequest scaleStage);

    /**
     * Invoked to explicitly resubmit a particular worker.
     *
     * @param r
     */
    void onResubmitWorker(ResubmitWorkerRequest r);

    /**
     * Returns a list of active workers for this job using {@link JobClusterManagerProto.ListWorkersResponse}.
     *
     * @param request
     */
    void onListActiveWorkers(JobClusterManagerProto.ListWorkersRequest request);

    /**
     * Send worker assignments if there have been changes.
     * @param p
     */
    void onSendWorkerAssignments(JobProto.SendWorkerAssignementsIfChanged p);
}
