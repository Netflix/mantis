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

import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import java.time.Instant;
import java.util.List;
import rx.subjects.BehaviorSubject;


/**
 * Declares the behavior of the WorkerManager which is embedded within a JobManager.
 */
public interface IWorkerManager {

    /**
     * Perform any cleanup during job shutdown.
     */
    void shutdown();

    /**
     * Handle worker related events.
     *
     * @param event
     * @param jobState
     */
    void processEvent(WorkerEvent event, JobState jobState);

    /**
     * Iterate through all active workers and identify and restart workers that have not sent a heart beat
     * within a configured time.
     *
     * @param now
     */
    void checkHeartBeats(Instant now);

    /**
     * Invoked during Agent deploy. Resubmit workers that are currently running on old VMs.
     *
     * @param now
     */
    void migrateDisabledVmWorkers(Instant now);

    /**
     * Increase or decrease the number of workers associated with the given stage.
     *
     * @param stageMetaData
     * @param ruleMax
     * @param ruleMin
     * @param numWorkers
     * @param reason
     *
     * @return
     */
    int scaleStage(MantisStageMetadataImpl stageMetaData, int ruleMax, int ruleMin, int numWorkers, String reason);

    /**
     * Explicitly kill and resubmit worker associated with the given workerNumber.
     *
     * @param workerNumber
     *
     * @throws Exception
     */
    void resubmitWorker(int workerNumber) throws Exception;

    /**
     * Get a list of currently active workers {@link IMantisWorkerMetadata}.
     *
     * @param limit
     *
     * @return
     */
    List<IMantisWorkerMetadata> getActiveWorkers(int limit);

    /**
     * Returns a {@link BehaviorSubject} where job status updates are published.
     *
     * @return
     */
    BehaviorSubject<JobSchedulingInfo> getJobStatusSubject();

    /**
     * Force sending any updates in worker data.
     */
    void refreshAndSendWorkerAssignments();
}
