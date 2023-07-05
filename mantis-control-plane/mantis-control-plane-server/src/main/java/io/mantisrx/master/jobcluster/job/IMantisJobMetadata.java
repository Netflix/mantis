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

import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.domain.Costs;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * The Metadata associated with a Mantis Job.
 */
public interface IMantisJobMetadata {

    long DEFAULT_STARTED_AT_EPOCH = 0;
    /**
     * Returns the {@link JobId}.
     * @return
     */
    JobId getJobId();

    /**
     * Returns the Job Cluster Name for this job.
     * @return
     */
    String getClusterName();

    /**
     * Returns the submitter of this job.
     * @return
     */
    String getUser();

    /**
     * Returns the {@link Instant} this job was submitted.
     * @return
     */
    Instant getSubmittedAtInstant();

    /**
     * Returns an optional Instant this job went into started state.
     * @return
     */
    Optional<Instant> getStartedAtInstant();

    /**
     * Returns an optional Instant this job completed.
     * @return
     */
    Optional<Instant> getEndedAtInstant();

    /**
     * Returns the artifact associated with this job.
     * @return
     */
    String getArtifactName();

    /**
     * Returns an optional {@link JobSla} for this job if it exists.
     * @return
     */
    Optional<JobSla> getSla();

    /**
     * Returns the subscription timeout in seconds associated with this job.
     * @return
     */
    long getSubscriptionTimeoutSecs();

    /**
     * Returns the current state of this job.
     * @return
     */
    JobState getState();

    /**
     * Returns the list of {@link Parameter} associated with this job.
     * @return
     */
    List<Parameter> getParameters();

    /**
     * Returns the list of {@link Label} associated with this job.
     * @return
     */
    List<Label> getLabels();

    /**
     * Returns metadata about all the stages of this job.
     * @return
     */
    Map<Integer, ? extends IMantisStageMetadata> getStageMetadata();

    /**
     * Returns a count of the number of stages in this job.
     * @return
     */
    int getTotalStages();

    /**
     * Returns {@link IMantisStageMetadata} for the stage identified by the given stage number if one exists.
     * @param stageNum
     * @return
     */
    Optional<IMantisStageMetadata> getStageMetadata(int stageNum);

    /**
     * Returns {@link JobWorker} associated with the given stage number and worker index if one exists.
     * @param stageNumber
     * @param workerIndex
     * @return
     * @throws InvalidJobException
     */
    Optional<JobWorker> getWorkerByIndex(int stageNumber, int workerIndex) throws InvalidJobException;

    /**
     * Returns {@link JobWorker} associated with the given stage number and worker number if one exists.
     * @param workerNumber
     * @return
     * @throws InvalidJobException
     */
    Optional<JobWorker> getWorkerByNumber(int workerNumber) throws InvalidJobException;

    /**
     * Worker numbers are assigned in an incremental fashion. This method returns the next number to use.
     * @return
     */
    int getNextWorkerNumberToUse();

    /**
     * Returns the {@link SchedulingInfo} associated with this job.
     * @return
     */
    SchedulingInfo getSchedulingInfo();

    /**
     * Returns the min runtime in seconds associated with this job (defaults to -1).
     * @return
     */
    long getMinRuntimeSecs();

    /**
     * Returns a {@link URL} pointing to the artifact used by this job. In reality this is not interpreted
     * as a URL. The trailing portion of this is used to identify the artifact.
     * @return
     */
    URL getJobJarUrl();

    /**
     * Returns the {@link JobDefinition} associated with this Job.
     * @return
     */
    JobDefinition getJobDefinition();

    /**
     * Returns the costs associated with this job.
     * @return Costs
     */
    Costs getJobCosts();

    /**
     * Job level heartbeat configuration
     */
    long getHeartbeatIntervalSecs();

    /**
     * Job level timeout interval for worker
     * This resubmits a worker if existing worker
     * is past timeout secs
     */
    long getWorkerTimeoutSecs();
}
