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

import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Represents Metadata associated with a Mantis Job stage.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MantisStageMetadataImpl.class)
})

public interface IMantisStageMetadata {

    /**
     * Returns the {@link JobId} associated with this stage.
     * @return
     */
    JobId getJobId();

    /**
     * Returns the stage number of this stage.
     * @return
     */
    int getStageNum();

    /**
     * Returns the total number of stages.
     * @return
     */
    int getNumStages();

    /**
     * Returns the {@link MachineDefinition} associated with this stage. This is the resource configuration
     * of the workers of this stage.
     * @return
     */
    MachineDefinition getMachineDefinition();

    /**
     * Returns the total number of workers for this tage.
     * @return
     */
    int getNumWorkers();

    /**
     * Returns the List of {@link JobConstraints} (mandatory) associated with this job.
     * @return
     */
    List<JobConstraints> getHardConstraints();

    /**
     * Returns the List of {@link JobConstraints} (best effort) associated with this job.
     * @return
     */
    List<JobConstraints> getSoftConstraints();

    /**
     * Returns the scaling policy {@link StageScalingPolicy} for this stage.
     * @return
     */
    StageScalingPolicy getScalingPolicy();

    /**
     * Returns true if this stage is scalable.
     * @return
     */
    boolean getScalable();

    /**
     * Get list of {@link JobWorker} associated with this stage.
     * Use getAllWorkers instead.
     * @return
     */
    @Deprecated
    Collection<JobWorker> getWorkerByIndexMetadataSet();

    /**
     * Get list of {@link JobWorker} associated with this stage.
     * @return
     */
    Collection<JobWorker> getAllWorkers();

    /**
     * Returns the {@link JobWorker} with the given index.
     * @param workerIndex
     *
     * @return
     *
     * @throws InvalidJobException
     */
    JobWorker getWorkerByIndex(int workerIndex) throws InvalidJobException;


    /**
     * Returns the {@link JobWorker} with the given worker number.
     * @param workerNumber
     *
     * @return
     *
     * @throws InvalidJobException
     */
    JobWorker getWorkerByWorkerNumber(int workerNumber) throws InvalidJobException;


    /**
     * Retrieves the value of the "size" attribute from the stage container attributes map, if it's not null and the key exists.
     *
     * @return
     */
    Optional<String> getSizeAttribute();
}
