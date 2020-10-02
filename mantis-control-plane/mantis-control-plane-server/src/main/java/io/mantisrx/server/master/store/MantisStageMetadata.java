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

package io.mantisrx.server.master.store;

import java.util.Collection;
import java.util.List;

import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;


public interface MantisStageMetadata {

    String getJobId();

    int getStageNum();

    int getNumStages();

    MachineDefinition getMachineDefinition();

    int getNumWorkers();

    List<JobConstraints> getHardConstraints();

    List<JobConstraints> getSoftConstraints();

    StageScalingPolicy getScalingPolicy();

    boolean getScalable();

    Collection<MantisWorkerMetadata> getWorkerByIndexMetadataSet();

    Collection<MantisWorkerMetadata> getAllWorkers();

    MantisWorkerMetadata getWorkerByIndex(int workerIndex) throws InvalidJobException;

    MantisWorkerMetadata getWorkerByWorkerNumber(int workerNumber) throws InvalidJobException;
}
