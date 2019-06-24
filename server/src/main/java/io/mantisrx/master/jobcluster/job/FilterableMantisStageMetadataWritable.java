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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.master.store.MantisStageMetadataWritable;


@JsonFilter("stageMetadataList")
public class FilterableMantisStageMetadataWritable extends MantisStageMetadataWritable {

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public FilterableMantisStageMetadataWritable(@JsonProperty("jobId") String jobId,
                                       @JsonProperty("stageNum") int stageNum,
                                       @JsonProperty("numStages") int numStages,
                                       @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                                       @JsonProperty("numWorkers") int numWorkers,
                                       @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                                       @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                                       @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                                       @JsonProperty("scalable") boolean scalable) {
        super(jobId, stageNum, numStages, machineDefinition, numWorkers, hardConstraints, softConstraints, scalingPolicy, scalable);
    }
}
