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

package io.mantisrx.runtime.descriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;


public class SchedulingInfo {

    private Map<Integer, StageSchedulingInfo> stages = new HashMap<>();

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public SchedulingInfo(@JsonProperty("stages") Map<Integer, StageSchedulingInfo> stages) {
        this.stages = stages;
    }

    @JsonIgnore
    SchedulingInfo(Builder builder) {
        stages.putAll(builder.builderStages);
    }

    public static void main(String[] args) {
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.Memory, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, 0.1, 0.6, null));
        Builder builder = new Builder()
                .numberOfStages(2)
                .multiWorkerScalableStageWithConstraints(
                        2,
                        new MachineDefinition(1, 1.24, 0.0, 1, 1),
                        null, null,
                        new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap)
                )
                .multiWorkerScalableStageWithConstraints(
                        3,
                        new MachineDefinition(1, 1.24, 0.0, 1, 1),
                        null, null,
                        new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap)
                );
        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println(mapper.writeValueAsString(builder.build()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchedulingInfo that = (SchedulingInfo) o;
        return Objects.equals(stages, that.stages);
    }

    @Override
    public int hashCode() {

        return Objects.hash(stages);
    }

    public Map<Integer, StageSchedulingInfo> getStages() {
        return stages;
    }

    public void addJobMasterStage(StageSchedulingInfo schedulingInfo) {
        stages.put(0, schedulingInfo);
    }

    public StageSchedulingInfo forStage(int stageNum) {
        return stages.get(stageNum);
    }

    @Override
    public String toString() {
        return "SchedulingInfo{" +
                "stages=" + stages +
                '}';
    }

    public static class Builder {

        private Map<Integer, StageSchedulingInfo> builderStages = new HashMap<>();
        private Integer currentStage = 1;
        private int numberOfStages;

        public Builder numberOfStages(int numberOfStages) {
            this.numberOfStages = numberOfStages;
            return this;
        }

        public Builder singleWorkerStageWithConstraints(MachineDefinition machineDefinition,
                                                        List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints) {
            builderStages.put(currentStage, new StageSchedulingInfo(1, machineDefinition, hardConstraints,
                    softConstraints, null, false));
            currentStage++;
            return this;
        }

        public Builder singleWorkerStage(MachineDefinition machineDefinition) {
            builderStages.put(currentStage, new StageSchedulingInfo(1, machineDefinition, null,
                    null, null, false));
            currentStage++;
            return this;
        }

        public Builder multiWorkerScalableStageWithConstraints(int numberOfWorkers, MachineDefinition machineDefinition,
                                                               List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints,
                                                               StageScalingPolicy scalingPolicy) {
            StageScalingPolicy ssp = new StageScalingPolicy(currentStage, scalingPolicy.getMin(), scalingPolicy.getMax(),
                    scalingPolicy.getIncrement(), scalingPolicy.getDecrement(), scalingPolicy.getCoolDownSecs(), scalingPolicy.getStrategies());
            builderStages.put(currentStage, new StageSchedulingInfo(numberOfWorkers, machineDefinition,
                    hardConstraints, softConstraints, ssp, ssp.isEnabled()));
            currentStage++;
            return this;
        }

        public Builder multiWorkerStageWithConstraints(int numberOfWorkers, MachineDefinition machineDefinition,
                                                       List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints) {
            builderStages.put(currentStage, new StageSchedulingInfo(numberOfWorkers, machineDefinition,
                    hardConstraints, softConstraints, null, false));
            currentStage++;
            return this;
        }

        public Builder multiWorkerStage(int numberOfWorkers, MachineDefinition machineDefinition) {
            builderStages.put(currentStage, new StageSchedulingInfo(numberOfWorkers, machineDefinition,
                    null, null, null, false));
            currentStage++;
            return this;
        }

        public SchedulingInfo build() {
            if (numberOfStages == 0) {
                throw new IllegalArgumentException("Number of stages is 0, must be specified using builder.");
            }
            if (numberOfStages != builderStages.size()) {
                throw new IllegalArgumentException("Missing scheduling information, number of stages: " + numberOfStages
                        + " configured stages: " + builderStages.size());
            }
            return new SchedulingInfo(this);
        }
    }
}
