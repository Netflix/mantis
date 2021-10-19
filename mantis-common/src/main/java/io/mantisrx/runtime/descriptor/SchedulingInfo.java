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

import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class SchedulingInfo {

    private Map<Integer, StageSchedulingInfo> stages = new HashMap<>();

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public SchedulingInfo(
            @JsonProperty("stages") Map<Integer, StageSchedulingInfo> stages) {
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

    public Map<Integer, StageSchedulingInfo> getStages() {
        return stages;
    }

    public void addJobMasterStage(StageSchedulingInfo schedulingInfo) {
        stages.put(0, schedulingInfo);
    }

    public StageSchedulingInfo forStage(int stageNum) {
        return stages.get(stageNum);
    }

    public static class Builder {

        private final Map<Integer, StageSchedulingInfo> builderStages = new HashMap<>();
        private Integer currentStage = 1;
        private int numberOfStages;

        public Builder addStage(StageSchedulingInfo stageSchedulingInfo) {
            builderStages.put(currentStage, stageSchedulingInfo);
            currentStage++;
            return this;
        }

        public void addJobMasterStage(StageSchedulingInfo schedulingInfo) {
            builderStages.put(0, schedulingInfo);
        }

        public Builder numberOfStages(int numberOfStages) {
            this.numberOfStages = numberOfStages;
            return this;
        }

        public Builder singleWorkerStageWithConstraints(
                MachineDefinition machineDefinition,
                List<JobConstraints> hardConstraints,
                List<JobConstraints> softConstraints) {
            return this.addStage(
                    StageSchedulingInfo.builder()
                            .numberOfInstances(1)
                            .machineDefinition(machineDefinition)
                            .hardConstraints(hardConstraints)
                            .softConstraints(softConstraints)
                            .build());
        }

        public Builder singleWorkerStage(MachineDefinition machineDefinition) {
            return this.addStage(
                    StageSchedulingInfo.builder()
                            .numberOfInstances(1)
                            .machineDefinition(machineDefinition)
                            .build());
        }

        public Builder multiWorkerScalableStageWithConstraints(int numberOfWorkers, MachineDefinition machineDefinition,
                                                               List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints,
                                                               StageScalingPolicy scalingPolicy) {
            StageScalingPolicy ssp = new StageScalingPolicy(currentStage, scalingPolicy.getMin(), scalingPolicy.getMax(),
                    scalingPolicy.getIncrement(), scalingPolicy.getDecrement(), scalingPolicy.getCoolDownSecs(), scalingPolicy.getStrategies());
            return this.addStage(
                    StageSchedulingInfo.builder()
                            .numberOfInstances(numberOfWorkers)
                            .machineDefinition(machineDefinition)
                            .hardConstraints(hardConstraints)
                            .softConstraints(softConstraints)
                            .scalingPolicy(ssp)
                            .scalable(ssp.isEnabled())
                            .build());
        }

        public Builder multiWorkerStageWithConstraints(int numberOfWorkers, MachineDefinition machineDefinition,
                                                       List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints) {
            return this.addStage(
                    StageSchedulingInfo.builder()
                            .numberOfInstances(numberOfWorkers)
                            .machineDefinition(machineDefinition)
                            .hardConstraints(hardConstraints)
                            .softConstraints(softConstraints)
                            .build());
        }

        public Builder multiWorkerStage(int numberOfWorkers, MachineDefinition machineDefinition) {
            return multiWorkerStage(numberOfWorkers, machineDefinition, false);
        }

        public Builder multiWorkerStage(int numberOfWorkers, MachineDefinition machineDefinition, boolean scalable) {
            return this.addStage(
                    StageSchedulingInfo.builder()
                            .numberOfInstances(numberOfWorkers)
                            .machineDefinition(machineDefinition)
                            .scalable(scalable)
                            .build());
        }

        /**
         * Setup current builder instance to use clone the stages from given stage info map and apply instance
         * inheritance to each stage if the stage has inherit-config enabled or global force inheritance flag.
         * Note: to add more stages to this builder, the number of stages needs to be adjusted accordingly along with
         * calling other addStage methods.
         * @param givenStages Source stages to be cloned from.
         * @param getInstanceCountForStage Function to get inherited instance count for each stage.
         * @param inheritEnabled Function to get whether a given stage has inherit-enabled.
         * @param forceInheritance Global flag to force inheritance on all stages.
         * @return Current builder instance.
         */
        public Builder createWithInstanceInheritance(
                Map<Integer, StageSchedulingInfo> givenStages,
                Function<Integer, Optional<Integer>> getInstanceCountForStage,
                Function<Integer, Boolean> inheritEnabled,
                boolean forceInheritance) {
            this.numberOfStages(givenStages.size());
            givenStages.keySet().stream().sorted().forEach(k -> {
                Optional<Integer> prevCntO = getInstanceCountForStage.apply(k);
                StageSchedulingInfo resStage = givenStages.get(k);
                if (prevCntO.isPresent() && (forceInheritance || inheritEnabled.apply(k))) {
                    resStage = givenStages.get(k).toBuilder()
                            .numberOfInstances(prevCntO.get())
                            .build();
                }

                // handle JobMaster stage
                if (k == 0) { this.addJobMasterStage(resStage); }
                else { this.addStage(resStage); }
            });

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
