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
import java.util.List;

import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;


public class StageSchedulingInfo {

    private final int numberOfInstances;
    private final MachineDefinition machineDefinition;
    private final List<JobConstraints> hardConstraints;
    private final List<JobConstraints> softConstraints;
    private StageScalingPolicy scalingPolicy;
    private final boolean inheritInstanceCount;
    private final boolean scalable;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageSchedulingInfo(@JsonProperty("numberOfInstances") int numberOfInstances,
                               @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                               @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                               @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                               @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                               @JsonProperty("scalable") boolean scalable,
                               @JsonProperty("inheritInstanceCount") boolean inheritInstanceCount) {
        this.numberOfInstances = numberOfInstances;
        this.machineDefinition = machineDefinition;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.scalingPolicy = scalingPolicy;
        this.inheritInstanceCount = inheritInstanceCount;
        this.scalable = scalable;
    }

    public static void main(String[] args) {
        String json = "{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":2048.0,\"diskMB\":1.0,\"numPorts\":1},\"hardConstraints\":[\"UniqueHost\"],\"softConstraints\":[\"ExclusiveHost\"],\"scalable\":\"true\"}";
        ObjectMapper mapper = new ObjectMapper();
        try {
            StageSchedulingInfo info = mapper.readValue(json, StageSchedulingInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getNumberOfInstances() {
        return numberOfInstances;
    }

    public MachineDefinition getMachineDefinition() {
        return machineDefinition;
    }

    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    public StageScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }

    public void setScalingPolicy(StageScalingPolicy scalingPolicy) {
        this.scalingPolicy = scalingPolicy;
    }

    public boolean getInheritInstanceCount() { return inheritInstanceCount; }

    public boolean getScalable() {
        return scalable;
    }

    @Override
    public String toString() {
        return "StageSchedulingInfo{" +
                "numberOfInstances=" + numberOfInstances +
                ", machineDefinition=" + machineDefinition +
                ", hardConstraints=" + hardConstraints +
                ", softConstraints=" + softConstraints +
                ", scalingPolicy=" + scalingPolicy +
                ", inheritInstanceCount=" + inheritInstanceCount +
                ", scalable=" + scalable +
                '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hardConstraints == null) ? 0 : hardConstraints.hashCode());
        result = prime * result + ((machineDefinition == null) ? 0 : machineDefinition.hashCode());
        result = prime * result + numberOfInstances;
        result = prime * result + (scalable ? 1231 : 1237);
        result = prime * result + ((inheritInstanceCount ? 1231 : 1237));
        result = prime * result + ((scalingPolicy == null) ? 0 : scalingPolicy.hashCode());
        result = prime * result + ((softConstraints == null) ? 0 : softConstraints.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StageSchedulingInfo other = (StageSchedulingInfo) obj;
        if (hardConstraints == null) {
            if (other.hardConstraints != null)
                return false;
        } else if (!hardConstraints.equals(other.hardConstraints))
            return false;
        if (machineDefinition == null) {
            if (other.machineDefinition != null)
                return false;
        } else if (!machineDefinition.equals(other.machineDefinition))
            return false;
        if (numberOfInstances != other.numberOfInstances)
            return false;
        if (inheritInstanceCount != other.inheritInstanceCount)
            return false;
        if (scalable != other.scalable)
            return false;
        if (scalingPolicy == null) {
            if (other.scalingPolicy != null)
                return false;
        } else if (!scalingPolicy.equals(other.scalingPolicy))
            return false;
        if (softConstraints == null) {
            if (other.softConstraints != null)
                return false;
        } else if (!softConstraints.equals(other.softConstraints))
            return false;
        return true;
    }

    public static class Builder {
        private int numberOfInstances;
        private MachineDefinition machineDefinition;
        private List<JobConstraints> hardConstraints;
        private List<JobConstraints> softConstraints;
        private StageScalingPolicy scalingPolicy;
        private boolean scalable;
        private boolean inheritInstanceCount;

        public Builder setNumberOfInstances(int numberOfInstances) {
            this.numberOfInstances = numberOfInstances;
            return this;
        }

        public Builder setOptionalMachineDefinition(MachineDefinition machineDefinition) {
            this.machineDefinition = machineDefinition;
            return this;
        }

        public Builder setOptionalHardConstraints(List<JobConstraints> hardConstraints) {
            this.hardConstraints = hardConstraints;
            return this;
        }

        public Builder setOptionalSoftConstraints(List<JobConstraints> softConstraints) {
            this.softConstraints = softConstraints;
            return this;
        }

        public Builder setOptionalScalingPolicy(StageScalingPolicy scalingPolicy) {
            this.scalingPolicy = scalingPolicy;
            return this;
        }

        public Builder setScalable(boolean scalable) {
            this.scalable = scalable;
            return this;
        }

        public Builder setInheritInstanceCount(boolean inheritInstanceCount) {
            this.inheritInstanceCount = inheritInstanceCount;
            return this;
        }

        public Builder cloneWithoutNumberOfInstances(StageSchedulingInfo other) {
            return this.setInheritInstanceCount(other.getInheritInstanceCount())
                    .setOptionalMachineDefinition(other.getMachineDefinition())
                    .setScalable(other.getScalable())
                    .setOptionalHardConstraints(other.getHardConstraints())
                    .setOptionalSoftConstraints(other.getSoftConstraints())
                    .setOptionalScalingPolicy(other.getScalingPolicy());
        }

        public StageSchedulingInfo build() {
            return new StageSchedulingInfo(
                    numberOfInstances,
                    machineDefinition,
                    hardConstraints,
                    softConstraints,
                    scalingPolicy,
                    scalable,
                    inheritInstanceCount);
        }
    }
}
