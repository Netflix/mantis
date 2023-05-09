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
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonInclude;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Singular;

@Builder(toBuilder = true)
public class StageSchedulingInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int numberOfInstances;
    private final MachineDefinition machineDefinition;
    @Singular(ignoreNullCollections = true) private final List<JobConstraints> hardConstraints;
    @Singular(ignoreNullCollections = true) private final List<JobConstraints> softConstraints;
    @Nullable
    private final StageScalingPolicy scalingPolicy;
    private final boolean scalable;

    /**
     * Nullable field to store container attributes like sku ID assigned to this stage.
     */
    @JsonInclude(Include.NON_NULL)
    private final Map<String, String> containerAttributes;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageSchedulingInfo(@JsonProperty("numberOfInstances") int numberOfInstances,
                               @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                               @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                               @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                               @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                               @JsonProperty("scalable") boolean scalable,
                               @JsonProperty("containerAttributes") Map<String, String> containerAttributes) {
        this.numberOfInstances = numberOfInstances;
        this.machineDefinition = machineDefinition;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.scalingPolicy = scalingPolicy;

        this.scalable = scalable;
        this.containerAttributes = containerAttributes;
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

    @Nullable
    public StageScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }

    public boolean getScalable() {
        return scalable;
    }

    public Map<String, String> getContainerAttributes() {
        return this.containerAttributes;
    }

    @Override
    public String toString() {
        return "StageSchedulingInfo{" +
                "numberOfInstances=" + numberOfInstances +
                ", machineDefinition=" + machineDefinition +
                ", hardConstraints=" + hardConstraints +
                ", softConstraints=" + softConstraints +
                ", scalingPolicy=" + scalingPolicy +
                ", scalable=" + scalable +
                ", containerAttributes=" + containerAttributes +
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
        result = prime * result + ((scalingPolicy == null) ? 0 : scalingPolicy.hashCode());
        result = prime * result + ((softConstraints == null) ? 0 : softConstraints.hashCode());
        result = prime * result + ((containerAttributes == null) ? 0 : containerAttributes.hashCode());
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
        if (containerAttributes == null) {
            if (other.containerAttributes != null)
                return false;
        } else if (!containerAttributes.equals(other.containerAttributes))
            return false;
        return true;
    }


}
