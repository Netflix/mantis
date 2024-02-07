/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.server.core.scheduler;

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The `SizeDefinition` class defines the scheduling constraint for worker container sizes.
 * This constraint can be expressed in two ways:
 *   1. A formal machine definition, which includes attributes such as cores, memory, disk, and network bandwidth (represented by `machineDefinition`).
 *   2. A size name label, which logically binds a size definition to the task executors of a given resource cluster specification.
 */
@Getter
@EqualsAndHashCode
@ToString
public class SizeDefinition {
    public static final String SIZE_NAME_LABEL = "sizeName";

    /**
     * A reference to the `MachineDefinition` class instance, representing the formal machine specification.
     */
    MachineDefinition machineDefinition;

    /**
     * A label representing the size definition, which is used to logically bind a task executor to a given resource cluster specification. Ie. small, large
     */
    String sizeName;

    SizeDefinition(MachineDefinition machineDefinition, String sizeName) {
        this.machineDefinition = machineDefinition;
        this.sizeName = (sizeName == null || sizeName.trim().isEmpty()) ? null : sizeName.trim();
    }

    public static SizeDefinition of(MachineDefinition machineDefinition) {
        return of(machineDefinition, null);
    }

    public static SizeDefinition of(String sizeName) {
        return of(null, sizeName);
    }

    public static SizeDefinition of(MachineDefinition machineDefinition, String sizeName) {
        return new SizeDefinition(machineDefinition, sizeName);
    }

    /**
     * Calculates the fitness score between the current and passed instances.
     * If `sizeName` is present in both, and they match, it returns a fitness score of 1.0, and 0.0 otherwise.
     * If `sizeName` is missing in either of the instances, it falls back to calculating fitness based on the cores and memory attributes in the `machineDefinition`.
     *
     * @param o The `SizeDefinition` instance to compare with this instance.
     * @return The calculated fitness score which lies in the range of 0 to 1, inclusive. A score of 0.0 is returned if the `sizeName` doesn't match or if the current instance cannot fit the passed one.
     * A fitness score of 1.0 is returned if the `sizeName` match. When the `sizeName` is missing from either of the instances, it falls back to the `machineDefinition`'s `fitnessCoresAndMem()`.
     * This returns a score ranging from 0 (exclusive) to 1, which indicates how closely the current machine definition's cores and memory fit the passed instance.
     */
    public double calculateFitness(SizeDefinition o) {
        if (sizeName != null && o.sizeName != null) {
            if (sizeName.equalsIgnoreCase(o.sizeName)) {
                return 1.0;
            } else {
                return 0.0;
            }
        }
        if (machineDefinition != null & o.machineDefinition != null) {
            return machineDefinition.fitnessCoresAndMem(o.getMachineDefinition());
        }
        return 0.0;
    }

    /**
     * Checks if the current `SizeDefinition` can fit within another `SizeDefinition` (o).
     * The fit is considered to be true if:
     * -  The `sizeName` of the current instance matches that of the passed instance.
     * -  The `machineDefinition` of the current instance can fit within the `machineDefinition` of the passed instance.
     * Returns false if neither conditions are met.
     *
     * @param o The `SizeDefinition` instance to compare with this instance.
     * @return true if the current instance can fit within the passed instance, else returns false.
     */
    public boolean canFit(SizeDefinition o) {
        if (sizeName != null && o.sizeName != null) {
            return sizeName.equalsIgnoreCase(o.sizeName);
        }
        if (machineDefinition != null && o.machineDefinition != null) {
            return machineDefinition.canFit(o.getMachineDefinition());
        }
        return false;
    }

    public Map<String, String> getTags() {
        if (sizeName != null) {
            return ImmutableMap.of(SIZE_NAME_LABEL, sizeName);
        }
        return ImmutableMap.of(
            "cpuCores",
            String.valueOf(machineDefinition.getCpuCores()),
            "memoryMB",
            String.valueOf(machineDefinition.getMemoryMB()));
    }
}
