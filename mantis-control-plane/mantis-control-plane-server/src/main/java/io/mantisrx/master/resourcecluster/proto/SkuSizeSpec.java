/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.master.resourcecluster.proto;

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.Validate;

/**
 * Encapsulates the specifications of a SKU size in terms of the machine's resources.
 * Each SKU size has a name (similar to the label of t-shirt sizes), and a dedicated machine definition that
 * consists of specifications for CPU, disk, memory, and network.
 *
 * The name is a convenient way to refer to a particular configuration of machine resources while the machine definition represents the actual values.
 */
@Builder
@Value
public class SkuSizeSpec {
    // Includes the specifications of a machine: CPU cores, disk in MB, memory in MB, and network bandwidth in Mbps
    MachineDefinition machineDefinition;


    // The name for the SKU size. It is a short, descriptive term typically representing the size of the machine resource being specified (e.g., "small", "medium", "large")
    String name;

    @JsonCreator
    public SkuSizeSpec(
        @JsonProperty("machineDefinition") final MachineDefinition machineDefinition,
        @JsonProperty("name") final String name) {

        validateMachineDefinition(machineDefinition);
        validateName(name);

        this.name = name.trim();
        this.machineDefinition = machineDefinition;
    }

    private void validateMachineDefinition(MachineDefinition machineDefinition) {
        Validate.isTrue(machineDefinition.getCpuCores() >= 1, "CPU cores must be equal to or greater than 1");
        Validate.isTrue(machineDefinition.getDiskMB() >= 1, "Disk size must be equal to or greater than 1MB");
        Validate.isTrue(machineDefinition.getMemoryMB() >= 1, "Memory size must be equal to or greater than 1MB");
        Validate.isTrue(machineDefinition.getNetworkMbps() >= 1, "Network speed must be equal to or greater than 1Mbps");
    }

    private void validateName(String name) {
        Validate.notNull(name, "Name must not be null");
        Validate.isTrue(!name.trim().isEmpty(), "Name must not be empty or only whitespace");
    }
}
