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
import lombok.EqualsAndHashCode;
import lombok.Value;

/**
 * SkuSizeSpec class represents the SKU size specifications.
 * Contains the name of the SKU size (equivalent to the 't-shirt size') and the machine definition.
 */
@Builder
@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class SkuSizeSpec {
    // The name of the SKU size (e.g. small, large etc.)
    String name;

    // The MachineDefinition object that includes the specifications of a machine: CPU, disk, memory, and network.
    MachineDefinition machineDefinition;

    @JsonCreator
    public SkuSizeSpec(
        @JsonProperty("name") final String name,
        @JsonProperty("machineDefinition") final MachineDefinition machineDefinition) {
        this.name = name;
        this.machineDefinition = machineDefinition;
    }

    public boolean isSizeValid() {
        return machineDefinition.getCpuCores() >= 1 && machineDefinition.getDiskMB() >= 1 && machineDefinition.getMemoryMB() >= 1 && machineDefinition.getNetworkMbps() >= 1;
    }
}
