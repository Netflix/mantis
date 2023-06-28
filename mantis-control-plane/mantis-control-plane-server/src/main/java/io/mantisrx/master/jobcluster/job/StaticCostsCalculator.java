/*
 * Copyright 2023 Netflix, Inc.
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

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.domain.Costs;
import lombok.RequiredArgsConstructor;

/**
 * Calculates the cost of a job based on a static set of weights for CPU, memory, and network.
 */
@RequiredArgsConstructor
public class StaticCostsCalculator implements CostsCalculator {

    private final double costCpuHour;
    private final double costMemoryGbHour;
    private final double costNetworkGbpsHour;

    @Override
    public Costs calculateCosts(IMantisJobMetadata jobMetadata) {
        return jobMetadata
            .getStageMetadata()
            .entrySet()
            .stream()
            .map(stageMetadata -> {
                return forMachine(stageMetadata.getValue().getMachineDefinition()).multipliedBy(
                    stageMetadata.getValue().getNumWorkers());
            })
            .reduce(Costs.ZERO, Costs::plus);
    }

    private Costs forMachine(MachineDefinition machineDefinition) {
        return new Costs(
            costCpuHour * machineDefinition.getCpuCores() +
                costMemoryGbHour * machineDefinition.getMemoryMB() / 1024.0 +
                costNetworkGbpsHour * machineDefinition.getNetworkMbps() / 1024.0 * 24);
    }
}
