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

package io.mantisrx.master.scheduler;

import io.mantisrx.runtime.MachineDefinition;

/**
 * Implementation of FitnessCalculator. Uses cpu cores with higher weight than memory.
 */
public class CpuWeightedFitnessCalculator implements FitnessCalculator {
    @Override
    public double calculate(MachineDefinition requested, MachineDefinition available) {
        if (!available.canFit(requested)) {
            return 0.0;
        }

        double cpuScore = 1 - (available.getCpuCores() - requested.getCpuCores()) / available.getCpuCores();
        double memoryScore = 1 - (available.getMemoryMB() - requested.getMemoryMB()) / available.getMemoryMB();

        // The weight for cpuScore is 18 and for memoryScore is 1. Hence, the total weight is 19.
        // TODO: make these weights configurable.
        return ((18 * cpuScore) + memoryScore) / 19;
    }
}
