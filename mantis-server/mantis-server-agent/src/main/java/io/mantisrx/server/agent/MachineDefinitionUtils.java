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
package io.mantisrx.server.agent;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import java.util.Optional;

public class MachineDefinitionUtils {
    private static MachineDefinition sys(WorkerPorts workerPorts, double networkBandwidthInMB) {
        return new MachineDefinition(
            Hardware.getNumberCPUCores(),
            Hardware.getSizeOfPhysicalMemory() / 1024.0 / 1024.0,
            networkBandwidthInMB,
            Hardware.getSizeOfDisk() / 1024.0 / 1024.0,
            workerPorts.getNumberOfPorts());
    }

    public static MachineDefinition from(WorkerConfiguration workerConfiguration, WorkerPorts workerPorts) {
        return fromWorkerConfiguration(workerConfiguration, workerPorts).orElseGet(() -> sys(workerPorts, workerConfiguration.getNetworkBandwidthInMB()));
    }

    private static Optional<MachineDefinition> fromWorkerConfiguration(WorkerConfiguration workerConfiguration, WorkerPorts workerPorts) {
        if (workerConfiguration.getCpuCores() != null && workerConfiguration.getMemoryInMB() != null && workerConfiguration.getDiskInMB() != null) {
            return Optional.of(new MachineDefinition(
                    workerConfiguration.getNetworkBandwidthInMB(),
                    workerConfiguration.getNetworkBandwidthInMB(),
                    workerConfiguration.getNetworkBandwidthInMB(),
                    workerConfiguration.getNetworkBandwidthInMB(),
                    workerPorts.getNumberOfPorts()));
        }
        return Optional.empty();
    }
}
