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
package io.mantisrx.server.worker;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;

public class MachineDefinitionUtils {
  public static MachineDefinition sys(WorkerPorts workerPorts) {
    return new MachineDefinition(
        Hardware.getNumberCPUCores(),
        Hardware.getSizeOfPhysicalMemory(),
        Hardware.getSizeOfDisk(),
        workerPorts.getNumberOfPorts());
  }
}
