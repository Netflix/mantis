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

package io.mantisrx.runtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


public class MachineDefinition {

    private static final double defaultMbps = 128.0;
    private static final int minPorts = 1;
    private final double cpuCores;
    private final double memoryMB;
    private final double networkMbps;
    private final double diskMB;
    private final int numPorts;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MachineDefinition(@JsonProperty("cpuCores") double cpuCores,
                             @JsonProperty("memoryMB") double memoryMB,
                             @JsonProperty("networkMbps") double networkMbps,
                             @JsonProperty("diskMB") double diskMB,
                             @JsonProperty("numPorts") int numPorts) {
        this.cpuCores = cpuCores;
        this.memoryMB = memoryMB;
        this.networkMbps = networkMbps == 0 ? defaultMbps : networkMbps;
        this.diskMB = diskMB;
        this.numPorts = Math.max(minPorts, numPorts);
    }

    public MachineDefinition(double cpuCores, double memoryMB, double diskMB, int numPorts) {
        this.cpuCores = cpuCores;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.numPorts = Math.max(minPorts, numPorts);
        this.networkMbps = 128;
    }

    public double getCpuCores() {
        return cpuCores;
    }

    public double getMemoryMB() {
        return memoryMB;
    }

    public double getNetworkMbps() {
        return networkMbps;
    }

    public double getDiskMB() {
        return diskMB;
    }

    public int getNumPorts() {
        return numPorts;
    }

    @Override
    public String toString() {
        return "MachineDefinition{" +
                "cpuCores=" + cpuCores +
                ", memoryMB=" + memoryMB +
                ", networkMbps=" + networkMbps +
                ", diskMB=" + diskMB +
                ", numPorts=" + numPorts +
                '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(cpuCores);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(diskMB);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(memoryMB);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(networkMbps);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + numPorts;
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
        MachineDefinition other = (MachineDefinition) obj;
        if (Double.doubleToLongBits(cpuCores) != Double.doubleToLongBits(other.cpuCores))
            return false;
        if (Double.doubleToLongBits(diskMB) != Double.doubleToLongBits(other.diskMB))
            return false;
        if (Double.doubleToLongBits(memoryMB) != Double.doubleToLongBits(other.memoryMB))
            return false;
        if (Double.doubleToLongBits(networkMbps) != Double.doubleToLongBits(other.networkMbps))
            return false;
        if (numPorts != other.numPorts)
            return false;
        return true;
    }
}
