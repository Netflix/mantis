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

package io.mantisrx.server.master.scheduler;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.server.core.domain.WorkerId;
import java.util.Objects;
import java.util.Optional;


public class WorkerLaunched implements WorkerEvent {

    private final WorkerId workerId;
    private final int stageNum;
    private final String hostname;
    private final String vmId;
    private final Optional<String> clusterName;
    private final WorkerPorts ports;
    private final long eventTimeMs = System.currentTimeMillis();

    public WorkerLaunched(final WorkerId workerId,
                          final int stageNum,
                          final String hostname,
                          final String vmId,
                          final Optional<String> clusterName,
                          final WorkerPorts ports) {
        this.workerId = workerId;
        this.stageNum = stageNum;
        this.hostname = hostname;
        this.vmId = vmId;
        this.clusterName = clusterName;
        this.ports = ports;
    }

    @Override
    public WorkerId getWorkerId() {
        return workerId;
    }

    public int getStageNum() {
        return stageNum;
    }

    public String getHostname() {
        return hostname;
    }

    public String getVmId() {
        return vmId;
    }

    public Optional<String> getClusterName() {

        return clusterName;
    }

    public WorkerPorts getPorts() {
        return ports;
    }

    @Override
    public long getEventTimeMs() {
        return eventTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerLaunched that = (WorkerLaunched) o;
        return stageNum == that.stageNum &&
                eventTimeMs == that.eventTimeMs &&
                Objects.equals(workerId, that.workerId) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(vmId, that.vmId) &&
                Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(ports, that.ports);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId, stageNum, hostname, vmId, clusterName, ports, eventTimeMs);
    }

    @Override
    public String toString() {
        return "WorkerLaunched{" +
                "workerId=" + workerId +
                ", stageNum=" + stageNum +
                ", hostname='" + hostname + '\'' +
                ", vmId='" + vmId + '\'' +
                ", clusterName=" + clusterName +
                ", ports=" + ports +
                ", eventTimeMs=" + eventTimeMs +
                '}';
    }
}
