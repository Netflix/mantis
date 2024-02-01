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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class ScheduleRequest implements QueuableTask {

    public static final QAttributes DEFAULT_Q_ATTRIBUTES = new QAttributes() {
        @Override
        public String getBucketName() {
            return "default";
        }

        @Override
        public int getTierNumber() {
            return 0;
        }
    };

    private static final String defaultGrpName = "defaultGrp";
    private final WorkerId workerId;
    private final int stageNum;
    private final int numPortsRequested;
    private final JobMetadata jobMetadata;
    private final MantisJobDurationType durationType;
    private final SchedulingConstraints schedulingConstraints;
    private final List<ConstraintEvaluator> hardConstraints;
    private final List<VMTaskFitnessCalculator> softConstraints;
    private final Optional<String> preferredCluster;
    private volatile long readyAt;

    public ScheduleRequest(final WorkerId workerId,
                           final int stageNum,
                           final int numPortsRequested,
                           final JobMetadata jobMetadata,
                           final MantisJobDurationType durationType,
                           final SchedulingConstraints schedulingConstraints,
                           final List<ConstraintEvaluator> hardConstraints,
                           final List<VMTaskFitnessCalculator> softConstraints,
                           final long readyAt,
                           final Optional<String> preferredCluster) {
        this.workerId = workerId;
        this.stageNum = stageNum;
        this.numPortsRequested = numPortsRequested;
        this.jobMetadata = jobMetadata;
        this.durationType = durationType;
        this.schedulingConstraints = schedulingConstraints;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.readyAt = readyAt;
        this.preferredCluster = preferredCluster;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScheduleRequest that = (ScheduleRequest) o;

        return workerId != null ? workerId.equals(that.workerId) : that.workerId == null;
    }

    @Override
    public int hashCode() {
        return workerId != null ? workerId.hashCode() : 0;
    }

    @Override
    public String getId() {
        return workerId.getId();
    }

    public WorkerId getWorkerId() {
        return workerId;
    }

    @Override
    public String taskGroupName() {
        return defaultGrpName;
    }

    @Override
    public double getCPUs() {
        return schedulingConstraints.getMachineDefinition().getCpuCores();
    }

    @Override
    public double getMemory() {
        return schedulingConstraints.getMachineDefinition().getMemoryMB();
    }

    @Override
    public double getNetworkMbps() {
        return schedulingConstraints.getMachineDefinition().getNetworkMbps();
    }

    @Override
    public double getDisk() {
        return schedulingConstraints.getMachineDefinition().getDiskMB();
    }

    @Override
    public int getPorts() {
        return numPortsRequested;
    }

    public JobMetadata getJobMetadata() {
        return jobMetadata;
    }

    public SchedulingConstraints getSchedulingConstraints() {
        return schedulingConstraints;
    }

    public MachineDefinition getMachineDefinition() {
        return schedulingConstraints.getMachineDefinition();
    }

    @Override
    public Map<String, Double> getScalarRequests() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
        return Collections.emptyMap();
    }

    @Override
    public List<ConstraintEvaluator> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public List<VMTaskFitnessCalculator> getSoftConstraints() {
        return softConstraints;
    }

    @Override
    public AssignedResources getAssignedResources() {
        // not used by Mantis
        return null;
    }

    @Override
    public void setAssignedResources(AssignedResources assignedResources) {
        // no-op  Not using them at this time
    }

    public MantisJobDurationType getDurationType() {
        return durationType;
    }

    public int getStageNum() {
        return stageNum;
    }

    @Override
    public QAttributes getQAttributes() {
        return DEFAULT_Q_ATTRIBUTES;
    }

    @Override
    public long getReadyAt() {
        return readyAt;
    }

    @Override
    public void safeSetReadyAt(long when) {
        readyAt = when;
    }

    public Optional<String> getPreferredCluster() {
        return preferredCluster;
    }

    @Override
    public String toString() {
        return "ScheduleRequest{" +
                "workerId=" + workerId +
                ", readyAt=" + readyAt +
                '}';
    }
}
