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

package io.mantisrx.server.core;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.base.Optional;
import java.io.Serializable;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * ExecuteStageRequest represents the data structure that defines the StageTask workload a given worker needs to run.
 * The data structure is sent over the wire using java serialization when the server requests a given task executor to
 * perform a certain stage task.
 */
@EqualsAndHashCode
@ToString
public class ExecuteStageRequest implements Serializable {

    // indicates whether this is stage 0 or not. stage 0 runs the autoscaler for the mantis job.
    private final boolean hasJobMaster;
    // subscription threshold for when a sink should considered to be inactive so that ephemeral jobs producing the sink
    // can be shutdown.
    private final long subscriptionTimeoutSecs;
    private final long minRuntimeSecs;
    private final WorkerPorts workerPorts;
    private final String jobName;
    // jobId represents the instance of the job.
    private final String jobId;
    // index of the worker in that stage
    private final int workerIndex;
    // rolling counter of workers for that stage
    private final int workerNumber;
    private final URL jobJarUrl;
    // index of the stage
    private final int stage;
    private final int totalNumStages;
    private final int metricsPort;
    private final List<Integer> ports = new LinkedList<Integer>();
    private final long timeoutToReportStart;
    private List<Parameter> parameters = new LinkedList<Parameter>();
    private final SchedulingInfo schedulingInfo;
    private final MantisJobDurationType durationType;
    // class name that provides the job provider.
    @Nullable
    private final String nameOfJobProviderClass;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public ExecuteStageRequest(@JsonProperty("jobName") String jobName,
                               @JsonProperty("jobID") String jobId,
                               @JsonProperty("workerIndex") int workerIndex,
                               @JsonProperty("workerNumber") int workerNumber,
                               @JsonProperty("jobJarUrl") URL jobJarUrl,
                               @JsonProperty("stage") int stage,
                               @JsonProperty("totalNumStages") int totalNumStages,
                               @JsonProperty("ports") List<Integer> ports,
                               @JsonProperty("timeoutToReportStart") long timeoutToReportStart,
                               @JsonProperty("metricsPort") int metricsPort,
                               @JsonProperty("parameters") List<Parameter> parameters,
                               @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo,
                               @JsonProperty("durationType") MantisJobDurationType durationType,
                               @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                               @JsonProperty("minRuntimeSecs") long minRuntimeSecs,
                               @JsonProperty("workerPorts") WorkerPorts workerPorts,
                               @JsonProperty("jobProviderClass") java.util.Optional<String> nameOfJobProviderClass) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobJarUrl = jobJarUrl;
        this.stage = stage;
        this.totalNumStages = totalNumStages;
        this.nameOfJobProviderClass = nameOfJobProviderClass.orElse(null);
        this.ports.addAll(ports);
        this.metricsPort = metricsPort;
        this.timeoutToReportStart = timeoutToReportStart;
        if (parameters != null) {
            this.parameters = parameters;
        } else {
            this.parameters = new LinkedList<>();
        }
        this.schedulingInfo = schedulingInfo;
        this.durationType = durationType;
        hasJobMaster = schedulingInfo != null && schedulingInfo.forStage(0) != null;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.minRuntimeSecs = minRuntimeSecs;
        this.workerPorts = workerPorts;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobId() {
        return jobId;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public URL getJobJarUrl() {
        return jobJarUrl;
    }

    public int getStage() {
        return stage;
    }

    public int getTotalNumStages() {
        return totalNumStages;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public WorkerPorts getWorkerPorts() {
        return workerPorts;
    }

    public long getTimeoutToReportStart() {
        return timeoutToReportStart;
    }

    public MantisJobDurationType getDurationType() {
        return durationType;
    }

    public boolean getHasJobMaster() {
        return hasJobMaster;
    }

    public long getSubscriptionTimeoutSecs() {
        return subscriptionTimeoutSecs;
    }

    public long getMinRuntimeSecs() {
        return minRuntimeSecs;
    }

    public java.util.Optional<String> getNameOfJobProviderClass() {
        return java.util.Optional.ofNullable(nameOfJobProviderClass);
    }

    public WorkerId getWorkerId() {
        return new WorkerId(jobId, workerIndex, workerNumber);
    }
}
