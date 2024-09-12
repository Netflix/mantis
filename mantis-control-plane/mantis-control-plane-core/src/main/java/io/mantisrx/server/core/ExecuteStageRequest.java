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
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * ExecuteStageRequest represents the data structure that defines the StageTask workload a given worker needs to run.
 * The data structure is sent over the wire using java serialization when the server requests a given task executor to
 * perform a certain stage task.
 */
@Slf4j
@Getter
@ToString
@EqualsAndHashCode
public class ExecuteStageRequest implements Serializable {
    //todo: refactor into ConfigurationProvider or something equivalent and drive from there!
    public static final long DEFAULT_HEARTBEAT_INTERVAL_SECS = 20;
    private static final long serialVersionUID = 1L;

    // indicates whether this is stage 0 or not. stage 0 runs the autoscaler for the mantis job.
    private final boolean hasJobMaster;
    // interval in seconds between worker heartbeats
    private final long heartbeatIntervalSecs;
    // subscription threshold for when a sink should be considered inactive so that ephemeral jobs producing the sink
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
    private final List<Integer> ports = new ArrayList<>();
    private final long timeoutToReportStart;
    private final List<Parameter> parameters;
    private final SchedulingInfo schedulingInfo;
    private final MantisJobDurationType durationType;
    // class name that provides the job provider.
    @Nullable
    private final String nameOfJobProviderClass;
    private final String user;
    private final String jobVersion;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public ExecuteStageRequest(
        @JsonProperty("jobName") String jobName,
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
        @JsonProperty("heartbeatIntervalSecs") long heartbeatIntervalSecs,
        @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
        @JsonProperty("minRuntimeSecs") long minRuntimeSecs,
        @JsonProperty("workerPorts") WorkerPorts workerPorts,
        @JsonProperty("nameOfJobProviderClass") Optional<String> nameOfJobProviderClass,
        @JsonProperty("user") String user,
        @JsonProperty("jobVersion") String jobVersion) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobJarUrl = jobJarUrl;
        this.stage = stage;
        this.totalNumStages = totalNumStages;
        this.nameOfJobProviderClass = nameOfJobProviderClass.orElse(null);
        this.user = user;
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
        this.heartbeatIntervalSecs = (heartbeatIntervalSecs > 0) ? heartbeatIntervalSecs : DEFAULT_HEARTBEAT_INTERVAL_SECS;
        log.debug("heartbeat interval {}, using {}", heartbeatIntervalSecs, this.heartbeatIntervalSecs);
        this.hasJobMaster = schedulingInfo != null && schedulingInfo.forStage(0) != null;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.minRuntimeSecs = minRuntimeSecs;
        this.workerPorts = workerPorts;
        this.jobVersion = jobVersion;
    }

    public boolean getHasJobMaster() {
        return hasJobMaster;
    }

    public java.util.Optional<String> getNameOfJobProviderClass() {
        return java.util.Optional.ofNullable(nameOfJobProviderClass);
    }

    public WorkerId getWorkerId() {
        return new WorkerId(jobId, workerIndex, workerNumber);
    }
}
