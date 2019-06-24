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

import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;


public class ExecuteStageRequest {

    private final boolean hasJobMaster;
    private final long subscriptionTimeoutSecs;
    private final long minRuntimeSecs;
    private final WorkerPorts workerPorts;
    private String jobName;
    private String jobId;
    private int workerIndex;
    private int workerNumber;
    private URL jobJarUrl;
    private int stage;
    private int totalNumStages;
    private int metricsPort;
    private List<Integer> ports = new LinkedList<Integer>();
    private long timeoutToReportStart;
    private List<Parameter> parameters = new LinkedList<Parameter>();
    private SchedulingInfo schedulingInfo;
    private MantisJobDurationType durationType;

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
                               @JsonProperty("workerPorts") WorkerPorts workerPorts
    ) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobJarUrl = jobJarUrl;
        this.stage = stage;
        this.totalNumStages = totalNumStages;
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

    @Override
    public String toString() {
        return "ExecuteStageRequest{" +
                "jobName='" + jobName + '\'' +
                ", jobId='" + jobId + '\'' +
                ", workerIndex=" + workerIndex +
                ", workerNumber=" + workerNumber +
                ", jobJarUrl=" + jobJarUrl +
                ", stage=" + stage +
                ", totalNumStages=" + totalNumStages +
                ", metricsPort=" + metricsPort +
                ", ports=" + ports +
                ", timeoutToReportStart=" + timeoutToReportStart +
                ", parameters=" + parameters +
                ", schedulingInfo=" + schedulingInfo +
                ", durationType=" + durationType +
                ", hasJobMaster=" + hasJobMaster +
                ", subscriptionTimeoutSecs=" + subscriptionTimeoutSecs +
                ", minRuntimeSecs=" + minRuntimeSecs +
                ", workerPorts=" + workerPorts +
                '}';
    }
}
