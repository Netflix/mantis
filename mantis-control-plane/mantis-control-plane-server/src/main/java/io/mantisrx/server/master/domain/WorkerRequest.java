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

package io.mantisrx.server.master.domain;

import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class WorkerRequest {

    private final long subscriptionTimeoutSecs;
    private final long minRuntimeSecs;
    private final long jobSubmittedAt;
    private final String user;
    // preferred Cluster to launch the worker on
    private final Optional<String> preferredCluster;
    private String jobName;
    private String jobId;
    private int workerIndex;
    private int workerNumber;
    private URL jobJarUrl;
    private int workerStage;
    private int totalStages;
    private MachineDefinition definition;
    private int numInstancesAtStage;
    private int numPortsPerInstance;
    private int metricsPort = -1;
    private int debugPort = -1;
    private int consolePort = -1;
    private int customPort = -1;
    private List<Integer> ports;
    private List<Parameter> parameters;
    private JobSla jobSla;
    private List<JobConstraints> hardConstraints;
    private List<JobConstraints> softConstraints;
    private SchedulingInfo schedulingInfo;

    public WorkerRequest(MachineDefinition definition, String jobId,
                         int workerIndex, int workerNumber, URL jobJarUrl, int workerStage, int totalStages,
                         int numInstancesAtStage,
                         String jobName, int numPortsPerInstance,
                         List<Parameter> parameters, JobSla jobSla,
                         List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints,
                         SchedulingInfo schedulingInfo, long subscriptionTimeoutSecs, long minRuntimeSecs, long jobSubmittedAt,
                         final String user, final Optional<String> preferredCluster) {
        this.definition = definition;
        this.jobId = jobId;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobJarUrl = jobJarUrl;
        this.workerStage = workerStage;
        this.totalStages = totalStages;
        this.numInstancesAtStage = numInstancesAtStage;
        this.jobName = jobName;
        this.numPortsPerInstance = numPortsPerInstance + 4; // add additional ports for metricsPort, debugPort, consolePort and customPort
        ports = new ArrayList<>();
        this.parameters = parameters;
        this.jobSla = jobSla;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.schedulingInfo = schedulingInfo;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.minRuntimeSecs = minRuntimeSecs;
        this.jobSubmittedAt = jobSubmittedAt;
        this.user = user;
        this.preferredCluster = preferredCluster;
    }

    public static int getNumPortsPerInstance(MachineDefinition machineDefinition) {
        return machineDefinition.getNumPorts() + 1;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public MachineDefinition getDefinition() {
        return definition;
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

    public int getWorkerStage() {
        return workerStage;
    }

    public int getTotalStages() {
        return totalStages;
    }

    public int getNumInstancesAtStage() {
        return numInstancesAtStage;
    }

    public String getJobName() {
        return jobName;
    }

    public int getNumPortsPerInstance() {
        return numPortsPerInstance;
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public int getDebugPort() {
        return debugPort;
    }

    public int getConsolePort() {
        return consolePort;
    }

    public int getCustomPort() {
        return customPort;
    }

    public void addPort(int port) {
        if (metricsPort == -1) {
            metricsPort = port; // fill metricsPort first
        } else if (debugPort == -1) {
            debugPort = port; // fill debug port next
        } else if (consolePort == -1) {
            consolePort = port; // fill console port next
        } else if (customPort == -1) {
            customPort = port; // fill custom port next
        } else {
            ports.add(port);
        }
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public List<Integer> getAllPortsUsed() {
        List<Integer> allPorts = new ArrayList<>(ports);
        allPorts.add(metricsPort);
        allPorts.add(debugPort);
        allPorts.add(consolePort);
        allPorts.add(customPort);
        return allPorts;
    }

    public JobSla getJobSla() {
        return jobSla;
    }

    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    public long getSubscriptionTimeoutSecs() {
        return subscriptionTimeoutSecs;
    }

    public long getMinRuntimeSecs() {
        return minRuntimeSecs;
    }

    public long getJobSubmittedAt() {
        return jobSubmittedAt;
    }

    public String getUser() {
        return user;
    }

    public Optional<String> getPreferredCluster() {
        return preferredCluster;
    }

    @Override
    public String toString() {
        return jobId + "-Stage-" + workerStage + "-Worker-" + workerIndex;
    }
}
