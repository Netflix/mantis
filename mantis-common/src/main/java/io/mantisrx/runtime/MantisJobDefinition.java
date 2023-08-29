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

import io.mantisrx.common.Label;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.DeploymentStrategy;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;


public class MantisJobDefinition {

    private static final long serialVersionUID = 1L;

    private String name;
    private String user;
    private URL jobJarFileLocation;
    private String version;
    private List<Parameter> parameters;
    private JobSla jobSla;
    private long subscriptionTimeoutSecs = 0L;
    private SchedulingInfo schedulingInfo;
    private DeploymentStrategy deploymentStrategy;
    private int slaMin = 0;
    private int slaMax = 0;
    private String cronSpec = "";
    private NamedJobDefinition.CronPolicy cronPolicy = null;
    private boolean isReadyForJobMaster = false;
    private WorkerMigrationConfig migrationConfig;
    private List<Label> labels;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MantisJobDefinition(@JsonProperty("name") String name,
                               @JsonProperty("user") String user,
                               @JsonProperty("url") URL jobJarFileLocation,
                               @JsonProperty("version") String version,
                               @JsonProperty("parameters") List<Parameter> parameters,
                               @JsonProperty("jobSla") JobSla jobSla,
                               @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                               @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo,
                               @JsonProperty("slaMin") int slaMin,
                               @JsonProperty("slaMax") int slaMax,
                               @JsonProperty("cronSpec") String cronSpec,
                               @JsonProperty("cronPolicy") NamedJobDefinition.CronPolicy cronPolicy,
                               @JsonProperty("isReadyForJobMaster") boolean isReadyForJobMaster,
                               @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                               @JsonProperty("labels") List<Label> labels,
                               @JsonProperty("deploymentStrategy") DeploymentStrategy deploymentStrategy
    ) {
        this.name = name;
        this.user = user;
        this.jobJarFileLocation = jobJarFileLocation;
        this.version = version;
        if (parameters != null) {
            this.parameters = parameters;
        } else {
            this.parameters = new LinkedList<>();
        }

        if (labels != null) {
            this.labels = labels;
        } else {
            this.labels = new LinkedList<>();
        }
        this.jobSla = jobSla;
        if (subscriptionTimeoutSecs > 0)
            this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.schedulingInfo = schedulingInfo;
        this.deploymentStrategy = deploymentStrategy;
        this.slaMin = slaMin;
        this.slaMax = slaMax;
        this.cronSpec = cronSpec;
        this.cronPolicy = cronPolicy;
        this.isReadyForJobMaster = isReadyForJobMaster;
        this.migrationConfig = Optional.ofNullable(migrationConfig).orElse(WorkerMigrationConfig.DEFAULT);
    }

    public void validate(boolean schedulingInfoOptional) throws InvalidJobException {
        validateSla();
        validateSchedulingInfo(schedulingInfoOptional);
    }

    private void validateSla() throws InvalidJobException {
        if (jobSla == null)
            throw new InvalidJobException("No Job SLA provided (likely incorrect job submit request)");
        if (jobSla.getDurationType() == null)
            throw new InvalidJobException("Invalid null duration type in job sla (likely incorrect job submit request");
    }

    public void validateSchedulingInfo() throws InvalidJobException {
        validateSchedulingInfo(false);
    }

    private void validateSchedulingInfo(boolean schedulingInfoOptional) throws InvalidJobException {
        if (schedulingInfoOptional && schedulingInfo == null)
            return;
        if (schedulingInfo == null)
            throw new InvalidJobException("No scheduling info provided");
        if (schedulingInfo.getStages() == null)
            throw new InvalidJobException("No stages defined in scheduling info");
        int numStages = schedulingInfo.getStages().size();
        int startingIdx = 1;
        if (schedulingInfo.forStage(0) != null) {
            // jobMaster stage 0 definition exists, adjust index range
            startingIdx = 0;
            numStages--;
        }
        for (int i = startingIdx; i <= numStages; i++) {
            StageSchedulingInfo stage = schedulingInfo.getStages().get(i);
            if (stage == null)
                throw new InvalidJobException("No definition for stage " + i + " in scheduling info for " + numStages + " stage job");
            if (stage.getNumberOfInstances() < 1)
                throw new InvalidJobException("Number of instance for stage " + i + " must be >0, not " + stage.getNumberOfInstances());
            MachineDefinition machineDefinition = stage.getMachineDefinition();
            if (machineDefinition.getCpuCores() <= 0)
                throw new InvalidJobException("cpuCores must be >0.0, not " + machineDefinition.getCpuCores());
            if (machineDefinition.getMemoryMB() <= 0)
                throw new InvalidJobException("memory must be <0.0, not " + machineDefinition.getMemoryMB());
            if (machineDefinition.getDiskMB() < 0)
                throw new InvalidJobException("disk must be >=0, not " + machineDefinition.getDiskMB());
            if (machineDefinition.getNumPorts() < 0)
                throw new InvalidJobException("numPorts must be >=0, not " + machineDefinition.getNumPorts());
        }
    }

    public String getName() {
        return name;
    }

    public String getUser() {
        return user;
    }

    public String getVersion() {
        return version;
    }

    public URL getJobJarFileLocation() {
        return jobJarFileLocation;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public JobSla getJobSla() {
        return jobSla;
    }

    public long getSubscriptionTimeoutSecs() {
        return subscriptionTimeoutSecs;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public void setSchedulingInfo(SchedulingInfo schedulingInfo) {
        this.schedulingInfo = schedulingInfo;
    }

    public DeploymentStrategy getDeploymentStrategy() {
        return deploymentStrategy;
    }

    public int getSlaMin() {
        return slaMin;
    }

    public int getSlaMax() {
        return slaMax;
    }

    public String getCronSpec() {
        return cronSpec;
    }

    public NamedJobDefinition.CronPolicy getCronPolicy() {
        return cronPolicy;
    }

    public boolean getIsReadyForJobMaster() {
        return isReadyForJobMaster;
    }

    public WorkerMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    public List<Label> getLabels() {
        return this.labels;
    }

    @Override
    public String toString() {
        return "MantisJobDefinition{" +
                "name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", jobJarFileLocation=" + jobJarFileLocation +
                ", version='" + version + '\'' +
                ", parameters=" + parameters +
                ", labels=" + labels +
                ", jobSla=" + jobSla +
                ", subscriptionTimeoutSecs=" + subscriptionTimeoutSecs +
                ", schedulingInfo=" + schedulingInfo +
                ", slaMin=" + slaMin +
                ", slaMax=" + slaMax +
                ", cronSpec='" + cronSpec + '\'' +
                ", cronPolicy=" + cronPolicy +
                ", isReadyForJobMaster=" + isReadyForJobMaster +
                ", migrationConfig=" + migrationConfig +
                '}';
    }
}
