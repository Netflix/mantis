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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;


public class JobDefinition {

    private final String name;
    private final String user;
    private final String artifactName;
    private final String version;

    private final List<Parameter> parameters;
    private final JobSla jobSla;
    private final long subscriptionTimeoutSecs;
    private final SchedulingInfo schedulingInfo;
    private final int withNumberOfStages;
    private List<Label> labels;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobDefinition(@JsonProperty("name") String name,
                         @JsonProperty("user") String user,
                         @JsonProperty("artifactName") String artifactName,
                         @JsonProperty("version") String version,
                         @JsonProperty("parameters") List<Parameter> parameters,
                         @JsonProperty("jobSla") JobSla jobSla,
                         @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                         @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo,
                         @JsonProperty("numberOfStages") int withNumberOfStages,
                         @JsonProperty("labels") List<Label> labels
    ) throws InvalidJobException {
        this.name = name;
        this.user = user;
        this.artifactName = artifactName;
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
        if (subscriptionTimeoutSecs > 0) {
            this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        } else {
            this.subscriptionTimeoutSecs = 0;
        }
        this.schedulingInfo = schedulingInfo;
        this.withNumberOfStages = withNumberOfStages;
        validate(true);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDefinition that = (JobDefinition) o;
        return subscriptionTimeoutSecs == that.subscriptionTimeoutSecs &&
                withNumberOfStages == that.withNumberOfStages &&
                Objects.equals(name, that.name) &&
                Objects.equals(user, that.user) &&
                Objects.equals(artifactName, that.artifactName) &&
                Objects.equals(version, that.version) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(jobSla, that.jobSla) &&
                Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, user, artifactName, version, parameters, jobSla, subscriptionTimeoutSecs, labels, withNumberOfStages);
    }

    @Override
    public String toString() {
        return "JobDefinition{" +
                "name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", artifactName='" + artifactName + '\'' +
                ", version='" + version + '\'' +
                ", parameters=" + parameters +
                ", jobSla=" + jobSla +
                ", subscriptionTimeoutSecs=" + subscriptionTimeoutSecs +
                ", schedulingInfo=" + schedulingInfo +
                ", labels=" + labels +
                ", withNumberOfStages=" + withNumberOfStages +
                '}';
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
        int withNumberOfStages = schedulingInfo.getStages().size();
        int startingIdx = 1;
        if (schedulingInfo.forStage(0) != null) {
            // jobMaster stage 0 definition exists, adjust index range
            startingIdx = 0;
            withNumberOfStages--;
        }
        for (int i = startingIdx; i <= withNumberOfStages; i++) {
            StageSchedulingInfo stage = schedulingInfo.getStages().get(i);
            if (stage == null)
                throw new InvalidJobException("No definition for stage " + i + " in scheduling info for " + withNumberOfStages + " stage job");
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

    public String getArtifactName() {
        return artifactName;
    }

    public String getVersion() { return version;}

    public List<Parameter> getParameters() {
        return Collections.unmodifiableList(parameters);
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


    //    // TODO make immutable
    //    public void setSchedulingInfo(SchedulingInfo schedulingInfo) {
    //        this.schedulingInfo = schedulingInfo;
    //    }
    public List<Label> getLabels() {
        return Collections.unmodifiableList(this.labels);
    }

    public int getNumberOfStages() {
        return this.withNumberOfStages;
    }


    public static class Builder {

        private String name;

        private String user;

        private List<Parameter> parameters;


        private List<Label> labels;

        private String artifactName = null;
        private String version = null;

        private JobSla jobSla = new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, null);
        private long subscriptionTimeoutSecs = 0L;
        private SchedulingInfo schedulingInfo;
        private int withNumberOfStages = 1;

        public Builder() {

        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withArtifactName(String artifactName) {
            this.artifactName = artifactName;
            return this;
        }

        public Builder withJobSla(JobSla sla) {
            this.jobSla = sla;
            return this;
        }


        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withSchedulingInfo(SchedulingInfo schedInfo) {
            this.schedulingInfo = schedInfo;
            return this;
        }

        public Builder withNumberOfStages(int stages) {
            this.withNumberOfStages = stages;
            return this;
        }

        public Builder withSubscriptionTimeoutSecs(long t) {
            this.subscriptionTimeoutSecs = t;
            return this;
        }

        public Builder withParameters(List<Parameter> params) {
            this.parameters = params;
            return this;
        }


        public Builder withLabels(List<Label> labels) {
            this.labels = labels;
            return this;
        }

        public Builder withVersion(String version) {
            this.version = version;
            return this;
        }

        public Builder from(final JobDefinition jobDefinition) {
            this.withJobSla(jobDefinition.getJobSla());
            this.withNumberOfStages(jobDefinition.getNumberOfStages());
            this.withSubscriptionTimeoutSecs(jobDefinition.getSubscriptionTimeoutSecs());
            this.withUser(jobDefinition.user);
            this.withSchedulingInfo(jobDefinition.getSchedulingInfo());
            this.withParameters(jobDefinition.getParameters());
            this.withLabels(jobDefinition.getLabels());
            this.withName(jobDefinition.name);
            this.withArtifactName(jobDefinition.artifactName);
            this.withVersion(jobDefinition.getVersion());
            return this;
        }


        public JobDefinition build() throws InvalidJobException {
            Preconditions.checkNotNull(name, "cluster name cannot be null");
            Preconditions.checkNotNull(jobSla, "job sla cannot be null");
            //	Preconditions.checkNotNull(schedulingInfo, "schedulingInfo cannot be null");
            if (schedulingInfo != null) {
                withNumberOfStages = schedulingInfo.getStages().size();
            }
            Preconditions.checkArgument(withNumberOfStages > 0, "Number of stages cannot be less than 0");
            return new JobDefinition(name, user, artifactName, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, withNumberOfStages, labels);
        }
    }

}

