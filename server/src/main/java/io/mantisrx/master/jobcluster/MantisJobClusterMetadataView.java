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

package io.mantisrx.master.jobcluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;


import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.server.master.store.NamedJob;

import java.util.List;
import java.util.Objects;

@JsonFilter("topLevelFilter")
public class MantisJobClusterMetadataView {

    private final String name;
    private final List<NamedJob.Jar> jars;
    private final NamedJob.SLA sla;
    private final List<Parameter> parameters;
    private final JobOwner owner;
    private final long lastJobCount;
    private final boolean disabled;
    private final boolean isReadyForJobMaster;
    private final WorkerMigrationConfig migrationConfig;
    private final List<Label> labels;
    private final boolean cronActive;
    @JsonIgnore
    private final String latestVersion;

    @JsonCreator
    public MantisJobClusterMetadataView(@JsonProperty("name") String name, @JsonProperty("jars") List<NamedJob.Jar> jars, @JsonProperty("sla") NamedJob.SLA sla,
                                        @JsonProperty("parameters") List<Parameter> parameters, @JsonProperty("owner") JobOwner owner, @JsonProperty("lastJobCount") long lastJobCount,
                                        @JsonProperty("disabled") boolean disabled, @JsonProperty("isReadyForJobMaster") boolean isReadyForJobMaster, @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                                        @JsonProperty("labels") List<Label> labels, @JsonProperty("cronActive") boolean cronActive, @JsonProperty("latestVersion") String latestVersion) {
        this.name = name;
        this.jars = jars;
        this.sla = sla;
        this.parameters = parameters;
        this.owner = owner;
        this.lastJobCount = lastJobCount;
        this.disabled = disabled;
        this.isReadyForJobMaster = isReadyForJobMaster;
        this.migrationConfig = migrationConfig;
        this.labels = labels;
        this.cronActive = cronActive;
        this.latestVersion = latestVersion;
    }

    public String getName() {
        return name;
    }

    public List<NamedJob.Jar> getJars() {
        return jars;
    }

    public NamedJob.SLA getSla() {
        return sla;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public JobOwner getOwner() {
        return owner;
    }

    public long getLastJobCount() {
        return lastJobCount;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean getIsReadyForJobMaster() {
        return isReadyForJobMaster;
    }

    public WorkerMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    public List<Label> getLabels() {
        return labels;
    }

    public boolean isCronActive() {
        return cronActive;
    }

    public String getLatestVersion() {
        return latestVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantisJobClusterMetadataView that = (MantisJobClusterMetadataView) o;
        return lastJobCount == that.lastJobCount &&
                disabled == that.disabled &&
                isReadyForJobMaster == that.isReadyForJobMaster &&
                cronActive == that.cronActive &&
                Objects.equals(name, that.name) &&
                Objects.equals(jars, that.jars) &&
                Objects.equals(sla, that.sla) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(owner, that.owner) &&
                Objects.equals(migrationConfig, that.migrationConfig) &&
                Objects.equals(labels, that.labels) &&
                Objects.equals(latestVersion, that.latestVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, jars, sla, parameters, owner, lastJobCount, disabled, isReadyForJobMaster, migrationConfig, labels, cronActive, latestVersion);
    }

    @Override
    public String toString() {
        return "MantisJobClusterMetadataView{" +
                "name='" + name + '\'' +
                ", jars=" + jars +
                ", sla=" + sla +
                ", parameters=" + parameters +
                ", owner=" + owner +
                ", lastJobCount=" + lastJobCount +
                ", disabled=" + disabled +
                ", isReadyForJobMaster=" + isReadyForJobMaster +
                ", migrationConfig=" + migrationConfig +
                ", labels=" + labels +
                ", cronActive=" + cronActive +
                ", latestVersion='" + latestVersion + '\'' +
                '}';
    }

    public static class Builder {
        private  String name;
        private  List<NamedJob.Jar> jars = Lists.newArrayList();
        private  NamedJob.SLA sla;
        private  List<Parameter> parameters = Lists.newArrayList();
        private  JobOwner owner;
        private  long lastJobCount;
        private  boolean disabled = false;
        private  boolean isReadyForJobMaster = true;
        private  WorkerMigrationConfig migrationConfig;
        private  List<Label> labels = Lists.newArrayList();
        private  boolean cronActive = false;
        private String latestVersion;

        public Builder() {

        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withJars(List<JobClusterConfig> jars) {
            this.jars = DataFormatAdapter.convertJobClusterConfigsToJars(jars);
            return this;
        }

        public Builder withSla(SLA sla) {
            this.sla = DataFormatAdapter.convertSLAToNamedJobSLA(sla);
            return this;
        }

        public Builder withParameters(List<Parameter> params) {
            this.parameters = params;
            return this;
        }

        public Builder withJobOwner(JobOwner owner) {
            this.owner = owner;
            return this;
        }

        public Builder withLastJobCount(long cnt) {
            this.lastJobCount = cnt;
            return this;
        }

        public Builder withDisabled(boolean disabled) {
            this.disabled = disabled;
            return this;
        }

        public Builder withIsReadyForJobMaster(boolean isReadyForJobMaster) {
            this.isReadyForJobMaster = isReadyForJobMaster;
            return this;
        }

        public Builder withMigrationConfig(WorkerMigrationConfig config) {
            this.migrationConfig = config;
            return this;
        }

        public Builder withLabels(List<Label> labels) {
            this.labels = labels;
            return this;
        }

        public Builder isCronActive(boolean cronActive) {
            this.cronActive = cronActive;
            return this;

        }

        public Builder withLatestVersion(String version) {
            this.latestVersion = version;
            return this;
        }

        public MantisJobClusterMetadataView build() {
            Preconditions.checkNotNull(name, "name cannot be null");
            Preconditions.checkNotNull(jars, "Jars cannot be null");
            Preconditions.checkArg(!jars.isEmpty(),"Jars cannot be empty");
            Preconditions.checkNotNull(latestVersion, "version cannot be null");

            return new MantisJobClusterMetadataView(name,jars,sla,parameters,owner,lastJobCount,disabled,isReadyForJobMaster,migrationConfig,labels,cronActive,latestVersion);
        }

    }

}
