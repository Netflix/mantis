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

import com.netflix.fenzo.triggers.TriggerUtils;
import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.LabelManager.SystemLabels;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobClusterDefinitionImpl implements IJobClusterDefinition {

    private static final Logger logger = LoggerFactory.getLogger(JobClusterDefinitionImpl.class);
    private final String name;
    private final String user;
    private final JobOwner owner;
    private final SLA sla;
    private final WorkerMigrationConfig migrationConfig;
    private final List<JobClusterConfig> jobClusterConfigs = Lists.newArrayList();
    private final List<Parameter> parameters;
    private final List<Label> labels;
    private boolean isReadyForJobMaster = false;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobClusterDefinitionImpl(@JsonProperty("name") String name,
                                    @JsonProperty("jobClusterConfigs") List<JobClusterConfig> jobClusterConfigs,
                                    @JsonProperty("owner") JobOwner owner,
                                    @JsonProperty("user") String user,
                                    @JsonProperty("sla") SLA sla,
                                    @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                                    @JsonProperty("isReadyForJobMaster") boolean isReadyForJobMaster,
                                    @JsonProperty("parameters") List<Parameter> parameters,
                                    @JsonProperty("labels") List<Label> labels,
                                    @JsonProperty("isDisabled") boolean isDisabled
    ) {
        Preconditions.checkNotNull(jobClusterConfigs);
        Preconditions.checkArgument(!jobClusterConfigs.isEmpty());
        if (sla != null && sla.getCronSpec() != null)
            TriggerUtils.validateCronExpression(sla.getCronSpec());
        this.owner = owner;
        this.name = name;
        this.sla = Optional.ofNullable(sla).orElse(new SLA(0, 0, null, CronPolicy.KEEP_EXISTING));
        this.migrationConfig = Optional.ofNullable(migrationConfig).orElse(WorkerMigrationConfig.DEFAULT);
        this.isReadyForJobMaster = isReadyForJobMaster;
        this.jobClusterConfigs.addAll(jobClusterConfigs);
        this.labels = Optional.ofNullable(labels).orElse(Lists.newArrayList());
        this.parameters = Optional.ofNullable(parameters).orElse(Lists.newArrayList());

        this.user = user;

        // Todo move the resource cluster label to a property
        if (!isDisabled) {
            Preconditions.checkNotNull(labels, "labels cannot be empty.");
            Preconditions.checkArgument(
                labels.stream()
                    .anyMatch(l ->
                        l.getName().equalsIgnoreCase(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label)),
                "Missing required label: " + SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label);
        }
    }

    public JobClusterDefinitionImpl(String name,
        List<JobClusterConfig> jobClusterConfigs,
        JobOwner owner,
        String user,
        SLA sla,
        WorkerMigrationConfig migrationConfig,
        boolean isReadyForJobMaster,
        List<Parameter> parameters,
        List<Label> labels) {
        this(name, jobClusterConfigs, owner, user, sla, migrationConfig, isReadyForJobMaster, parameters, labels, false);
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getOwner()
     */
    @Override
    public JobOwner getOwner() {
        return owner;
    }


    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getSLA()
     */
    @Override
    public SLA getSLA() {
        return this.sla;
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getWorkerMigrationConfig()
     */
    @Override
    public WorkerMigrationConfig getWorkerMigrationConfig() {
        return this.migrationConfig;
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getIsReadyForJobMaster()
     */
    @Override
    public boolean getIsReadyForJobMaster() {
        return this.isReadyForJobMaster;
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getJobClusterConfigs()
     */
    @Override
    public List<JobClusterConfig> getJobClusterConfigs() {
        return Collections.unmodifiableList(this.jobClusterConfigs);
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getJobClusterConfig()
     */
    @Override
    public JobClusterConfig getJobClusterConfig() {
        return this.jobClusterConfigs.get(jobClusterConfigs.size() - 1);
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getName()
     */
    @Override
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
     * @see io.mantisrx.server.master.domain.IJobClusterDefinition#getUser()
     */
    @Override
    public String getUser() {
        return user;
    }

    @Override
    public List<Parameter> getParameters() {
        return Collections.unmodifiableList(this.parameters);
    }

    @Override
    public List<Label> getLabels() {
        return Collections.unmodifiableList(this.labels);
    }

    @Override
    public String toString() {
        return "JobClusterDefinitionImpl{" +
                "name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", owner=" + owner +
                ", sla=" + sla +
                ", migrationConfig=" + migrationConfig +
                ", isReadyForJobMaster=" + isReadyForJobMaster +
                ", jobClusterConfigs=" + jobClusterConfigs +
                ", parameters=" + parameters +
                ", labels=" + labels +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobClusterDefinitionImpl that = (JobClusterDefinitionImpl) o;
        return isReadyForJobMaster == that.isReadyForJobMaster &&
                Objects.equals(name, that.name) &&
                Objects.equals(user, that.user) &&
                Objects.equals(owner, that.owner) &&
                Objects.equals(sla, that.sla) &&
                Objects.equals(migrationConfig, that.migrationConfig) &&
                Objects.equals(jobClusterConfigs, that.jobClusterConfigs) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, user, owner, sla, migrationConfig, isReadyForJobMaster, jobClusterConfigs, parameters, labels);
    }


    public static class CompletedJob {

        private final String name;
        private final String jobId;
        private final String version;
        private final JobState state;
        private final long submittedAt;
        private final long terminatedAt;
        private final String user;
        private final List<Label> labelList;


        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public CompletedJob(
                @JsonProperty("name") String name,
                @JsonProperty("jobId") String jobId,
                @JsonProperty("version") String version,
                @JsonProperty("state") JobState state,
                @JsonProperty("submittedAt") long submittedAt,
                @JsonProperty("terminatedAt") long terminatedAt,
                @JsonProperty("user") String user,
                @JsonProperty("labels") List<Label> labels

        ) {
            this.name = name;
            this.jobId = jobId;
            this.version = version;
            this.state = state;
            this.submittedAt = submittedAt;
            this.terminatedAt = terminatedAt;
            this.user = user;
            this.labelList = labels;

        }

        public String getName() {
            return name;
        }

        public String getJobId() {
            return jobId;
        }

        public String getVersion() {
            return version;
        }

        public JobState getState() {
            return state;
        }

        public long getSubmittedAt() {
            return submittedAt;
        }

        public long getTerminatedAt() {
            return terminatedAt;
        }

        public String getUser() {
            return user;
        }

        public List<Label> getLabelList() {
            return labelList;
        }


        @Override
        public String toString() {
            return "CompletedJob{" +
                    "name='" + name + '\'' +
                    ", jobId='" + jobId + '\'' +
                    ", version='" + version + '\'' +
                    ", state=" + state +
                    ", submittedAt=" + submittedAt +
                    ", terminatedAt=" + terminatedAt +
                    ", user='" + user + '\'' +
                    ", labelList=" + labelList +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompletedJob that = (CompletedJob) o;
            return submittedAt == that.submittedAt &&
                    terminatedAt == that.terminatedAt &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(jobId, that.jobId) &&
                    Objects.equals(version, that.version) &&
                    state == that.state &&
                    Objects.equals(user, that.user);
        }

        @Override
        public int hashCode() {

            return Objects.hash(name, jobId, version, state, submittedAt, terminatedAt, user);
        }

    }

    public static class Builder {

        List<JobClusterConfig> jobClusterConfigs = new ArrayList<>();
        JobOwner owner = null;
        SLA sla = new SLA(0, 0, null, null);
        WorkerMigrationConfig migrationConfig = WorkerMigrationConfig.DEFAULT;
        boolean isReadyForJobMaster = true;
        String name = null;
        String user = "default";
        List<Parameter> parameters = Lists.newArrayList();
        List<Label> labels = Lists.newArrayList();
        boolean isDisabled = false;

        public Builder() {}

        public Builder withName(String name) {
            Preconditions.checkNotNull(name, "Cluster name cannot be null");
            Preconditions.checkArgument(!name.isEmpty(), "cluster Name cannot be empty");
            this.name = name;
            return this;
        }

        public Builder withUser(String user) {
            Preconditions.checkNotNull(user, "user  cannot be null");
            Preconditions.checkArgument(!user.isEmpty(), "user cannot be empty");
            this.user = user;
            return this;
        }


        public Builder withJobClusterConfig(JobClusterConfig config) {
            Preconditions.checkNotNull(config, "config cannot be null");
            if (!jobClusterConfigs.contains(config)) { // skip if this config already exists
                jobClusterConfigs.add(config);
            }
            return this;
        }

        public Builder withJobClusterConfigs(List<JobClusterConfig> jars) {
            Preconditions.checkNotNull(jars, "config list cannot be null");
            this.jobClusterConfigs = jars;
            return this;
        }

        public Builder withOwner(JobOwner owner) {
            Preconditions.checkNotNull(owner, "owner  cannot be null");
            this.owner = owner;
            return this;
        }

        public Builder withSla(SLA sla) {
            Preconditions.checkNotNull(sla, "sla  cannot be null");
            this.sla = sla;
            return this;
        }

        public Builder withMigrationConfig(WorkerMigrationConfig config) {
            Preconditions.checkNotNull(config, "migration config cannot be null");
            this.migrationConfig = config;
            return this;
        }

        public Builder withIsReadyForJobMaster(boolean ready) {
            this.isReadyForJobMaster = ready;
            return this;
        }

        public Builder withParameters(List<Parameter> ps) {
            this.parameters = ps;
            return this;
        }

        public Builder withLabels(List<Label> labels) {
            this.labels = labels;
            return this;
        }

        public Builder withLabel(Label label) {
            Preconditions.checkNotNull(label, "label cannot be null");
            this.labels.add(label);
            return this;
        }

        public Builder withIsDisabled(boolean isDisabled) {
            this.isDisabled = isDisabled;
            return this;
        }

        public Builder from(IJobClusterDefinition defn) {
            migrationConfig = defn.getWorkerMigrationConfig();
            name = defn.getName();
            sla = defn.getSLA();
            isReadyForJobMaster = defn.getIsReadyForJobMaster();
            owner = defn.getOwner();
            user = defn.getUser();
            parameters = defn.getParameters();
            labels = defn.getLabels();
            // we don't want to duplicates but retain the order so cannot use Set
            for (JobClusterConfig jcConfig : defn.getJobClusterConfigs()) {
                if (!jobClusterConfigs.contains(jcConfig)) {
                    jobClusterConfigs.add(jcConfig);
                }
            }
            //defn.getJobClusterConfigs().forEach(jobClusterConfigs::add);
            return this;
        }

        public Builder mergeConfigsAndOverrideRest(IJobClusterDefinition oldDefn, IJobClusterDefinition newDefn) {
            List<JobClusterConfig> oldConfigs = oldDefn.getJobClusterConfigs();
            logger.info("Existing JobClusterConfigs {} ", oldConfigs);
            logger.info("New JobClusterConfig {} ", newDefn.getJobClusterConfig());

            if (oldConfigs != null) {
                List<JobClusterConfig> subList = Collections.unmodifiableList(oldConfigs.subList(
                    Math.max(0, oldConfigs.size() - 3),
                    oldConfigs.size()
                ));
                this.jobClusterConfigs.addAll(subList);
            }
            this.jobClusterConfigs.add(newDefn.getJobClusterConfig());
            logger.info("Merged JobClusterConfigs {} ", this.jobClusterConfigs);
            this.sla = newDefn.getSLA();
            this.parameters = newDefn.getParameters();
            this.labels = newDefn.getLabels();
            this.user = newDefn.getUser();
            this.migrationConfig = newDefn.getWorkerMigrationConfig();
            this.owner = newDefn.getOwner();
            this.isReadyForJobMaster = newDefn.getIsReadyForJobMaster();
            this.name = oldDefn.getName();
            return this;
        }

        public JobClusterDefinitionImpl build() {
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(user);
            Preconditions.checkNotNull(jobClusterConfigs);
            Preconditions.checkArgument(!jobClusterConfigs.isEmpty());
            return new JobClusterDefinitionImpl(
                name,
                jobClusterConfigs,
                owner,
                user,
                sla,
                migrationConfig,
                isReadyForJobMaster,
                parameters,
                labels,
                isDisabled);
        }

    }

}
