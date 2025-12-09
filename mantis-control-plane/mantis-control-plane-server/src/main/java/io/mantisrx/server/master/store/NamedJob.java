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

package io.mantisrx.server.master.store;

import com.netflix.fenzo.triggers.CronTrigger;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.JobPrincipal;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.MantisJobOperations;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

public class NamedJob {

    private static final Logger logger = LoggerFactory.getLogger(NamedJob.class);
    private static final int MaxValueForSlaMin = 5;
    private static final int MaxValueForSlaMax = 100;
    private final String name;
    private final List<Jar> jars = new ArrayList<>();
    private JobOwner owner;
    private volatile SLA sla;
    private List<Parameter> parameters;
    private boolean isReadyForJobMaster = false;
    private WorkerMigrationConfig migrationConfig;
    private volatile long lastJobCount = 0;
    private volatile boolean disabled = false;
    private volatile boolean cronActive = false;
    private volatile boolean isActive = true;
    private MantisJobOperations jobOps;
    private List<Label> labels;
    private JobPrincipal jobPrincipal;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public NamedJob(@JsonProperty("jobOps") MantisJobOperations jobOps, @JsonProperty("name") String name,
                    @JsonProperty("jars") List<Jar> jars, @JsonProperty("sla") SLA sla,
                    @JsonProperty("parameters") List<Parameter> parameters,
                    @JsonProperty("owner") JobOwner owner, @JsonProperty("lastJobCount") long lastJobCount,
                    @JsonProperty("disabled") boolean disabled,
                    @JsonProperty("isReadyForJobMaster") boolean isReadyForJobMaster,
                    @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                    @JsonProperty("labels") List<Label> labels,
                    @JsonProperty("jobPrincipal") JobPrincipal jobPrincipal) {

        this.jobOps = jobOps;
        this.name = name;
        if (sla == null)
            sla = new SLA(0, 0, null, null);
        this.disabled = disabled;
        this.isReadyForJobMaster = isReadyForJobMaster;
        this.migrationConfig = Optional.ofNullable(migrationConfig).orElse(WorkerMigrationConfig.DEFAULT);
        this.sla = sla;
        try {
            this.sla.validate();
        } catch (InvalidNamedJobException e) {
            logger.warn(name + ": disabling due to unexpected error validating sla: " + e.getMessage());
            this.disabled = true;
        }
        if (labels != null) {
            this.labels = labels;
        } else {
            this.labels = new LinkedList<>();
        }
        this.parameters = parameters;
        this.owner = owner;
        this.lastJobCount = lastJobCount;
        this.jobPrincipal = jobPrincipal;
        if (jars != null) {
            this.jars.addAll(jars);
        }
    }

    public static String getJobId(String name, long number) {
        return name + "-" + number;
    }

    static String getJobName(String jobId) {
        return jobId.substring(0, jobId.lastIndexOf('-'));
    }

    private static long getJobIdNumber(String jobId) {
        return Long.parseLong(jobId.substring(jobId.lastIndexOf('-') + 1));
    }

    @Override
    public String toString() {
        return "NamedJob [name=" + name + ", jars=" + jars + ", owner=" + owner + ", sla=" + sla + ", parameters="
                + parameters + ", isReadyForJobMaster=" + isReadyForJobMaster + ", migrationConfig=" + migrationConfig
                + ", lastJobCount=" + lastJobCount + ", disabled=" + disabled + ", isActive=" + isActive + ", labels="
                + labels + ", jobPrincipal=" + jobPrincipal + "]";
    }

    /* package */ void setJobOps(MantisJobOperations jobOps) {
        this.jobOps = jobOps;
    }

    public String getName() {
        return name;
    }

    public List<Jar> getJars() {
        return Collections.unmodifiableList(jars);
    }

    public SLA getSla() {
        return sla;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public List<Label> getLabels() {
        return this.labels;
    }

    void setLabels(List<Label> labels) {
        this.labels = labels;
    }

    public JobOwner getOwner() {
        return owner;
    }

    void setOwner(JobOwner owner) {
        this.owner = owner;
    }

    public long getLastJobCount() {
        return lastJobCount;
    }

    @JsonIgnore
    public long getNextJobNumber() {
        return ++lastJobCount;
    }

    public boolean getDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
        //    enforceSla(Optional.empty());
    }

    public boolean getIsReadyForJobMaster() {
        return isReadyForJobMaster;
    }

    public void setIsReadyForJobMaster(boolean b) {
        isReadyForJobMaster = b;
    }

    public WorkerMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    public void setMigrationConfig(final WorkerMigrationConfig migrationConfig) {
        this.migrationConfig = migrationConfig;
    }

    public JobPrincipal getJobPrincipal() {
        return jobPrincipal;
    }

    public void setJobPrincipal(JobPrincipal jobPrincipal) {
        this.jobPrincipal = jobPrincipal;
    }

    @JsonIgnore
    public boolean getIsActive() {
        return isActive;
    }

    @JsonIgnore
    public void setInactive() throws NamedJobDeleteException {
        isActive = false;
    }

    public boolean getCronActive() {
        return cronActive;
    }

    /**
     * Get the Jar for the job that matches the <code>version</code> argument.
     *
     * @param version The version to match.
     *
     * @return Latest jar uploaded if <code>version</code> is <code>null</code> or empty, or jar whose version
     * matches the argument, or null if no such version exists.
     */
    @JsonIgnore
    Jar getJar(String version) {
        if (version == null || version.isEmpty())
            return jars.get(jars.size() - 1);
        for (Jar j : jars)
            if (version.equals(j.version))
                return j;
        return null;
    }

    public static class CompletedJob {
        private final String name;
        private final String jobId;
        private final String version;
        private final MantisJobState state;
        private final long submittedAt;
        private final long terminatedAt;
        private final String user;
        private final List<Label> labels;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public CompletedJob(
                @JsonProperty("name") String name,
                @JsonProperty("jobId") String jobId,
                @JsonProperty("version") String version,
                @JsonProperty("state") MantisJobState state,
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
            if (labels != null) {
                this.labels = labels;
            } else {
                this.labels = new ArrayList<>();
            }
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

        public MantisJobState getState() {
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

        public List<Label> getLabels() { return labels; }

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
                    ", labels=" + labels +
                    '}';
        }
    }

    public static class Jar {

        private final URL url;
        private final String version;
        private final long uploadedAt;
        private final SchedulingInfo schedulingInfo;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Jar(@JsonProperty("url") URL url,
                   @JsonProperty("uploadedAt") long uploadedAt,
                   @JsonProperty("version") String version, @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo) {
            this.url = url;
            this.uploadedAt = uploadedAt;
            this.version = (version == null || version.isEmpty()) ?
                    "" + System.currentTimeMillis() :
                    version;
            this.schedulingInfo = schedulingInfo;
        }

        public URL getUrl() {
            return url;
        }

        public long getUploadedAt() {
            return uploadedAt;
        }

        public String getVersion() {
            return version;
        }

        public SchedulingInfo getSchedulingInfo() {
            return schedulingInfo;
        }
    }

    public static class SLA {

        @JsonIgnore
        private static final TriggerOperator triggerOperator;

        static {
            triggerOperator = new TriggerOperator(1);
            try {
                triggerOperator.initialize();
            } catch (SchedulerException e) {
                logger.error("Unexpected: " + e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        private final int min;
        private final int max;
        private final String cronSpec;
        private final NamedJobDefinition.CronPolicy cronPolicy;
        @JsonIgnore
        private final boolean hasCronSpec;
        @JsonIgnore
        private final NamedJobDefinition.CronPolicy defaultPolicy = NamedJobDefinition.CronPolicy.KEEP_EXISTING;
        @JsonIgnore
        private CronTrigger<NamedJob> scheduledTrigger;
        @JsonIgnore
        private String triggerGroup = null;
        @JsonIgnore
        private String triggerId = null;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public SLA(
                @JsonProperty("min") int min,
                @JsonProperty("max") int max,
                @JsonProperty("cronSpec") String cronSpec,
                @JsonProperty("cronPolicy") NamedJobDefinition.CronPolicy cronPolicy
        ) {
            if (cronSpec != null && !cronSpec.isEmpty()) {
                this.cronSpec = cronSpec;
                hasCronSpec = true;
                this.max = 1;
                this.min = 0;
                this.cronPolicy = cronPolicy == null ? defaultPolicy : cronPolicy;
            } else {
                hasCronSpec = false;
                this.min = min;
                this.max = max;
                this.cronSpec = null;
                this.cronPolicy = null;
            }
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public String getCronSpec() {
            return cronSpec;
        }

        public NamedJobDefinition.CronPolicy getCronPolicy() {
            return cronPolicy;
        }

        private void validate() throws InvalidNamedJobException {
            if (max < min)
                throw new InvalidNamedJobException("Cannot have max=" + max + " < min=" + min);
            if (min > MaxValueForSlaMin)
                throw new InvalidNamedJobException("Specified min sla value " + min + " cannot be >" + MaxValueForSlaMin);
            if (max > MaxValueForSlaMax)
                throw new InvalidNamedJobException("Max sla value " + max + " cannot be >" + MaxValueForSlaMax);
        }

        // caller must lock to avoid concurrent access with destroyCron()
        private void initCron(NamedJob job) throws SchedulerException {
            // DISABLED AS Master V2 does not use this class for cron
        }

        // caller must lock to avoid concurrent access with initCron()
        private void destroyCron() {
            // DISABLED AS Master V2 does not use this class for cron
        }
    }

    // Keep this public since Quartz needs to call it when triggering cron.
    public static class CronTriggerAction implements Action1<NamedJob> {

        @Override
        public void call(NamedJob job) {
            logger.info("Cron fired for " + job.getName());
        }
    }
}
