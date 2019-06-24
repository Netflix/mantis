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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.triggers.CronTrigger;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.MantisJobOperations;
import io.mantisrx.server.master.config.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

//import com.google.common.collect.Lists;


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
    //    @JsonIgnore
    //    private Map<String, CompletedJob> completedJobs = new HashMap<>();
    //    @JsonIgnore
    //    private final BehaviorSubject<String> jobIds;
    //    @JsonIgnore
    //    private final SortedSet<MantisJobMgr> sortedJobMgrs;
    //    @JsonIgnore
    //    private final SortedSet<MantisJobMgr> sortedRegisteredJobMgrs;
    //    @JsonIgnore
    //    private final ReentrantLock lock = new ReentrantLock();
    //    @JsonIgnore
    private volatile boolean isActive = true;
    //    @JsonIgnore
    private MantisJobOperations jobOps;
    //    @JsonIgnore
    //    private final static String JobSubmissionsCounterName = "JobSlaNumSubmissions";
    //    @JsonIgnore
    //    private final Counter jobSubmissionsCounter;
    //    @JsonIgnore
    //    private final static String JobTerminationsCounterName = "JobSlaNumTerminations";
    //    @JsonIgnore
    //    private final Counter jobTerminationsCounter;
    //    @JsonIgnore
    //    private final static String SLA_FAILED_JOB_CLUSTERS = "slaFailedJobClusters";
    //    @JsonIgnore
    //    private final Counter slaFailedJobClustersCounter;
    //    @JsonIgnore
    //    private final Comparator<MantisJobMgr> comparator = (o1, o2) -> {
    //        if (o2 == null)
    //            return -1;
    //        if (o1 == null)
    //            return 1;
    //        return Long.compare(getJobIdNumber(o1.getJobId()), getJobIdNumber(o2.getJobId()));
    //    };
    //    @JsonIgnore
    //    private MantisStorageProvider storageProvider = null;
    //    @JsonIgnore
    //    AtomicBoolean isEnforcingSla = new AtomicBoolean(false);
    //
    private List<Label> labels;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public NamedJob(@JsonProperty("jobOps") MantisJobOperations jobOps, @JsonProperty("name") String name,
                    @JsonProperty("jars") List<Jar> jars, @JsonProperty("sla") SLA sla,
                    @JsonProperty("parameters") List<Parameter> parameters,
                    @JsonProperty("owner") JobOwner owner, @JsonProperty("lastJobCount") long lastJobCount,
                    @JsonProperty("disabled") boolean disabled,
                    @JsonProperty("isReadyForJobMaster") boolean isReadyForJobMaster,
                    @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                    @JsonProperty("labels") List<Label> labels) {

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
        if (jars != null)
            this.jars.addAll(jars);
        ////        jobIds = BehaviorSubject.create();
        ////        sortedJobMgrs = new TreeSet<>(comparator);
        ////        sortedRegisteredJobMgrs = new TreeSet<>(comparator);
        ////        Metrics m = new Metrics.Builder()
        ////                .id(NamedJob.class.getCanonicalName(), new BasicTag("jobcluster", name))
        ////                .addCounter(JobSubmissionsCounterName)
        ////                .addCounter(JobTerminationsCounterName)
        ////                .addCounter(SLA_FAILED_JOB_CLUSTERS)
        ////                .build();
        ////        m = MetricsRegistry.getInstance().registerAndGet(m);
        ////        jobSubmissionsCounter = m.getCounter(JobSubmissionsCounterName);
        ////        jobTerminationsCounter = m.getCounter(JobTerminationsCounterName);
        ////        slaFailedJobClustersCounter = m.getCounter(SLA_FAILED_JOB_CLUSTERS);
        ////        try {
        //            setupCron();
        //        } catch (SchedulerException e) {
        //            logger.error(name + ": error setting up cron: " + e.getMessage());
        //        }
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
                + labels + "]";
    }

    private int getMaxNumberOfJars() {
        return ConfigurationProvider.getConfig().getMaximumNumberOfJarsPerJobName();
    }

    //    @JsonIgnore
    //    void setStorageProvider(MantisStorageProvider storageProvider) {
    //        this.storageProvider = storageProvider;
    //    }
    //
    //    private void setupCron() throws SchedulerException {
    //        try {
    //            if (!disabled) {
    //                sla.initCron(this);
    //                cronActive = sla.hasCronSpec;
    //            }
    //        } catch (SchedulerException e) {
    //            cronActive = false;
    //            disabled = true;
    //            throw e;
    //        }
    //    }

    /* package */ void setJobOps(MantisJobOperations jobOps) {
        this.jobOps = jobOps;
    }

    //    private void trim() {
    //        int maxNumberOfJars = getMaxNumberOfJars();
    //        if (jars.size() > maxNumberOfJars) {
    //            final Iterator<Jar> iterator = jars.iterator();
    //            int toRemove = jars.size() - maxNumberOfJars;
    //            while (iterator.hasNext() && toRemove-- > 0) {
    //                final Jar next = iterator.next();
    //                if (notReferencedByJobs(next))
    //                    iterator.remove();
    //            }
    //        }
    //    }

    //    private boolean notReferencedByJobs(Jar next) {
    //        for (MantisJobMgr jobMgr : sortedJobMgrs) {
    //            if (jobMgr.isActive() && jobMgr.getJobMetadata().getJarUrl().toString().equals(next.getUrl().toString()))
    //                return false;
    //        }
    //        return true;
    //    }

    public String getName() {
        return name;
    }

    //    void addJar(Jar jar) throws InvalidNamedJobException {
    //        // add only if version is unique
    //        for (Jar j : jars)
    //            if (j.version.equals(jar.version))
    //                throw new InvalidNamedJobException("Jar version " + jar.version + " already used, must be unique");
    //        jars.add(jar);
    //        trim();
    //    }

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

    @JsonIgnore
    public boolean getIsActive() {
        return isActive;
    }

    //    @JsonIgnore
    //    public Map<String, CompletedJob> getCompletedJobs() {
    //        return completedJobs;
    //    }

    @JsonIgnore
    public void setInactive() throws NamedJobDeleteException {
        //        setDisabled(true);
        //        // delete all completed jobs
        //        // In order to avoid concurrent modification, copy the completed jobs from the map's values since the map will
        //        // get modified from within the call of operations triggered to deleted each job.
        //        List<CompletedJob> cjobs = new LinkedList<>(completedJobs.values());
        //        for (CompletedJob cj : cjobs) {
        //            try {
        //                jobOps.deleteJob(cj.getJobId());
        //            } catch (IOException e) {
        //                throw new NamedJobDeleteException("Error deleting job " + cj.jobId, e);
        //            }
        //        }
        //        completedJobs.clear();
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

    //    void setSla(SLA sla) throws InvalidNamedJobException {
    //        try (AutoCloseable l = obtainLock()) {
    //            sla.validate();
    //            this.sla.destroyCron();
    //            this.sla = sla;
    //            try {
    //                setupCron();
    //            } catch (SchedulerException e) {
    //                throw new InvalidNamedJobException(e.getMessage(), e);
    //            }
    //            enforceSla(Optional.empty());
    //        } catch (Exception e) { // shouldn't happen, this is only to make obtainlock() happy
    //            logger.warn("Unexpected exception setting sla: " + e.getMessage());
    //            throw new InvalidNamedJobException("Unexpected error: " + e.getMessage(), e);
    //        }
    //    }

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

            //            if(!hasCronSpec || triggerId != null)
            //                return;
            //            logger.info("Init'ing cron for " + job.getName());
            //            triggerGroup = job.getName() + "-" + this;
            //            try {
            //                scheduledTrigger = new CronTrigger<>(cronSpec, job.getName(), job, NamedJob.class, CronTriggerAction.class);
            //                triggerId = triggerOperator.registerTrigger(triggerGroup, scheduledTrigger);
            //            } catch (IllegalArgumentException e) {
            //                throw new SchedulerException(e.getMessage(), e);
            //            }
        }

        // caller must lock to avoid concurrent access with initCron()
        private void destroyCron() {
            // DISABLED AS Master V2 does not use this class for cron
            //            try {
            //                if (triggerId != null) {
            //                    logger.info("Destroying cron " + triggerId);
            //                    triggerOperator.deleteTrigger(triggerGroup, triggerId);
            //                    triggerId = null;
            //                }
            //            } catch (TriggerNotFoundException | SchedulerException e) {
            //                logger.warn("Couldn't delete trigger group " + triggerGroup + ", id " + triggerId);
            //            }
        }
    }

    //    MantisJobMgr getJobWithUniqueTag(String unique) {
    //        try (AutoCloseable l = obtainLock()) {
    //            MantisJobMgr mgr = getActiveJobMgrWithUniqueTag(unique);
    //            return mgr == null ?
    //                    getRegisteredJobMgrWithUniqueTag(unique) :
    //                    mgr;
    //        } catch (Exception e) {
    //            logger.warn("Unexpected exception: " + e.getMessage());
    //            return null;
    //        }
    //    }
    //
    //    private MantisJobMgr getRegisteredJobMgrWithUniqueTag(String unique) {
    //        for (MantisJobMgr jobMgr : sortedRegisteredJobMgrs) {
    //            final String uniqueTag = NamedJobs.getUniqueTag(jobMgr.getJobMetadata().getSla().getUserProvidedType());
    //            if (jobMgr.isActive() && uniqueTag != null && !uniqueTag.isEmpty() && uniqueTag.equals(unique))
    //                return jobMgr;
    //        }
    //        return null;
    //    }
    //
    //    private MantisJobMgr getActiveJobMgrWithUniqueTag(String unique) {
    //        for (MantisJobMgr jobMgr : sortedJobMgrs) {
    //            final String uniqueTag = NamedJobs.getUniqueTag(jobMgr.getJobMetadata().getSla().getUserProvidedType());
    //            if (jobMgr.isActive() && uniqueTag != null && !uniqueTag.isEmpty() && uniqueTag.equals(unique))
    //                return jobMgr;
    //        }
    //        return null;
    //    }

    // Keep this public since Quartz needs to call it when triggering cron.
    public static class CronTriggerAction implements Action1<NamedJob> {

        @Override
        public void call(NamedJob job) {
            logger.info("Cron fired for " + job.getName());
            //                try (AutoCloseable l = job.obtainLock()) {
            //                    if (job.sla.cronPolicy != null) {
            //                        MantisJobMgr jobMgr = null;
            //                        if (!job.sortedJobMgrs.isEmpty())
            //                            jobMgr = job.sortedJobMgrs.last();
            //                        else if (!job.sortedRegisteredJobMgrs.isEmpty())
            //                            jobMgr = job.sortedRegisteredJobMgrs.last();
            //                        if (job.sla.cronPolicy == NamedJobDefinition.CronPolicy.KEEP_NEW ||
            //                                jobMgr == null || MantisJobState.isTerminalState(jobMgr.getJobMetadata().getState()))
            //                            job.quickSubmitWithDefaults("Cron");
            //                        else
            //                            logger.info(job.getName() + ": Skipping submitting new job upon cron trigger, one exists already");
            //                    }
            //                } catch (Exception e) {
            //                   // logger.warn(job.getName() + ": Unexpected error in cron trigger execution: " + e.getMessage(), e);
            //                }
        }
    }

    //    @JsonIgnore
    //    public Observable<String> getJobIds() {
    //        return jobIds;
    //    }

    //    public void init(Collection<MantisJobMgr> jobMgrs) {
    //        logger.info("Init'ing Job Cluster " + name + " with " + (jobMgrs == null ? 0 : jobMgrs.size()) + " jobs");
    //        if (jobMgrs == null || jobMgrs.isEmpty())
    //            return;
    //        for (MantisJobMgr m : jobMgrs) {
    //            if (m.getJobMetadata().getState() == MantisJobState.Accepted)
    //                sortedRegisteredJobMgrs.add(m);
    //            else if (m.getJobMetadata().getState() == MantisJobState.Launched)
    //                sortedJobMgrs.add(m);
    //            // else, ignore other states
    //        }
    //        if (!sortedJobMgrs.isEmpty())
    //            jobIds.onNext(sortedJobMgrs.last().getJobId());
    //    }
    //
    //    public void registerJobMgr(MantisJobMgr m) {
    //        try (AutoCloseable l = obtainLock()) {
    //            sortedRegisteredJobMgrs.add(m);
    //        } catch (Exception e) {
    //            logger.warn("Unexpected error: " + e.getMessage());
    //        }
    //    }
    //
    //    public void addJobMgr(MantisJobMgr m) {
    //        try (AutoCloseable l = obtainLock()) {
    //            sortedRegisteredJobMgrs.remove(m);
    //            if (!sortedJobMgrs.add(m))
    //                return; // already present in our set
    //        } catch (Exception e) {
    //            logger.error("Unexpected error adding jobMgr for " + m.getJobId() + ": " + e.getMessage(), e);
    //        }
    //        jobIds.onNext(m.getJobId());
    //        enforceSla(Optional.empty());
    //    }
    //
    //    public void jobComplete(final MantisJobMgr m, final MantisJobState state, final long submittedAt, final String user)
    //            throws IOException {
    //        try (AutoCloseable l = obtainLock()) {
    //            sortedJobMgrs.remove(m);
    //            sortedRegisteredJobMgrs.remove(m);
    //        } catch (Exception e) {
    //            logger.error("Unexpected error removing complete job: " + e.getMessage(), e);
    //        }
    //        final CompletedJob completedJob = new CompletedJob(name, m.getJobId(), null, state,
    //                submittedAt, System.currentTimeMillis(), user, new ArrayList<>());
    //        completedJobs.put(m.getJobId(), completedJob);
    //        storageProvider.storeCompletedJobForNamedJob(name, completedJob);
    //        if (m.getJobMetadata() != null) {
    //            enforceSla(Optional.of(m.getJobMetadata()));
    //        } else {
    //            enforceSla(m.getCompletedJobMetadata());
    //        }
    //    }
    //
    //    void initCompletedJob(CompletedJob c) {
    //        if (c != null) {
    //            completedJobs.put(c.getJobId(), c);
    //        }
    //    }
    //
    //    @JsonIgnore
    //    public Collection<MantisJobMgr> getAllJobMgrs() {
    //        List<MantisJobMgr> result = new ArrayList<>(sortedJobMgrs);
    //        result.addAll(sortedRegisteredJobMgrs);
    //        return result;
    //    }
    //
    //    /* package */ void enforceSla(final Optional<MantisJobMetadata> lastRemovedMantisJobMetadata) {
    //        if (!isEnforcingSla.compareAndSet(false, true))
    //            return; // already running
    //        List<MantisJobMgr> toKill = new ArrayList<>();
    //        try (AutoCloseable l = obtainLock()) {
    //            if (disabled) {
    //                List<MantisJobMgr> jobsToKill = new ArrayList<>();
    //                jobsToKill.addAll(sortedRegisteredJobMgrs);
    //                jobsToKill.addAll(sortedJobMgrs);
    //                if (!jobsToKill.isEmpty()) {
    //                    // ensure no job is running
    //                    jobsToKill.stream().filter(MantisJobMgr::isActive).forEach(jobMgr -> {
    //                        jobOps.killJob("MantisMaster", jobMgr.getJobId(), "job " + getName() + " is disabled");
    //                        jobTerminationsCounter.increment();
    //                    });
    //                }
    //                sla.destroyCron();
    //                return;
    //            }
    //            if (sla == null || (sla.min == 0 && sla.max == 0))
    //                return;
    //            try {
    //                setupCron();
    //            } catch (SchedulerException e) {
    //                // this is unexpected since sla would have been validated before it was set
    //                logger.error(name + ": Unexpected to fail initializing cron: " + e.getMessage());
    //            }
    //            List<MantisJobMgr> activeJobMgrs =
    //                    sortedJobMgrs.stream().filter(MantisJobMgr::isActive).collect(Collectors.toList());
    //            List<MantisJobMgr> activeRgstrdJobMgrs =
    //                    sortedRegisteredJobMgrs.stream().filter(MantisJobMgr::isActive).collect(Collectors.toList());
    //            // there could be some jobs running and some registered but not running yet. Eagerly enforcing the sla.max
    //            // could result in killing the running job in favor of the new job, which may not start successfully. Instead,
    //            // we take the following approach:
    //            // Manage min by combining the total of both running and registered jobs. This ensures we don't start
    //            // too many new jobs if previously started ones stay in registered for too long for not successfully starting.
    //            if (sla != null && (activeJobMgrs.size() + activeRgstrdJobMgrs.size()) < sla.min) {
    //                logger.info("Submitting " + (sla.min - activeJobMgrs.size()) + " jobs per sla min of " + sla.min +
    //                        " for job name " + name);
    //                for (int i = 0; i < sla.min - activeJobMgrs.size(); i++) {
    //                    MantisJobMetadata last = null;
    //                    if (lastRemovedMantisJobMetadata.isPresent()) {
    //                        logger.info("got last removed job {}", lastRemovedMantisJobMetadata.get().getJobId());
    //                        last = lastRemovedMantisJobMetadata.get();
    //                    }
    //                    if (!sortedJobMgrs.isEmpty()) {
    //                        final MantisJobMetadata lastSorted = sortedJobMgrs.last().getJobMetadata();
    //                        if (last == null || (getJobIdNumber(lastSorted.getJobId()) > getJobIdNumber(last.getJobId()))) {
    //                            logger.info("last removed job from sortedJobMgrs {}", lastSorted.getJobId());
    //                            last = lastSorted;
    //                        }
    //                    }
    //                    if (last == null) {
    //                        // get it from archived jobs
    //                        if (!completedJobs.isEmpty()) {
    //                            long latestCompletedAt = 0L;
    //                            CompletedJob latest = null;
    //                            for (CompletedJob j : completedJobs.values()) {
    //                                if (latest == null || latestCompletedAt < j.getTerminatedAt()) {
    //                                    latest = j;
    //                                    latestCompletedAt = j.getTerminatedAt();
    //                                }
    //                            }
    //                            if (latest != null) {
    //                                last = storageProvider.loadArchivedJob(latest.getJobId());
    //                                logger.info("last job from completedJobs {}", last.getJobId());
    //                            }
    //                        }
    //                    }
    //                    if (last == null) {
    //                        logger.warn("Can't submit new job to maintain sla for job cluster " + name + ": no previous job to clone");
    //                        slaFailedJobClustersCounter.increment();
    //                    } else {
    //                        logger.info("submitting new job using job metadata from last job {}", last.getJobId());
    //                        if (submitNewJob(last) != null) {
    //                            jobSubmissionsCounter.increment();
    //                        }
    //                    }
    //                }
    //            }
    //            // Manage max by killing any excess running jobs. Also, kill any registered jobs older than remaining
    //            // running jobs, or in excess of sla.max.
    //            // For this we sort running and registered JobMgrs and walk the list in descending order to apply this logic.
    //            SortedSet<MantisJobMgr> allSortedJobMgrs = new TreeSet<>(comparator);
    //            allSortedJobMgrs.addAll(activeJobMgrs);
    //            allSortedJobMgrs.addAll(activeRgstrdJobMgrs);
    //            final MantisJobMgr[] mantisJobMgrs = allSortedJobMgrs.toArray(new MantisJobMgr[allSortedJobMgrs.size()]);
    //            if (mantisJobMgrs.length > 0) {
    //                boolean slaSatisfied = false;
    //                int activeCount = 0;
    //                int registeredCount = 0;
    //                for (int i = mantisJobMgrs.length - 1; i >= 0; i--) {
    //                    MantisJobMgr m = mantisJobMgrs[i];
    //                    boolean isActive = m.getJobMetadata() != null &&
    //                            m.getJobMetadata().getState() == MantisJobState.Launched;
    //                    if (!isActive)
    //                        registeredCount++;
    //                    if (!isActive && !slaSatisfied && (registeredCount + activeCount) <= sla.max) {
    //                        continue;
    //                    }
    //                    if (slaSatisfied || (!isActive && (registeredCount + activeCount) > sla.max)) {
    //                        toKill.add(m); // carry out the kills after unlocking this object
    //                    } else if (isActive)
    //                        activeCount++;
    //                    if (activeCount >= sla.max)
    //                        slaSatisfied = true;
    //                }
    //            }
    //        } catch (Exception e) {
    //            logger.error("Unknown error enforcing SLA for " + name + ": " + e.getMessage(), e);
    //        } // shouldn't happen
    //        finally {
    //            try {
    //                if (!toKill.isEmpty()) {
    //                    for (MantisJobMgr m : toKill) {
    //                        slaKill(m);
    //                    }
    //                    logger.info(name + ": killed " + toKill.size() + " jobs per sla max of " + sla.max);
    //                }
    //                removeExpiredCompletedJobs();
    //            } finally {
    //                isEnforcingSla.set(false); // mark exit of enforceSla
    //            }
    //        }
    //    }

    //    private void removeExpiredCompletedJobs() {
    //        if (!completedJobs.isEmpty()) {
    //            final long cutOff = System.currentTimeMillis() - (ConfigurationProvider.getConfig().getTerminatedJobToDeleteDelayHours() * 3600000L);
    //            new LinkedList<>(completedJobs.values()).stream().filter(j -> j.getTerminatedAt() < cutOff).forEach(j -> {
    //                try {
    //                    storageProvider.removeCompledtedJobForNamedJob(name, j.getJobId());
    //                    completedJobs.remove(j.getJobId());
    //                } catch (IOException e) {
    //                    logger.warn("Error removing completed job " + j.getJobId() + ": " + e.getMessage(), e);
    //                }
    //            });
    //        }
    //    }
    //
    //    private void slaKill(MantisJobMgr jobMgr) {
    //        jobOps.killJob("MantisMaster", jobMgr.getJobId(), "#jobs exceeded for SLA max of " + sla.max);
    //        jobTerminationsCounter.increment();
    //    }
    //
    //    private String quickSubmitWithDefaults(String user) throws InvalidJobException {
    //        try (AutoCloseable l = obtainLock()) {
    //            final MantisJobMgr jobMgr = sortedJobMgrs.isEmpty() ? null : sortedJobMgrs.last();
    //            if (jobMgr == null) {
    //                CompletedJob lastJob = getLastCompletedJob();
    //                if (lastJob == null) {
    //                    // create a default job with info we have
    //                    final MantisJobStatus status = jobOps.submit(
    //                            new MantisJobDefinition(
    //                                    name, user, null, null,
    //                                    parameters, new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Perpetual, ""),
    //                                    0, jars.get(jars.size() - 1).schedulingInfo, sla.min, sla.max, sla.cronSpec, sla.cronPolicy,
    //                                    isReadyForJobMaster, migrationConfig, labels)
    //                    );
    //                    if (status.getFatalError() != null) {
    //                        throw new InvalidJobException(name + ": Couldn't submit job with defaults: " + status.getFatalError());
    //                    } else
    //                        return status.getJobId();
    //                } else {
    //                    try {
    //                        return submitFromCompletedJob(lastJob.getJobId(), user);
    //                    } catch (IOException e) {
    //                        throw new InvalidJobException(lastJob.getJobId(), e);
    //                    }
    //                }
    //            } else
    //                return submitNewJob(jobMgr.getJobMetadata(), user);
    //        } catch (Exception e) {
    //            logger.warn("Unexpected error submitting job with defaults: " + e.getMessage(), e);
    //            return null;
    //        }
    //    }
    //
    //    String quickSubmit(String user) throws InvalidJobException {
    //        final MantisJobMgr jobMgr = sortedJobMgrs.isEmpty() ? null : sortedJobMgrs.last();
    //        if (jobMgr == null) {
    //            CompletedJob lastJob = getLastCompletedJob();
    //            if (lastJob == null)
    //                throw new InvalidJobException("No previous job to copy parameters or scheduling info for quick submit");
    //            try {
    //                return submitFromCompletedJob(lastJob.getJobId(), user);
    //            } catch (IOException e) {
    //                throw new InvalidJobException(lastJob.getJobId(), e);
    //            }
    //        } else
    //            return submitNewJob(jobMgr.getJobMetadata(), user);
    //    }
    //
    //    private String submitFromCompletedJob(String jobId, String user) throws IOException, InvalidJobException {
    //        final MantisJobMetadataWritable jobMetadata = storageProvider.loadArchivedJob(jobId);
    //        if (jobMetadata == null) {
    //            throw new InvalidJobException(jobId, new Exception("Can't load completed job from archive"));
    //        }
    //        return submitNewJob(new MantisJobDefinition(
    //                name, user, null, null,
    //                jobMetadata.getParameters(), jobMetadata.getSla(), jobMetadata.getSubscriptionTimeoutSecs(),
    //                MantisJobStore.getSchedulingInfo(jobMetadata),
    //                sla.min, sla.max, sla.cronSpec, sla.cronPolicy, isReadyForJobMaster, migrationConfig, jobMetadata.getLabels()
    //        ));
    //    }
    //
    //    private CompletedJob getLastCompletedJob() {
    //        CompletedJob last = null;
    //        if (!completedJobs.isEmpty()) {
    //            for (CompletedJob c : completedJobs.values()) {
    //                if (last == null || last.getTerminatedAt() < c.getTerminatedAt()) {
    //                    last = c;
    //                }
    //            }
    //        }
    //        return last;
    //    }
    //
    //    private String submitNewJob(MantisJobMetadata jobMetadata) {
    //        return submitNewJob(jobMetadata, jobMetadata.getUser());
    //    }
    //
    //    private String submitNewJob(MantisJobMetadata jobMetadata, String user) {
    //        return submitNewJob(new MantisJobDefinition(name, user,
    //                null, // don't specify jar, let it pick latest
    //                null, // don't specify jar version, let it pick latest
    //                jobMetadata.getParameters(),
    //                jobMetadata.getSla(),
    //                jobMetadata.getSubscriptionTimeoutSecs(),
    //                MantisJobStore.getSchedulingInfo(jobMetadata), sla.min, sla.max, sla.cronSpec, sla.cronPolicy,
    //                jobMetadata.getStageMetadata(0) != null, migrationConfig, jobMetadata.getLabels()));
    //    }
    //
    //    private String submitNewJob(MantisJobDefinition jobDefinition) {
    //        try (AutoCloseable l = obtainLock()) {
    //            final MantisJobStatus status = jobOps.submit(jobDefinition);
    //            if (status.getFatalError() != null) {
    //                logger.error("Couldn't submit replacement job for " + name + " - " + status.getFatalError());
    //                return null;
    //            } else
    //                return status.getJobId();
    //        } catch (Exception e) {
    //            logger.error("Unexpected error obtaining lock: " + e.getMessage(), e);
    //            return null;
    //        }
    //    }
    //
    //    void removeJobMgr(final Optional<MantisJobMgr> jobMgrO, String jobId) {
    //        if (jobMgrO.isPresent()) {
    //            final MantisJobMgr jobMgr = jobMgrO.get();
    //            logger.info("Removing job " + jobMgr.getJobId());
    //            try (AutoCloseable l = obtainLock()) {
    //                sortedRegisteredJobMgrs.remove(jobMgr);
    //                if (sortedJobMgrs.remove(jobMgr)) {
    //                    if (jobMgr.getJobMetadata() != null) {
    //                        enforceSla(Optional.of(jobMgr.getJobMetadata()));
    //                    } else {
    //                        enforceSla(jobMgr.getCompletedJobMetadata());
    //                    }
    //                } else {
    //                    enforceSla(Optional.empty());
    //                }
    //            } catch (Exception e) {
    //                logger.error("Unexpected error locking: " + e.getMessage(), e);
    //            }
    //        }
    //        completedJobs.remove(jobId);
    //    }
    //
    //    public String submitWithLatestJar(String user) throws InvalidJobException {
    //        try (AutoCloseable l = obtainLock()) {
    //            if (sortedJobMgrs.isEmpty()) {
    //                final CompletedJob lastCompletedJob = getLastCompletedJob();
    //                if (lastCompletedJob == null)
    //                    return null;
    //                try {
    //                    String jobId = submitFromCompletedJob(lastCompletedJob.getJobId(), user);
    //                    if (jobId != null)
    //                        jobSubmissionsCounter.increment();
    //                    return jobId;
    //                } catch (IOException e) {
    //                    throw new InvalidJobException(lastCompletedJob.getJobId(), e);
    //                }
    //            } else {
    //                final String jobId = submitNewJob(sortedJobMgrs.last().getJobMetadata(), user);
    //                if (jobId != null) {
    //                    jobSubmissionsCounter.increment();
    //                }
    //                return jobId;
    //            }
    //        } catch (InvalidJobException e) {
    //            throw e;
    //        } catch (Exception e) {
    //            logger.error("Unexpected error submitting with latest jar: " + e.getMessage(), e);
    //            return null;
    //        }
    //    }
    //
    //    /**
    //     * Obtain a lock on this object, All operations on this object work without checking for concurrent updates. Callers
    //     * are expected to call this method to lock this object for safe modifications and unlock after use. The return object
    //     * can be used in try with resources for reliable unlocking.
    //     *
    //     * @return {@link AutoCloseable} lock object.
    //     */
    //    public AutoCloseable obtainLock() {
    //        lock.lock();
    //        return lock::unlock;
    //    }
    //
    //    static boolean isValidJobName(String name) {
    //        return Pattern.matches("^[A-Za-z]+[A-Za-z0-9+-_=:;]*", name);
    //    }

}
