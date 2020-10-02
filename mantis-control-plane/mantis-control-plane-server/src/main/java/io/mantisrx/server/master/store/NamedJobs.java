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

//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
////import com.google.common.base.Preconditions;
//
//import com.google.common.base.Preconditions;
//import io.mantisrx.common.Label;
//import io.mantisrx.runtime.JobOwner;
//import io.mantisrx.runtime.JobSla;
//import io.mantisrx.runtime.MantisJobDefinition;
//import io.mantisrx.runtime.NamedJobDefinition;
//import io.mantisrx.runtime.WorkerMigrationConfig;
//import io.mantisrx.runtime.descriptor.SchedulingInfo;
//import io.mantisrx.runtime.parameter.Parameter;
//import io.mantisrx.server.master.MantisAuditLogEvent;
//import io.mantisrx.server.master.MantisAuditLogWriter;
//import io.mantisrx.server.master.MantisJobMgr;
//import io.mantisrx.server.master.MantisJobOperations;
//import io.mantisrx.server.master.jobmgmt.JobRegistry;
//import org.json.JSONException;
//import org.json.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.functions.Action2;
//import rx.functions.Func1;
//import rx.schedulers.Schedulers;
//import rx.subjects.ReplaySubject;
////import scala.util.parsing.json.JSONObject;
//
//import java.io.IOException;
//import java.net.URL;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.concurrent.locks.ReentrantLock;
//
public class NamedJobs {
    //
    //    static class NamedJobLock {
    //        private final ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    //
    //        synchronized AutoCloseable obtainLock(final String jobName) {
    //            ReentrantLock newLock = new ReentrantLock();
    //            final ReentrantLock oldLock = locks.putIfAbsent(jobName, newLock);
    //            final ReentrantLock lock = oldLock==null? newLock : oldLock;
    //            lock.lock();
    //            return new AutoCloseable() {
    //                @Override
    //                public void close() throws Exception {
    //                    lock.unlock();
    //                    if(!lock.isLocked())
    //                        locks.remove(jobName);
    //                }
    //            };
    //        }
    //    }
    //
    //    public static class JobIdForSubmit {
    //        private final String jobId;
    //        private final boolean isNewJobId;
    //
    //        public JobIdForSubmit(String jobId, boolean newJobId) {
    //            this.jobId = jobId;
    //            isNewJobId = newJobId;
    //        }
    //        public String getJobId() {
    //            return jobId;
    //        }
    //        public boolean isNewJobId() {
    //            return isNewJobId;
    //        }
    //    }
    //
    //    private static final ObjectMapper mapper = new ObjectMapper();
    //
    //    private final JobRegistry jobRegistry;
    //    private static final Logger logger = LoggerFactory.getLogger(NamedJobs.class);
    //    private MantisStorageProvider storageProvider;
    //    private final MantisJobOperations jobOps;
    //    private final NamedJobLock namedJobLock = new NamedJobLock();
    //
    //    public NamedJobs(final JobRegistry jobRegistry, MantisJobOperations jobOps) {
    //        this.jobRegistry = jobRegistry;
    //        this.jobOps = jobOps;
    //        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    //        mapper.registerModule(new Jdk8Module());
    //    }
    //
    //    public void init(MantisStorageProvider storageProvider) {
    //        this.storageProvider = storageProvider;
    //        try {
    //            for(NamedJob job: storageProvider.initNamedJobs()) {
    //                job.setJobOps(jobOps);
    //                if(this.jobRegistry.addJobClusterIfAbsent(job) != null)
    //                    throw new IllegalStateException("Unexpected to add duplicate namedJob entry for " + job.getName());
    //                job.setStorageProvider(storageProvider);
    //            }
    //            final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    //            storageProvider.initNamedJobCompletedJobs()
    //                    .doOnNext(cj -> {
    //                        final Optional<NamedJob> namedJob = jobRegistry.getJobCluster(cj.getName());
    //                        if (namedJob.isPresent())
    //                            namedJob.get().initCompletedJob(cj);
    //                    })
    //                    .doOnError(errorRef::set)
    //                    .toBlocking()
    //                    .lastOrDefault(null);
    //            if(errorRef.get() != null)
    //                throw new IOException(errorRef.get());
    //        } catch (IOException e) {
    //            // can't handle this
    //            throw new IllegalStateException(e.getMessage(), e);
    //        }
    //        Schedulers.computation().createWorker().schedulePeriodically(() -> {
    //            for(NamedJob job: jobRegistry.getAllJobClusters()) {
    //                job.enforceSla(Optional.empty());
    //            }
    //        }, 60, 60, TimeUnit.SECONDS);
    //    }
    //
    //    public Optional<NamedJob> getJobByName(String name) {
    //        return jobRegistry.getJobCluster(name);
    //    }
    //
    //    public Collection<NamedJob> getAllNamedJobs() {
    //        return jobRegistry.getAllJobClusters().isEmpty() ? Collections.emptyList() : Collections.unmodifiableCollection(jobRegistry.getAllJobClusters());
    //    }
    //
    //    public void deleteJob(String jobId) throws IOException {
    //        final Optional<NamedJob> namedJob = jobRegistry.getJobCluster(NamedJob.getJobName(jobId));
    //        if (namedJob.isPresent()) {
    //            final Optional<MantisJobMgr> jobMgrO = jobRegistry.getJobManager(jobId);
    //            namedJob.get().removeJobMgr(jobMgrO, jobId);
    //            storageProvider.removeCompledtedJobForNamedJob(namedJob.get().getName(), jobId);
    //        }
    //    }
    //
    //    public NamedJob createNamedJob(NamedJobDefinition namedJobDefinition) throws InvalidNamedJobException {
    //        final String name = namedJobDefinition.getJobDefinition().getName();
    //        final String user = namedJobDefinition.getJobDefinition().getUser();
    //        if(user==null || user.isEmpty())
    //            throw new InvalidNamedJobException("Must set user in request");
    //        final JobOwner owner = namedJobDefinition.getOwner();
    //        if(!NamedJob.isValidJobName(name))
    //            throw new InvalidNamedJobException("Invalid name for Job Cluster: " + name);
    //        if(jobRegistry.addJobClusterIfAbsent(
    //        		new NamedJob(jobOps,
    //        					name,
    //        					null,
    //        					null,
    //        					namedJobDefinition.getJobDefinition().getParameters(),
    //        					owner,
    //        					0,
    //        					false,
    //        					namedJobDefinition.getJobDefinition().getIsReadyForJobMaster(),
    //        					namedJobDefinition.getJobDefinition().getMigrationConfig(),
    //        					namedJobDefinition.getJobDefinition().getLabels()
    //        					)
    //        		) != null
    //        	) {
    //            throw new InvalidNamedJobException(name+" Job Cluster already exists");
    //        }
    //        final NamedJob job = jobRegistry.getJobCluster(name).get();
    //        logger.info("Creating NamedJob->:" + job);
    //        job.setStorageProvider(storageProvider);
    //        boolean success=false;
    //        try(AutoCloseable lock = job.obtainLock()) {
    //            job.addJar(new NamedJob.Jar(namedJobDefinition.getJobDefinition().getJobJarFileLocation(),
    //                    System.currentTimeMillis(), namedJobDefinition.getJobDefinition().getVersion(),
    //                    namedJobDefinition.getJobDefinition().getSchedulingInfo()));
    //            job.setSla(new NamedJob.SLA(namedJobDefinition.getJobDefinition().getSlaMin(),
    //                    namedJobDefinition.getJobDefinition().getSlaMax(),
    //                    namedJobDefinition.getJobDefinition().getCronSpec(),
    //                    namedJobDefinition.getJobDefinition().getCronPolicy()));
    //            storageProvider.storeNewNamedJob(job);
    //            success=true;
    //            MantisAuditLogWriter.getInstance()
    //                    .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.NAMED_JOB_CREATE, name,
    //                    "user=" + user));
    //        } catch(IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        } catch (Exception e) {
    //            logLockError(e);
    //            throw new InvalidNamedJobException(name+": "+e.getMessage(), e);
    //        }
    //        finally {
    //            if(!success)
    //                jobRegistry.removeJobCluster(name);
    //        }
    //        return job;
    //    }
    //
    //    public Action2<String, Collection<MantisJobMgr>> getNamedJobIdInitializer() {
    //        return new Action2<String, Collection<MantisJobMgr>>() {
    //            @Override
    //            public void call(String name, Collection<MantisJobMgr> jobMgrs) {
    //                Optional<NamedJob> jobCluster = jobRegistry.getJobCluster(name);
    //                if(!jobCluster.isPresent())
    //                    logger.error("Can't find Job Cluster for name=" + name);
    //                else
    //                    jobCluster.get().init(jobMgrs);
    //            }
    //        };
    //    }
    //
    //    public void deleteNamedJob(String name, String user, Func1<String, Boolean> jobIdDeleter) throws NamedJobDeleteException {
    //        Optional<NamedJob> jobClusterO = jobRegistry.getJobCluster(name);
    //        if(!jobClusterO.isPresent())
    //            return;
    //        final NamedJob jobCluster = jobClusterO.get();
    //        try (AutoCloseable lock = jobCluster.obtainLock()) {
    //            if(!jobCluster.getIsActive())
    //                return;
    //            String activeJobId = deleteAllJobs(name, jobIdDeleter);
    //            if(activeJobId != null) {
    //                logger.warn("Active job " + activeJobId + " exists, not deleting job cluster " + name);
    //                throw new NamedJobDeleteException("Active job exists - " + activeJobId);
    //            }
    //            boolean deleted = storageProvider.deleteNamedJob(name);
    //            jobCluster.setInactive();
    //            if(deleted) {
    //                jobRegistry.removeJobCluster(name);
    //                MantisAuditLogWriter.getInstance()
    //                        .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.NAMED_JOB_DELETE, name,
    //                        "user=" + user));
    //            }
    //        } catch(NamedJobDeleteException njde) {
    //            throw njde;
    //        } catch(Exception e) {
    //            logger.error("Error deleting job cluster " + name + ": " + e.getMessage(), e);
    //            throw new NamedJobDeleteException("Unknown error deleting Job Cluster " + name, e);
    //        }
    //    }
    //
    //    public void setDisabled(String name, String user, boolean status) throws InvalidNamedJobException {
    //        final Optional<NamedJob> jobClusterO = jobRegistry.getJobCluster(name);
    //        if (!jobClusterO.isPresent())
    //            return;
    //        final NamedJob jobCluster = jobClusterO.get();
    //        try (AutoCloseable lock = jobCluster.obtainLock()) {
    //            if(!jobCluster.getIsActive())
    //                throw new InvalidNamedJobException("Job Cluster " + name + " not active");
    //            if(jobCluster.getDisabled() == status)
    //                return; // no-op
    //            jobCluster.setDisabled(status);
    //            storageProvider.updateNamedJob(jobCluster);
    //            MantisAuditLogWriter.getInstance()
    //                    .getObserver().onNext(new MantisAuditLogEvent(
    //                    (status ? MantisAuditLogEvent.Type.NAMED_JOB_DISABLED : MantisAuditLogEvent.Type.NAMED_JOB_ENABLED),
    //                    name, "user=" + user));
    //        } catch (InvalidNamedJobException e) {
    //            throw e;
    //        } catch (IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        } catch (Exception e) {
    //            logger.warn("Unexpected error locking job " + name + ": " + e.getMessage(), e);
    //            throw new InvalidNamedJobException("Internal error disabling job " + name, e);
    //        }
    //    }
    //
    //    private String deleteAllJobs(String name, Func1<String, Boolean> jobIdDeleter) {
    //        // a simple linear search of all entries should suffice, this isn't expected to be called frequently
    //        boolean foundActive=false;
    //        List<MantisJobMgr> jobMgrs = new ArrayList<>();
    //        for(MantisJobMgr jobMgr: jobRegistry.getAllJobManagers()) {
    //            MantisJobMetadata jobMetadata = jobMgr.getJobMetadata();
    //            if(jobMetadata.getName().equals(name)) {
    //                jobMgrs.add(jobMgr);
    //                if(jobMgr.isActive())
    //                    return jobMetadata.getJobId();
    //            }
    //        }
    //        for(MantisJobMgr jobMgr: jobMgrs) {
    //            if(!jobIdDeleter.call(jobMgr.getJobId()))
    //                return jobMgr.getJobId();
    //        }
    //        return null;
    //    }
    //
    //    public NamedJob updateNamedJob(NamedJobDefinition namedJobDefinition, boolean createIfNeeded) throws InvalidNamedJobException {
    //        final String name = namedJobDefinition.getJobDefinition().getName();
    //        final String user = namedJobDefinition.getJobDefinition().getUser();
    //        if(user==null || user.isEmpty())
    //            throw new InvalidNamedJobException("Must set user in request");
    //        Optional<NamedJob> jobO = jobRegistry.getJobCluster(name);
    //        final int slaMin = namedJobDefinition.getJobDefinition().getSlaMin();
    //        final int slaMax = namedJobDefinition.getJobDefinition().getSlaMax();
    //        final String cronSpec = namedJobDefinition.getJobDefinition().getCronSpec();
    //        final NamedJobDefinition.CronPolicy cronPolicy = namedJobDefinition.getJobDefinition().getCronPolicy();
    //        if(!jobO.isPresent() && createIfNeeded) {
    //            try {
    //                jobO = Optional.ofNullable(createNamedJob(namedJobDefinition));
    //            } catch (InvalidNamedJobException e) {
    //                jobO = getJobByName(name);
    //            }
    //        }
    //        if(!jobO.isPresent())
    //            throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //        NamedJob jobCluster = jobO.get();
    //        String version = namedJobDefinition.getJobDefinition().getVersion();
    //        if(version==null || version.isEmpty())
    //            version = "" + System.currentTimeMillis();
    //        SchedulingInfo schedulingInfo = namedJobDefinition.getJobDefinition().getSchedulingInfo();
    //        if(schedulingInfo==null) {
    //            final List<NamedJob.Jar> jars = jobCluster.getJars();
    //            schedulingInfo = jars.get(jars.size()-1).getSchedulingInfo();
    //        }
    //        JobOwner owner = namedJobDefinition.getOwner();
    //        if(owner == null)
    //            owner = jobCluster.getOwner();
    //        List<Parameter> parameters = namedJobDefinition.getJobDefinition().getParameters();
    //        if(parameters==null || parameters.isEmpty())
    //            parameters = jobCluster.getParameters();
    //
    //        List<Label> labels = namedJobDefinition.getJobDefinition().getLabels();
    //        if(labels == null || labels.isEmpty()) {
    //        	labels = jobCluster.getLabels();
    //        }
    //        final NamedJob.Jar jar = new NamedJob.Jar(namedJobDefinition.getJobDefinition().getJobJarFileLocation(), System.currentTimeMillis(),
    //                version, schedulingInfo);
    //        try (AutoCloseable lock = jobCluster.obtainLock()) {
    //            if(!jobCluster.getIsActive())
    //                throw new InvalidNamedJobException("Job cluster" + name + " not active");
    //            jobCluster.setSla(new NamedJob.SLA(slaMin, slaMax,
    //                    namedJobDefinition.getJobDefinition().getCronSpec(),
    //                    cronPolicy));
    //            jobCluster.addJar(jar);
    //            jobCluster.setOwner(owner);
    //            jobCluster.setParameters(parameters);
    //            jobCluster.setLabels(labels);
    //            jobCluster.setIsReadyForJobMaster(namedJobDefinition.getJobDefinition().getIsReadyForJobMaster());
    //            jobCluster.setMigrationConfig(namedJobDefinition.getJobDefinition().getMigrationConfig());
    //            storageProvider.updateNamedJob(jobCluster);
    //        } catch (InvalidNamedJobException inje) {
    //            throw inje;
    //        } catch (IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        } catch (Exception e) {
    //            logLockError(e);
    //            throw new InvalidNamedJobException(name+": "+e.getMessage(), e);
    //        }
    //        try {
    //            MantisAuditLogWriter.getInstance()
    //                    .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.NAMED_JOB_UPDATE, name,
    //                    "user: " + user + ", sla: min=" + slaMin + ", max=" +
    //                            slaMax + "; jar=" + mapper.writeValueAsString(jar) + "; cronSpec=" + cronSpec +
    //                            ", cronPolicy=" + cronPolicy));
    //        } catch (JsonProcessingException e) {
    //            logger.warn("Error writing jar object value as json: " + e.getMessage());
    //        }
    //        return jobCluster;
    //    }
    //
    //    public NamedJob quickUpdateNamedJob(String user, String name, URL jobJar, String version) throws InvalidNamedJobException {
    //        final Optional<NamedJob> jobO = getJobByName(name);
    //        if (!jobO.isPresent())
    //            throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //        final NamedJob job = jobO.get();
    //        final NamedJob.Jar latestJar = job.getJar(null);
    //        if(version==null || version.isEmpty())
    //            version = ""+System.currentTimeMillis();
    //        NamedJobDefinition namedJobDefinition = new NamedJobDefinition(
    //                new MantisJobDefinition(
    //                        name, user, jobJar, version, job.getParameters(), null, 0,
    //                        latestJar.getSchedulingInfo(), job.getSla().getMin(), job.getSla().getMax(),
    //                        job.getSla().getCronSpec(), job.getSla().getCronPolicy(), job.getIsReadyForJobMaster(), job.getMigrationConfig(),job.getLabels()
    //                ),
    //                job.getOwner());
    //        return updateNamedJob(namedJobDefinition, false);
    //    }
    //
    //    public void updateSla(String user, String name, NamedJob.SLA sla, boolean forceEnable) throws InvalidNamedJobException {
    //        if(sla == null)
    //            throw new InvalidNamedJobException("Invalid null SLA");
    //        final Optional<NamedJob> jobO = getJobByName(name);
    //        if (!jobO.isPresent())
    //            throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //
    //        final NamedJob job = jobO.get();
    //        if(forceEnable)
    //            job.setDisabled(false);
    //        job.setSla(sla);
    //        try {
    //            storageProvider.updateNamedJob(job);
    //        } catch (IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        }
    //        MantisAuditLogWriter.getInstance()
    //                .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.NAMED_JOB_UPDATE, name,
    //                "user: " + user + ", sla: min=" + sla.getMin() + ", max=" +
    //                        sla.getMax() + "; cronSpec=" + sla.getCronSpec() + ", cronPolicy=" + sla.getCronPolicy()));
    //    }
    //
    //    public void quickUpdateLabels(String user, String name, List<Label> newLabels) throws InvalidNamedJobException {
    //		Preconditions.checkNotNull(newLabels);
    //		Preconditions.checkNotNull(name);
    //		final Optional<NamedJob> jobO = getJobByName(name);
    //		if (!jobO.isPresent()) {
    //			throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //		}
    //		final NamedJob job = jobO.get();
    //		job.setLabels(newLabels);
    //		try {
    //			storageProvider.updateNamedJob(job);
    //		} catch (IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        }
    //		MantisAuditLogWriter.getInstance().getObserver().onNext(
    //				new MantisAuditLogEvent(MantisAuditLogEvent.Type.NAMED_JOB_UPDATE, name,"user: " + user + ", labels: " + newLabels));
    //	}
    //
    //    public void updateMigrateStrategy(String user, String name, WorkerMigrationConfig migrationConfig) throws InvalidNamedJobException {
    //        if(migrationConfig == null)
    //            throw new InvalidNamedJobException("Invalid null migrationConfig");
    //        final Optional<NamedJob> jobO = getJobByName(name);
    //        if (!jobO.isPresent())
    //            throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //        final NamedJob job = jobO.get();
    //        job.setMigrationConfig(migrationConfig);
    //        try {
    //            storageProvider.updateNamedJob(job);
    //        } catch (IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        }
    //        MantisAuditLogWriter.getInstance()
    //            .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.NAMED_JOB_UPDATE, name,
    //            "user: " + user + " migrationConfig: " + migrationConfig.toString()));
    //    }
    //
    //    public String quickSubmit(String jobName, String user) throws InvalidNamedJobException, InvalidJobException {
    //        final Optional<NamedJob> jobO = getJobByName(jobName);
    //        if (!jobO.isPresent())
    //            throw new InvalidNamedJobException(jobName + " job cluster doesn't exist");
    //        return jobO.get().quickSubmit(user);
    //    }
    //
    //    // Resolve fields of the job definition:
    //    //   - if job parameters not specified, inherit from existing ones in name job
    //    //   - if new jar given, expect scheduling info as well, and use them, else inherit from existing name job
    //    //   - always use worker migration config from NamedJob
    //    // Return new object containing the above resolved fields.
    //    public MantisJobDefinition getResolvedJobDefinition(final MantisJobDefinition jobDefinition) throws InvalidNamedJobException {
    //        String version = jobDefinition.getVersion();
    //        Optional<NamedJob> namedJobO = getJobByName(jobDefinition.getName());
    //        if (!namedJobO.isPresent())
    //            throw new InvalidNamedJobException(jobDefinition.getName()+" job cluster doesn't exist");
    //        final NamedJob namedJob = namedJobO.get();
    //        List<Parameter> parameters = jobDefinition.getParameters();
    //        if(parameters==null || parameters.isEmpty())
    //            parameters = namedJob.getParameters();
    //
    //        List<Label> labels = jobDefinition.getLabels();
    //        if(labels == null || labels.isEmpty()) {
    //            labels = namedJob.getLabels();
    //        }
    //        NamedJob.Jar jar=null;
    //        SchedulingInfo schedulingInfo=null;
    //        if(jobDefinition.getJobJarFileLocation()!=null) {
    //            if(jobDefinition.getSchedulingInfo()==null)
    //                throw new InvalidNamedJobException("Scheduling info must be provided along with new job Jar");
    //            schedulingInfo = jobDefinition.getSchedulingInfo();
    //            if(version==null || version.isEmpty())
    //                version = ""+System.currentTimeMillis();
    //            jar = new NamedJob.Jar(jobDefinition.getJobJarFileLocation(), System.currentTimeMillis(), version,
    //                    jobDefinition.getSchedulingInfo());
    //            updateNamedJob(new NamedJobDefinition(
    //                            new MantisJobDefinition(
    //                                    namedJob.getName(), jobDefinition.getUser(), jar.getUrl(), version, parameters, null, 0,
    //                                    schedulingInfo, namedJob.getSla().getMin(), namedJob.getSla().getMax(),
    //                                    namedJob.getSla().getCronSpec(), namedJob.getSla().getCronPolicy(),
    //                                    jobDefinition.getIsReadyForJobMaster(), namedJob.getMigrationConfig(),labels
    //                            ),
    //                            namedJob.getOwner()
    //                    ),
    //                    false);
    //        }
    //        if(jar == null) {
    //            jar = namedJob.getJar(version);
    //            schedulingInfo = jobDefinition.getSchedulingInfo()==null?
    //                    cloneSchedulingInfo(jar.getSchedulingInfo()) :
    //                    getVerifiedSchedulingInfo(namedJob, jar, jobDefinition.getSchedulingInfo(), version);
    //        }
    //        if(jar == null)
    //            throw new InvalidNamedJobException(version+": no such versioned jar found for job cluster " + jobDefinition.getName());
    //        return new MantisJobDefinition(
    //                jobDefinition.getName(), jobDefinition.getUser(), jar.getUrl(), jar.getVersion(),
    //                parameters, jobDefinition.getJobSla(), jobDefinition.getSubscriptionTimeoutSecs(), schedulingInfo,
    //                namedJob.getSla().getMin(), namedJob.getSla().getMax(),
    //                namedJob.getSla().getCronSpec(), namedJob.getSla().getCronPolicy(), jobDefinition.getIsReadyForJobMaster(),
    //                namedJob.getMigrationConfig(),labels
    //        );
    //    }
    //
    //    private SchedulingInfo cloneSchedulingInfo(SchedulingInfo schedulingInfo) {
    //        // we basically need a "deep copy" of the scheduling info, trying to get it done easily via ObjectMapper since
    //        // we already have a reference to it.
    //        try {
    //            return mapper.readValue(mapper.writeValueAsString(schedulingInfo), SchedulingInfo.class);
    //        } catch (IOException e) {
    //            logger.warn("Unexpected: " + e.getMessage(), e);
    //            return null;
    //        }
    //    }
    //
    //    private SchedulingInfo getVerifiedSchedulingInfo(NamedJob namedJob, NamedJob.Jar jar, SchedulingInfo schedulingInfo, String version) throws InvalidNamedJobException {
    //        int givenNumStages = schedulingInfo.getStages().size();
    //        int existingNumStages = jar.getSchedulingInfo().getStages().size();
    //        if (namedJob.getIsReadyForJobMaster()) {
    //            if (schedulingInfo.forStage(0) != null)
    //                givenNumStages--; // decrement to get net numStages without job master
    //            if (jar.getSchedulingInfo().forStage(0) != null)
    //                existingNumStages--;
    //        }
    //        if(givenNumStages != existingNumStages)
    //            throw new InvalidNamedJobException("Mismatched scheduling info: expecting #stages=" +
    //                    existingNumStages + " for given jar version [" + version +
    //                    "], where as, given scheduling info has #stages=" + givenNumStages);
    //        return schedulingInfo;
    //    }
    //
    //    public JobIdForSubmit getJobIdForSubmit(String name, MantisJobDefinition jobDefinition) throws InvalidNamedJobException {
    //        final Optional<NamedJob> jobO = getJobByName(name);
    //        if (!jobO.isPresent()) {
    //            throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //        }
    //        final NamedJob job = jobO.get();
    //        try (AutoCloseable lock = job.obtainLock()) {
    //            if(job.getDisabled())
    //                throw new InvalidNamedJobException("Job " + name + " is disabled, submit disallowed");
    //            final String uniqueTag = getUniqueTag(jobDefinition.getJobSla().getUserProvidedType());
    //            final MantisJobMgr jobWithUniqueTag = job.getJobWithUniqueTag(uniqueTag);
    //            if(jobWithUniqueTag!=null && jobWithUniqueTag.markNewSubscriber()) {
    //                return new JobIdForSubmit(jobWithUniqueTag.getJobId(), false);
    //            }
    //            String jobId = NamedJob.getJobId(name, job.getNextJobNumber());
    //            storageProvider.updateNamedJob(job);
    //            return new JobIdForSubmit(jobId, true);
    //        }
    //        catch (IOException e) {
    //            throw new InvalidNamedJobException(e.getMessage(), e);
    //        }
    //        catch (InvalidNamedJobException e) {
    //            throw e;
    //        }
    //        catch (Exception e) {
    //            logLockError(e);
    //            throw new InvalidNamedJobException("Unexpected to not get next id for job cluster " + name);
    //        }
    //    }
    //
    //    static String getUniqueTag(String userProvidedType) {
    //        if(userProvidedType==null || userProvidedType.isEmpty())
    //            return null;
    //        try {
    //            JSONObject jsonObject = new JSONObject(userProvidedType);
    //            return jsonObject.optString(JobSla.uniqueTagName);
    //        }
    //        catch (Exception e) {
    //            return null;
    //        }
    //    }
    //
    //    public String getLatestJobId(String name) throws InvalidNamedJobException {
    //        final Optional<NamedJob> jobO = getJobByName(name);
    //        if (!jobO.isPresent()) {
    //            throw new InvalidNamedJobException(name+" job cluster doesn't exist");
    //        }
    //        final NamedJob job = jobO.get();
    //        if(!job.getIsActive())
    //            throw new InvalidNamedJobException(name+" job cluster not active");
    //        return NamedJob.getJobId(name, job.getLastJobCount());
    //    }
    //
    //    private void logLockError(Exception e) {
    //        logger.warn("Unexpected to not get a lock: " + e.getMessage(), e);
    //    }
    //
    //    public AutoCloseable lockJobName(String jobName) {
    //        return namedJobLock.obtainLock(jobName);
    //    }
    //
    //
}
