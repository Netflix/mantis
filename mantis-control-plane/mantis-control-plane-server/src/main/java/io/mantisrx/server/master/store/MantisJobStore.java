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

public final class MantisJobStore {

}
//
//import com.google.common.cache.Cache;
//import com.google.common.cache.CacheBuilder;
//import io.mantisrx.common.metrics.Gauge;
//import io.mantisrx.common.metrics.Metrics;
//import io.mantisrx.common.metrics.MetricsRegistry;
//import io.mantisrx.runtime.MantisJobDefinition;
//import io.mantisrx.runtime.MantisJobState;
//import io.mantisrx.runtime.descriptor.SchedulingInfo;
//import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
//import io.mantisrx.server.core.JobCompletedReason;
//import io.mantisrx.server.master.*;
//import io.mantisrx.server.master.config.ConfigurationProvider;
//import io.mantisrx.server.master.domain.WorkerRequest;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.functions.Func2;
//
//import java.io.IOException;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicReference;
//
//
//public final class MantisJobStore {
//
//    private class ArchivedWorkersCache {
//        private final Cache<String, ConcurrentMap<Integer, MantisWorkerMetadata>> cache;
//        ArchivedWorkersCache(int cacheSize) {
//            cache = CacheBuilder
//                    .newBuilder()
//                    .maximumSize(cacheSize)
//                    .build();
//        }
//        ConcurrentMap<Integer, MantisWorkerMetadata> getArchivedWorkerMap(final String jobId) throws ExecutionException {
//            return cache.get(jobId, () -> {
//                List<MantisWorkerMetadataWritable> workers = storageProvider.getArchivedWorkers(jobId);
//                ConcurrentMap<Integer, MantisWorkerMetadata> theMap = new ConcurrentHashMap<>();
//                if(workers != null) {
//                    for(MantisWorkerMetadata mwmd: workers) {
//                        theMap.putIfAbsent(mwmd.getWorkerNumber(), mwmd);
//                    }
//                }
//                return theMap;
//            });
//        }
//
//        void remove(String jobId) {
//            cache.invalidate(jobId);
//        }
//    }
//
//    private class ArchivedJobsMetadataCache {
//        private final Cache<String, MantisJobMetadata> cache;
//
//        ArchivedJobsMetadataCache(int cacheSize) {
//            cache = CacheBuilder
//                    .newBuilder()
//                    .maximumSize(cacheSize)
//                    .build();
//        }
//
//        MantisJobMetadata getJob(String jobId) {
//            try {
//                return cache.get(jobId, () -> loadArchivedJob(jobId));
//            } catch (ExecutionException e) {
//                return null;
//            }
//        }
//
//        private MantisJobMetadata loadArchivedJob(String jobId) throws IOException, InvalidJobException, ExecutionException {
//            final MantisJobMetadataWritable jobMetadata = storageProvider.loadArchivedJob(jobId);
//            if (jobMetadata == null)
//                throw new ExecutionException(new InvalidJobException(jobId));
//            return jobMetadata;
//        }
//
//        void add(MantisJobMetadataWritable job) {
//            cache.put(job.getJobId(), job);
//        }
//
//        void remove(String jobId) {
//            cache.invalidate(jobId);
//        }
//    }
//
//    private static class TerminatedJob implements Comparable<TerminatedJob> {
//        private final String jobId;
//        private final long terminatedTime;
//        private TerminatedJob(String jobId, long terminatedTime) {
//            this.jobId = jobId;
//            this.terminatedTime = terminatedTime;
//        }
//        @Override
//        public int compareTo(TerminatedJob o) {
//            return Long.compare(terminatedTime, o.terminatedTime);
//        }
//    }
//    public static final String JOB_DELETE = "MantisJobDelete";
//    private static final Logger logger = LoggerFactory.getLogger(MantisJobStore.class);
//    private final Func2<MantisJobStore, Map<String, MantisJobDefinition>, Collection<NamedJob>> jobsInitializer;
//    private final MantisStorageProvider storageProvider;
//    private final ConcurrentMap<String, MantisJobMetadataWritable> activeJobsMap;
//    private final ConcurrentMap<String, String> archivedJobIds;
//    private final ArchivedJobsMetadataCache archivedJobsMetadataCache;
//    private final ArchivedWorkersCache archivedWorkersCache;
//    private static final long DELETE_TERMINATED_JOBS_DELAY_SECS = 60;
//    private static final long maxInitTimeSecs = ConfigurationProvider.getConfig().getMasterInitTimeoutSecs();
//    private final PriorityBlockingQueue<TerminatedJob> terminatedJobsToDelete;
//    private final MantisJobOperations jobOps;
//    private static final String initMillisGaugeName="JobStoreInitMillis";
//    private static final String postInitMillisGaugeName="JobStorePostInitMillis";
//    private final Gauge initMillis;
//    private final Gauge postInitMillis;
//
//    public MantisJobStore(final MantisStorageProvider storageProvider,
//                          final Func2<MantisJobStore, Map<String, MantisJobDefinition>, Collection<NamedJob>> jobsInitializer,
//                          final MantisJobOperations jobOps) {
//        this.storageProvider = storageProvider;
//        this.jobsInitializer = jobsInitializer;
//        this.jobOps = jobOps;
//        activeJobsMap = new ConcurrentHashMap<>();
//        archivedJobIds = new ConcurrentHashMap<>();
//        archivedWorkersCache = new ArchivedWorkersCache(ConfigurationProvider.getConfig().getMaxArchivedJobsToCache());
//        archivedJobsMetadataCache = new ArchivedJobsMetadataCache(ConfigurationProvider.getConfig().getMaxArchivedJobsToCache());
//        terminatedJobsToDelete = new PriorityBlockingQueue<>();
//        Metrics m = new Metrics.Builder()
//                .name(MantisJobStore.class.getCanonicalName())
//                .addGauge(initMillisGaugeName)
//                .addGauge(postInitMillisGaugeName)
//                .build();
//        m = MetricsRegistry.getInstance().registerAndGet(m);
//        initMillis = m.getGauge(initMillisGaugeName);
//        postInitMillis = m.getGauge(postInitMillisGaugeName);
//        (new ScheduledThreadPoolExecutor(1)).scheduleWithFixedDelay(this::deleteOldTerminatedJobs,
//                DELETE_TERMINATED_JOBS_DELAY_SECS, DELETE_TERMINATED_JOBS_DELAY_SECS, TimeUnit.SECONDS);
//    }
//
//    private void deleteOldTerminatedJobs() {
//        final long tooOldCutOff = System.currentTimeMillis() - (getTerminatedJobToDeleteDelayHours()*3600000L);
//        while(true) {
//            try {
//                TerminatedJob jobToDelete = terminatedJobsToDelete.poll();
//                if (jobToDelete == null)
//                    return;
//                logger.info("terminateTime=" + jobToDelete.terminatedTime + ", cutOff=" + tooOldCutOff);
//                if (jobToDelete.terminatedTime > (tooOldCutOff)) {
//                    // job not ready to be deleted yet, add it back
//                    terminatedJobsToDelete.add(jobToDelete);
//                    return;
//                }
//                logger.info("Deleting old job " + jobToDelete.jobId);
//                try {
//                    // this will in turn call deleteJob(jobId) which will delete it from various maps that contain the job
//                    jobOps.deleteJob(jobToDelete.jobId);
//                } catch (IOException e) {
//                    logger.warn("Error deleting job \" + jobToDelete.jobId + \", will try again later:" + e.getMessage(), e);
//                    terminatedJobsToDelete.add(jobToDelete); // add it back
//                }
//            }
//            catch (Exception e) {
//                logger.error("Unexpected error deleting jobs: " + e.getMessage(), e);
//            }
//        }
//    }
//
//    private long getTerminatedJobToDeleteDelayHours() {
//        return ConfigurationProvider.getConfig().getTerminatedJobToDeleteDelayHours();
//    }
//
//    public MantisJobMetadata storeNewJob(WorkerJobDetails jobDetails) throws IOException, JobAlreadyExistsException {
//        logger.info("Storing job " + jobDetails.getJobId());
//        MantisJobMetadataWritable jobMetadata = new MantisJobMetadataWritable(jobDetails.getJobId(), jobDetails.getJobName(),
//                jobDetails.getUser(), System.currentTimeMillis(), jobDetails.getJobJarUrl(),
//                jobDetails.getRequest().getJobDefinition().getSchedulingInfo().getStages().size(),
//                jobDetails.getRequest().getJobDefinition().getJobSla(), MantisJobState.Accepted,
//                jobDetails.getRequest().getJobDefinition().getSubscriptionTimeoutSecs(),
//                jobDetails.getRequest().getJobDefinition().getParameters(), 1,
//                jobDetails.getRequest().getJobDefinition().getMigrationConfig(),
//                jobDetails.getRequest().getJobDefinition().getLabels());
//        storageProvider.storeNewJob(jobMetadata);
//        activeJobsMap.put(jobMetadata.getJobId(), jobMetadata);
//        return jobMetadata;
//    }
//
//    public MantisStorageProvider getStorageProvider() {
//        return storageProvider;
//    }
//
//    public void storeJobState(String jobId, MantisJobState state)
//            throws InvalidJobException, IOException, InvalidJobStateChangeException {
//        MantisJobMetadataWritable job = activeJobsMap.get(jobId);
//        if(job == null) {
//            throw new InvalidJobException(jobId);
//        }
//        job.setJobState(state);
//        storageProvider.updateJob(job);
//        if(MantisJobState.isTerminalState(state)) {
//            terminatedJobsToDelete.add(new TerminatedJob(jobId, System.currentTimeMillis()));
//            archiveJob(job);
//            archivedJobsMetadataCache.add(job);
//            activeJobsMap.remove(jobId);
//            archivedJobIds.put(jobId, jobId);
//            jobOps.terminateJob(jobId);
//        }
//    }
//
//    private void archiveJob(MantisJobMetadataWritable job) throws IOException {
//        storageProvider.archiveJob(job.getJobId());
//    }
//
//    public void storeJobNextWorkerNumber(String jobId, int n)
//            throws InvalidJobException, IOException {
//        MantisJobMetadataWritable job = activeJobsMap.get(jobId);
//        if(job == null) {
//            throw new InvalidJobException(jobId);
//        }
//        job.setNextWorkerNumberToUse(n);
//        storageProvider.updateJob(job);
//    }
//
//    public void deleteJob(String jobId) throws IOException, InvalidJobException {
//        storageProvider.deleteJob(jobId);
//        activeJobsMap.remove(jobId);
//        archivedJobIds.remove(jobId);
//        archivedJobsMetadataCache.remove(jobId);
//        archivedWorkersCache.remove(jobId);
//        MantisAuditLogWriter.getInstance()
//                .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.JOB_DELETE, jobId, ""));
//    }
//
//    public Collection<String> getTerminatedJobIds() {
//        return new LinkedList<>(archivedJobIds.keySet());
//    }
//
//    public List<? extends MantisWorkerMetadata> storeNewWorkers(List<WorkerRequest> workerRequests)
//            throws IOException, InvalidJobException {
//        if (workerRequests == null || workerRequests.isEmpty())
//            return null;
//        String jobId = workerRequests.get(0).getJobId();
//        logger.info("Adding " + workerRequests.size() + " workers for job " + jobId);
//        MantisJobMetadataWritable job = activeJobsMap.get(jobId);
//        if (job == null)
//            throw new InvalidJobException(jobId, -1, -1);
//        List<MantisWorkerMetadataWritable> addedWorkers = new ArrayList<>();
//        for (WorkerRequest workerRequest : workerRequests) {
//            if (job.getStageMetadata(workerRequest.getWorkerStage()) == null) {
//                MantisStageMetadataWritable msmd = new MantisStageMetadataWritable(workerRequest.getJobId(),
//                        workerRequest.getWorkerStage(), workerRequest.getTotalStages(), workerRequest.getDefinition(),
//                        workerRequest.getNumInstancesAtStage(), workerRequest.getHardConstraints(),
//                        workerRequest.getSoftConstraints(),
//                        workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalingPolicy(),
//                        workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalable());
//                boolean added = job.addJobStageIfAbsent(msmd);
//                if (added)
//                    storageProvider.storeMantisStage(msmd); // store the new
//            }
//            MantisWorkerMetadataWritable mwmd = new MantisWorkerMetadataWritable(workerRequest.getWorkerIndex(),
//                    workerRequest.getWorkerNumber(), workerRequest.getJobId(),
//                    workerRequest.getWorkerStage(), workerRequest.getNumPortsPerInstance());
//            if (!job.addWorkerMedata(workerRequest.getWorkerStage(), mwmd, null)) {
//                MantisWorkerMetadata tmp = job.getWorkerByIndex(workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
//                throw new InvalidJobException(job.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex(),
//                        new Exception("Couldn't add worker " + workerRequest.getWorkerNumber() + " as index " +
//                                workerRequest.getWorkerIndex() + ", that index already has worker " +
//                                tmp.getWorkerNumber()));
//            }
//            addedWorkers.add(mwmd);
//        }
//        storageProvider.storeWorkers(jobId, addedWorkers);
//        return addedWorkers;
//    }
//
//    public MantisWorkerMetadata storeNewWorker(WorkerRequest workerRequest)
//        throws IOException, InvalidJobException {
//        logger.info("Adding worker index=" + workerRequest.getWorkerIndex());
//        MantisJobMetadataWritable job = activeJobsMap.get(workerRequest.getJobId());
//        if(job == null)
//            throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
//        if(job.getStageMetadata(workerRequest.getWorkerStage()) == null) {
//            MantisStageMetadataWritable msmd = new MantisStageMetadataWritable(workerRequest.getJobId(),
//                    workerRequest.getWorkerStage(), workerRequest.getTotalStages(), workerRequest.getDefinition(),
//                    workerRequest.getNumInstancesAtStage(), workerRequest.getHardConstraints(), workerRequest.getSoftConstraints(),
//                    workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalingPolicy(),
//                    workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalable());
//            boolean added = job.addJobStageIfAbsent(msmd);
//            if(added)
//                storageProvider.storeMantisStage(msmd); // store the new
//        }
//        MantisWorkerMetadataWritable mwmd = new MantisWorkerMetadataWritable(workerRequest.getWorkerIndex(),
//                workerRequest.getWorkerNumber(), workerRequest.getJobId(),
//                workerRequest.getWorkerStage(), workerRequest.getNumPortsPerInstance());
//        if(!job.addWorkerMedata(workerRequest.getWorkerStage(), mwmd, null)) {
//            MantisWorkerMetadata tmp = job.getWorkerByIndex(workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
//            throw new InvalidJobException(job.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex(),
//                    new Exception("Couldn't add worker " + workerRequest.getWorkerNumber() + " as index " +
//                    workerRequest.getWorkerIndex() + ", that index already has worker " +
//                            tmp.getWorkerNumber()));
//        }
//        storageProvider.storeWorker(mwmd);
//        return mwmd;
//    }
//
//    public void updateStage(MantisStageMetadata msmd)
//        throws IOException, InvalidJobException {
//        MantisJobMetadataWritable job = activeJobsMap.get(msmd.getJobId());
//        if(job == null)
//            throw new InvalidJobException(msmd.getJobId(), msmd.getStageNum(), -1);
//        storageProvider.updateMantisStage((MantisStageMetadataWritable)msmd);
//    }
//
//    /**
//     * Atomically replace worker with new one created from the given worker request.
//     * @param workerRequest
//     * @param replacedWorker
//     * @return The newly created worker.
//     * @throws IOException Upon error from storage provider.
//     * @throws InvalidJobException If there is no such job or stage referred to in the worker metadata.
//     * @throws InvalidJobStateChangeException If the replaced worker's state cannot be changed. In which case no new
//     * worker is created.
//     */
//    public MantisWorkerMetadata replaceTerminatedWorker(WorkerRequest workerRequest, MantisWorkerMetadata replacedWorker)
//            throws IOException, InvalidJobException, InvalidJobStateChangeException {
//        logger.info("Replacing worker index=" + workerRequest.getWorkerIndex() + " number=" + replacedWorker.getWorkerNumber() +
//                " with number=" + workerRequest.getWorkerNumber());
//        MantisJobMetadataWritable job = activeJobsMap.get(workerRequest.getJobId());
//        if(job == null)
//            throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
//        if(job.getStageMetadata(workerRequest.getWorkerStage()) == null) {
//            throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getWorkerStage(), replacedWorker.getWorkerIndex());
//        }
//        if(!MantisJobState.isTerminalState(replacedWorker.getState()))
//            throw new InvalidJobStateChangeException(replacedWorker.getJobId(), replacedWorker.getState());
//        MantisWorkerMetadataWritable mwmd = new MantisWorkerMetadataWritable(workerRequest.getWorkerIndex(),
//                workerRequest.getWorkerNumber(), workerRequest.getJobId(),
//                workerRequest.getWorkerStage(), workerRequest.getNumPortsPerInstance());
//        mwmd.setResubmitInfo(replacedWorker.getWorkerNumber(), replacedWorker.getTotalResubmitCount() + 1);
//        if(!job.addWorkerMedata(replacedWorker.getStageNum(), mwmd, replacedWorker))
//            throw new InvalidJobStateChangeException(replacedWorker.getJobId(), replacedWorker.getState(), MantisJobState.Failed);
//        storageProvider.storeAndUpdateWorkers(mwmd, (MantisWorkerMetadataWritable) replacedWorker);
//        MantisStageMetadataWritable msmd = (MantisStageMetadataWritable) job.getStageMetadata(replacedWorker.getStageNum());
//        if(msmd.removeWorkerInErrorState(replacedWorker.getWorkerNumber()) != null)
//            archiveWorker((MantisWorkerMetadataWritable)replacedWorker);
//        return mwmd;
//    }
//
//    public void storeWorkerState(String jobId, int workerNumber, MantisJobState state)
//            throws InvalidJobException, InvalidJobStateChangeException, IOException {
//        this.storeWorkerState(jobId, workerNumber, state, null);
//    }
//
//    public void storeWorkerState(String jobId, int workerNumber, MantisJobState state, JobCompletedReason reason)
//            throws InvalidJobException, InvalidJobStateChangeException, IOException {
//        storeWorkerState(jobId, workerNumber, state, reason, true);
//    }
//
//    public void storeWorkerState(String jobId, int workerNumber, MantisJobState state, JobCompletedReason reason, boolean archiveIfError)
//            throws InvalidJobException, InvalidJobStateChangeException, IOException {
//        MantisWorkerMetadataWritable mwmd = (MantisWorkerMetadataWritable)getWorkerByNumber(jobId, workerNumber);
//        mwmd.setState(state, System.currentTimeMillis(), reason);
//        storageProvider.updateWorker(mwmd);
//        if(archiveIfError && MantisJobState.isErrorState(state)) {
//            final MantisJobMetadata activeJob = getActiveJob(jobId);
//            if (activeJob != null) {
//                MantisStageMetadataWritable msmd = (MantisStageMetadataWritable) activeJob.getStageMetadata(mwmd.getStageNum());
//                if(msmd.removeWorkerInErrorState(workerNumber) != null)
//                    archiveWorker(mwmd);
//            }
//        }
//    }
//
//    public void archiveWorker(MantisWorkerMetadataWritable mwmd) throws IOException {
//        storageProvider.archiveWorker(mwmd);
//        ConcurrentMap<Integer, MantisWorkerMetadata> workersMap = null;
//        try {
//            workersMap = archivedWorkersCache.getArchivedWorkerMap(mwmd.getJobId());
//        } catch (ExecutionException e) {
//            throw new IOException(e);
//        }
//        workersMap.putIfAbsent(mwmd.getWorkerNumber(), mwmd);
//    }
//
//    public List<? extends MantisWorkerMetadata> getArchivedWorkers(String jobId) throws IOException {
//        try {
//            return new ArrayList<>(archivedWorkersCache.getArchivedWorkerMap(jobId).values());
//        } catch (ExecutionException e) {
//            throw new IOException(e);
//        }
//    }
//
//    public MantisWorkerMetadata getArchivedWorker(String jobId, int workerNumber) throws IOException {
//        try {
//            return archivedWorkersCache.getArchivedWorkerMap(jobId).get(workerNumber);
//        } catch (ExecutionException e) {
//            throw new IOException(e);
//        }
//    }
//
//    private MantisWorkerMetadata getWorkerByNumber(String jobId, int workerNumber) throws InvalidJobException {
//        MantisJobMetadata mjmd = activeJobsMap.get(jobId);
//        if(mjmd == null)
//            throw new InvalidJobException(jobId);
//        return mjmd.getWorkerByNumber(workerNumber);
//    }
//
//    /**
//     * Get the job metadata object for the given Id.
//     * @param jobId Job ID.
//     * @return Job object if it exists, null otherwise.
//     */
//    public MantisJobMetadata getActiveJob(final String jobId) {
//        final MantisJobMetadataWritable mjmd = activeJobsMap.get(jobId);
//        if (mjmd == null) {
//            logger.info("activeJobsMap found no job for job ID {}", jobId);
//        }
//        return mjmd;
//    }
//
//    public MantisJobMetadata getCompletedJob(final String jobId) throws IOException {
//        final MantisJobMetadata job = archivedJobsMetadataCache.getJob(jobId);
//        if (job == null) {
//            logger.info("archivedJobsMetadataCache found no job for job ID {}", jobId);
//        }
//        return job;
//    }
//
//    /**
//     * Get all workers of a stage for a given job. Generally, this is expected to return quickly, for
//     * example, by looking up a cache if possible.
//     * @param jobId
//     * @param stageNum
//     * @return
//     * @throws InvalidJobException
//     * @throws IOException
//     */
//    final public Collection<MantisWorkerMetadata> getWorkers(String jobId, int stageNum)
//        throws InvalidJobException, IOException {
//        return activeJobsMap.get(jobId).getStageMetadata(stageNum).getWorkerByIndexMetadataSet();
//    }
//
//    private void archiveWorkers(MantisJobMetadataWritable mjmd) throws IOException {
//        for(MantisStageMetadata msmd: mjmd.getStageMetadata()) {
//            for(MantisWorkerMetadataWritable removedWorker:
//                    ((MantisStageMetadataWritable)msmd).removeArchiveableWorkers()) {
//                archiveWorker(removedWorker);
//            }
//        }
//    }
//
//    static SchedulingInfo getSchedulingInfo(MantisJobMetadata mjmd) {
//        int numStages = mjmd.getNumStages();
//        logger.info("numStages=" + numStages);
//        Map<Integer, StageSchedulingInfo> stagesMap = new HashMap<>();
//        for (MantisStageMetadata stageMetadata: mjmd.getStageMetadata()) {
//            stagesMap.put(stageMetadata.getStageNum(), new StageSchedulingInfo(stageMetadata.getNumWorkers(),
//                    stageMetadata.getMachineDefinition(), stageMetadata.getHardConstraints(),
//                    stageMetadata.getSoftConstraints(), stageMetadata.getScalingPolicy(),
//                    stageMetadata.getScalable()));
//        }
//        return new SchedulingInfo(stagesMap);
//    }
//
//    final public void start() {
//        logger.info("Mantis store starting now");
//        final CountDownLatch latch = new CountDownLatch(1);
//        final List<MantisJobMetadataWritable> jobsToArchive = new LinkedList<>();
//        final AtomicReference<Collection<NamedJob>> ref = new AtomicReference<>();
//        new Thread() {
//            @Override
//            public void run() {
//                long st = System.currentTimeMillis();
//                try {
//                    for(MantisJobMetadataWritable mjmd: storageProvider.initJobs()) {
//                        archiveWorkers(mjmd);
//                        if(MantisJobState.isTerminalState(mjmd.getState())) {
//                            terminatedJobsToDelete.add(new TerminatedJob(mjmd.getJobId(), getTerminatedAt(mjmd)));
//                            jobsToArchive.add(mjmd);
//                        }
//                        else
//                            activeJobsMap.put(mjmd.getJobId(), mjmd);
//                    }
//                    logger.info("Read " + activeJobsMap.size() + " job records from persistence in " + (System.currentTimeMillis() - st) + " ms");
//                    if(jobsInitializer != null) {
//                        Map<String, MantisJobDefinition> jobDefsMap = new HashMap<>();
//                        for(MantisJobMetadata mjmd: activeJobsMap.values()) {
//                            // we derive the value for being ready for job master by looking to see if stage 0 was
//                            // created when this job was created.
//                            final boolean isReadyForJobMaster = mjmd.getStageMetadata(0) != null;
//                            jobDefsMap.put(mjmd.getJobId(), new MantisJobDefinition(mjmd.getName(), mjmd.getUser(), mjmd.getJarUrl(), "",
//                                    mjmd.getParameters(), mjmd.getSla(), mjmd.getSubscriptionTimeoutSecs(), getSchedulingInfo(mjmd),
//                                    0, 0, null, null, isReadyForJobMaster, mjmd.getMigrationConfig(),mjmd.getLabels())); // min/max and cron spec don't matter for job instance
//                        }
//                        ref.set(jobsInitializer.call(MantisJobStore.this, jobDefsMap));
//                    }
//                } catch (IOException e) {
//                    logger.error(
//                            String.format("Exiting due to storage init failure: %s: ", e.getMessage()),
//                            e
//                    );
//                    System.exit(1); // can't deal with storage error
//                }
//                initMillis.set(System.currentTimeMillis() - st);
//                latch.countDown();
//            }
//        }
//                .start();
//        long until = System.currentTimeMillis() + maxInitTimeSecs*1000L;
//        while(System.currentTimeMillis() < until) {
//            try {
//                if(!latch.await(until - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
//                    logger.error("Timed out waiting for initialization after " + maxInitTimeSecs + " secs, committing suicide");
//                    System.exit(3);
//                }
//                break;
//            } catch (InterruptedException e) {
//                logger.warn("Interrupted waiting for initialization");
//            }
//        }
//        // asynchronously start post init
//        new Thread() {
//            @Override
//            public void run() {
//                doPostInit(jobsToArchive, ref.get());
//            }
//        }.start();
//    }
//
//    private long getTerminatedAt(MantisJobMetadata mjmd) {
//        long terminatedAt = mjmd.getSubmittedAt();
//        for(MantisStageMetadata msmd: mjmd.getStageMetadata()) {
//            for(MantisWorkerMetadata mwmd: msmd.getAllWorkers()) {
//                terminatedAt = Math.max(terminatedAt, mwmd.getCompletedAt());
//            }
//        }
//        return terminatedAt;
//    }
//
//    private void doPostInit(List<MantisJobMetadataWritable> jobsToArchive, Collection<NamedJob> namedJobs) {
//        long start1 = System.currentTimeMillis();
//        final AtomicInteger count = new AtomicInteger();
//        storageProvider.initArchivedJobs()
//                .doOnNext(job -> {
//                    archivedJobsMetadataCache.add((MantisJobMetadataWritable) job);
//                    archivedJobIds.put(job.getJobId(), job.getJobId());
//                    terminatedJobsToDelete.add(new TerminatedJob(job.getJobId(), getTerminatedAt(job)));
//                    count.incrementAndGet();
//                })
//                .toBlocking()
//                .lastOrDefault(null);
//        logger.info("Read " + count.get() + " archived job records from persistence in " + (System.currentTimeMillis() - start1) + " ms");
//        if (!jobsToArchive.isEmpty()) {
//            long start2 = System.currentTimeMillis();
//            Map<String, NamedJob> namedJobMap = new HashMap<>();
//            if (!namedJobs.isEmpty()) {
//                for (NamedJob nj: namedJobs)
//                    namedJobMap.put(nj.getName(), nj);
//            }
//            for (MantisJobMetadataWritable job: jobsToArchive) {
//                try {
//                    archiveJob(job);
//                    final NamedJob namedJob = namedJobMap.get(job.getName());
//                    if (namedJob != null)
//                        namedJob.initCompletedJob(new NamedJob.CompletedJob(job.getName(), job.getJobId(),
//                                null, job.getState(), job.getSubmittedAt(), getTerminatedAt(job), job.getUser(),namedJob.getLabels()));
//                } catch (IOException e) {
//                    logger.error("Error archiving job " + job.getJobId() + ": " + e.getMessage(), e);
//                }
//            }
//            logger.info("Moved " + jobsToArchive.size() + " completed jobs to archived storage in " + (System.currentTimeMillis() - start2) + " ms");
//        }
//        postInitMillis.set(System.currentTimeMillis() - start1);
//    }
//
//    public final void shutdown() {
//        activeJobsMap.clear();
//        storageProvider.shutdown();
//    }
//}
