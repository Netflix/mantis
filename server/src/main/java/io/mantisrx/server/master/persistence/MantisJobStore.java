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

package io.mantisrx.server.master.persistence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.MantisStageMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;


public class MantisJobStore {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobStore.class);
    private final IMantisStorageProvider storageProvider;

    private final ConcurrentMap<String, String> archivedJobIds;
    private final ArchivedJobsMetadataCache archivedJobsMetadataCache;
    private final ArchivedWorkersCache archivedWorkersCache;
    private final PriorityBlockingQueue<TerminatedJob> terminatedJobsToDelete;

    public MantisJobStore(IMantisStorageProvider storageProvider) {
        this.storageProvider = storageProvider;

        archivedJobIds = new ConcurrentHashMap<>();
        archivedWorkersCache = new ArchivedWorkersCache(ConfigurationProvider.getConfig().getMaxArchivedJobsToCache());
        archivedJobsMetadataCache = new ArchivedJobsMetadataCache(ConfigurationProvider.getConfig().getMaxArchivedJobsToCache());
        terminatedJobsToDelete = new PriorityBlockingQueue<>();

    }

    public void loadAllArchivedJobsAsync() {
        logger.info("Beginning load of Archived Jobs");
        storageProvider.loadAllArchivedJobs()
                .subscribeOn(Schedulers.io())
                .subscribe((job) -> {
                    archivedJobsMetadataCache.add(job);
                    archivedJobIds.put(job.getJobId().getId(), job.getJobId().getId());
                    terminatedJobsToDelete.add(new TerminatedJob(job.getJobId().getId(), getTerminatedAt(job)));
                }, (e) -> {
                    logger.warn("Exception loading archived Jobs", e);
                }, () -> {
                    logger.info("Finished Loading all archived Jobs!");
                });
    }


    private long getTerminatedAt(IMantisJobMetadata mjmd) {
        long terminatedAt = mjmd.getSubmittedAtInstant().toEpochMilli();
        for (IMantisStageMetadata msmd : mjmd.getStageMetadata().values()) {
            for (JobWorker mwmd : msmd.getAllWorkers()) {
                terminatedAt = Math.max(terminatedAt, mwmd.getMetadata().getCompletedAt());
            }
        }
        return terminatedAt;
    }

    public List<IJobClusterMetadata> loadAllJobClusters() throws IOException {
        if (logger.isTraceEnabled()) {logger.trace("Loading all job clusters"); }
        List<IJobClusterMetadata> iJobClusterMetadataList = storageProvider.loadAllJobClusters();
        logger.info("Loaded {} job clusters", iJobClusterMetadataList.size());
        return iJobClusterMetadataList;
    }

    public List<IMantisJobMetadata> loadAllActiveJobs() throws IOException {
        if (logger.isTraceEnabled()) {logger.trace("Loading all active jobs"); }
        List<IMantisJobMetadata> mantisJobMetadataList = storageProvider.loadAllJobs();
        logger.info("Loaded {} active jobs", mantisJobMetadataList.size());
        return mantisJobMetadataList;
    }

    public List<CompletedJob> loadAllCompletedJobs() throws IOException {
        if (logger.isTraceEnabled()) {logger.trace("Loading all completed jobs"); }
        List<CompletedJob> completedJobs = storageProvider.loadAllCompletedJobs();
        logger.info("Loaded {} completed jobs", completedJobs.size());
        return completedJobs;
    }

    public void createJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        if (logger.isTraceEnabled()) {logger.trace("Creating Job Cluster {}", jobCluster); }
        storageProvider.createJobCluster(jobCluster);
        if (logger.isTraceEnabled()) {
            logger.trace("Created Job Cluster {}", jobCluster.getJobClusterDefinition().getName());
        }
    }

    public void updateJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        if (logger.isTraceEnabled()) {logger.trace("Updating Job Cluster {}", jobCluster); }
        storageProvider.updateJobCluster(jobCluster);
        if (logger.isTraceEnabled()) {
            logger.trace("Updated Job Cluster {}", jobCluster.getJobClusterDefinition().getName());
        }
    }

    public void deleteJobCluster(String name) throws Exception {
        if (logger.isTraceEnabled()) {logger.trace("Deleting Job Cluster {}", name); }
        storageProvider.deleteJobCluster(name);
        if (logger.isTraceEnabled()) {logger.trace("Deleted Job Cluster {}", name); }

    }

    public void deleteJob(String jobId) throws Exception {
        if (logger.isTraceEnabled()) {logger.trace("Deleting Job  {}", jobId); }
        archivedJobsMetadataCache.remove(jobId);
        archivedWorkersCache.remove(jobId);
        storageProvider.deleteJob(jobId);
        if (logger.isTraceEnabled()) {logger.trace("Deleted Job  {}", jobId); }

    }

    public void deleteCompletedJob(String clusterName, String jobId) throws IOException {
        if (logger.isTraceEnabled()) {logger.trace("Deleting completed Job  {}", jobId); }
        storageProvider.removeCompletedJobForCluster(clusterName, jobId);
        if (logger.isTraceEnabled()) {logger.trace("Deleted completed job {}", jobId); }
    }

    public void storeCompletedJobForCluster(String name, CompletedJob completedJob) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Storing completed Job for cluster {}", completedJob);}
        storageProvider.storeCompletedJobForCluster(name, completedJob);
        if (logger.isTraceEnabled()) { logger.trace("Stored completed Job for cluster {}", completedJob);}
    }

    public void storeNewJob(IMantisJobMetadata jobMetadata) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Storing new Job{}", jobMetadata);}
        storageProvider.storeNewJob(jobMetadata);
        if (logger.isTraceEnabled()) { logger.trace("Stored new Job {}", jobMetadata);}

    }

    public void replaceTerminatedWorker(IMantisWorkerMetadata oldWorker, IMantisWorkerMetadata replacement) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Replace terminated worker  {} with new worker {}", oldWorker, replacement);
        }
        storageProvider.storeAndUpdateWorkers(oldWorker, replacement);
        if (logger.isTraceEnabled()) { logger.trace("Replaced terminated worker {}", oldWorker);}

    }

    public void updateJob(final IMantisJobMetadata jobMetadata) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Update Job {}", jobMetadata);}
        storageProvider.updateJob(jobMetadata);
        if (logger.isTraceEnabled()) { logger.trace("Updated Job {}", jobMetadata);}
    }

    public void updateStage(IMantisStageMetadata stageMeta) throws IOException {
        storageProvider.updateMantisStage(stageMeta);
    }

    public List<? extends IMantisWorkerMetadata> storeNewWorkers(IMantisJobMetadata job, List<IMantisWorkerMetadata> workerRequests)
            throws IOException, InvalidJobException {
        if (logger.isTraceEnabled()) { logger.trace("Storing new workers for  Job {} ", job);}
        if (workerRequests == null || workerRequests.isEmpty())
            return null;
        String jobId = workerRequests.get(0).getJobId();
        if (logger.isDebugEnabled()) { logger.debug("Adding " + workerRequests.size() + " workers for job " + jobId); }

        List<IMantisWorkerMetadata> addedWorkers = new ArrayList<>();
        List<Integer> savedStageList = Lists.newArrayList();
        for (IMantisWorkerMetadata workerRequest : workerRequests) {
            // store stage if not stored already
            if (!savedStageList.contains(workerRequest.getStageNum())) {
                Optional<IMantisStageMetadata> stageMetadata = job.getStageMetadata(workerRequest.getStageNum());
                if (stageMetadata.isPresent()) {
                    storageProvider.storeMantisStage(stageMetadata.get());
                } else {
                    throw new RuntimeException(String.format("No such stage {}", workerRequest.getStageNum()));
                }
                savedStageList.add(workerRequest.getStageNum());
            }

            addedWorkers.add(workerRequest);
        }
        storageProvider.storeWorkers(jobId, addedWorkers);
        if (logger.isTraceEnabled()) { logger.trace("Stored new workers for Job {}", addedWorkers);}
        return addedWorkers;
    }

    public void storeNewWorker(IMantisWorkerMetadata workerRequest)
            throws IOException, InvalidJobException {
        if (logger.isTraceEnabled()) { logger.trace("Adding worker index=" + workerRequest.getWorkerIndex()); }

        //if(job == null)
        //    throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getStageNum(), workerRequest.getWorkerIndex());
        //	        if(job.getStageMetadata(workerRequest.getWorkerStage()) == null) {
        //	            IMantisStageMetadata msmd = new MantisStageMetadataImpl.Builder().from(workerRequest).build();
        //	            boolean added = job.addJobStageIfAbsent(msmd);
        //	            if(added)
        //	                storageProvider.storeMantisStage(msmd); // store the new
        //	        }
        //	        IMantisWorkerMetadata mwmd = new MantisWorkerMetadataImpl.Builder().from(workerRequest).build();
        //	        if(!job.addWorkerMetadata(workerRequest.getWorkerStage(), mwmd, null)) {
        //	            IMantisWorkerMetadata tmp = job.getWorkerByIndex(workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
        //	            throw new InvalidJobException(job.getJobId().getId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex(),
        //	                    new Exception("Couldn't add worker " + workerRequest.getWorkerNumber() + " as index " +
        //	                    workerRequest.getWorkerIndex() + ", that index already has worker " +
        //	                            tmp.getWorkerNumber()));
        //	        }
        storageProvider.storeWorker(workerRequest);

    }

    public void updateWorker(IMantisWorkerMetadata worker) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Updating worker index=" + worker.getWorkerIndex()); }
        storageProvider.updateWorker(worker);
        if (logger.isTraceEnabled()) { logger.trace("Updated worker index=" + worker.getWorkerIndex()); }
        // make archive explicit
        //        if(archiveIfError && WorkerState.isErrorState(worker.getState())) {
        //            archiveWorker(worker);
        //        }
    }

    private void archiveWorkersIfAny(IMantisJobMetadata mjmd) throws IOException {
        for (IMantisStageMetadata msmd : mjmd.getStageMetadata().values()) {
            for (JobWorker removedWorker :
                    ((MantisStageMetadataImpl) msmd).removeArchiveableWorkers()) {
                archiveWorker(removedWorker.getMetadata());
            }
        }
    }

    public void archiveWorker(IMantisWorkerMetadata worker) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Archiving worker index=" + worker.getWorkerIndex()); }
        storageProvider.archiveWorker(worker);
        ConcurrentMap<Integer, IMantisWorkerMetadata> workersMap = null;
        try {
            workersMap = archivedWorkersCache.getArchivedWorkerMap(worker.getJobId());
            workersMap.putIfAbsent(worker.getWorkerNumber(), worker);
        } catch (ExecutionException e) {
            logger.warn("Error adding worker to archived cache {}", e);
        }
        if (logger.isTraceEnabled()) { logger.trace("Archived worker index=" + worker.getWorkerIndex()); }

    }

    public Optional<IMantisJobMetadata> getArchivedJob(final String jobId) {
        if (logger.isTraceEnabled()) { logger.trace("Get Archived Job {}", jobId);}
        final Optional<IMantisJobMetadata> jobOp = Optional.ofNullable(archivedJobsMetadataCache.getJob(jobId));
        if (!jobOp.isPresent()) {
            logger.error("archivedJobsMetadataCache found no job for job ID {}", jobId);
        }
        if (logger.isTraceEnabled()) { logger.trace("Got archived Job {}", jobOp);}
        return jobOp;
    }

    public void archiveJob(IMantisJobMetadata job) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Archiving Job {}", job);}
        archivedJobsMetadataCache.add(job);
        storageProvider.archiveJob(job.getJobId().getId());
        if (logger.isTraceEnabled()) { logger.trace("Archived Job {}", job.getJobId());}
    }

    /**
     * @param jobId
     * @param workerNumber
     *
     * @return
     */
    public Optional<IMantisWorkerMetadata> getArchivedWorker(String jobId, int workerNumber) {
        try {
            ConcurrentMap<Integer, IMantisWorkerMetadata> workersMap = archivedWorkersCache.getArchivedWorkerMap(jobId);
            if (workersMap != null) {
                return Optional.ofNullable(workersMap.get(workerNumber));
            }
        } catch (ExecutionException e) {
            logger.warn("Exception getting archived Worker {}", e);
        }

        return Optional.empty();
    }

    public List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Getting Archived workers for Job {}", jobId);}
        List archivedWorkers = new ArrayList<>(archivedWorkersCache.getArchivedWorkerMap(jobId).values());
        if (logger.isTraceEnabled()) {
            logger.trace("Fetched archived {} workers for Job  {}", archivedWorkers.size(), jobId);
        }
        return archivedWorkers;


    }

    private static class TerminatedJob implements Comparable<TerminatedJob> {

        private final String jobId;
        private final long terminatedTime;

        private TerminatedJob(String jobId, long terminatedTime) {
            this.jobId = jobId;
            this.terminatedTime = terminatedTime;
        }

        @Override
        public int compareTo(TerminatedJob o) {
            return Long.compare(terminatedTime, o.terminatedTime);
        }
    }

    private class ArchivedWorkersCache {

        private final Cache<String, ConcurrentMap<Integer, IMantisWorkerMetadata>> cache;

        ArchivedWorkersCache(int cacheSize) {
            cache = CacheBuilder
                    .newBuilder()
                    .maximumSize(cacheSize)
                    .build();
        }

        ConcurrentMap<Integer, IMantisWorkerMetadata> getArchivedWorkerMap(final String jobId) throws ExecutionException {
            return cache.get(jobId, () -> {
                List<IMantisWorkerMetadata> workers = storageProvider.getArchivedWorkers(jobId);
                ConcurrentMap<Integer, IMantisWorkerMetadata> theMap = new ConcurrentHashMap<>();
                if (workers != null) {
                    for (IMantisWorkerMetadata mwmd : workers) {
                        theMap.putIfAbsent(mwmd.getWorkerNumber(), mwmd);
                    }
                }
                return theMap;
            });
        }

        void remove(String jobId) {
            cache.invalidate(jobId);
        }
    }

    private class ArchivedJobsMetadataCache {

        private final Cache<String, IMantisJobMetadata> cache;

        ArchivedJobsMetadataCache(int cacheSize) {
            cache = CacheBuilder
                    .newBuilder()
                    .maximumSize(cacheSize)
                    .build();
        }

        IMantisJobMetadata getJob(String jobId) {
            try {
                return cache.get(jobId, () -> loadArchivedJob(jobId));
            } catch (ExecutionException e) {
                return null;
            }
        }

        private IMantisJobMetadata loadArchivedJob(String jobId) throws IOException, InvalidJobException, ExecutionException {
            if (logger.isTraceEnabled()) {logger.trace("Loading archived job {}", jobId);}
            final Optional<IMantisJobMetadata> jobMetadata = storageProvider.loadArchivedJob(jobId);
            if (!jobMetadata.isPresent())
                throw new ExecutionException(new InvalidJobException(jobId));
            if (logger.isTraceEnabled()) {logger.trace("Loaded archived job {}", jobMetadata);}
            return jobMetadata.get();
        }

        void add(IMantisJobMetadata job) {
            cache.put(job.getJobId().getId(), job);
        }

        void remove(String jobId) {
            cache.invalidate(jobId);
        }
    }


}
