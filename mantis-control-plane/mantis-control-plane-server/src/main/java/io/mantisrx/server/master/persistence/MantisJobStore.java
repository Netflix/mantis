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

import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.MantisStageMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.JobArtifact;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;


public class MantisJobStore {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobStore.class);
    private final IMantisPersistenceProvider storageProvider;

    private final ConcurrentMap<String, String> archivedJobIds;
    private final ArchivedJobsMetadataCache archivedJobsMetadataCache;
    private final ArchivedWorkersCache archivedWorkersCache;
    private final PriorityBlockingQueue<TerminatedJob> terminatedJobsToDelete;

    public MantisJobStore(IMantisPersistenceProvider storageProvider) {
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
                },
                    (e) -> logger.warn("Exception loading archived Jobs", e),
                    () -> logger.info("Finished Loading all archived Jobs!"));
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
        List<IJobClusterMetadata> iJobClusterMetadataList = storageProvider.loadAllJobClusters();
        logger.info("Loaded {} job clusters", iJobClusterMetadataList.size());
        return iJobClusterMetadataList;
    }

    public List<IMantisJobMetadata> loadAllActiveJobs() throws IOException {
        List<IMantisJobMetadata> mantisJobMetadataList = storageProvider.loadAllJobs();
        logger.info("Loaded {} active jobs", mantisJobMetadataList.size());
        return mantisJobMetadataList;
    }

    public List<CompletedJob> loadCompletedJobsForCluster(String clusterName, int limit, @Nullable JobId startJobIdExclusive) throws IOException {
        return storageProvider.loadLatestCompletedJobsForCluster(clusterName, limit, startJobIdExclusive);
    }

    public void deleteCompletedJobsForCluster(String clusterName) throws IOException {
        storageProvider.deleteCompletedJobsForCluster(clusterName);
    }

    public void createJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        storageProvider.createJobCluster(jobCluster);
    }

    public void updateJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        storageProvider.updateJobCluster(jobCluster);
    }

    public void deleteJobCluster(String name) throws Exception {
        storageProvider.deleteJobCluster(name);
    }

    public void deleteJob(String jobId) throws IOException {
        archivedJobsMetadataCache.remove(jobId);
        archivedWorkersCache.remove(jobId);
        storageProvider.deleteJob(jobId);
    }

    public void storeCompletedJobForCluster(String name, CompletedJob completedJob) throws IOException {
        storageProvider.storeCompletedJobForCluster(name, completedJob);
    }

    public void storeNewJob(IMantisJobMetadata jobMetadata) throws Exception {
        storageProvider.storeNewJob(jobMetadata);

    }

    public TaskExecutorRegistration getTaskExecutor(TaskExecutorID taskExecutorID) throws IOException {
        return storageProvider.getTaskExecutorFor(taskExecutorID);
    }

    public void storeNewTaskExecutor(TaskExecutorRegistration registration) throws IOException {
        storageProvider.storeNewTaskExecutor(registration);
    }

    public void storeNewDisabledTaskExecutorsRequest(DisableTaskExecutorsRequest request) throws IOException {
        storageProvider.storeNewDisableTaskExecutorRequest(request);
    }

    public void deleteExpiredDisableTaskExecutorsRequest(DisableTaskExecutorsRequest request) throws IOException {
        storageProvider.deleteExpiredDisableTaskExecutorRequest(request);
    }

    public List<DisableTaskExecutorsRequest> loadAllDisableTaskExecutorsRequests(ClusterID clusterID) throws IOException {
        return storageProvider.loadAllDisableTaskExecutorsRequests(clusterID);
    }

    public void replaceTerminatedWorker(IMantisWorkerMetadata oldWorker, IMantisWorkerMetadata replacement) throws Exception {
        storageProvider.storeAndUpdateWorkers(oldWorker, replacement);
    }

    public void updateJob(final IMantisJobMetadata jobMetadata) throws Exception {
        storageProvider.updateJob(jobMetadata);
    }

    public void updateStage(IMantisStageMetadata stageMeta) throws IOException {
        storageProvider.updateMantisStage(stageMeta);
    }

    public List<? extends IMantisWorkerMetadata> storeNewWorkers(IMantisJobMetadata job, List<IMantisWorkerMetadata> workerRequests)
            throws IOException, InvalidJobException {
        if (workerRequests == null || workerRequests.isEmpty())
            return null;
        String jobId = workerRequests.get(0).getJobId();
        logger.debug("Adding {} workers for job {}", workerRequests.size(), jobId);

        List<IMantisWorkerMetadata> addedWorkers = new ArrayList<>();
        List<Integer> savedStageList = Lists.newArrayList();
        for (IMantisWorkerMetadata workerRequest : workerRequests) {
            // store stage if not stored already
            if (!savedStageList.contains(workerRequest.getStageNum())) {
                Optional<IMantisStageMetadata> stageMetadata = job.getStageMetadata(workerRequest.getStageNum());
                if (stageMetadata.isPresent()) {
                    storageProvider.storeMantisStage(stageMetadata.get());
                } else {
                    throw new RuntimeException(String.format("No such stage %d", workerRequest.getStageNum()));
                }
                savedStageList.add(workerRequest.getStageNum());
            }

            addedWorkers.add(workerRequest);
        }
        storageProvider.storeWorkers(jobId, addedWorkers);
        return addedWorkers;
    }

    public void storeNewWorker(IMantisWorkerMetadata workerRequest)
            throws IOException, InvalidJobException {
        storageProvider.storeWorker(workerRequest);
    }

    public void updateWorker(IMantisWorkerMetadata worker) throws IOException {
        storageProvider.updateWorker(worker);
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
        storageProvider.archiveWorker(worker);
        ConcurrentMap<Integer, IMantisWorkerMetadata> workersMap;
        try {
            workersMap = archivedWorkersCache.getArchivedWorkerMap(worker.getJobId());
            workersMap.putIfAbsent(worker.getWorkerNumber(), worker);
        } catch (ExecutionException e) {
            logger.warn("Error adding worker to archived cache", e);
        }

    }

    public Optional<IMantisJobMetadata> getArchivedJob(final String jobId) {
        final Optional<IMantisJobMetadata> jobOp = Optional.ofNullable(archivedJobsMetadataCache.getJob(jobId));
        if (!jobOp.isPresent()) {
            logger.debug("archivedJobsMetadataCache found no job for job ID {}", jobId);
        }
        return jobOp;
    }

    public void archiveJob(IMantisJobMetadata job) throws IOException {
        archivedJobsMetadataCache.add(job);
        storageProvider.archiveJob(job.getJobId().getId());
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
            logger.warn("Exception getting archived worker", e);
        }

        return Optional.empty();
    }

    public List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) throws Exception {
        return ImmutableList.copyOf(archivedWorkersCache.getArchivedWorkerMap(jobId).values());
    }

    public void addNewJobArtifactsToCache(ClusterID clusterID, List<ArtifactID> artifacts) throws IOException {
        storageProvider.addNewJobArtifactsToCache(clusterID, artifacts);
    }

    public void removeJobArtifactsToCache(ClusterID clusterID, List<ArtifactID> artifacts) throws IOException {
        storageProvider.removeJobArtifactsToCache(clusterID, artifacts);
    }

    public List<String> getJobArtifactsToCache(ClusterID clusterID) throws IOException {
        return storageProvider.listJobArtifactsToCache(clusterID);
    }

    public JobArtifact getJobArtifact(ArtifactID artifactID) throws IOException {
        return storageProvider.getArtifactById(artifactID.getResourceID());
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

        private final Cache<String, Optional<IMantisJobMetadata>> cache;

        ArchivedJobsMetadataCache(int cacheSize) {
            cache = CacheBuilder
                    .newBuilder()
                    .maximumSize(cacheSize)
                    .build();
        }

        IMantisJobMetadata getJob(String jobId) {
            try {
                final Optional<IMantisJobMetadata> jobMetadata = cache.get(jobId, () -> loadArchivedJobImpl(jobId));
                return jobMetadata.orElse(null);
            } catch (Exception e) {
                return null;
            }
        }

        private Optional<IMantisJobMetadata> loadArchivedJobImpl(String jobId) throws IOException, ExecutionException {
            final Optional<IMantisJobMetadata> jobMetadata = storageProvider.loadArchivedJob(jobId);
            if (!jobMetadata.isPresent()) {
                logger.warn("Failed to load archived job {}. No job found!", jobId);
            }
            return jobMetadata;
        }

        void add(IMantisJobMetadata job) {
            cache.put(job.getJobId().getId(), Optional.ofNullable(job));
        }

        void remove(String jobId) {
            cache.invalidate(jobId);
        }
    }


}
