/*
 * Copyright 2023 Netflix, Inc.
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

import static java.util.Optional.of;

import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.JobClusterActor.LabelCache;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidates all processing of completed jobs
 */
class CompletedJobCache {

    private final Logger logger = LoggerFactory.getLogger(CompletedJobCache.class);

    // Set of sorted terminal jobs
    private final Set<CompletedJob> terminalSortedJobSet = new TreeSet<>((o1, o2) -> {
        if (o1.getTerminatedAt() < o2.getTerminatedAt()) {
            return 1;
        } else if (o1.getTerminatedAt() > o2.getTerminatedAt()) {
            return -1;
        } else {
            return 0;
        }
    });

    // cluster name
    private final String name;
    // Map of completed jobs
//        Make this bounded
    private final Map<JobId, CompletedJob> completedJobs = new HashMap<>();
    // Map of jobmetadata
    private final Map<JobId, IMantisJobMetadata> jobIdToMetadataMap = new HashMap<>();

    // Labels lookup map
    private final LabelCache labelsCache;
    private final MantisJobStore jobStore;

    public CompletedJobCache(String clusterName, LabelCache labelsCache, MantisJobStore jobStore) {
        this.name = clusterName;
        this.labelsCache = labelsCache;
        this.jobStore = jobStore;
    }

    public Set<CompletedJob> getCompletedJobSortedSet() {
        return terminalSortedJobSet;
    }

    public Optional<CompletedJob> getCompletedJob(JobId jId) {
        return Optional.ofNullable(completedJobs.computeIfAbsent(jId, (k) -> {
            Optional<IMantisJobMetadata> jobMetadata = getJobDataForCompletedJob(jId, jobStore);
            return jobMetadata.map(this::fromMantisJobMetadata).orElse(null);
        }));
    }

    /**
     * If job data exists in cache return it else call getArchiveJob
     *
     * @param jId
     * @param jobStore
     * @return
     */

    public Optional<IMantisJobMetadata> getJobDataForCompletedJob(JobId jId,
        MantisJobStore jobStore) {
        if (this.jobIdToMetadataMap.containsKey(jId)) {
            return of(jobIdToMetadataMap.get(jId));
        } else {
            return jobStore.getArchivedJob(jId.getId());
        }
    }

    public Set<JobId> getJobIdsMatchingLabels(List<Label> labelList, boolean isAnd) {
        return labelsCache.getJobIdsMatchingLabels(labelList, isAnd);
    }

    private CompletedJob fromMantisJobMetadata(IMantisJobMetadata jobMetadata) {
        return new CompletedJob(
            name,
            jobMetadata.getJobId().getId(),
            jobMetadata.getJobDefinition().getVersion(),
            jobMetadata.getState(),
            jobMetadata.getSubmittedAtInstant().toEpochMilli(),
            jobMetadata.getEndedAtInstant().orElse(Instant.MIN).toEpochMilli(),
            jobMetadata.getUser(),
            jobMetadata.getLabels());
    }

    public Optional<CompletedJob> markCompleted(IMantisJobMetadata jobMetadata) {
        final CompletedJob completedJob = fromMantisJobMetadata(jobMetadata);
        onJobComplete(completedJob, jobMetadata);
        return Optional.of(completedJob);
    }

    private void onJobComplete(CompletedJob completedJob, @Nullable IMantisJobMetadata jobMetadata) {
        JobId jobId = JobId.fromId(completedJob.getJobId()).get();
        if (!completedJobs.containsKey(jobId)) {
            // add to sorted set
            terminalSortedJobSet.add(completedJob);
            try {
                // add to local cache and store table
                addToCacheAndSaveCompletedJobToStore(completedJob, jobMetadata);
            } catch (Exception e) {
                logger.warn("Unable to save {} to completed jobs table due to {}", completedJob,
                    e.getMessage(), e);
            }
        } else {
            logger.warn("Job {}  already marked completed", jobId);
        }
    }

    public Optional<CompletedJob> markCompleted(JobId jId, long submittedAt, long completionTime, String user, String version, JobState finalState, List<Label> labels) {
        final CompletedJob completedJob = new CompletedJob(name, jId.getId(), version, finalState, submittedAt, completionTime, user, labels);
        onJobComplete(completedJob, null);
        return Optional.of(completedJob);
    }

        /**
         * Completely delete jobs that are older than cut off
         *
         * @param tooOldCutOff timestamp, all jobs having an older timestamp should be deleted
         */
    public void purgeOldCompletedJobs(long tooOldCutOff) {
        long numDeleted = 0;
        int maxJobsToPurge = ConfigurationProvider.getConfig().getMaxJobsToPurge();
        final long startNanos = System.nanoTime();

        for (Iterator<CompletedJob> it = completedJobs.values().iterator(); it.hasNext(); ) {
            if (numDeleted == maxJobsToPurge) {
                logger.info("{} Max clean up limit of {} reached. Stop clean up", name,
                    maxJobsToPurge);
                break;
            }
            CompletedJob completedJob = it.next();
            if (completedJob.getTerminatedAt() < tooOldCutOff) {
                try {
                    logger.info(
                        "Purging Job {} as it was terminated at {} which is older than cutoff {}",
                        completedJob, completedJob.getTerminatedAt(), tooOldCutOff);
                    terminalSortedJobSet.remove(completedJob);
                    jobStore.deleteJob(completedJob.getJobId());
                    jobStore.deleteCompletedJob(name, completedJob.getJobId());
                    it.remove();
                    Optional<JobId> jobId = JobId.fromId(completedJob.getJobId());
                    if (jobId.isPresent()) {
                        this.jobIdToMetadataMap.remove(jobId.get());
                        labelsCache.removeJobIdFromLabelCache(jobId.get());
                    }

                } catch (Exception e) {
                    logger.warn("Unable to purge job {} due to {}", completedJob, e);
                }
                numDeleted++;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Job {} was terminated at {} which is not older than cutoff {}",
                        completedJob, completedJob.getTerminatedAt(), tooOldCutOff);
                }
            }
        }
        if (numDeleted > 0) {
            final long endNanos = System.nanoTime();
            logger.info("Took {} micros to clean up {} jobs in cluster {} ",
                (endNanos - startNanos) / 1000, numDeleted, this.name);
        }
    }

    /**
     * During Job Cluster delete, purge all records of completed jobs
     */

    void forcePurgeCompletedJobs() {
        for (Iterator<CompletedJob> it = completedJobs.values().iterator(); it.hasNext(); ) {
            CompletedJob completedJob = it.next();

            try {
                logger.info("Purging Job {} during job cluster cleanup", completedJob);
                terminalSortedJobSet.remove(completedJob);
                jobStore.deleteJob(completedJob.getJobId());
                jobStore.deleteCompletedJob(name, completedJob.getJobId());
                it.remove();
                Optional<JobId> jobId = JobId.fromId(completedJob.getJobId());
                if (jobId.isPresent()) {
                    this.jobIdToMetadataMap.remove(jobId.get());
                    labelsCache.removeJobIdFromLabelCache(jobId.get());
                }
            } catch (Exception e) {
                logger.warn("Unable to purge job {} due to {}", completedJob, e);
            }

        }
    }

    private void addToCacheAndSaveCompletedJobToStore(CompletedJob completedJob, @Nullable IMantisJobMetadata jobMetaData) throws Exception {
        Optional<JobId> jId = JobId.fromId(completedJob.getJobId());
        if (jId.isPresent()) {
            labelsCache.addJobIdToLabelCache(jId.get(), completedJob.getLabelList());
            completedJobs.put(jId.get(), completedJob);
            terminalSortedJobSet.add(completedJob);
            if (jobMetaData != null) {
                jobIdToMetadataMap.put(jId.get(), jobMetaData);
            }
            jobStore.storeCompletedJobForCluster(name, completedJob);
        } else {
            logger.warn("Invalid job id {} in addToCAcheAndSaveCompletedJobToStore ", completedJob);
        }

    }

    /**
     * Bulk add completed jobs to cache
     *
     * @param completedJobsList
     */
    public void addCompletedJobsToCache(List<CompletedJob> completedJobsList) {
        if (completedJobsList == null) {
            logger.warn("addCompletedJobsToCache called with null completedJobsList");
            return;
        }
        this.terminalSortedJobSet.addAll(completedJobsList);

        completedJobsList.forEach((compJob) -> {
            Optional<JobId> jId = JobId.fromId(compJob.getJobId());
            if (jId.isPresent()) {
                completedJobs.put(jId.get(), compJob);
                labelsCache.addJobIdToLabelCache(jId.get(), compJob.getLabelList());
            } else {
                logger.warn("Invalid job Id {}", compJob.getJobId());
            }
        });
    }


    public boolean containsKey(JobId jobId) {
        return completedJobs.containsKey(jobId);
    }
}
