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

import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.JobClusterActor.LabelCache;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface ICompletedJobsStore {

    void initialize() throws IOException;

    Optional<CompletedJob> getCompletedJob(JobId jId) throws IOException;

    Optional<IMantisJobMetadata> getJobMetadata(JobId jId) throws IOException;

    List<CompletedJob> getCompletedJobs(int limit) throws IOException;

    List<CompletedJob> getCompletedJobs(int limit, JobId endExclusive) throws IOException;

    CompletedJob onJobCompletion(IMantisJobMetadata jobMetadata) throws IOException;

    void onJobClusterDeletion() throws IOException;
}

/**
 * Consolidates all processing of completed jobs
 */
class CompletedJobStore implements ICompletedJobsStore {

    private final Logger logger = LoggerFactory.getLogger(CompletedJobStore.class);

    // Set of sorted terminal jobs ordered by the most recent first
    private final Set<CompletedJob> terminalSortedJobSet =
        new TreeSet<>((o1, o2) -> Long.compare(o2.getSubmittedAt(), o1.getSubmittedAt()));

    // cluster name
    private final String name;
    // Map of completed jobs
    private final Map<JobId, CompletedJobEntry> completedJobs = new HashMap<>();

    // Labels lookup map
    private final LabelCache labelsCache;
    private final MantisJobStore jobStore;
    private final int initialNumberOfJobsToCache = 100;
    private JobId cachedUpto;

    public CompletedJobStore(String clusterName, LabelCache labelsCache, MantisJobStore jobStore) {
        this.name = clusterName;
        this.labelsCache = labelsCache;
        this.jobStore = jobStore;
    }

    private int getCachedSize() {
        return completedJobs.size();
    }

    @Override
    public void initialize() throws IOException {
        logger.info("Initializing completed jobs for cluster {}", name);
        List<CompletedJob> completedJobsList =
            jobStore.loadCompletedJobsForCluster(name, initialNumberOfJobsToCache, null);
        if (completedJobsList.isEmpty()) {
            cachedUpto = null;
        } else {
            addCompletedJobsToCache(completedJobsList);
            cachedUpto = JobId.fromId(
                completedJobsList.get(completedJobsList.size() - 1).getJobId()).orElse(null);
        }
    }

    @Override
    public Optional<CompletedJob> getCompletedJob(JobId jId) {
        CompletedJobEntry res = fetchJobId(jId);
        if (res != null) {
            return Optional.of(res.getJob());
        } else {
            return Optional.empty();
        }
    }

    private CompletedJobEntry fetchJobId(JobId jId) {
        return completedJobs.computeIfAbsent(jId, (k) -> {
            Optional<IMantisJobMetadata> jobMetadata = jobStore.getArchivedJob(jId.getId());
            return jobMetadata.map(
                    mantisJobMetadata -> new CompletedJobEntry(fromMantisJobMetadata(mantisJobMetadata),
                        mantisJobMetadata))
                .orElse(null);
        });
    }

    /**
     * If job data exists in cache return it else call getArchiveJob
     *
     * @param jId
     * @return
     */
    @Override
    public Optional<IMantisJobMetadata> getJobMetadata(JobId jId) {
        CompletedJobEntry res = fetchJobId(jId);
        if (res != null) {
            return Optional.ofNullable(res.getJobMetadata());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<CompletedJob> getCompletedJobs(int limit) throws IOException {
        if (getCachedSize() < limit) {
            // need to load more
            List<CompletedJob> completedJobsList =
                jobStore.loadCompletedJobsForCluster(name, limit - getCachedSize(), cachedUpto);
            addCompletedJobsToCache(completedJobsList);
        }

        return terminalSortedJobSet.stream().limit(limit).collect(Collectors.toList());
    }

    @Override
    public List<CompletedJob> getCompletedJobs(int limit, JobId endExclusive) throws IOException {
        List<CompletedJob> completedJobsList =
            jobStore.loadCompletedJobsForCluster(name, limit, endExclusive);
        addCompletedJobsToCache(completedJobsList);
        return terminalSortedJobSet
            .stream()
            .filter(job ->
                JobId.fromId(job.getJobId()).get().getJobNum() < endExclusive.getJobNum())
            .limit(limit)
            .collect(Collectors.toList());
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
            jobMetadata.getEndedAtInstant().get().toEpochMilli(),
            jobMetadata.getUser(),
            jobMetadata.getLabels());
    }

    public CompletedJob onJobCompletion(IMantisJobMetadata jobMetadata) throws IOException {
        JobId jobId = jobMetadata.getJobId();
        if (!completedJobs.containsKey(jobId)) {
            CompletedJob completedJob = fromMantisJobMetadata(jobMetadata);
            jobStore.storeCompletedJobForCluster(name, completedJob);
            // add to local cache and store table
            addCompletedJobToCache(completedJob, jobMetadata);
            return completedJob;
        } else {
            logger.warn("Job {}  already marked completed", jobId);
            return completedJobs.get(jobId).getJob();
        }
    }

    public CompletedJob onJobCompletion(JobId jId, long submittedAt, long completionTime,
        String user, String version, JobState finalState, List<Label> labels) throws IOException {
        final CompletedJob completedJob = new CompletedJob(name, jId.getId(), version, finalState,
            submittedAt, completionTime, user, labels);
        jobStore.storeCompletedJobForCluster(name, completedJob);
        addCompletedJobToCache(completedJob, null);
//        onJobCompletion(completedJob, null);
        return completedJob;
    }

    /**
     * During Job Cluster delete, purge all records of completed jobs
     */

    @Override
    public void onJobClusterDeletion() throws IOException {
        List<CompletedJob> jobs = jobStore.loadCompletedJobsForCluster(name, 100, null);
        while (!jobs.isEmpty()) {
            for (CompletedJob completedJob : jobs) {
                jobStore.deleteJob(completedJob.getJobId());
            }
            jobs = jobStore.loadCompletedJobsForCluster(name, 100, JobId.fromId(jobs.get(jobs.size() - 1).getJobId()).get());
        }

        jobStore.deleteCompletedJobsForCluster(name);
        completedJobs.forEach(
            (jobId, tuple) -> {
                if (tuple.getJobMetadata() != null) {
                    labelsCache.removeJobIdFromLabelCache(tuple.getJobMetadata().getJobId());
                }
            });
        completedJobs.clear();
        terminalSortedJobSet.clear();
    }

    private void addCompletedJobToCache(CompletedJob completedJob,
        @Nullable IMantisJobMetadata jobMetaData) {
        JobId jobId = JobId.fromId(completedJob.getJobId()).get();
        labelsCache.addJobIdToLabelCache(jobId, completedJob.getLabelList());
        completedJobs.put(jobId, new CompletedJobEntry(completedJob, jobMetaData));
        terminalSortedJobSet.add(completedJob);
    }

    /**
     * Bulk add completed jobs to cache
     *
     * @param completedJobsList
     */
    private void addCompletedJobsToCache(List<CompletedJob> completedJobsList) throws IOException {
        if (!completedJobsList.isEmpty()) {
            Map<JobId, CompletedJobEntry> cache = completedJobsList.stream()
                .flatMap(compJob -> {
                    Optional<IMantisJobMetadata> jobMetadata =
                        jobStore.getArchivedJob(compJob.getJobId());
                    if (jobMetadata.isPresent()) {
                        return Stream.of(new CompletedJobEntry(compJob, jobMetadata.get()));
                    } else {
                        return Stream.empty();
                    }
                }).collect(Collectors.toMap(
                    tuple -> JobId.fromId(tuple.getJob().getJobId()).get(),
                    tuple -> tuple,
                    (a, b) -> a
                ));
            this.terminalSortedJobSet.addAll(
                cache.values().stream().map(CompletedJobEntry::getJob)
                    .collect(Collectors.toList()));
            this.completedJobs.putAll(cache);
            cache.values().stream().map(CompletedJobEntry::getJob).forEach((compJob) -> {
                Optional<JobId> jId = JobId.fromId(compJob.getJobId());
                if (jId.isPresent()) {
                    labelsCache.addJobIdToLabelCache(jId.get(), compJob.getLabelList());
                } else {
                    logger.warn("Invalid job Id {}", compJob.getJobId());
                }
            });
            Optional<JobId> lastJobId =
                JobId.fromId(completedJobsList.get(completedJobsList.size() - 1).getJobId());
            lastJobId.ifPresent(jobId -> cachedUpto = jobId);
        }
    }

    public boolean containsKey(JobId jobId) {
        return completedJobs.containsKey(jobId);
    }

    @Value
    private static class CompletedJobEntry {

        CompletedJob job;
        @Nullable
        IMantisJobMetadata jobMetadata;
    }
}
