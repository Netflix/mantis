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

import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.Label;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.jobcluster.JobClusterActor.LabelCache;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
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

    /**
     * Initialize the store
     * @throws IOException if there is an error
     */
    void initialize() throws IOException;


    /**
     * Gets a completed job given a job id
     * @param jId job id
     * @return completed job if found else empty
     * @throws IOException if there is an error
     */
    Optional<CompletedJob> getCompletedJob(JobId jId) throws IOException;

    /**
     * Gets a completed job given a job id
     * @param jId job id
     * @return completed job if found else empty
     * @throws IOException if there is an error
     */
    Optional<IMantisJobMetadata> getJobMetadata(JobId jId) throws IOException;

    /**
     * Gets a list of completed jobs
     * @param limit number of jobs to return
     * @return list of completed jobs
     * @throws IOException if there is an error
     */
    List<CompletedJob> getCompletedJobs(int limit) throws IOException;

    /**
     * Gets a list of completed jobs
     * @param limit number of jobs to return
     * @param endExclusive end job id
     * @return list of completed jobs
     * @throws IOException if there is an error
     */
    List<CompletedJob> getCompletedJobs(int limit, JobId endExclusive) throws IOException;

    /**
     * Adds a new completed job to the store
     * @param jobMetadata job metadata
     *                    @return completed job
     *                    @throws IOException if there is an error
     */
    CompletedJob onJobCompletion(IMantisJobMetadata jobMetadata) throws IOException;

    /**
     * Adds a new completed job to the store in case the mantis job metadata was not created.
     * @param jId job id
     * @param submittedAt job submission time
     * @param completionTime job completion time
     * @param user user who submitted the job
     * @param version job version
     * @param finalState final state of the job
     * @param labels labels associated with the job
     * @return completed job
     * @throws IOException if there is an error
     */
    CompletedJob onJobCompletion(JobId jId, long submittedAt, long completionTime,
        String user, String version, JobState finalState, List<Label> labels) throws IOException;

    /**
     * Clean up in case of job cluster deletion
     * @throws IOException if there is an error
     */
    void onJobClusterDeletion() throws IOException;
}

/**
 * Consolidates all processing of completed jobs
 */
class CompletedJobStore implements ICompletedJobsStore {

    private final Logger logger = LoggerFactory.getLogger(CompletedJobStore.class);

    // Set of sorted terminal jobs ordered by the most recent first
    private final Set<CompletedJob> terminalSortedJobSet =
        new TreeSet<>(Comparator.comparingLong(CompletedJob::getSubmittedAt).reversed().thenComparing(CompletedJob::getJobId));

    // cluster name
    private final String name;
    // Map of completed jobs
    private final Map<JobId, CompletedJobEntry> completedJobs = new HashMap<>();

    // Labels lookup map
    private final LabelCache labelsCache;
    private final MantisJobStore jobStore;
    private final int initialNumberOfJobsToCache;
    private JobId cachedUpto;
    private final Metrics metrics;

    @VisibleForTesting
    CompletedJobStore(String clusterName, LabelCache labelsCache, MantisJobStore jobStore,
        int initialNumberOfJobsToCache) {
        this.name = clusterName;
        this.labelsCache = labelsCache;
        this.jobStore = jobStore;
        this.initialNumberOfJobsToCache = initialNumberOfJobsToCache;
        MetricGroupId metricGroupId = getMetricGroupId(name);
        Metrics m =
            new Metrics.Builder()
                .id(metricGroupId)
                .addGauge(new GaugeCallback(metricGroupId, "completedJobsGauge", () -> 1.0 * this.getCachedSize()))
                .build();
        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
    }

    public CompletedJobStore(String clusterName, LabelCache labelsCache, MantisJobStore jobStore) {
        this(clusterName, labelsCache, jobStore, 100);
    }

    MetricGroupId getMetricGroupId(String name) {
        return new MetricGroupId("CompletedJobStore", new BasicTag("jobCluster", name));
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
    public Optional<CompletedJob> getCompletedJob(JobId jId) throws IOException {
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
     * @param jId job id
     * @return job metadata if found else empty
     */
    @Override
    public Optional<IMantisJobMetadata> getJobMetadata(JobId jId) throws IOException {
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
    public List<CompletedJob> getCompletedJobs(int limit, JobId startExclusive) throws IOException {
        List<CompletedJob> completedJobsList =
            jobStore.loadCompletedJobsForCluster(name, limit, startExclusive);
        addCompletedJobsToCache(completedJobsList);
        return terminalSortedJobSet
            .stream()
            .filter(job ->
                JobId.fromId(job.getJobId()).get().getJobNum() < startExclusive.getJobNum())
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
            jobMetadata.getEndedAtInstant().orElse(Instant.ofEpochMilli(0l)).toEpochMilli(),
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

    @Override
    public CompletedJob onJobCompletion(JobId jId, long submittedAt, long completionTime,
        String user, String version, JobState finalState, List<Label> labels) throws IOException {
        final CompletedJob completedJob = new CompletedJob(name, jId.getId(), version, finalState,
            submittedAt, completionTime, user, labels);
        jobStore.storeCompletedJobForCluster(name, completedJob);
        addCompletedJobToCache(completedJob, null);
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
                try {
//                    todo(sundaram): Clean this up. This is a hack to get around the fact that the job store
                    jobStore.deleteJob(completedJob.getJobId());
                } catch (IOException e) {
                    logger.error("Unable to purge job", e);
                }
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
        cachedUpto = null;
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
     * @param completedJobsList list of completed jobs
     */
    private void addCompletedJobsToCache(List<CompletedJob> completedJobsList) {
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
