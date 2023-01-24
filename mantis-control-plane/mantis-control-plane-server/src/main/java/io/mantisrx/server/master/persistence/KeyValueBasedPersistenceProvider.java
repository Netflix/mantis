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

import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest;
import io.mantisrx.server.core.domain.JobArtifact;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.store.InvalidJobException;
import io.mantisrx.server.master.store.KeyValueStore;
import io.mantisrx.server.master.store.MantisJobMetadataWritable;
import io.mantisrx.server.master.store.MantisStageMetadata;
import io.mantisrx.server.master.store.MantisStageMetadataWritable;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.server.master.store.NamedJob;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


/**
 * Provide a key-value aware implementation of IMantisStorageProvider.
 * This has all the table-models and mantis specific logic for how
 * mantis master should store job, cluster and related metadata
 * assuming the underlying storage provides key based lookups.
 *
 * The instance of this class needs an actual key-value storage
 * implementation (implements KeyValueStorageProvider) to function.
 *
 * Look at {@code io.mantisrx.server.master.store.KeyValueStorageProvider}
 * to see the features needed from the storage.
 *   Effectively, an apache-cassandra like storage with primary key made
 *   up of partition key and composite key and a way to iterate over all
 *   partition keys in the table.
 */
public class KeyValueBasedPersistenceProvider implements IMantisPersistenceProvider {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueBasedPersistenceProvider.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String JOB_STAGEDATA_NS = "MantisJobStageData";
    private static final String ARCHIVED_JOB_STAGEDATA_NS = "MantisArchivedJobStageData";
    private static final String WORKERS_NS = "MantisWorkers";
    private static final String ARCHIVED_WORKERS_NS = "MantisArchivedWorkers";
    private static final String NAMED_JOBS_NS = "MantisNamedJobs";
    private static final String NAMED_COMPLETEDJOBS_NS = "MantisNamedJobCompletedJobs";
    private static final String ACTIVE_ASGS_NS = "MantisActiveASGs";
    private static final String TASK_EXECUTOR_REGISTRATION = "TaskExecutorRegistration";
    private static final String DISABLE_TASK_EXECUTOR_REQUESTS = "MantisDisableTaskExecutorRequests";
    private static final String CONTROLPLANE_NS = "mantis_controlplane";
    private static final String JOB_ARTIFACTS_NS = "mantis_global_job_artifacts";

    private static final String JOB_METADATA_SECONDARY_KEY = "jobMetadata";
    private static final String JOB_STAGE_METADATA_SECONDARY_KEY_PREFIX = "stageMetadata";
    private static final String NAMED_JOB_SECONDARY_KEY = "jobNameInfo";
    private static final String JOB_ARTIFACTS_BY_NAME_PARTITION_KEY = "JobArtifactsByName";

    private static final int WORKER_BATCH_SIZE = 1000;
    private static final int WORKER_MAX_INDEX = 30000;
    private static final long TTL_IN_MS = TimeUnit.DAYS.toMillis(7);

    static {
        mapper
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());
    }

    private final KeyValueStore kvStore;
    private final LifecycleEventPublisher eventPublisher;

    public KeyValueBasedPersistenceProvider(KeyValueStore kvStore, LifecycleEventPublisher eventPublisher) {
        this.kvStore = kvStore;
        this.eventPublisher = eventPublisher;
    }

    protected String getJobMetadataFieldName() {
        return JOB_METADATA_SECONDARY_KEY;
    }

    protected String getStageMetadataFieldPrefix() {
        return JOB_STAGE_METADATA_SECONDARY_KEY_PREFIX;
    }

    protected String getJobArtifactsByNamePartitionKey() {
        return JOB_ARTIFACTS_BY_NAME_PARTITION_KEY;
    }

    protected Duration getArchiveDataTtlInMs() {
        return Duration.ofMillis(TTL_IN_MS);
    }

    protected String getJobStageFieldName(int stageNum) {
        return String.format("%s-%d", getStageMetadataFieldPrefix(), stageNum);
    }

    protected String getJobClusterFieldName() {
        return NAMED_JOB_SECONDARY_KEY;
    }

    private boolean jobIsValid(MantisJobMetadataWritable job) {
        final int numStages = job.getNumStages();
        final Collection<? extends MantisStageMetadata> stageMetadata = job.getStageMetadata();
        if (stageMetadata == null) {
            logger.error("Could not find stage metadata for jobId {}", job.getJobId());
            return false;
        }
        if (stageMetadata.size() != numStages) {
            logger.error("Invalid stage metadata for job {}: stage count mismatch expected {} vs found {}",
                job.getJobId(), numStages, stageMetadata.size());
            return false;
        }
        return true;
    }

    private MantisJobMetadataWritable readJobStageData(final String namespace, final String jobId)
        throws IOException {
        return readJobStageData(jobId, kvStore.getAll(namespace, jobId));
    }

    private MantisJobMetadataWritable readJobStageData(final String jobId, final Map<String, String> items) throws IOException {
        String jobMetadataColumnName = getJobMetadataFieldName();

        final AtomicReference<MantisJobMetadataWritable> wrapper = new AtomicReference<>();
        final List<MantisStageMetadataWritable> stages = new LinkedList<>();
        for (Entry<String, String> entry : items.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            try {
                if (k != null && v != null) {
                    if (jobMetadataColumnName.equals(k)) {
                        wrapper.set(mapper.readValue(v, MantisJobMetadataWritable.class));
                    } else if (k.startsWith(getStageMetadataFieldPrefix())) {
                        stages.add(mapper.readValue(v, MantisStageMetadataWritable.class));
                    }
                }
            } catch (JsonProcessingException e) {
                throw new IOException(
                    String.format("failed to deserialize job metadata for jobId %s, column name %s",
                        jobId, k), e);
            }
        }
        final MantisJobMetadataWritable job = wrapper.get();
        if (job == null) {
            throw new IOException("No " + jobMetadataColumnName + " column found for key jobId=" + jobId);
        }

        if (stages.isEmpty()) {
            throw new IOException(
                "No stage metadata columns with prefix "
                    + getStageMetadataFieldPrefix()
                    + " found for jobId="
                    + jobId);
        }
        for (MantisStageMetadataWritable msmd : stages) {
            job.addJobStageIfAbsent(msmd);
        }
        if (jobIsValid(job)) {
            return job;
        }
        throw new IOException(String.format("Invalid job for jobId %s", jobId));
    }

    @Override
    public void storeNewJob(IMantisJobMetadata jobMetadata) throws Exception {
        MantisJobMetadataWritable mjmw = DataFormatAdapter.convertMantisJobMetadataToMantisJobMetadataWriteable(jobMetadata);
        try {
            kvStore.upsert(JOB_STAGEDATA_NS, jobMetadata.getJobId().toString(), getJobMetadataFieldName(), mapper.writeValueAsString(mjmw));
        } catch (IOException e) {
            throw new Exception(e);
        }
    }

    @Override
    public void updateJob(IMantisJobMetadata jobMetadata) throws Exception {
        MantisJobMetadataWritable mjmw = DataFormatAdapter.convertMantisJobMetadataToMantisJobMetadataWriteable(jobMetadata);
        kvStore.upsert(JOB_STAGEDATA_NS, jobMetadata.getJobId().toString(), getJobMetadataFieldName(), mapper.writeValueAsString(mjmw));
    }

    @Override
    public void archiveJob(String jobId) throws IOException {
        Map<String, String> all = kvStore.getAll(JOB_STAGEDATA_NS, jobId);
        int workerMaxPartitionKey = workerMaxPartitionKey(readJobStageData(jobId, all));
        kvStore.upsertAll(ARCHIVED_JOB_STAGEDATA_NS, jobId, all, getArchiveDataTtlInMs());
        kvStore.deleteAll(JOB_STAGEDATA_NS, jobId);

        for (int i = 0; i < workerMaxPartitionKey; i += WORKER_BATCH_SIZE) {
            String pkey = makeBucketizedPartitionKey(jobId, i);
            Map<String, String> workersData = kvStore.getAll(WORKERS_NS, pkey);
            kvStore.upsertAll(ARCHIVED_WORKERS_NS, pkey, workersData, getArchiveDataTtlInMs());
            kvStore.deleteAll(WORKERS_NS, pkey);
        }
    }

    @Override
    public void deleteJob(String jobId) throws Exception {
        MantisJobMetadataWritable jobMeta = readJobStageData(JOB_STAGEDATA_NS, jobId);
        int workerMaxPartitionKey = workerMaxPartitionKey(jobMeta);

        kvStore.deleteAll(JOB_STAGEDATA_NS, jobId);
        rangeOperation(workerMaxPartitionKey, idx -> {
            try {
                kvStore.deleteAll(WORKERS_NS, makeBucketizedPartitionKey(jobId, idx));
            } catch (IOException e) {
                logger.warn("failed to delete worker for jobId {} with index {}", jobId, idx, e);
            }
        });

        // delete from archive as well
        kvStore.deleteAll(ARCHIVED_JOB_STAGEDATA_NS, jobId);
        rangeOperation(workerMaxPartitionKey, idx -> {
            try {
                kvStore.deleteAll(ARCHIVED_WORKERS_NS, makeBucketizedPartitionKey(jobId, idx));
            } catch (IOException e) {
                logger.warn("failed to delete worker for jobId {} with index {}", jobId, idx, e);
            }
        });
    }

    @Override
    public void storeMantisStage(IMantisStageMetadata msmd) throws IOException {
        MantisStageMetadataWritable msmw =
            DataFormatAdapter.convertMantisStageMetadataToMantisStageMetadataWriteable(msmd);
        final String expected = mapper.writeValueAsString(msmw);
        Preconditions.checkState(
            kvStore.upsert(
                JOB_STAGEDATA_NS,
                msmd.getJobId().toString(),
                getJobStageFieldName(msmd.getStageNum()),
                expected));
        String data =
            kvStore.get(
                JOB_STAGEDATA_NS,
                msmd.getJobId().toString(),
                getJobStageFieldName(msmd.getStageNum()));
        MantisStageMetadataWritable actual =
            mapper.readValue(data, MantisStageMetadataWritable.class);
        if (!actual.equals(msmw)) {
            logger.error("Mantis Stage write was not successful. actual={} expected={}", data, expected);
        }
    }

    @Override
    public void updateMantisStage(IMantisStageMetadata msmd) throws IOException {
        storeMantisStage(msmd);
    }

    private int workerMaxPartitionKey(MantisJobMetadataWritable jobMetadata) {
        try {
            return jobMetadata.getNextWorkerNumberToUse();
        } catch (Exception ignored) {
        }
        // big number in case we don't find the job
        return WORKER_MAX_INDEX;
    }

    private int bucketizePartitionKey(int num) {
        num = Math.max(1, num);
        return (int) (WORKER_BATCH_SIZE * Math.ceil(1.0 * num / WORKER_BATCH_SIZE));
    }

    private String makeBucketizedPartitionKey(String pkeyPart, int suffix) {
        int bucketized = bucketizePartitionKey(suffix);
        return String.format("%s-%d", pkeyPart, bucketized);
    }

    private String makeBucketizedSecondaryKey(int stageNum, int workerIdx, int workerNum) {
        return String.format("%d-%d-%d", stageNum, workerIdx, workerNum);
    }

    private void rangeOperation(int nextJobNumber, Consumer<Integer> fn) {
        int maxIndex = bucketizePartitionKey(nextJobNumber);
        for (int i = WORKER_BATCH_SIZE; i <= maxIndex; i += WORKER_BATCH_SIZE) {
            fn.accept(i);
        }
    }

    @Override
    public void storeWorker(IMantisWorkerMetadata workerMetadata) throws IOException {
        storeWorkers(workerMetadata.getJobId(), Collections.singletonList(workerMetadata));
    }

    @Override
    public void storeWorkers(List<IMantisWorkerMetadata> workers) throws IOException {
        for (IMantisWorkerMetadata worker : workers) {
            final MantisWorkerMetadataWritable mwmw = DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(worker);
            final String pkey = makeBucketizedPartitionKey(mwmw.getJobId(), mwmw.getWorkerNumber());
            final String skey = makeBucketizedSecondaryKey(mwmw.getStageNum(), mwmw.getWorkerIndex(), mwmw.getWorkerNumber());
            kvStore.upsert(WORKERS_NS, pkey, skey, mapper.writeValueAsString(mwmw));
        }
    }

    @Override
    public void storeAndUpdateWorkers(IMantisWorkerMetadata existingWorker, IMantisWorkerMetadata newWorker) throws IOException {
        storeWorkers(ImmutableList.of(existingWorker, newWorker));
    }

    @Override
    public void updateWorker(IMantisWorkerMetadata worker) throws IOException {
        storeWorker(worker);
    }

    private Map<String, List<MantisWorkerMetadataWritable>> getAllWorkersByJobId(final String namespace) throws IOException {
        Map<String, List<MantisWorkerMetadataWritable>> workersByJobId = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> worker : kvStore.getAllRows(namespace).entrySet()) {
            if (worker.getValue().values().size() <= 0) {
                continue;
            }
            List<MantisWorkerMetadataWritable> workers = worker.getValue().values().stream()
                .map(data -> {
                    try {
                        return mapper.readValue(data, MantisWorkerMetadataWritable.class);
                    } catch (JsonProcessingException e) {
                        logger.warn("failed to parse worker against pkey {} json {}", worker.getKey(), data, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            workersByJobId
                .computeIfAbsent(workers.get(0).getJobId(), k -> Lists.newArrayList())
                .addAll(workers);
        }
        return workersByJobId;
    }

    @Override
    public List<IMantisJobMetadata> loadAllJobs() throws IOException {
        logger.info("MantisStorageProviderAdapter:Enter loadAllJobs");
        final Map<String, List<MantisWorkerMetadataWritable>> workersByJobId = getAllWorkersByJobId(WORKERS_NS);
        final List<IMantisJobMetadata> jobMetas = Lists.newArrayList();
        final Map<String, Map<String, String>> allRows = kvStore.getAllRows(JOB_STAGEDATA_NS);
        for (Map.Entry<String, Map<String, String>> jobInfo : allRows.entrySet()) {
            final String jobId = jobInfo.getKey();
            try {
                final MantisJobMetadataWritable jobMeta = readJobStageData(jobId, jobInfo.getValue());
                if (CollectionUtils.isEmpty(workersByJobId.get(jobId))) {
                    logger.warn("No workers found for job {}, skipping", jobId);
                    continue;
                }
                for (MantisWorkerMetadataWritable workerMeta : workersByJobId.get(jobId)) {
                    jobMeta.addWorkerMedata(workerMeta.getStageNum(), workerMeta, null);
                }
                jobMetas.add(DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(jobMeta, eventPublisher));
            } catch (Exception e) {
                logger.warn("Exception loading job {}", jobId, e);
            }
        }
        // need to load all workers for the jobMeta and then ensure they are added to jobMetas!
        logger.info("MantisStorageProviderAdapter:Exit loadAllJobs {}", jobMetas.size());
        return jobMetas;
    }

    @Override
    public Observable<IMantisJobMetadata> loadAllArchivedJobs() {
        return Observable.create(
            subscriber -> {
                try {
                    for (String pkey : kvStore.getAllPartitionKeys(ARCHIVED_JOB_STAGEDATA_NS)) {
                        Optional<IMantisJobMetadata> jobMetaOpt = loadArchivedJob(pkey);
                        jobMetaOpt.ifPresent(subscriber::onNext);
                    }
                    subscriber.onCompleted();
                } catch (IOException e) {
                    subscriber.onError(e);
                }
            });
    }

    @Override
    public List<IJobClusterMetadata> loadAllJobClusters() throws IOException {
        AtomicInteger failedCount = new AtomicInteger();
        AtomicInteger successCount = new AtomicInteger();
        final List<IJobClusterMetadata> jobClusters = Lists.newArrayList();
        for (Map.Entry<String, Map<String, String>> rows : kvStore.getAllRows(NAMED_JOBS_NS).entrySet()) {
            String name = rows.getKey();
            try {
                String data = rows.getValue().get(getJobClusterFieldName());
                final NamedJob jobCluster = getJobCluster(NAMED_JOBS_NS, name, data);
                jobClusters.add(DataFormatAdapter.convertNamedJobToJobClusterMetadata(jobCluster));
                successCount.getAndIncrement();
            } catch (Exception e) {
                logger.error("Exception {} getting job cluster for {} ", e.getMessage(), name, e);
                failedCount.getAndIncrement();
            }
        }
        return jobClusters;
    }

    @Override
    public List<CompletedJob> loadAllCompletedJobs() throws IOException {
        AtomicInteger failedCount = new AtomicInteger();
        AtomicInteger successCount = new AtomicInteger();
        final List<CompletedJob> completedJobsList = kvStore.getAllRows(NAMED_COMPLETEDJOBS_NS)
            .values().stream()
            .flatMap(x -> x.values().stream())
            .map(data -> {
                try {
                    NamedJob.CompletedJob cj = mapper.readValue(data, NamedJob.CompletedJob.class);
                    CompletedJob completedJob = DataFormatAdapter.convertNamedJobCompletedJobToCompletedJob(cj);
                    successCount.getAndIncrement();
                    return completedJob;
                } catch (JsonProcessingException e) {
                    logger.warn("failed to parse CompletedJob from {}", data, e);
                    failedCount.getAndIncrement();
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        logger.info("Read and converted job clusters. Successful - {}, Failed - {}", successCount.get(), failedCount.get());
        return completedJobsList;
    }


    @Override
    public void archiveWorker(IMantisWorkerMetadata mwmd) throws IOException {
        MantisWorkerMetadataWritable worker = DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(mwmd);
        String pkey = makeBucketizedPartitionKey(worker.getJobId(), worker.getWorkerNumber());
        String skey = makeBucketizedSecondaryKey(worker.getStageNum(), worker.getWorkerIndex(), worker.getWorkerNumber());
        kvStore.delete(WORKERS_NS, pkey, skey);
        kvStore.upsert(ARCHIVED_WORKERS_NS, pkey, skey, mapper.writeValueAsString(worker), getArchiveDataTtlInMs());
    }

    @Override
    public List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) throws IOException {
        // try loading the active job first and then the archived job
        MantisJobMetadataWritable jobInfo;
        try {
            jobInfo = readJobStageData(JOB_STAGEDATA_NS, jobId);
        } catch (Exception e) {
            jobInfo = readJobStageData(ARCHIVED_JOB_STAGEDATA_NS, jobId);
        }
        if (jobInfo == null) {
            return Collections.emptyList();
        }
        int workerMaxPartitionKey = workerMaxPartitionKey(jobInfo);
        final List<IMantisWorkerMetadata> archivedWorkers = Lists.newArrayList();
        rangeOperation(workerMaxPartitionKey, idx -> {
            String pkey = makeBucketizedPartitionKey(jobId, idx);
            final Map<String, String> items;
            try {
                items = kvStore.getAll(ARCHIVED_WORKERS_NS, pkey);
                for (Map.Entry<String, String> entry : items.entrySet()) {
                    try {
                        final JobWorker jobWorker = DataFormatAdapter.convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(
                            mapper.readValue(entry.getValue(), MantisWorkerMetadataWritable.class),
                            eventPublisher);
                        archivedWorkers.add(jobWorker.getMetadata());
                    } catch (Exception e) {
                        logger.warn("Exception converting worker for jobId {} ({}, {})", jobId, pkey, entry.getKey(), e);
                    }
                }
            } catch (IOException e) {
                logger.warn("Error reading archive workers for jobId {} for pkey {}", jobId, pkey, e);
            }
        });
        return archivedWorkers;
    }

    @Override
    public void createJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        updateJobCluster(jobCluster);
    }

    @Override
    public void updateJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        kvStore.upsert(
            NAMED_JOBS_NS,
            jobCluster.getJobClusterDefinition().getName(),
            getJobClusterFieldName(),
            mapper.writeValueAsString(DataFormatAdapter.convertJobClusterMetadataToNamedJob(jobCluster)));
    }

    @Override
    public void deleteJobCluster(String name) throws Exception {
        NamedJob namedJob = getJobCluster(NAMED_JOBS_NS, name);
        kvStore.deleteAll(NAMED_JOBS_NS, name);
        rangeOperation((int) namedJob.getNextJobNumber(),
            idx -> {
                try {
                    removeCompletedJobForCluster(name, makeBucketizedPartitionKey(name, idx));
                } catch (IOException e) {
                    logger.warn("failed to completed job for named job {} with index {}", name, idx, e);
                }
            });
    }


    private NamedJob getJobCluster(String namespace, String name) throws Exception {
        return getJobCluster(namespace, name, kvStore.get(namespace, name, getJobClusterFieldName()));

    }

    private NamedJob getJobCluster(String namespace, String name, String data) throws Exception {
        return mapper.readValue(data, NamedJob.class);
    }

    private int parseJobId(String jobId) {
        final int idx = jobId.lastIndexOf("-");
        return Integer.parseInt(jobId.substring(idx + 1));
    }

    @Override
    public void storeCompletedJobForCluster(String name, CompletedJob job) throws IOException {
        int jobIdx = parseJobId(job.getJobId());
        NamedJob.CompletedJob completedJob = DataFormatAdapter.convertCompletedJobToNamedJobCompletedJob(job);
        kvStore.upsert(NAMED_COMPLETEDJOBS_NS,
            makeBucketizedPartitionKey(name, jobIdx),
            String.valueOf(jobIdx),
            mapper.writeValueAsString(completedJob));
    }

    @Override
    public void removeCompletedJobForCluster(String name, String jobId) throws IOException {
        int jobIdx = parseJobId(jobId);
        kvStore.deleteAll(NAMED_COMPLETEDJOBS_NS,
            makeBucketizedPartitionKey(name, jobIdx));
    }

    @Override
    public Optional<IMantisJobMetadata> loadArchivedJob(String jobId) throws IOException {
        try {
            MantisJobMetadataWritable jmw = readJobStageData(ARCHIVED_JOB_STAGEDATA_NS, jobId);

            final List<IMantisWorkerMetadata> archivedWorkers = getArchivedWorkers(jmw.getJobId());
            if (CollectionUtils.isNotEmpty(archivedWorkers)) {
                for (IMantisWorkerMetadata w : archivedWorkers) {
                    try {
                        MantisWorkerMetadataWritable wmw = DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(w);
                        jmw.addWorkerMedata(w.getStageNum(), wmw, null);
                    } catch (InvalidJobException e) {
                        logger.warn(
                            "Unexpected error adding worker index={}, number={} to job {}",
                            w.getWorkerIndex(), w.getWorkerNumber(), jmw.getJobId());
                    }
                }
                return Optional.of(DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(jmw, eventPublisher));
            }
        } catch (Exception e) {
            logger.error("Exception loading archived job {}", jobId, e);
        }
        return Optional.empty();
    }

    @Override
    public List<String> initActiveVmAttributeValuesList() throws IOException {
        final String data = kvStore.get(ACTIVE_ASGS_NS,
            "activeASGs", "thelist");
        logger.info("read active VMs data {} from Cass", data);
        if (StringUtils.isBlank(data)) {
            return Collections.emptyList();
        }
        return mapper.readValue(data, new TypeReference<List<String>>() {});
    }

    @Override
    public void setActiveVmAttributeValuesList(List<String> vmAttributesList) throws IOException {
        logger.info("Setting active ASGs {}", vmAttributesList);
        kvStore.upsert(ACTIVE_ASGS_NS,
            "activeASGs", "thelist",
            mapper.writeValueAsString(vmAttributesList));
    }

    @Override
    public TaskExecutorRegistration getTaskExecutorFor(TaskExecutorID taskExecutorID) throws IOException {
        try {
            final String value =
                kvStore.get(CONTROLPLANE_NS,
                        TASK_EXECUTOR_REGISTRATION + "-" + taskExecutorID.getResourceId(),
                    taskExecutorID.getResourceId());
            return mapper.readValue(value, TaskExecutorRegistration.class);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void storeNewTaskExecutor(TaskExecutorRegistration registration) throws IOException {
        final String resourceId = registration.getTaskExecutorID().getResourceId();
        final String keyId = String.format("%s-%s", TASK_EXECUTOR_REGISTRATION, resourceId);
        kvStore.upsert(CONTROLPLANE_NS, keyId, resourceId,
            mapper.writeValueAsString(registration));
    }

    @Override
    public void storeNewDisableTaskExecutorRequest(DisableTaskExecutorsRequest request) throws IOException {
        String data = mapper.writeValueAsString(request);
        kvStore.upsert(
            DISABLE_TASK_EXECUTOR_REQUESTS,
            request.getClusterID().getResourceID(),
            request.getHash(), data);
    }

    @Override
    public void deleteExpiredDisableTaskExecutorRequest(DisableTaskExecutorsRequest request) throws IOException {
        kvStore.delete(
            DISABLE_TASK_EXECUTOR_REQUESTS,
            request.getClusterID().getResourceID(),
            request.getHash());
    }

    @Override
    public List<DisableTaskExecutorsRequest> loadAllDisableTaskExecutorsRequests(ClusterID clusterID) throws IOException {

        return kvStore.getAll(DISABLE_TASK_EXECUTOR_REQUESTS, clusterID.getResourceID())
            .values().stream()
            .map(
                value -> {
                    try {
                        return mapper.readValue(value, DisableTaskExecutorsRequest.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
            .collect(Collectors.toList());
    }

    @Override
    public boolean isArtifactExists(String resourceId) throws IOException {
        return kvStore.isRowExists(JOB_ARTIFACTS_NS, resourceId, resourceId);
    }

    @Override
    public JobArtifact getArtifactById(String resourceId) throws IOException {
        String data = kvStore.get(JOB_ARTIFACTS_NS, resourceId, resourceId);
        return mapper.readValue(data, JobArtifact.class);
    }

    @Override
    public List<JobArtifact> listJobArtifacts(String name, String version) throws IOException {
        final Collection<String> artifacts;
        if (version == null) {
            artifacts = kvStore.getAll(JOB_ARTIFACTS_NS, name).values();
        } else {
            artifacts = ImmutableList.of(kvStore.get(JOB_ARTIFACTS_NS, name, version));
        }
        return artifacts.stream()
            .map(e -> {
                try {
                    return mapper.readValue(e, JobArtifact.class);
                } catch (JsonProcessingException ex) {
                    logger.warn("Failed to deserialize job artifact metadata for {} (data={})", name, e, ex);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public List<String> listJobArtifactsByName(String prefix, String contains) throws IOException {
        Set<String> artifactNames;
        if (prefix.isEmpty()) {
            artifactNames = kvStore.getAll(JOB_ARTIFACTS_NS, getJobArtifactsByNamePartitionKey()).keySet();
        } else {
            artifactNames = kvStore.getAllWithPrefix(JOB_ARTIFACTS_NS, getJobArtifactsByNamePartitionKey(), prefix).keySet();
        }

        if (!contains.isEmpty()) {
            return artifactNames.stream().filter(artifact -> artifact.toLowerCase().contains(contains.toLowerCase())).distinct().collect(Collectors.toList());
        }
        return new ArrayList<>(artifactNames);
    }

    private void addNewJobArtifact(String partitionKey, String secondaryKey, JobArtifact jobArtifact) {
        try {
            final String data = mapper.writeValueAsString(jobArtifact);
            kvStore.upsert(JOB_ARTIFACTS_NS, partitionKey, secondaryKey, data);
        } catch (IOException e) {
            logger.error("Error while storing keyId {} for artifact {}", partitionKey, jobArtifact, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addNewJobArtifact(JobArtifact jobArtifact) throws IOException {
        try {
            CompletableFuture.runAsync(
                    () -> addNewJobArtifact(getJobArtifactsByNamePartitionKey(), jobArtifact.getName(), jobArtifact))
                .thenRunAsync(
                    () -> addNewJobArtifact(jobArtifact.getName(), jobArtifact.getVersion(), jobArtifact))
                // Given the lack of transactions in key-value stores we want to make sure that if one of these
                // writes fail, we don't leave the metadata store with partial information.
                // Storing artifactID in the last call should do the trick because the artifact discovery
                // service will eventually retry on missing artifactIDs.
                .thenRunAsync(
                    () ->
                        addNewJobArtifact(
                            jobArtifact.getArtifactID().getResourceID(),
                            jobArtifact.getArtifactID().getResourceID(),
                            jobArtifact))
                .get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error while storing job artifact {} to Cassandra Storage Provider.", jobArtifact, e);
            throw new IOException(e);
        }
    }
}
