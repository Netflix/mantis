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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.store.InvalidNamedJobException;
import io.mantisrx.server.master.store.JobAlreadyExistsException;
import io.mantisrx.server.master.store.JobNameAlreadyExistsException;
import io.mantisrx.server.master.store.MantisJobMetadataWritable;
import io.mantisrx.server.master.store.MantisStorageProvider;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.server.master.store.NamedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class MantisStorageProviderAdapter implements IMantisStorageProvider {

    private static final Logger logger = LoggerFactory.getLogger(MantisStorageProviderAdapter.class);

    private final MantisStorageProvider sProvider;
    private final LifecycleEventPublisher eventPublisher;

    public MantisStorageProviderAdapter(MantisStorageProvider actualStorageProvider, LifecycleEventPublisher eventPublisher) {
        this.sProvider = actualStorageProvider;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void storeNewJob(IMantisJobMetadata jobMetadata) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Enter storeNewJob {}", jobMetadata); }
        MantisJobMetadataWritable mjmw = DataFormatAdapter.convertMantisJobMetadataToMantisJobMetadataWriteable(jobMetadata);
        try {
            sProvider.storeNewJob(mjmw);
        } catch (JobAlreadyExistsException | IOException e) {
            throw new Exception(e);
        }
        if (logger.isTraceEnabled()) { logger.trace("Exit store New job");}
    }

    @Override
    public void updateJob(IMantisJobMetadata jobMetadata) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Enter updateJob {}", jobMetadata);}
        MantisJobMetadataWritable mjmw = DataFormatAdapter.convertMantisJobMetadataToMantisJobMetadataWriteable(jobMetadata);
        try {
            sProvider.updateJob(mjmw);
        } catch (io.mantisrx.server.master.store.InvalidJobException e) {
            throw new Exception(e);
        }
        if (logger.isTraceEnabled()) { logger.trace("Exit updateJob");}
    }

    @Override
    public void archiveJob(String jobId) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter archiveJob {}", jobId);}
        sProvider.archiveJob(jobId);
        if (logger.isTraceEnabled()) { logger.trace("Exit archiveJob");}
    }

    @Override
    public void deleteJob(String jobId) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Enter delete job {}", jobId);}
        try {
            sProvider.deleteJob(jobId);
        } catch (io.mantisrx.server.master.store.InvalidJobException e) {
            throw new Exception(e);
        }

        if (logger.isTraceEnabled()) { logger.trace("Exit deleteJob");}

    }

    @Override
    public void storeMantisStage(IMantisStageMetadata msmd) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter storeMantisStage {}", msmd);}
        sProvider.storeMantisStage(DataFormatAdapter.convertMantisStageMetadataToMantisStageMetadataWriteable(msmd));
        if (logger.isTraceEnabled()) { logger.trace("Exit storeMantisStage");}

    }

    @Override
    public void updateMantisStage(IMantisStageMetadata msmd) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter updateMantisStage {}", msmd);}
        sProvider.updateMantisStage(DataFormatAdapter.convertMantisStageMetadataToMantisStageMetadataWriteable(msmd));
        if (logger.isTraceEnabled()) { logger.trace("Exit updateMantisStage");}

    }

    @Override
    public void storeWorker(IMantisWorkerMetadata workerMetadata) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter storeWorker {}", workerMetadata);}
        sProvider.storeWorker(DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(workerMetadata));
        if (logger.isTraceEnabled()) { logger.trace("Exit storeWorker");}

    }

    @Override
    public void storeWorkers(String jobId, List<IMantisWorkerMetadata> workers) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter storeWorkers {} for Job {}", workers.size(), jobId);}
        List<MantisWorkerMetadataWritable> convertedList = new ArrayList<>(workers.size());
        for (IMantisWorkerMetadata worker : workers) {
            convertedList.add(DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(worker));
        }
        sProvider.storeWorkers(jobId, convertedList);
        if (logger.isTraceEnabled()) { logger.trace("Exit storeWorkers");}
    }

    @Override
    public void storeAndUpdateWorkers(IMantisWorkerMetadata existingWorker, IMantisWorkerMetadata newWorker)
            throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Enter storeAndUpdateWorkers");}
        try {
            sProvider.storeAndUpdateWorkers(DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(existingWorker), DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(newWorker));
        } catch (io.mantisrx.server.master.store.InvalidJobException e) {
            throw new Exception(e);
        }
        if (logger.isTraceEnabled()) { logger.trace("Exit storeAndUpdateWorkers");}

    }

    @Override
    public void updateWorker(IMantisWorkerMetadata worker) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter updateWorker {}", worker);}
        sProvider.updateWorker(DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(worker));
        if (logger.isTraceEnabled()) { logger.trace("Exit updateWorker");}
    }

    @Override
    public List<IMantisJobMetadata> loadAllJobs() throws IOException {
        logger.info("MantisStorageProviderAdapter:Enter loadAllJobs");
        List<IMantisJobMetadata> jobMetas = Lists.newArrayList();
        sProvider.initJobs().forEach((mw) -> {
            try {
                jobMetas.add(DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(mw, eventPublisher));
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("Exception loading job {}", e.getMessage());
            }
        });
        logger.info("MantisStorageProviderAdapter:Exit loadAllJobs {}", jobMetas.size());
        return jobMetas;
    }

    @Override
    public Observable<IMantisJobMetadata> loadAllArchivedJobs() {
        if (logger.isTraceEnabled()) { logger.trace("In StorageAdapter.loadAllArchivedJobs"); }
        return sProvider.initArchivedJobs().map((mjm) -> {
            try {
                if (logger.isDebugEnabled()) { logger.debug("Reading Archived Job {}", mjm); }

                IMantisJobMetadata archivedJob = DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(mjm, eventPublisher, true);
                if (logger.isDebugEnabled()) { logger.debug("Read Archived Job {}", archivedJob); }
                return archivedJob;
            } catch (Exception e) {
                logger.error("Exception {} occurred converting archived job {}", e, Optional.ofNullable(mjm).map(j -> j.getJobId()).orElse(""));
                return null;
            }
        })
                .filter((j) -> j != null)
                ;
    }

    @Override
    public List<IJobClusterMetadata> loadAllJobClusters() throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter StorageAdapter.loadAllJobClusters"); }
        List<IJobClusterMetadata> jobClusters = Lists.newArrayList();
        List<NamedJob> namedJobList = sProvider.initNamedJobs();
        AtomicInteger failedCount = new AtomicInteger();
        AtomicInteger successCount = new AtomicInteger();

        jobClusters = namedJobList
                .stream()
                .map((nJob) -> {
                    try {
                        IJobClusterMetadata jobClusterMetadata = DataFormatAdapter.convertNamedJobToJobClusterMetadata(nJob);
                        successCount.getAndIncrement();
                        return jobClusterMetadata;
                    } catch (Exception e) {
                        logger.error("Exception {} converting {} ", e.getMessage(), nJob);
                        logger.error("Exception is", e);
                        failedCount.getAndIncrement();
                    }
                    return null;
                })
                .filter((jobClusterMeta) -> jobClusterMeta != null)
                .collect(Collectors.toList());

        logger.info("Succesfully read and converted {} job clusters", successCount.get());
        logger.info("Failed to read and converted {} job clusters", failedCount.get());

        if (logger.isTraceEnabled()) { logger.trace("Exit StorageAdapter.loadAllJobClusters"); }
        return jobClusters;
    }


    @Override
    public List<CompletedJob> loadAllCompletedJobs() throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("In StorageAdapter.loadAllCompletedJobs"); }
        List<CompletedJob> completedJobsList = Lists.newArrayList();

        Observable<NamedJob.CompletedJob> namedJobCompletedJobs = sProvider.initNamedJobCompletedJobs();
        AtomicInteger failedCount = new AtomicInteger();
        AtomicInteger successCount = new AtomicInteger();
        AtomicReference<String> errorMsg = new AtomicReference<>("");
        namedJobCompletedJobs.map((completedJob) -> {
            try {
                CompletedJob convertedCompletedJob = DataFormatAdapter.convertNamedJobCompletedJobToCompletedJob(completedJob);
                successCount.getAndIncrement();
                return convertedCompletedJob;
            } catch (Exception e) {
                logger.error("Exception {} converting {} ", e.getMessage(), completedJob);
                logger.error("Conversion errors is", e);
                failedCount.getAndIncrement();
            }
            return null;
        })
                .filter((convertedCompletedJob) -> convertedCompletedJob != null)
                .forEach((jb) -> completedJobsList.add(jb), error -> {
                    errorMsg.set(error.getMessage());
                });

        if (!errorMsg.get().isEmpty()) {
            logger.error("Exception occurred loading completed jobs {}", errorMsg.get());
            throw new IOException(errorMsg.get());
        }

        logger.info("Succesfully read and converted {} job clusters", successCount.get());
        logger.info("Failed to read and converted {} job clusters", failedCount.get());

        if (logger.isTraceEnabled()) { logger.trace("Exit StorageAdapter.loadAllCompletedJobs"); }
        return completedJobsList;
    }


    @Override
    public void archiveWorker(IMantisWorkerMetadata mwmd) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter MantisStorageProviderAdapter:archiveWorker {}", mwmd); }
        sProvider.archiveWorker(DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(mwmd));
        if (logger.isTraceEnabled()) { logger.trace("Exit MantisStorageProviderAdapter:archiveWorker {}", mwmd); }
    }

    @Override
    public List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Enter MantisStorageProviderAdapter:getArchivedWorkers {}", jobId);
        }
        List<IMantisWorkerMetadata> archivedWorkers = Lists.newArrayList();
        for (MantisWorkerMetadataWritable mantisWorkerMetadataWritable : sProvider.getArchivedWorkers(jobId)) {
            try {
                archivedWorkers.add(DataFormatAdapter.convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(mantisWorkerMetadataWritable, eventPublisher).getMetadata());
            } catch (Exception e) {
                logger.error("Exception {} converting {} ", e.getMessage(), mantisWorkerMetadataWritable);
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Exit MantisStorageProviderAdapter:getArchivedWorkers {} with {} workers", jobId, archivedWorkers.size());
        }
        return archivedWorkers;
    }

    @Override
    public void createJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Enter MantisStorageProviderAdapter:createJobCluster {}", jobCluster);
        }
        try {
            sProvider.storeNewNamedJob(DataFormatAdapter.convertJobClusterMetadataToNamedJob(jobCluster));
        } catch (JobNameAlreadyExistsException e) {
            throw new Exception(e);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Exit MantisStorageProviderAdapter:createJobCluster {}", jobCluster);
        }

    }

    @Override
    public void updateJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Enter MantisStorageProviderAdapter:updateJobCluster {}", jobCluster);
        }
        try {
            sProvider.updateNamedJob(DataFormatAdapter.convertJobClusterMetadataToNamedJob(jobCluster));
        } catch (InvalidNamedJobException e) {
            throw new Exception(e);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Exit MantisStorageProviderAdapter:createJobCluster {}", jobCluster);
        }
    }

    @Override
    public void deleteJobCluster(String name) throws Exception {
        if (logger.isTraceEnabled()) { logger.trace("Enter MantisStorageProviderAdapter:deleteJobCluster {}", name); }
        try {
            sProvider.deleteNamedJob(name);
        } catch (IOException e) {
            throw new Exception(e);
        }
        if (logger.isTraceEnabled()) { logger.trace("Exit MantisStorageProviderAdapter:createJobCluster {}", name); }
    }

    @Override
    public void storeCompletedJobForCluster(String name, CompletedJob job) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Enter MantisStorageProviderAdapter:storeCompletedJobForCluster {}", name);
        }
        sProvider.storeCompletedJobForNamedJob(name, DataFormatAdapter.convertCompletedJobToNamedJobCompletedJob(job));
        if (logger.isTraceEnabled()) {
            logger.trace("Enter MantisStorageProviderAdapter:storeCompletedJobForCluster {}", name);
        }

    }

    @Override
    public void removeCompletedJobForCluster(String name, String jobId) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("Enter MantisStorageProviderAdapter:removeCompletedJobForCluster {}", jobId);
        }
        sProvider.removeCompledtedJobForNamedJob(name, jobId);
        if (logger.isTraceEnabled()) {
            logger.trace("Exit MantisStorageProviderAdapter:removeCompletedJobForCluster {}", jobId);
        }
    }

    @Override
    public Optional<IMantisJobMetadata> loadArchivedJob(String jobId) throws IOException {
        if (logger.isTraceEnabled()) { logger.trace("Enter MantisStorageProviderAdapter:loadArchivedJob {}", jobId); }
        IMantisJobMetadata mantisJobMetadata;
        try {
            MantisJobMetadataWritable archJob = sProvider.loadArchivedJob(jobId);
            mantisJobMetadata = (DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(archJob, eventPublisher, true));
        } catch (Exception e) {
            logger.error("Exception loading archived Job", e);
            return Optional.empty();
        }
        if (logger.isTraceEnabled()) { logger.trace("Exit MantisStorageProviderAdapter:loadArchivedJob {}", jobId); }
        return Optional.ofNullable(mantisJobMetadata);

    }


    @Override
    public List<String> initActiveVmAttributeValuesList() throws IOException {
        return sProvider.initActiveVmAttributeValuesList();
    }

    @Override
    public void setActiveVmAttributeValuesList(List<String> vmAttributesList) throws IOException {
        sProvider.setActiveVmAttributeValuesList(vmAttributesList);

    }


}
