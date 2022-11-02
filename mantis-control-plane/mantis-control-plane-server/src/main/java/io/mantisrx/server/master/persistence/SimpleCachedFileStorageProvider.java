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

import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.JobClusterMetadataImpl;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.master.jobcluster.job.MantisStageMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl;
import io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.persistence.exceptions.JobClusterAlreadyExistsException;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.store.InvalidNamedJobException;
import io.mantisrx.server.master.store.JobAlreadyExistsException;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;


/**
 * Simple File based storage provider. Intended mainly as a sample implementation for
 * {@link IMantisStorageProvider} interface. This implementation is complete in its functionality, but, isn't
 * expected to be scalable or performant for production loads.
 * <P>This implementation uses <code>/tmp/MantisSpool/</code> as the spool directory. The directory is created
 * if not present already. It will fail only if either a file with that name exists or if a directory with that
 * name exists but isn't writable.</P>
 */
public class SimpleCachedFileStorageProvider implements IMantisStorageProvider {
    private static final Logger logger = LoggerFactory.getLogger(SimpleCachedFileStorageProvider.class);

    private final File spoolDir;
    private final File archiveDir;
    private final File jobClustersDir;
    private final File resourceClustersDir;

    private static final String JOB_CLUSTERS_COMPLETED_JOBS_FILE_NAME_SUFFIX = "-completedJobs";
    private static final String ACTIVE_VMS_FILENAME = "activeVMs";
    private static final SimpleFilterProvider DEFAULT_FILTER_PROVIDER;

    static {
        DEFAULT_FILTER_PROVIDER = new SimpleFilterProvider();
        DEFAULT_FILTER_PROVIDER.setFailOnUnknownId(false);
    }

    private final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(),
            new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

    public SimpleCachedFileStorageProvider() {
        this(new File("/tmp"));
    }

    public SimpleCachedFileStorageProvider(boolean cleanupExistingData) {
        this(new File("/tmp"), cleanupExistingData);
    }

    public SimpleCachedFileStorageProvider(File rootDir) {
        this(rootDir, false);
    }

    public SimpleCachedFileStorageProvider(File rootDir, boolean cleanupExistingData) {
        if (cleanupExistingData) {
            deleteAllFiles();
        }

        this.spoolDir = new File(rootDir, "MantisSpool");
        createDir(spoolDir);

        this.archiveDir = new File(rootDir, "MantisArchive");
        createDir(archiveDir);

        this.jobClustersDir = new File(spoolDir, "jobClusters");
        createDir(jobClustersDir);

        this.resourceClustersDir = new File(spoolDir, "resourceClusters");
        createDir(resourceClustersDir);

        logger.debug(" created");
        mapper.setFilterProvider(DEFAULT_FILTER_PROVIDER);
    }

    private static File getWorkerFilename(File rootDir, String jobId, int workerIndex, int workerNumber) {
        return new File(rootDir, "Worker-" + jobId + "-" + workerIndex + "-" + workerNumber);
    }

    //
    @Override
    public void archiveJob(String jobId) throws IOException {
        File jobFile = getJobFileName(spoolDir, jobId);
        Preconditions.checkState(jobFile.renameTo(getJobFileName(archiveDir, jobId)));
        archiveStages(jobId);
        archiveWorkers(jobId);
    }

    //
    @Override
    public Optional<IMantisJobMetadata> loadArchivedJob(String jobId) throws IOException {

        return loadJob(archiveDir, jobId);

    }

    public Optional<IMantisJobMetadata> loadActiveJob(String jobId) throws IOException {
        return loadJob(spoolDir, jobId);
    }

    public Optional<IMantisJobMetadata> loadArchiveJob(String jobId) throws IOException {
        return loadJob(archiveDir, jobId);
    }

    private Optional<IMantisJobMetadata> loadJob(File dir, String jobId) throws IOException {
        File jobFile = getJobFileName(dir, jobId);
        IMantisJobMetadata job = null;
        if (jobFile.exists()) {
            try (FileInputStream fis = new FileInputStream(jobFile)) {
                job = mapper.readValue(fis, MantisJobMetadataImpl.class);
            }
            for (IMantisStageMetadata stage : readStagesFor(dir, jobId))
                ((MantisJobMetadataImpl) job).addJobStageIfAbsent(stage);
            for (IMantisWorkerMetadata worker : readWorkersFor(dir, jobId)) {
                try {
                    JobWorker jobWorker = new JobWorker.Builder()
                            .from(worker)
                            .withLifecycleEventsPublisher(eventPublisher)
                            .build();
                    ((MantisJobMetadataImpl) job).addWorkerMetadata(worker.getStageNum(), jobWorker);
                } catch (InvalidJobException e) {
                    logger.warn("Unexpected error adding worker index=" + worker.getWorkerIndex() + ", number=" +
                            worker.getWorkerNumber() + " for job " + jobId + ": " + e.getMessage(), e);
                }
            }
        }
        return Optional.ofNullable(job);
    }

    @Override
    public void storeMantisStage(IMantisStageMetadata msmd) throws IOException {
        storeStage(msmd, false);
    }

    private void storeStage(IMantisStageMetadata msmd, boolean rewrite) throws IOException {
        System.out.println("Storing stage " + msmd);
        File stageFile = getStageFileName(spoolDir, msmd.getJobId().getId(), msmd.getStageNum());
        if (rewrite)
            stageFile.delete();
        try {stageFile.createNewFile();} catch (SecurityException se) {
            throw new IOException("Can't create new file " + stageFile.getAbsolutePath(), se);
        }
        try (PrintWriter pwrtr = new PrintWriter(stageFile)) {
            mapper.writeValue(pwrtr, msmd);
        }
        System.out.println("Stored stage " + msmd);
    }

    @Override
    public void updateMantisStage(IMantisStageMetadata msmd) throws IOException {
        storeStage(msmd, true);
    }

    private void archiveStages(String jobId) {
        for (File sFile : spoolDir.listFiles((dir, name) -> {
            return name.startsWith("Stage-" + jobId + "-");
        })) {
            sFile.renameTo(new File(archiveDir, sFile.getName()));
        }
    }

    private File getStageFileName(File dirName, String jobId, int stageNum) {
        return new File(dirName, "/Stage-" + jobId + "-" + stageNum);
    }

    @Override
    public void storeWorker(IMantisWorkerMetadata workerMetadata)
            throws IOException {
        storeWorker(workerMetadata.getJobIdObject(), workerMetadata, false);
    }

    @Override
    public void storeWorkers(String jobId, List<IMantisWorkerMetadata> workers)
            throws IOException {
        storeWorkers(workers);
    }

    @Override
    public void storeWorkers(List<IMantisWorkerMetadata> workers) throws IOException {
        for (IMantisWorkerMetadata w : workers)
            storeWorker(w);
    }

    @Override
    public void storeAndUpdateWorkers(IMantisWorkerMetadata existingWorker, IMantisWorkerMetadata newWorker)
            throws InvalidJobException, IOException {
        if (!existingWorker.getJobId().equals(newWorker.getJobId()))
            throw new InvalidJobException(existingWorker.getJobId());
        // As the name indicates, this is a simple storage implementation that does not actually have the
        // atomicity. Instead, we update worker2, followed by storing worker1
        updateWorker(existingWorker);
        storeWorker(newWorker);
        // now move the terminated worker to archived state
        archiveWorker(existingWorker);
    }

    @Override
    public void updateWorker(IMantisWorkerMetadata mwmd) throws IOException {
        storeWorker(mwmd.getJobIdObject(), mwmd, true);
    }

    private void createDir(File spoolDirLocation) {
        if (spoolDirLocation.exists() &&
                !(spoolDirLocation.isDirectory() && spoolDirLocation.canWrite()))
            throw new UnsupportedOperationException("Directory [" + spoolDirLocation + "] not writeable");
        if (!spoolDirLocation.exists())
            try {spoolDirLocation.mkdirs();} catch (SecurityException se) {
                throw new UnsupportedOperationException("Can't create dir for writing state - " + se.getMessage(), se);
            }
    }
    //

    @Override
    public List<IMantisJobMetadata> loadAllJobs() {
        List<IMantisJobMetadata> jobList = Lists.newArrayList();

        File spoolDirFile = spoolDir;
        for (File jobFile : spoolDirFile.listFiles((dir, name) -> {
            return name.startsWith("Job-");
        })) {

            try {
                String jobId = jobFile.getName().substring("Job-".length());
                Optional<IMantisJobMetadata> jobMetaOp = loadJob(spoolDir, jobId);
                if (jobMetaOp.isPresent()) {
                    jobList.add(jobMetaOp.get());
                }
            } catch (IOException e) {
                logger.error("Error reading job metadata - " + e.getMessage());
            }

        }
        // if(_debug) {
        //            // print all jobs read
        //            for(MantisJobMetadata mjmd: retList) {
        //                logger.info("  JOB " + mjmd.getJobId());
        //                for(MantisStageMetadata msmd: mjmd.getStageMetadata()) {
        //                    logger.info("      Stage " + msmd.getStageNum() + " of " + msmd.getNumStages());
        //                    for(MantisWorkerMetadata mwmd: msmd.getWorkerByIndexMetadataSet()) {
        //                        logger.info("        " + mwmd);
        //                    }
        //                }
        //            }
        //        }


        return jobList;
    }

    @Override
    public Observable<IMantisJobMetadata> loadAllArchivedJobs() {
        List<IMantisJobMetadata> jobList = Lists.newArrayList();

        File archiveDirFile = archiveDir;
        for (File jobFile : archiveDirFile.listFiles((dir, name) -> {
            return name.startsWith("Job-");
        })) {

            try {
                String jobId = jobFile.getName().substring("Job-".length());
                Optional<IMantisJobMetadata> jobMetaOp = loadJob(archiveDir, jobId);
                if (jobMetaOp.isPresent()) {
                    jobList.add(jobMetaOp.get());
                }

            } catch (IOException e) {
                logger.error("Error reading job metadata - " + e.getMessage());
            }

        }
        return Observable.from(jobList);
    }

    @Override
    public List<IJobClusterMetadata> loadAllJobClusters() {
        final List<IJobClusterMetadata> jobClusterMetadataList = new ArrayList<>();
        for (File jobClusterFile : jobClustersDir.listFiles()) {
            try (FileInputStream fis = new FileInputStream(jobClusterFile)) {
                jobClusterMetadataList.add(mapper.readValue(fis, JobClusterMetadataImpl.class));
            } catch (Exception e) {
                logger.error("skipped file {} due to exception when loading job cluster", jobClusterFile.getName(), e);
            }
        }
        return jobClusterMetadataList;
    }

    //    @Override
    public Optional<IJobClusterMetadata> loadJobCluster(String clusterName) {
        File jobClusterFile = new File(jobClustersDir, clusterName);
        if (jobClusterFile.exists()) {
            try (FileInputStream fis = new FileInputStream(jobClusterFile)) {
                IJobClusterMetadata jobClustermeta = mapper.readValue(fis, JobClusterMetadataImpl.class);
                return Optional.ofNullable(jobClustermeta);
            } catch (Exception e) {
                logger.error("skipped file {} due to exception when loading job cluster", jobClusterFile.getName(), e);
            }
        }
        logger.warn("No such job cluster {} ", clusterName);
        return Optional.empty();

    }

    @Override
    public List<CompletedJob> loadAllCompletedJobs() throws IOException {
        List<CompletedJob> completedJobs = Lists.newArrayList();
        File clustersDir = jobClustersDir;

        for (File jobClusterFile : clustersDir.listFiles(
                (dir, name) -> name.endsWith(JOB_CLUSTERS_COMPLETED_JOBS_FILE_NAME_SUFFIX)
        )) {
            try (FileInputStream fis = new FileInputStream(jobClusterFile)) {
                final List<CompletedJob> list =
                        mapper.readValue(fis, new TypeReference<List<CompletedJob>>() {});
                if (list != null && !list.isEmpty())
                    list.forEach(completedJobs::add);
            } catch (Exception e) {
                logger.error("Exception loading completedJob ", e);
            }
        }


        return completedJobs;
    }

    private void storeWorker(JobId jobId, IMantisWorkerMetadata workerMetadata, boolean rewrite)
            throws IOException {
        logger.info("Storing worker {}", workerMetadata);
        File workerFile = getWorkerFilename(spoolDir, jobId.getId(), workerMetadata.getWorkerIndex(), workerMetadata.getWorkerNumber());
        if (rewrite)
            workerFile.delete();
        workerFile.createNewFile();
        try (PrintWriter pwrtr = new PrintWriter(workerFile)) {
            mapper.writeValue(pwrtr, workerMetadata);
        }
        logger.info("Stored worker {}", workerMetadata);
    }

    //
    private List<IMantisStageMetadata> readStagesFor(File spoolDir, final String id) throws IOException {
        List<IMantisStageMetadata> stageList = new ArrayList<>();
        for (File stageFile : spoolDir.listFiles((dir, name) -> {
            return name.startsWith("Stage-" + id + "-");
        })) {
            logger.info("Reading stage file " + stageFile.getName());
            try (FileInputStream fis = new FileInputStream(stageFile)) {
                stageList.add(mapper.readValue(fis, MantisStageMetadataImpl.class));
            }
        }
        return stageList;
    }

    private List<IMantisWorkerMetadata> readWorkersFor(File spoolDir, final String id) {
        List<IMantisWorkerMetadata> workerList = new ArrayList<>();
        for (File workerFile : spoolDir.listFiles((dir, name) -> {
            return name.startsWith("Worker-" + id + "-");
        })) {
            logger.info("Reading worker file " + workerFile.getName());
            try (FileInputStream fis = new FileInputStream(workerFile)) {
                workerList.add(mapper.readValue(fis, MantisWorkerMetadataImpl.class));
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return workerList;
    }

    private void archiveWorkers(String jobId)
            throws IOException {
        for (File wFile : spoolDir.listFiles((dir, name) -> {
            return name.startsWith("Worker-" + jobId + "-");
        })) {
            wFile.renameTo(new File(archiveDir, wFile.getName()));
        }
    }
    //
    //    private String getNamedJobFileName(String name) {
    //        return JOB_CLUSTERS_DIR+"/"+name+".job";
    //    }
    //

    //
    @Override
    public void archiveWorker(IMantisWorkerMetadata mwmd) throws IOException {
        File wFile = getWorkerFilename(spoolDir, mwmd.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber());
        if (wFile.exists())
            wFile.renameTo(getWorkerFilename(spoolDir, mwmd.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()));
    }

    @Override
    public void createJobCluster(IJobClusterMetadata jobCluster) throws JobClusterAlreadyExistsException, IOException {

        String name = jobCluster.getJobClusterDefinition().getName();
        File tmpFile = new File(jobClustersDir, name);
        logger.info("Storing job cluster " + name + " to file " + tmpFile.getAbsolutePath());
        if (!tmpFile.createNewFile()) {
            throw new JobClusterAlreadyExistsException(name);
        }
        PrintWriter pwrtr = new PrintWriter(tmpFile);
        mapper.writeValue(pwrtr, jobCluster);
        logger.info("Stored job cluster " + name + " to file " + tmpFile.getAbsolutePath());
    }

    @Override
    public void deleteJobCluster(String name) {


        File jobFile = new File(jobClustersDir, name);
        try {
            if (!jobFile.exists()) {
                throw new InvalidNamedJobException(name + " doesn't exist");
            }
            boolean jobClusterDeleted = jobFile.delete();

            File completedJobsFile = new File(jobClustersDir, name + JOB_CLUSTERS_COMPLETED_JOBS_FILE_NAME_SUFFIX);
            boolean completedJobClusterDeleted = completedJobsFile.delete();

            if (!jobClusterDeleted) { //|| !completedJobClusterDeleted) {
                throw new Exception("JobCluster " + name + " could not be deleted");
            } else {
                logger.info(" job cluster " + name + " deleted ");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public void updateJobCluster(IJobClusterMetadata jobCluster) {

        String name = jobCluster.getJobClusterDefinition().getName();
        File tmpFile = new File(jobClustersDir, name);
        logger.info("Updating job cluster " + name + " to file " + tmpFile.getAbsolutePath());
        try {
            if (!tmpFile.exists()) {
                throw new InvalidNamedJobException(name + " does not exist");
            }
            tmpFile.delete();
            tmpFile.createNewFile();
            PrintWriter pwrtr = new PrintWriter(tmpFile);
            mapper.writeValue(pwrtr, jobCluster);
        } catch (IOException | InvalidNamedJobException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void storeNewJob(IMantisJobMetadata jobMetadata) {

        File tmpFile = new File(spoolDir, "/Job-" + jobMetadata.getJobId());
        try {
            if (!tmpFile.createNewFile()) {
                throw new JobAlreadyExistsException(jobMetadata.getJobId().getId());
            }
            try (PrintWriter pwrtr = new PrintWriter(tmpFile)) {
                mapper.writeValue(pwrtr, jobMetadata);
            }
        } catch (IOException | JobAlreadyExistsException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void updateJob(IMantisJobMetadata jobMetadata) throws InvalidJobException, IOException {
        File jobFile = getJobFileName(spoolDir, jobMetadata.getJobId().getId());
        if (!jobFile.exists()) {
            throw new InvalidJobException(jobMetadata.getJobId().getId());
        }
        jobFile.delete();
        jobFile.createNewFile();
        try (PrintWriter pwrtr = new PrintWriter(jobFile)) {
            mapper.writeValue(pwrtr, jobMetadata);
        }
    }

    public void deleteAllFiles() {
        try {
            deleteDir(spoolDir);
            deleteDir(archiveDir);
        } catch (Exception e) {
            logger.error("caught unexpected exception ", e);
        }
    }

    private void deleteDir(File dir) {
        if (dir != null) {
            for (File file : dir.listFiles()) {
                if (file.isDirectory()) {
                    deleteDir(file);
                } else {

                    boolean delete = file.delete();
                    logger.info("deleted file {}? {}", file.getName(), delete);
                }
            }
        }
    }

    private static void deleteFiles(File rootDir, final String jobId, final String filePrefix) {
        for (File stageFile : rootDir.listFiles((dir, name) -> {
            return name.startsWith(filePrefix + jobId + "-");
        })) {
            stageFile.delete();
        }
    }

    @Override
    public void deleteJob(String jobId) throws InvalidJobException, IOException {
        File tmpFile = getJobFileName(spoolDir, jobId);
        tmpFile.delete();
        deleteFiles(spoolDir, jobId, "Stage-");
        deleteFiles(spoolDir, jobId, "Worker-");
        tmpFile = getJobFileName(archiveDir, jobId);
        tmpFile.delete();
        deleteFiles(archiveDir, jobId, "Stage-");
        deleteFiles(archiveDir, jobId, "Worker-");

    }

    private File getJobFileName(File jobsDir, String jobId) {
        return new File(jobsDir, "/Job-" + jobId);
    }

    @Override
    public void storeCompletedJobForCluster(String name, CompletedJob job) throws IOException {
        modifyCompletedJobsForCluster(name, list -> list.add(job));
    }

    private void modifyCompletedJobsForCluster(String name, Action1<List<CompletedJob>> modifier)
            throws IOException {
        File completedJobsFile = new File(jobClustersDir, name + JOB_CLUSTERS_COMPLETED_JOBS_FILE_NAME_SUFFIX);
        List<CompletedJob> completedJobs = new LinkedList<>();
        if (completedJobsFile.exists()) {
            try (FileInputStream fis = new FileInputStream(completedJobsFile)) {
                completedJobs.addAll(mapper.readValue(fis, new TypeReference<List<CompletedJob>>() {}));
            }
        }
        modifier.call(completedJobs);
        completedJobsFile.delete();
        completedJobsFile.createNewFile();
        try (PrintWriter w = new PrintWriter(completedJobsFile)) {
            mapper.writeValue(w, completedJobs);
        }
    }

    @Override
    public void removeCompletedJobForCluster(String name, String jobId) throws IOException {
        modifyCompletedJobsForCluster(name, list -> {
            if (list != null) {
                final Iterator<CompletedJob> iterator = list.iterator();
                while (iterator.hasNext()) {
                    final CompletedJob next = iterator.next();
                    if (next.getJobId().equals(jobId)) {
                        iterator.remove();
                        break;
                    }
                }
            }
        });
    }

    @Override
    public void setActiveVmAttributeValuesList(List<String> vmAttributesList) throws IOException {
        File activeSlavesFile = new File(spoolDir, ACTIVE_VMS_FILENAME);
        logger.info("Storing file " + activeSlavesFile.getAbsolutePath());
        if (activeSlavesFile.exists())
            activeSlavesFile.delete();
        activeSlavesFile.createNewFile();
        try (PrintWriter wrtr = new PrintWriter(activeSlavesFile)) {
            mapper.writeValue(wrtr, vmAttributesList);
        }
    }

    @Override
    public List<String> initActiveVmAttributeValuesList() throws IOException {
        File activeSlavesFile = new File(spoolDir, ACTIVE_VMS_FILENAME);
        if (!activeSlavesFile.exists())
            return Collections.EMPTY_LIST;
        try (FileInputStream fis = new FileInputStream(activeSlavesFile)) {
            return mapper.readValue(fis, new TypeReference<List<String>>() {});
        }
    }

    //    @Override
    //    public Optional<IJobClusterMetadata> getJobCluster(String clusterName) throws Exception {
    //        File jobClustersDir = new File(JOB_CLUSTERS_DIR);
    //        final List<JobClusterMetadataImpl> jobClusterMetadataList = new ArrayList<>();
    //        File jobClusterFile = new File(JOB_CLUSTERS_DIR + File.separator + clusterName);
    //        if(jobClusterFile.exists()) {
    //            try (FileInputStream fis = new FileInputStream(jobClusterFile)) {
    //                IJobClusterMetadata clusterMeta = mapper.readValue(fis, JobClusterMetadataImpl.class);
    //                return Optional.of(clusterMeta);
    //            } catch (Exception e) {
    //                logger.error("skipped file {} due to exception when loading job cluster", jobClusterFile.getName(), e);
    //                throw e;
    //            }
    //        } else {
    //            logger.error("No such file {} ", jobClusterFile);
    //            return Optional.empty();
    //        }
    //
    //
    //    }

    @Override
    public List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) {
        // TODO Auto-generated method stub
        return null;
    }

    private File getFileFor(TaskExecutorID taskExecutorID) {
        return new File(spoolDir, "TaskExecutor-" + taskExecutorID.getResourceId());
    }

    @Override
    public TaskExecutorRegistration getTaskExecutorFor(TaskExecutorID taskExecutorID) throws IOException {
        File tmpFile = getFileFor(taskExecutorID);
        if (tmpFile.exists()) {
            try (FileInputStream fis = new FileInputStream(tmpFile)) {
                return mapper.readValue(fis, TaskExecutorRegistration.class);
            }
        } else {
            throw new IOException(String.format("File %s for taskExecutor %s does not exist", tmpFile, taskExecutorID));
        }
    }

    @Override
    public void storeNewTaskExecutor(TaskExecutorRegistration registration) throws IOException {
        File tmpFile = getFileFor(registration.getTaskExecutorID());
        if (!tmpFile.exists()) {
            if (!tmpFile.delete()) {
                throw new IOException(String.format("File %s cannot be deleted for storing taskExecutor %s", tmpFile, registration.getTaskExecutorID()));
            }
        }
        if (!tmpFile.createNewFile()) {
            throw new IOException(String.format("File %s cannot be created for storing taskExecutor %s", tmpFile, registration.getTaskExecutorID()));
        }

        try (PrintWriter pwrtr = new PrintWriter(tmpFile)) {
            mapper.writeValue(pwrtr, registration);
        }
    }

    private File getDisableTaskExecutorsRequestFile(ClusterID clusterID) throws IOException {
        File file = new File(new File(resourceClustersDir, clusterID.getResourceID()), "disableTaskExecutorRequests");
        file.createNewFile();
        return file;
    }

    @Override
    public void storeNewDisableTaskExecutorRequest(DisableTaskExecutorsRequest request) throws IOException {
        List<DisableTaskExecutorsRequest> requests =
            loadAllDisableTaskExecutorsRequests(request.getClusterID());
        List<DisableTaskExecutorsRequest> newRequests =
            ImmutableList.<DisableTaskExecutorsRequest>builder().addAll(requests).add(request).build();
        try (PrintWriter printWriter = new PrintWriter(getDisableTaskExecutorsRequestFile(request.getClusterID()))) {
            mapper.writeValue(printWriter, newRequests);
        }
    }

    @Override
    public void deleteExpiredDisableTaskExecutorRequest(DisableTaskExecutorsRequest request) throws IOException {
        List<DisableTaskExecutorsRequest> requests =
            loadAllDisableTaskExecutorsRequests(request.getClusterID());
        List<DisableTaskExecutorsRequest> newRequests =
            requests.stream().filter(request1 -> !request1.equals(request)).collect(Collectors.toList());
        try (PrintWriter printWriter = new PrintWriter(getDisableTaskExecutorsRequestFile(request.getClusterID()))) {
            mapper.writeValue(printWriter, newRequests);
        }
    }

    @Override
    public List<DisableTaskExecutorsRequest> loadAllDisableTaskExecutorsRequests(ClusterID clusterID) throws IOException {
        File output = getDisableTaskExecutorsRequestFile(clusterID);
        return mapper.readValue(output, new TypeReference<List<DisableTaskExecutorsRequest>>() {});
    }
}
