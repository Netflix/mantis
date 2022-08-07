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

import com.netflix.fenzo.functions.Action1;
import io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


/**
 * Simple File based storage provider. Intended mainly as a sample implementation for
 * {@link MantisStorageProvider} interface. This implementation is complete in its functionality, but, isn't
 * expected to be scalable or performant for production loads.
 * <P>This implementation uses <code>/tmp/MantisSpool/</code> as the spool directory. The directory is created
 * if not present already. It will fail only if either a file with that name exists or if a directory with that
 * name exists but isn't writable.</P>
 */
public class SimpleCachedFileStorageProvider implements MantisStorageProvider {

    private final File spoolDir;
    private final File archiveDir;
    private final File namedJobsDir;
    private final File resourceClustersDir;
    private final ObjectMapper mapper;

    private static final Logger logger = LoggerFactory.getLogger(SimpleCachedFileStorageProvider.class);
    private static final String NAMED_JOBS_COMPLETED_JOBS_FILE_NAME_SUFFIX = "-completedJobs";
    private static final String ACTIVE_VMS_FILENAME = "activeVMs";

    public SimpleCachedFileStorageProvider() {
        this(new File("/tmp"));
    }

    public SimpleCachedFileStorageProvider(File rootDir) {
        this.spoolDir = new File(rootDir, "MantisSpool");
        createDir(spoolDir);

        this.archiveDir = new File(rootDir, "MantisArchive");
        createDir(archiveDir);

        this.namedJobsDir = new File(rootDir, "namedJobs");
        createDir(namedJobsDir);

        this.resourceClustersDir = new File(spoolDir, "resourceClusters");
        createDir(resourceClustersDir);

        this.mapper = new ObjectMapper().registerModule(new Jdk8Module()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static File getWorkerFilename(File rootDir, String jobId, int workerIndex, int workerNumber) {
        return new File(rootDir, "Worker-" + jobId + "-" + workerIndex + "-" + workerNumber);
    }

    @Override
    public void storeNewJob(MantisJobMetadataWritable jobMetadata)
            throws JobAlreadyExistsException, IOException {
        File tmpFile = getJobFileName(spoolDir, jobMetadata.getJobId());
        if (!tmpFile.createNewFile()) {
            throw new JobAlreadyExistsException(jobMetadata.getJobId());
        }
        try (PrintWriter pwrtr = new PrintWriter(tmpFile)) {
            mapper.writeValue(pwrtr, jobMetadata);
        }
    }

    @Override
    public void updateJob(MantisJobMetadataWritable jobMetadata) throws InvalidJobException, IOException {
        File jobFile = getJobFileName(spoolDir, jobMetadata.getJobId());
        if (!jobFile.exists()) {
            throw new InvalidJobException(jobMetadata.getJobId());
        }
        jobFile.delete();
        jobFile.createNewFile();
        try (PrintWriter pwrtr = new PrintWriter(jobFile)) {
            mapper.writeValue(pwrtr, jobMetadata);
        }
    }

    private File getJobFileName(File jobsDir, String jobId) {
        return new File(jobsDir, "/Job-" + jobId);
    }

    @Override
    public void archiveJob(String jobId) throws IOException {
        File jobFile = getJobFileName(spoolDir, jobId);
        jobFile.renameTo(getJobFileName(archiveDir, jobId));
        archiveStages(jobId);
        archiveWorkers(jobId);
    }

    @Override
    public MantisJobMetadataWritable loadArchivedJob(String jobId) throws IOException {
        File jobFile = getJobFileName(archiveDir, jobId);
        MantisJobMetadataWritable job = null;
        if (jobFile.exists()) {
            try (FileInputStream fis = new FileInputStream(jobFile)) {
                job = mapper.readValue(fis, MantisJobMetadataWritable.class);
            }
            for (MantisStageMetadataWritable stage : loadArchivedJobStages(jobId))
                job.addJobStageIfAbsent(stage);
            for (MantisWorkerMetadataWritable worker : loadArchivedJobWorkers(jobId, job.getNextWorkerNumberToUse())) {
                try {
                    job.addWorkerMedata(worker.getStageNum(), worker, null);
                } catch (InvalidJobException e) {
                    logger.warn("Unexpected error adding worker index=" + worker.getWorkerIndex() + ", number=" +
                            worker.getWorkerNumber() + " for job " + jobId + ": " + e.getMessage(), e);
                }
            }
        }
        return job;
    }

    private List<MantisStageMetadataWritable> loadArchivedJobStages(String jobId) throws IOException {
        List<MantisStageMetadataWritable> result = new LinkedList<>();
        for (File jobFile : archiveDir.listFiles((dir, name) -> {
            return name.startsWith("Stage-" + jobId + "-");
        })) {
            try (FileInputStream fis = new FileInputStream(jobFile)) {
                result.add(mapper.readValue(fis, MantisStageMetadataWritable.class));
            }
        }
        return result;
    }

    private List<MantisWorkerMetadataWritable> loadArchivedJobWorkers(String jobId, int maxWorkerNumber) throws IOException {
        List<MantisWorkerMetadataWritable> result = new LinkedList<>();
        for (File wFile : archiveDir.listFiles((dir, name) -> {
            return name.startsWith("Worker-" + jobId + "-");
        })) {
            try (FileInputStream fis = new FileInputStream(wFile)) {
                result.add(mapper.readValue(fis, MantisWorkerMetadataWritable.class));
            }
        }
        return result;
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

    private static void deleteFiles(File rootDir, final String jobId, final String filePrefix) {
        for (File stageFile : rootDir.listFiles((dir, name) -> {
            return name.startsWith(filePrefix + jobId + "-");
        })) {
            stageFile.delete();
        }
    }

    @Override
    public void storeMantisStage(MantisStageMetadataWritable msmd) throws IOException {
        storeStage(msmd, false);
    }

    private void storeStage(MantisStageMetadataWritable msmd, boolean rewrite) throws IOException {
        File stageFile = getStageFileName(spoolDir, msmd.getJobId(), msmd.getStageNum());
        if (rewrite)
            stageFile.delete();
        try {stageFile.createNewFile();} catch (SecurityException se) {
            throw new IOException("Can't create new file " + stageFile.getAbsolutePath(), se);
        }
        try (PrintWriter pwrtr = new PrintWriter(stageFile)) {
            mapper.writeValue(pwrtr, msmd);
        }
    }

    @Override
    public void updateMantisStage(MantisStageMetadataWritable msmd) throws IOException {
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
    public void storeWorker(MantisWorkerMetadataWritable workerMetadata)
            throws IOException {
        storeWorker(workerMetadata.getJobId(), workerMetadata, false);
    }

    @Override
    public void storeWorkers(String jobId, List<MantisWorkerMetadataWritable> workers)
            throws IOException {
        for (MantisWorkerMetadataWritable w : workers)
            storeWorker(w);
    }

    @Override
    public void storeAndUpdateWorkers(MantisWorkerMetadataWritable worker1, MantisWorkerMetadataWritable worker2)
            throws InvalidJobException, IOException {
        if (!worker1.getJobId().equals(worker2.getJobId()))
            throw new InvalidJobException(worker1.getJobId());
        // As the name indicates, this is a simple storage implementation that does not actually have the
        // atomicity. Instead, we update worker2, followed by storing worker1
        updateWorker(worker2);
        storeWorker(worker1);
    }

    @Override
    public void updateWorker(MantisWorkerMetadataWritable mwmd) throws IOException {
        storeWorker(mwmd.getJobId(), mwmd, true);
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

    @Override
    public List<MantisJobMetadataWritable> initJobs() throws IOException {
        createDir(spoolDir);
        createDir(archiveDir);
        List<MantisJobMetadataWritable> retList = new ArrayList<>();
        File spoolDirFile = spoolDir;
        for (File jobFile : spoolDirFile.listFiles((dir, name) -> {
            return name.startsWith("Job-");
        })) {
            try (FileInputStream fis = new FileInputStream(jobFile)) {
                MantisJobMetadataWritable mjmd = mapper.readValue(fis, MantisJobMetadataWritable.class);
                for (MantisStageMetadataWritable msmd : readStagesFor(spoolDirFile, mjmd.getJobId()))
                    mjmd.addJobStageIfAbsent(msmd);
                for (MantisWorkerMetadataWritable mwmd : readWorkersFor(spoolDirFile, mjmd.getJobId()))
                    mjmd.addWorkerMedata(mwmd.getStageNum(), mwmd, null);
                retList.add(mjmd);
            } catch (IOException e) {
                logger.error("Error reading job metadata - " + e.getMessage());
            } catch (InvalidJobException e) {
                // shouldn't happen
                logger.warn(e.getMessage());
            }
        }
        boolean _debug = false;
        if (_debug) {
            // print all jobs read
            for (MantisJobMetadata mjmd : retList) {
                logger.info("  JOB " + mjmd.getJobId());
                for (MantisStageMetadata msmd : mjmd.getStageMetadata()) {
                    logger.info("      Stage " + msmd.getStageNum() + " of " + msmd.getNumStages());
                    for (MantisWorkerMetadata mwmd : msmd.getWorkerByIndexMetadataSet()) {
                        logger.info("        " + mwmd);
                    }
                }
            }
        }
        return retList;
    }

    @Override
    public Observable<MantisJobMetadata> initArchivedJobs() {
        return Observable.create(subscriber -> {
            for (File jobFile : archiveDir.listFiles((dir, name) -> {
                return name.startsWith("Job-");
            })) {
                try (FileInputStream fis = new FileInputStream(jobFile)) {
                    MantisJobMetadataWritable job = mapper.readValue(fis, MantisJobMetadataWritable.class);
                    for (MantisStageMetadataWritable msmd : readStagesFor(archiveDir, job.getJobId()))
                        job.addJobStageIfAbsent(msmd);
                    for (MantisWorkerMetadataWritable mwmd : readWorkersFor(archiveDir, job.getJobId())) {
                        try {
                            job.addWorkerMedata(mwmd.getStageNum(), mwmd, null);
                        } catch (InvalidJobException e) {
                            // shouldn't happen
                        }
                    }
                    subscriber.onNext(job);
                } catch (IOException e) {
                    subscriber.onError(e);
                }
            }
            subscriber.onCompleted();
        });
    }

    @Override
    public List<NamedJob> initNamedJobs() throws IOException {
        createDir(namedJobsDir);
        List<NamedJob> returnList = new ArrayList<>();
        for (File namedJobFile : namedJobsDir.listFiles(
                (dir, name) -> !name.endsWith(NAMED_JOBS_COMPLETED_JOBS_FILE_NAME_SUFFIX)
        )) {
            try (FileInputStream fis = new FileInputStream(namedJobFile)) {
                returnList.add(mapper.readValue(fis, NamedJob.class));
            }
        }
        return returnList;
    }

    @Override
    public Observable<NamedJob.CompletedJob> initNamedJobCompletedJobs() throws IOException {
        createDir(namedJobsDir);
        List<NamedJob> returnList = new ArrayList<>();
        return Observable.create(subscriber -> {
            for (File namedJobFile : namedJobsDir.listFiles(
                    (dir, name) -> name.endsWith(NAMED_JOBS_COMPLETED_JOBS_FILE_NAME_SUFFIX)
            )) {
                try (FileInputStream fis = new FileInputStream(namedJobFile)) {
                    final List<NamedJob.CompletedJob> list =
                            mapper.readValue(fis, new TypeReference<List<NamedJob.CompletedJob>>() {});
                    if (list != null && !list.isEmpty())
                        list.forEach(subscriber::onNext);
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
            subscriber.onCompleted();
        });
    }

    @Override
    public void shutdown() {
        // no clean up needed
    }

    private void storeWorker(String jobId, MantisWorkerMetadataWritable workerMetadata, boolean rewrite)
            throws IOException {
        File workerFile = getWorkerFilename(spoolDir, jobId, workerMetadata.getWorkerIndex(), workerMetadata.getWorkerNumber());
        if (rewrite)
            workerFile.delete();
        workerFile.createNewFile();
        try (PrintWriter pwrtr = new PrintWriter(workerFile)) {
            mapper.writeValue(pwrtr, workerMetadata);
        }
    }

    private List<MantisStageMetadataWritable> readStagesFor(File spoolDir, final String id) throws IOException {
        List<MantisStageMetadataWritable> stageList = new ArrayList<>();
        for (File stageFile : spoolDir.listFiles((dir, name) -> {
            return name.startsWith("Stage-" + id + "-");
        })) {
            logger.info("Reading stage file " + stageFile.getName());
            try (FileInputStream fis = new FileInputStream(stageFile)) {
                stageList.add(mapper.readValue(fis, MantisStageMetadataWritable.class));
            }
        }
        return stageList;
    }

    private List<MantisWorkerMetadataWritable> readWorkersFor(File spoolDir, final String id) {
        List<MantisWorkerMetadataWritable> workerList = new ArrayList<>();
        for (File workerFile : spoolDir.listFiles((dir, name) -> {
            return name.startsWith("Worker-" + id + "-");
        })) {
            logger.info("Reading worker file " + workerFile.getName());
            try (FileInputStream fis = new FileInputStream(workerFile)) {
                workerList.add(mapper.readValue(fis, MantisWorkerMetadataWritable.class));
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

    @Override
    public void archiveWorker(MantisWorkerMetadataWritable mwmd) throws IOException {
        File wFile = getWorkerFilename(spoolDir, mwmd.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber());
        if (wFile.exists())
            wFile.renameTo(getWorkerFilename(archiveDir, mwmd.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()));
    }

    public List<MantisWorkerMetadataWritable> getArchivedWorkers(final String jobid)
            throws IOException {
        List<MantisWorkerMetadataWritable> workerList = new ArrayList<>();
        for (File workerFile : archiveDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("Worker-" + jobid + "-");
            }
        })) {
            try (FileInputStream fis = new FileInputStream(workerFile)) {
                workerList.add(mapper.readValue(fis, MantisWorkerMetadataWritable.class));
            }
        }
        return workerList;
    }

    private File getNamedJobFileName(String name) {
        return new File(namedJobsDir, name + ".job");
    }

    @Override
    public void storeNewNamedJob(NamedJob namedJob) throws JobNameAlreadyExistsException, IOException {
        File tmpFile = getNamedJobFileName(namedJob.getName());
        logger.info("Storing job cluster " + namedJob.getName() + " to file " + tmpFile.getAbsolutePath());
        if (!tmpFile.createNewFile())
            throw new JobNameAlreadyExistsException(namedJob.getName());
        try (PrintWriter pwrtr = new PrintWriter(tmpFile)) {
            mapper.writeValue(pwrtr, namedJob);
        }
    }

    @Override
    public void updateNamedJob(NamedJob namedJob) throws InvalidNamedJobException, IOException {
        File jobFile = getNamedJobFileName(namedJob.getName());
        if (!jobFile.exists())
            throw new InvalidNamedJobException(namedJob.getName() + " doesn't exist");
        jobFile.delete();
        jobFile.createNewFile();
        try (PrintWriter pwrtr = new PrintWriter(jobFile)) {
            mapper.writeValue(pwrtr, namedJob);
        }
    }

    @Override
    public boolean deleteNamedJob(String name) throws IOException {
        File jobFile = getNamedJobFileName(name);
        final boolean deleted = jobFile.delete();
        File completedJobsFile = new File(namedJobsDir, name + NAMED_JOBS_COMPLETED_JOBS_FILE_NAME_SUFFIX);
        completedJobsFile.delete();
        return deleted;
    }

    @Override
    public void storeCompletedJobForNamedJob(String name, NamedJob.CompletedJob job) throws IOException {
        modifyCompletedJobsForNamedJob(name, list -> list.add(job));
    }

    private void modifyCompletedJobsForNamedJob(String name, Action1<List<NamedJob.CompletedJob>> modifier)
            throws IOException {
        File completedJobsFile = new File(namedJobsDir, name + NAMED_JOBS_COMPLETED_JOBS_FILE_NAME_SUFFIX);
        List<NamedJob.CompletedJob> completedJobs = new LinkedList<>();
        if (completedJobsFile.exists()) {
            try (FileInputStream fis = new FileInputStream(completedJobsFile)) {
                completedJobs.addAll(mapper.readValue(fis, new TypeReference<List<NamedJob.CompletedJob>>() {}));
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
    public void removeCompledtedJobForNamedJob(String name, String jobId) throws IOException {
        modifyCompletedJobsForNamedJob(name, list -> {
            if (list != null) {
                final Iterator<NamedJob.CompletedJob> iterator = list.iterator();
                while (iterator.hasNext()) {
                    final NamedJob.CompletedJob next = iterator.next();
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
