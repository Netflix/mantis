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
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import rx.Observable;


public interface IMantisStorageProvider {


    void storeNewJob(IMantisJobMetadata jobMetadata) throws Exception;

    void updateJob(final IMantisJobMetadata jobMetadata) throws Exception;

    /**
     * //     * Mark the job as not active and move it to an inactive archived collection of jobs.
     * //     * @param jobId The Job Id of the job to archive
     * //     * @throws IOException upon errors with storage invocation
     * //
     */
    void archiveJob(final String jobId) throws IOException;

    void deleteJob(String jobId) throws Exception;

    void storeMantisStage(final IMantisStageMetadata msmd) throws IOException;

    void updateMantisStage(final IMantisStageMetadata msmd) throws IOException;


    /**
     * Store a new worker for the given job and stage number. This will be called only once for a given
     * worker. However, it is possible that concurrent calls can be made on a <code>jobId</code>, each with a
     * different worker.
     *
     * @param workerMetadata The worker metadata to store.
     *
     * @throws IOException
     */
    void storeWorker(final IMantisWorkerMetadata workerMetadata)
            throws IOException;

    /**
     * Store multiple new workers for the give job. This is called only once for a given worker. This method enables
     * optimization by calling storage once for multiple workers.
     *
     * @param jobId   The Job ID.
     * @param workers The list of workers to store.
     *
     * @throws IOException if there were errors storing the workers.
     */
    void storeWorkers(final String jobId, final List<IMantisWorkerMetadata> workers)
            throws IOException;


    /**
     * Store a new worker and update existing worker of a job atomically. Either both are stored or none is.
     *
     * @param existingWorker Existing worker to update.
     * @param newWorker      New worker to store.
     *
     * @throws IOException
     * @throws InvalidJobException If workers don't have the same JobId.
     * @throws Exception
     */
    void storeAndUpdateWorkers(final IMantisWorkerMetadata existingWorker, final IMantisWorkerMetadata newWorker)
            throws InvalidJobException, IOException, Exception;

    void updateWorker(IMantisWorkerMetadata worker) throws IOException;


    List<IMantisJobMetadata> loadAllJobs() throws IOException;

    Observable<IMantisJobMetadata> loadAllArchivedJobs();

    //  /**
    //  * Initialize and return all existing NamedJobs from persistence.
    //  * @return List of {@link NamedJob} objects.
    //  * @throws IOException Upon error connecting to or reading from persistence.
    //  */
    List<IJobClusterMetadata> loadAllJobClusters() throws IOException;

    List<CompletedJob> loadAllCompletedJobs() throws IOException;

    void archiveWorker(IMantisWorkerMetadata mwmd) throws IOException;

    List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) throws IOException;


    void createJobCluster(IJobClusterMetadata jobCluster) throws Exception;

    void updateJobCluster(IJobClusterMetadata jobCluster) throws Exception;

    void deleteJobCluster(String name) throws Exception;

    void storeCompletedJobForCluster(String name, CompletedJob job) throws IOException;


    void removeCompletedJobForCluster(String name, String jobId) throws IOException;

    Optional<IMantisJobMetadata> loadArchivedJob(String jobId) throws IOException;

    //////////////////////////////////

    // Optional<IJobClusterMetadata> getJobCluster(String clusterName) throws Exception;

    //Optional<IJobClusterMetadata> loadJobCluster(String clusterName);

    //
    //    CompletionStage<Void> shutdown();
    //
    List<String> initActiveVmAttributeValuesList() throws IOException;

    void setActiveVmAttributeValuesList(final List<String> vmAttributesList) throws IOException;

    TaskExecutorRegistration getTaskExecutorFor(TaskExecutorID taskExecutorID) throws IOException;

    void storeNewTaskExecutor(TaskExecutorRegistration registration) throws IOException;

    void storeNewDisableTaskExecutorRequest(DisableTaskExecutorsRequest request) throws IOException;

    void deleteExpiredDisableTaskExecutorRequest(DisableTaskExecutorsRequest request) throws IOException;

    List<DisableTaskExecutorsRequest> loadAllDisableTaskExecutorsRequests(ClusterID clusterID) throws IOException;
}
