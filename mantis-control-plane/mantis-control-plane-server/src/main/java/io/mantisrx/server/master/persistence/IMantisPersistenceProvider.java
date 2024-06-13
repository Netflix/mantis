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
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.JobArtifact;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import rx.Observable;


/**
 * A way to persist mantis master related metadata to a durable storage.
 * See {@link KeyValueBasedPersistenceProvider} for how mantis job cluster,
 * mantis job info is persisted to a key-value based storage (like cassandra)
 */
public interface IMantisPersistenceProvider {


    void storeNewJob(IMantisJobMetadata jobMetadata) throws Exception;

    void updateJob(final IMantisJobMetadata jobMetadata) throws Exception;

    /**
     * //     * Mark the job as not active and move it to an inactive archived collection of jobs.
     * //     * @param jobId The Job Id of the job to archive
     * //     * @throws IOException upon errors with storage invocation
     * //
     */
    void archiveJob(final String jobId) throws IOException;

    void deleteJob(String jobId) throws IOException;

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
    default void storeWorker(final IMantisWorkerMetadata workerMetadata) throws IOException {
        storeWorkers(Collections.singletonList(workerMetadata));
    }

    /**
     * Store multiple new workers for the give job. This is called only once for a given worker. This method enables
     * optimization by calling storage once for multiple workers.
     *
     * @param jobId   The Job ID.
     * @param workers The list of workers to store.
     *
     * @throws IOException if there were errors storing the workers.
     */
    default void storeWorkers(final String jobId, final List<IMantisWorkerMetadata> workers) throws IOException {
        storeWorkers(workers);
    }

    void storeWorkers(final List<IMantisWorkerMetadata> workers) throws IOException;

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

    /**
     * Initialize and return all existing NamedJobs from persistence.
     *
     * @return List of {@link io.mantisrx.server.master.store.NamedJob} objects.
     * @throws IOException Upon error connecting to or reading from persistence.
     */
    List<IJobClusterMetadata> loadAllJobClusters() throws IOException;

    /**
     * load all completed jobs for a given cluster sorted by descending order of job id
     * @param name name of cluster
     * @param limit max number of jobs to return
     * @param startJobIdExclusive if not null, start from this job id
     * @return list of completed jobs
     * @throws IOException upon errors with storage invocation
     */
    List<CompletedJob> loadLatestCompletedJobsForCluster(String name, int limit, @Nullable JobId startJobIdExclusive) throws IOException;

    void archiveWorker(IMantisWorkerMetadata mwmd) throws IOException;

    List<IMantisWorkerMetadata> getArchivedWorkers(String jobId) throws IOException;


    void createJobCluster(IJobClusterMetadata jobCluster) throws Exception;

    void updateJobCluster(IJobClusterMetadata jobCluster) throws Exception;

    void deleteJobCluster(String name) throws Exception;

    void storeCompletedJobForCluster(String name, CompletedJob job) throws IOException;

    void deleteCompletedJobsForCluster(String name) throws IOException;

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

    boolean isArtifactExists(String resourceId) throws IOException;

    JobArtifact getArtifactById(String resourceId) throws IOException;

    List<JobArtifact> listJobArtifacts(String name, String version) throws IOException;

    void addNewJobArtifactsToCache(ClusterID clusterID, List<ArtifactID> artifacts) throws IOException;

    void removeJobArtifactsToCache(ClusterID clusterID, List<ArtifactID> artifacts) throws IOException;

    List<String> listJobArtifactsToCache(ClusterID clusterID) throws IOException;

    List<String> listJobArtifactsByName(String prefix, String contains) throws IOException;

    void addNewJobArtifact(JobArtifact jobArtifact) throws IOException;

    /**
     * Register and save the given cluster spec. Once the returned CompletionStage
     * finishes successfully the given cluster should be available in list cluster response.
     */
    ResourceClusterSpecWritable registerAndUpdateClusterSpec(ResourceClusterSpecWritable spec) throws IOException;

    RegisteredResourceClustersWritable deregisterCluster(ClusterID clusterId) throws IOException;

    RegisteredResourceClustersWritable getRegisteredResourceClustersWritable() throws IOException;

    @Nullable
    ResourceClusterSpecWritable getResourceClusterSpecWritable(ClusterID id) throws IOException;

    ResourceClusterScaleRulesWritable getResourceClusterScaleRules(ClusterID clusterId) throws IOException;

    ResourceClusterScaleRulesWritable registerResourceClusterScaleRule(
        ResourceClusterScaleRulesWritable ruleSpec) throws IOException;

    ResourceClusterScaleRulesWritable registerResourceClusterScaleRule(ResourceClusterScaleSpec rule) throws IOException;
}
