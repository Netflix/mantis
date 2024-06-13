/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.master.resourcecluster;

import io.mantisrx.common.Ack;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.worker.TaskExecutorGateway;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Abstraction to deal with all interactions with the resource cluster such as
 * 1). listing the set of task executors registered
 * 2). listing the set of task executors available
 * 3). listing the set of task executors busy
 * 4). get the current state of a task executor
 * 5). get the current state of the system
 * 6). assign a task executor for a given worker
 */
public interface ResourceCluster extends ResourceClusterGateway {
    /**
     * Get the name of the resource cluster
     *
     * @return name of the resource cluster
     */
    String getName();

    /**
     * API that gets invoked when the resource cluster migrates from one machine to another and needs to be initialized.
     *
     * @param taskExecutorID taskExecutorID that was originally running the worker
     * @param workerId       workerID of the task that being run on the task executor
     * @return Ack when the initialization is done
     */
    CompletableFuture<Ack> initializeTaskExecutor(TaskExecutorID taskExecutorID, WorkerId workerId);

    default CompletableFuture<List<TaskExecutorID>> getRegisteredTaskExecutors() {
        return getRegisteredTaskExecutors(Collections.emptyMap());
    }

    /**
     * Get the registered set of task executors
     * @param attributes attributes to filter out the set of task executors to be considered for registration.
     * @return the list of task executor IDs
     */
    CompletableFuture<List<TaskExecutorID>> getRegisteredTaskExecutors(Map<String, String> attributes);

    default CompletableFuture<List<TaskExecutorID>> getAvailableTaskExecutors() {
        return getAvailableTaskExecutors(Collections.emptyMap());
    }

    /**
     * Get the available set of task executors
     *
     * @param attributes attributes to filter out the set of task executors to be considered for availability.
     * @return the list of task executor IDs
     */
    CompletableFuture<List<TaskExecutorID>> getAvailableTaskExecutors(Map<String, String> attributes);

    default CompletableFuture<List<TaskExecutorID>> getBusyTaskExecutors() {
        return getBusyTaskExecutors(Collections.emptyMap());
    }

    CompletableFuture<List<TaskExecutorID>> getBusyTaskExecutors(Map<String, String> attributes);

    CompletableFuture<List<TaskExecutorID>> getDisabledTaskExecutors(Map<String, String> attributes);

    default CompletableFuture<List<TaskExecutorID>> getUnregisteredTaskExecutors() {
        return getUnregisteredTaskExecutors(Collections.emptyMap());
    }

    CompletableFuture<List<TaskExecutorID>> getUnregisteredTaskExecutors(Map<String, String> attributes);

    CompletableFuture<ResourceOverview> resourceOverview();

    CompletableFuture<Ack> addNewJobArtifactsToCache(ClusterID clusterID, List<ArtifactID> artifacts);

    CompletableFuture<Ack> markTaskExecutorWorkerCancelled(WorkerId workerId);

    CompletableFuture<Ack> removeJobArtifactsToCache(List<ArtifactID> artifacts);

    CompletableFuture<List<ArtifactID>> getJobArtifactsToCache();

    /**
     * Can throw {@link NoResourceAvailableException} wrapped within the CompletableFuture in case there
     * are no enough available task executors.
     *
     * @param allocationRequests set of machine definitions requested for 1 or more workers
     * @return task executors assigned for the particular request
     */
    CompletableFuture<Map<TaskExecutorAllocationRequest, TaskExecutorID>> getTaskExecutorsFor(
        Set<TaskExecutorAllocationRequest> allocationRequests);

    /**
     * Returns the Gateway instance to talk to the task executor. If unable to make connection with
     * the task executor, then a ConnectionFailedException is thrown wrapped inside the future.
     *
     * @param taskExecutorID executor for which the gateway is requested
     * @return gateway corresponding to the executor wrapped inside a future.
     */
    CompletableFuture<TaskExecutorGateway> getTaskExecutorGateway(TaskExecutorID taskExecutorID);

    CompletableFuture<TaskExecutorRegistration> getTaskExecutorInfo(String hostName);

    /**
     * Gets the task executor's ID that's currently either running or is assigned for the given
     * workerId
     *
     * @param workerId workerId whose current task executor is needed
     * @return TaskExecutorID
     */
    CompletableFuture<TaskExecutorID> getTaskExecutorAssignedFor(WorkerId workerId);

    CompletableFuture<TaskExecutorRegistration> getTaskExecutorInfo(TaskExecutorID taskExecutorID);

    CompletableFuture<TaskExecutorStatus> getTaskExecutorState(TaskExecutorID taskExecutorID);

    /**
     * Trigger a request to this resource cluster's ResourceClusterScalerActor to refresh the local scale rule set.
     */
    CompletableFuture<Ack> refreshClusterScalerRuleSet();

    /**
     * Disables task executors that match the passed set of attributes
     *
     * @param attributes attributes that need to be present in the task executor's set of
     *                   attributes.
     * @param expiry     instant at which the request can be marked as complete. this is important
     *                   because we cannot be constantly checking if new task executors match the
     *                   disabled criteria or not.
     * @return a future that completes when the underlying operation is registered by the system
     */
    CompletableFuture<Ack> disableTaskExecutorsFor(Map<String, String> attributes, Instant expiry, Optional<TaskExecutorID> taskExecutorID);

    /**
     * Enables/Disables scaler for a given skuID of a given clusterID
     *
     * @param skuID   skuID whom scaler will be enabled/disabled.
     * @param enabled whether the scaler will be enabled/disabled.
     * @return a future that completes when the underlying operation is registered by the system
     */
    CompletableFuture<Ack> setScalerStatus(ClusterID clusterID, ContainerSkuID skuID,
        Boolean enabled, Long expirationDurationInSeconds);

    /**
     * Get a paged result of all active jobs associated with this resource cluster.
     *
     * @param startingIndex Starting index for the paged list of all the active jobs.
     * @param pageSize      Max size of returned paged list.
     * @return PagedActiveJobOverview instance.
     */
    CompletableFuture<PagedActiveJobOverview> getActiveJobOverview(
        Optional<Integer> startingIndex,
        Optional<Integer> pageSize);

    /**
     * Gets the task executors to worker mapping for the given resource cluster
     *
     * @return a future mapping task executor IDs to the work they are doing
     */
    CompletableFuture<Map<TaskExecutorID, WorkerId>> getTaskExecutorWorkerMapping();

    /**
     * Gets the task executors to worker mapping for all task executors in the resource cluster that
     * match the filtering criteria as represented by the attributes.
     *
     * @param attributes filtering criteria
     * @return a future mapping task executor IDs to the work they are doing
     */
    CompletableFuture<Map<TaskExecutorID, WorkerId>> getTaskExecutorWorkerMapping(
        Map<String, String> attributes);

    class NoResourceAvailableException extends Exception {

        public NoResourceAvailableException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown to indicate unable to make a connection
     */
    static class ConnectionFailedException extends Exception {

        private static final long serialVersionUID = 1L;

        public ConnectionFailedException(Throwable cause) {
            super(cause);
        }
    }

    @Value
    class ResourceOverview {

        long numRegisteredTaskExecutors;
        long numAvailableTaskExecutors;
        long numOccupiedTaskExecutors;
        long numAssignedTaskExecutors;
        long numDisabledTaskExecutors;
    }

    @Value
    class TaskExecutorStatus {

        TaskExecutorRegistration registration;
        boolean registered;
        boolean runningTask;
        boolean assignedTask;
        boolean disabled;
        @Nullable
        WorkerId workerId;
        long lastHeartbeatInMs;
        @Nullable
        WorkerId cancelledWorkerId; // exposing this to allow better testing
    }

    /**
     * Exception when asked {@link TaskExecutorID} cannot be found in control plane.
     */
    static class TaskExecutorNotFoundException extends Exception {

        private static final long serialVersionUID = 2913026730940135991L;

        public TaskExecutorNotFoundException(TaskExecutorID taskExecutorID) {
            super("TaskExecutor " + taskExecutorID + " not found");
        }
    }
}
