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

package io.mantisrx.master.resourcecluster;

import io.mantisrx.master.resourcecluster.ResourceClusterActor.BestFit;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetActiveJobsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A component to manage the states of {@link TaskExecutorState} for a given {@link ResourceClusterActor}.
 */
interface ExecutorStateManager {
    /**
     * Store and track the given task executor's state inside this {@link ExecutorStateManager} if there is no existing
     * state already. Ignore the given state instance if there is already a state associated with the given ID.
     * @param taskExecutorID TaskExecutorID
     * @param state new task executor state
     */
    void trackIfAbsent(TaskExecutorID taskExecutorID, TaskExecutorState state);

    /**
     * Try to mark the given task executor as available if its tracked state is available.
     * @param taskExecutorID TaskExecutorID
     * @return whether the given task executor became available.
     */
    boolean tryMarkAvailable(TaskExecutorID taskExecutorID);

    /**
     * Try to mark the given task executor as unavailable.
     * @param taskExecutorID TaskExecutorID
     */
    boolean tryMarkUnavailable(TaskExecutorID taskExecutorID);

    @Nullable
    TaskExecutorState get(TaskExecutorID taskExecutorID);

    @Nullable
    TaskExecutorState getIncludeArchived(TaskExecutorID taskExecutorID);

    @Nullable
    TaskExecutorState archive(TaskExecutorID taskExecutorID);

    ResourceOverview getResourceOverview();

    GetClusterUsageResponse getClusterUsage(GetClusterUsageRequest req);

    List<TaskExecutorID> getIdleInstanceList(GetClusterIdleInstancesRequest req);

    List<TaskExecutorID> getTaskExecutors(Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate);

    List<String> getActiveJobs(GetActiveJobsRequest req);

    Optional<Entry<TaskExecutorID, TaskExecutorState>> findFirst(
        Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate);

    /**
     * Finds set of matched task executors best fitting the given assignment requests.
     *
     * @param request Assignment request.
     * @return Optional of matched task executors.
     */
    Optional<BestFit> findBestFit(TaskExecutorBatchAssignmentRequest request);

    Set<Entry<TaskExecutorID, TaskExecutorState>> getActiveExecutorEntry();

    Predicate<Entry<TaskExecutorID, TaskExecutorState>> isRegistered =
        e -> e.getValue().isRegistered();

    Predicate<Entry<TaskExecutorID, TaskExecutorState>> isBusy =
        e -> e.getValue().isRunningTask();

    Predicate<Entry<TaskExecutorID, TaskExecutorState>> unregistered =
        e -> e.getValue().isDisconnected();

    Predicate<Entry<TaskExecutorID, TaskExecutorState>> isAvailable =
        e -> e.getValue().isAvailable();

    Predicate<Entry<TaskExecutorID, TaskExecutorState>> isDisabled =
        e -> e.getValue().isDisabled() && e.getValue().isRegistered();

    Predicate<Entry<TaskExecutorID, TaskExecutorState>> isAssigned =
        e -> e.getValue().isAssigned();
}
