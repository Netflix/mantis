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

import io.mantisrx.common.WorkerConstants;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.BestFit;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetActiveJobsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.GetClusterUsageResponseBuilder;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.base.Splitter;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import io.mantisrx.shaded.com.google.common.cache.RemovalListener;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
class ExecutorStateManagerImpl implements ExecutorStateManager {
    private final Map<TaskExecutorID, TaskExecutorState> taskExecutorStateMap = new HashMap<>();

    /**
     * Cache the available executors ready to accept assignments. Note these executors' state are not strongly
     * synchronized and requires state level check when matching.
     */
    private final Map<SchedulingConstraints, NavigableSet<TaskExecutorHolder>> executorsBySchedulingConstraints = new HashMap<>();

    /**
     * A map holding the assignment attributes as keys and their corresponding default values.
     * These assignment attributes are used during worker scheduling. The keys correspond to attributes
     * like jdk or springBoot version, and if any of these attributes is not explicitly provided
     * or not found during allocation, the associated default value from this map is used.
     */
    private final Map<String, String> assignmentAttributesAndDefaults;

    private final Cache<String, JobRequirements> pendingJobRequests = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .removalListener((RemovalListener<String, JobRequirements>) notification -> {
            log.info("Removing key {} from pending job requests due to reason {}", notification.getKey(), notification.getCause());
        })
        .build();

    private final Cache<TaskExecutorID, TaskExecutorState> archivedState = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(24, TimeUnit.HOURS)
        .removalListener(notification ->
            log.info("Archived TaskExecutor: {} removed due to: {}", notification.getKey(), notification.getCause()))
        .build();

    public ExecutorStateManagerImpl(String assignmentAttributesAndDefaults) {
        this.assignmentAttributesAndDefaults = assignmentAttributesAndDefaults.isEmpty() ? ImmutableMap.of() :Splitter.on(",").withKeyValueSeparator(':').split(assignmentAttributesAndDefaults);
    }

    @ToString
    class JobRequirements {
        public final Map<SchedulingConstraints, Integer> constraintsToWorkerCount;

        public JobRequirements(Map<SchedulingConstraints, Integer> constraintsToWorkerCount) {
            this.constraintsToWorkerCount = constraintsToWorkerCount
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> findBestFitAllocationConstraints(entry.getKey()).orElseGet(entry::getKey), Entry::getValue));
        }

        public int getTotalWorkers() {
            return constraintsToWorkerCount.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    @Override
    public void trackIfAbsent(TaskExecutorID taskExecutorID, TaskExecutorState state) {
        this.taskExecutorStateMap.putIfAbsent(taskExecutorID, state);
        if (this.archivedState.getIfPresent(taskExecutorID) != null) {
            log.info("Reviving archived executor: {}", taskExecutorID);
            this.archivedState.invalidate(taskExecutorID);
        }

        tryMarkAvailable(taskExecutorID, state);
    }

    /**
     * Add to buckets. however new executors won't have valid registration at this moment and requires marking
     * again later when registration is ready.
     * @param taskExecutorID taskExecutorID
     * @param state state
     * @return whether the target executor is marked as available.
     */
    private boolean tryMarkAvailable(TaskExecutorID taskExecutorID, TaskExecutorState state) {
        if (state.isAvailable() && state.getRegistration() != null) {
            TaskExecutorHolder teHolder = TaskExecutorHolder.of(taskExecutorID, state.getRegistration());
            log.debug("Marking executor {} as available for matching.", teHolder);
            SchedulingConstraints schedulingConstraints = state.getRegistration().getSchedulingConstraints(assignmentAttributesAndDefaults);
            if (!this.executorsBySchedulingConstraints.containsKey(schedulingConstraints)) {
                log.info("[executorsBySchedulingConstraints] adding {} from TE: {}", schedulingConstraints, teHolder);
                this.executorsBySchedulingConstraints.putIfAbsent(
                    schedulingConstraints,
                    new TreeSet<>(TaskExecutorHolder.generationFirstComparator));
            }

            log.info("Assign {} to available.", teHolder.getId());
            return this.executorsBySchedulingConstraints.get(schedulingConstraints).add(teHolder);
        }
        else {
            log.debug("Ignore unavailable TE: {}", taskExecutorID);
            return false;
        }
    }

    @Override
    public boolean tryMarkAvailable(TaskExecutorID taskExecutorID) {
        if (!this.taskExecutorStateMap.containsKey(taskExecutorID)) {
            log.warn("marking invalid executor as available: {}", taskExecutorID);
            return false;
        }

        TaskExecutorState taskExecutorState = this.taskExecutorStateMap.get(taskExecutorID);
        return tryMarkAvailable(taskExecutorID, taskExecutorState);
    }

    @Override
    public boolean tryMarkUnavailable(TaskExecutorID taskExecutorID) {
        if (this.taskExecutorStateMap.containsKey(taskExecutorID)) {
            TaskExecutorState taskExecutorState = this.taskExecutorStateMap.get(taskExecutorID);
            if (taskExecutorState.getRegistration() != null) {
                SchedulingConstraints constraints = taskExecutorState.getRegistration().getSchedulingConstraints(assignmentAttributesAndDefaults);
                if (this.executorsBySchedulingConstraints.containsKey(constraints)) {
                    this.executorsBySchedulingConstraints.get(constraints)
                        .remove(TaskExecutorHolder.of(taskExecutorID, taskExecutorState.getRegistration()));
                }
                return true;
            }
        }

        // todo: check archive map as well?
        log.warn("invalid task executor to mark as unavailable: {}", taskExecutorID);
        return false;
    }

    @Override
    public ResourceOverview getResourceOverview() {
        long numRegistered = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isRegistered).count();
        long numAvailable = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isAvailable).count();
        long numOccupied = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isRunningTask).count();
        long numAssigned = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isAssigned).count();
        long numDisabled = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isDisabled).count();

        return new ResourceOverview(numRegistered, numAvailable, numOccupied, numAssigned, numDisabled);
    }

    @Override
    public List<TaskExecutorID> getIdleInstanceList(GetClusterIdleInstancesRequest req) {
        return this.taskExecutorStateMap.entrySet().stream()
            .filter(kv -> {
                if (kv.getValue().getRegistration() == null) {
                    return false;
                }

                Optional<ContainerSkuID> skuIdO =
                    kv.getValue().getRegistration().getTaskExecutorContainerDefinitionId();
                return skuIdO.isPresent() && skuIdO.get().equals(req.getSkuId());
            })
            .filter(isAvailable)
            .map(Entry::getKey)
            .limit(req.getMaxInstanceCount())
            .collect(Collectors.toList());
    }

    @Override
    public TaskExecutorState get(TaskExecutorID taskExecutorID) {
        return this.taskExecutorStateMap.get(taskExecutorID);
    }

    @Override
    public TaskExecutorState getIncludeArchived(TaskExecutorID taskExecutorID) {
        if (this.taskExecutorStateMap.containsKey(taskExecutorID)) {
            return this.taskExecutorStateMap.get(taskExecutorID);
        }
        return this.archivedState.getIfPresent(taskExecutorID);
    }

    @Override
    public TaskExecutorState archive(TaskExecutorID taskExecutorID) {
        if (this.taskExecutorStateMap.containsKey(taskExecutorID)) {
            this.archivedState.put(taskExecutorID, this.taskExecutorStateMap.get(taskExecutorID));
            this.taskExecutorStateMap.remove(taskExecutorID);
            return this.archivedState.getIfPresent(taskExecutorID);
        }
        else {
            log.warn("archiving invalid TaskExecutor: {}", taskExecutorID);
            return null;
        }
    }

    @Override
    public List<TaskExecutorID> getTaskExecutors(Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate) {
        return this.taskExecutorStateMap
            .entrySet()
            .stream()
            .filter(predicate)
            .map(Entry::getKey)
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getActiveJobs(GetActiveJobsRequest req) {
        return this.taskExecutorStateMap
            .values()
            .stream()
            .map(TaskExecutorState::getWorkerId)
            .filter(Objects::nonNull)
            .map(WorkerId::getJobId)
            .distinct()
            .sorted((String::compareToIgnoreCase))
            .skip(req.getStartingIndex().orElse(0))
            .limit(req.getPageSize().orElse(3000))
            .collect(Collectors.toList());
    }

    @Override
    public Optional<Entry<TaskExecutorID, TaskExecutorState>> findFirst(
        Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate) {
        return taskExecutorStateMap
            .entrySet()
            .stream()
            .filter(predicate)
            .findFirst();
    }

    @Override
    public Optional<BestFit> findBestFit(TaskExecutorBatchAssignmentRequest request) {

        if (request.getAllocationRequests().isEmpty()) {
            log.warn("TaskExecutorBatchAssignmentRequest {} with empty allocation requests.", request);
            return Optional.empty();
        }

        boolean noResourcesAvailable = false;
        final BestFit bestFit = new BestFit();
        final boolean isJobIdAlreadyPending = pendingJobRequests.getIfPresent(request.getJobId()) != null;

        for (Entry<SchedulingConstraints, List<TaskExecutorAllocationRequest>> entry : request.getGroupedByConstraints().entrySet()) {
            final SchedulingConstraints constraints = entry.getKey();
            final List<TaskExecutorAllocationRequest> allocationRequests = entry.getValue();

            Optional<Map<TaskExecutorID, TaskExecutorState>> taskExecutors = findTaskExecutorsFor(request, allocationRequests, constraints, isJobIdAlreadyPending, bestFit);

            // Mark noResourcesAvailable if we can't find enough TEs for a given machine def
            if (!taskExecutors.isPresent()) {
                noResourcesAvailable = true;
                break;
            }

            // Map each TE to a given allocation request
            int index = 0;
            for (Entry<TaskExecutorID, TaskExecutorState> taskToStateEntry : taskExecutors.get().entrySet()) {
                bestFit.add(allocationRequests.get(index), Pair.of(taskToStateEntry.getKey(), taskToStateEntry.getValue()));
                index++;
            }
        }

        if (noResourcesAvailable) {
            log.warn("Not all machine def had enough workers available to fulfill the request {}", request);
            return Optional.empty();
        } else {
            // Return best fit only if there are enough available TEs for all machine def
            return Optional.of(bestFit);
        }

    }

    /**
     * Finds the best fit scheduling constraints from the current set of executors
     * based on the provided `requestedConstraints`.
     *
     * @param requestedConstraints - Constraints of the scheduling request which serves as the reference for determining the best fit.
     *
     * @return - An Optional<SchedulingConstraints> which holds the best fit scheduling constraints. If no fitting constraints
     * are found, an empty Optional is returned. The chosen constraints are the ones with the highest fitness score
     * when compared with the `requestedConstraints`.
     */
    private Optional<SchedulingConstraints> findBestFitAllocationConstraints(SchedulingConstraints requestedConstraints) {
        return executorsBySchedulingConstraints.keySet()
            .stream()
            .max(Comparator.comparing(target ->
                target.fitness(requestedConstraints, assignmentAttributesAndDefaults)
            ));
    }

    private Optional<Map<TaskExecutorID, TaskExecutorState>> findBestFitFor(TaskExecutorBatchAssignmentRequest request, Integer numWorkers, SchedulingConstraints constraints, BestFit currentBestFit) {
        Optional<SchedulingConstraints> targetConstraints = findBestFitAllocationConstraints(constraints);
        if (!targetConstraints.isPresent()) {
            log.warn("Cannot find any matching sku for request: {}", request);
            return Optional.empty();
        }
        log.debug("Applying assignment request: {} to constraints {}.", request, targetConstraints);
        if (this.executorsBySchedulingConstraints.getOrDefault(targetConstraints.get(), Collections.emptyNavigableSet()).isEmpty()) {
            log.warn("No available TE found for constraints: {}, request: {}", targetConstraints.get(), request);
            return Optional.empty();
        }

        return Optional.of(
            this.executorsBySchedulingConstraints.get(targetConstraints.get())
                .descendingSet()
                .stream()
                .filter(teHolder -> {
                    if (!this.taskExecutorStateMap.containsKey(teHolder.getId())) {
                        return false;
                    }

                    if (currentBestFit.contains(teHolder.getId())) {
                        return false;
                    }

                    TaskExecutorState st = this.taskExecutorStateMap.get(teHolder.getId());
                    return st.isAvailable() &&
                        st.getRegistration() != null &&
                        // TODO: figure out whether we can make assignmentAttributesAndDefaults a better use?!
                        st.getRegistration().getSchedulingConstraints(assignmentAttributesAndDefaults).canFit(constraints, assignmentAttributesAndDefaults);
                })
                .limit(numWorkers)
                .map(TaskExecutorHolder::getId)
                .collect(Collectors.toMap(
                    taskExecutorID -> taskExecutorID,
                    this.taskExecutorStateMap::get)));
    }

    @Override
    public Set<Entry<TaskExecutorID, TaskExecutorState>> getActiveExecutorEntry() {
        return this.taskExecutorStateMap.entrySet();
    }

    @Override
    public GetClusterUsageResponse getClusterUsage(GetClusterUsageRequest req) {
        // default grouping is containerSkuID to usage
        Map<String, Integer> pendingCountByGroupKey = new HashMap<>();
        Map<String, Pair<Integer, Integer>> usageByGroupKey = new HashMap<>();
        // helper struct to verify job has been fully deployed so we can remove it from pending
        Map<String, List<SchedulingConstraints>> jobIdToMachineDef = new HashMap<>();

        taskExecutorStateMap.forEach((key, value) -> {
            if (value == null ||
                value.getRegistration() == null) {
                log.info("Empty registration: {}, {}. Skip usage request.", req.getClusterID(), key);
                return;
            }

            // do not count the disabled TEs.
            if (value.isDisabled()) {
                return;
            }

            Optional<String> groupKeyO =
                req.getGroupKeyFunc().apply(value.getRegistration());

            if (!groupKeyO.isPresent()) {
                log.info("Empty groupKey from: {}, {}. Skip usage request.", req.getClusterID(), key);
                return;
            }

            String groupKey = groupKeyO.get();

            Pair<Integer, Integer> kvState = Pair.of(
                value.isAvailable() ? 1 : 0,
                value.isRegistered() ? 1 : 0);

            if (usageByGroupKey.containsKey(groupKey)) {
                Pair<Integer, Integer> prevState = usageByGroupKey.get(groupKey);
                usageByGroupKey.put(
                    groupKey,
                    Pair.of(
                        kvState.getLeft() + prevState.getLeft(), kvState.getRight() + prevState.getRight()));
            } else {
                usageByGroupKey.put(groupKey, kvState);
            }

            SchedulingConstraints constraints = value.getRegistration().getSchedulingConstraints(assignmentAttributesAndDefaults);
            if ((value.isAssigned() || value.isRunningTask()) && value.getWorkerId() != null) {
                if (pendingJobRequests.getIfPresent(value.getWorkerId().getJobId()) != null) {
                    List<SchedulingConstraints> workers = jobIdToMachineDef.getOrDefault(value.getWorkerId().getJobId(), new ArrayList<>());
                    workers.add(constraints);
                    jobIdToMachineDef.put(value.getWorkerId().getJobId(), workers);
                }
            }

            if (!pendingCountByGroupKey.containsKey(groupKey)) {
                pendingCountByGroupKey.put(
                    groupKey,
                    getPendingCountByAllocationConstraints(constraints));
            }
        });

        // remove jobs from pending set which have all pending workers
        jobIdToMachineDef.forEach((jobId, workers) -> {
            final JobRequirements jobStats = pendingJobRequests.getIfPresent(jobId);
            if (jobStats != null && jobStats.getTotalWorkers() <= workers.size()) {
                log.info("Removing job {} from pending requests", jobId);
                pendingJobRequests.invalidate(jobId);
            }
        });

        GetClusterUsageResponseBuilder resBuilder = GetClusterUsageResponse.builder().clusterID(req.getClusterID());
        usageByGroupKey.forEach((key, value) -> resBuilder.usage(UsageByGroupKey.builder()
            .usageGroupKey(key)
            .idleCount(value.getLeft() - pendingCountByGroupKey.get(key))
            .totalCount(value.getRight())
            .build()));

        GetClusterUsageResponse res = resBuilder.build();
        log.info("Usage result: {}", res);
        return res;
    }

    private int getPendingCountByAllocationConstraints(SchedulingConstraints constraints) {
        return pendingJobRequests
            .asMap()
            .values()
            .stream()
            // TODO: probably in here we can use best fitting function instead ???
            .map(req -> req.constraintsToWorkerCount.getOrDefault(constraints, 0))
            .reduce(Integer::sum)
            .orElse(0);
    }

    private Optional<Map<TaskExecutorID, TaskExecutorState>> findTaskExecutorsFor(TaskExecutorBatchAssignmentRequest request, List<TaskExecutorAllocationRequest> allocationRequests, SchedulingConstraints constraints, boolean isJobIdAlreadyPending, BestFit currentBestFit) {
        // Finds best fit for N workers of the same machine def & allocation attributes
        final Optional<Map<TaskExecutorID, TaskExecutorState>> taskExecutors = findBestFitFor(
            request, allocationRequests.size(), constraints, currentBestFit);

        // Verify that the number of task executors returned matches the asked
        if (taskExecutors.isPresent() && taskExecutors.get().size() == allocationRequests.size()) {
            return taskExecutors;
        } else {
            log.warn("Not enough available TEs found for request with constraints {}, request: {}",
                constraints, request);

            // If there are not enough workers with the given spec then add the request the pending ones
            if (!isJobIdAlreadyPending && request.getAllocationRequests().size() > 2) {
                // Add jobId to pending requests only once
                if (pendingJobRequests.getIfPresent(request.getJobId()) == null) {
                    log.info("Adding job {} to pending requests for {} constraints {}", request.getJobId(), allocationRequests.size(), constraints);
                    pendingJobRequests.put(request.getJobId(), new JobRequirements(request.getGroupedByConstraintsCount()));
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Holder class in {@link ExecutorStateManagerImpl} to wrap task executor ID with other metatdata needed during
     * scheduling e.g. generation.
     */
    @Builder
    @Value
    protected static class TaskExecutorHolder {
        TaskExecutorID Id;
        String generation;

        static TaskExecutorHolder of(TaskExecutorID id, TaskExecutorRegistration reg) {
            String generation = reg.getAttributeByKey(WorkerConstants.MANTIS_WORKER_CONTAINER_GENERATION)
                .orElse(reg.getAttributeByKey(WorkerConstants.AUTO_SCALE_GROUP_KEY).orElse("empty-generation"));
            return TaskExecutorHolder.builder()
                .Id(id)
                .generation(generation)
                .build();
        }

        static Comparator<TaskExecutorHolder> generationFirstComparator =
            Comparator.comparing(TaskExecutorHolder::getGeneration)
                .thenComparing(teh -> teh.getId().getResourceId());
    }
}
