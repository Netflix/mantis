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
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import io.mantisrx.shaded.com.google.common.cache.RemovalListener;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ExecutorStateManagerImpl implements ExecutorStateManager {
    private final Map<TaskExecutorID, TaskExecutorState> taskExecutorStateMap = new HashMap<>();
    Cache<String, JobRequirements> pendingJobRequests = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .removalListener((RemovalListener<String, JobRequirements>) notification -> {
            log.info("Removing key {} from pending job requests due to reason {}", notification.getKey(), notification.getCause());
        })
        .build();

    @RequiredArgsConstructor
    @ToString
    static class JobRequirements {
        public final Map<Double, Integer> coresToWorkerCount;

        public int getTotalWorkers() {
            return coresToWorkerCount.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    /**
     * Cache the available executors ready to accept assignments. Note these executors' state are not strongly
     * synchronized and requires state level check when matching.
     */
    private final SortedMap<Double, NavigableSet<TaskExecutorHolder>> executorByCores = new ConcurrentSkipListMap<>();

    private final Cache<TaskExecutorID, TaskExecutorState> archivedState = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(24, TimeUnit.HOURS)
        .removalListener(notification ->
            log.info("Archived TaskExecutor: {} removed due to: {}", notification.getKey(), notification.getCause()))
        .build();

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
            double cpuCores = state.getRegistration().getMachineDefinition().getCpuCores();
            if (!this.executorByCores.containsKey(cpuCores)) {
                this.executorByCores.putIfAbsent(
                    cpuCores,
                    new TreeSet<>(TaskExecutorHolder.generationFirstComparator));
            }

            log.info("Assign {} to available.", teHolder.getId());
            return this.executorByCores.get(cpuCores).add(teHolder);
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
                double cpuCores = taskExecutorState.getRegistration().getMachineDefinition().getCpuCores();
                if (this.executorByCores.containsKey(cpuCores)) {
                    this.executorByCores.get(cpuCores)
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
    public void unscheduleJob(String jobId) {
        pendingJobRequests.invalidate(jobId);
    }

    @Override
    public Optional<BestFit> findBestFit(TaskExecutorBatchAssignmentRequest request) {

        if (request.getAllocationRequests().isEmpty()) {
            log.warn("TaskExecutorBatchAssignmentRequest {} with empty allocation requests.", request);
            return Optional.empty();
        }

        boolean noResourcesAvailable = false;
        final Map<TaskExecutorAllocationRequest, Pair<TaskExecutorID, TaskExecutorState>> bestFit = new HashMap<>();
        final boolean isJobIdAlreadyPending = pendingJobRequests.getIfPresent(request.getJobId()) != null;

        for (Entry<MachineDefinition, List<TaskExecutorAllocationRequest>> entry : request.getGroupedByMachineDef().entrySet()) {
            final MachineDefinition machineDefinition = entry.getKey();
            final List<TaskExecutorAllocationRequest> allocationRequests = entry.getValue();

            Optional<Map<TaskExecutorID, TaskExecutorState>> taskExecutors = findTaskExecutorsFor(request, machineDefinition, allocationRequests, isJobIdAlreadyPending);

            // Mark noResourcesAvailable if we can't find enough TEs for a given machine def
            if (!taskExecutors.isPresent() || noResourcesAvailable) {
                noResourcesAvailable = true;
                continue;
            }

            // Map each TE to a given allocation request
            int index = 0;
            for (Entry<TaskExecutorID, TaskExecutorState> taskToStateEntry : taskExecutors.get().entrySet()) {
                bestFit.put(allocationRequests.get(index), Pair.of(taskToStateEntry.getKey(), taskToStateEntry.getValue()));
                index++;
            }
        }

        if (noResourcesAvailable) {
            log.warn("Not all machine def had enough workers available to fulfill the request {}", request);
            return Optional.empty();
        } else {
            // Return best fit only if there are enough available TEs for all machine def
            return Optional.of(new BestFit(bestFit));
        }

    }

    private Optional<Map<TaskExecutorID, TaskExecutorState>> findBestFitFor(TaskExecutorBatchAssignmentRequest request, MachineDefinition machineDefinition, Integer numWorkers) {
        // only allow allocation in the lowest CPU cores matching group.
        SortedMap<Double, NavigableSet<TaskExecutorHolder>> targetMap =
            this.executorByCores.tailMap(machineDefinition.getCpuCores());

        if (targetMap.isEmpty()) {
            log.warn("Cannot find any executor for request: {}", request);
            return Optional.empty();
        }
        Double targetCoreCount = targetMap.firstKey();
        log.debug("Applying assignmentReq: {} to {} cores.", request, targetCoreCount);

        Double requestedCoreCount = machineDefinition.getCpuCores();
        if (Math.abs(targetCoreCount - requestedCoreCount) > 1E-10) {
            // this mismatch should not happen in production and indicates TE registration/spec problem.
            log.warn("Requested core count mismatched. requested: {}, found: {} for {}", requestedCoreCount,
                targetCoreCount,
                request);
        }

        if (this.executorByCores.get(targetCoreCount).isEmpty()) {
            log.warn("No available TE found for core count: {}, request: {}", targetCoreCount, request);
            return Optional.empty();
        }

        return Optional.of(
            this.executorByCores.get(targetCoreCount)
                .descendingSet()
                .stream()
                .filter(teHolder -> {
                    if (!this.taskExecutorStateMap.containsKey(teHolder.getId())) {
                        return false;
                    }

                    TaskExecutorState st = this.taskExecutorStateMap.get(teHolder.getId());
                    return st.isAvailable() &&
                        st.getRegistration() != null &&
                        st.getRegistration().getMachineDefinition().canFit(machineDefinition);
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
        Map<String, List<MachineDefinition>> jobIdToMachineDef = new HashMap<>();

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

            if ((value.isAssigned() || value.isRunningTask()) && value.getWorkerId() != null) {
                if (pendingJobRequests.getIfPresent(value.getWorkerId().getJobId()) != null) {
                    List<MachineDefinition> workers = jobIdToMachineDef.getOrDefault(value.getWorkerId().getJobId(), new ArrayList<>());
                    workers.add(value.getRegistration().getMachineDefinition());
                    jobIdToMachineDef.put(value.getWorkerId().getJobId(), workers);
                }
            }

            if (!pendingCountByGroupKey.containsKey(groupKey)) {
                pendingCountByGroupKey.put(
                    groupKey,
                    getPendingCountyByCores(value.getRegistration().getMachineDefinition().getCpuCores()));
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

    private int getPendingCountyByCores(Double cores) {
        return pendingJobRequests
            .asMap()
            .values()
            .stream()
            .map(req -> req.coresToWorkerCount.getOrDefault(cores, 0))
            .reduce(Integer::sum)
            .orElse(0);
    }

    private Optional<Map<TaskExecutorID, TaskExecutorState>> findTaskExecutorsFor(TaskExecutorBatchAssignmentRequest request, MachineDefinition machineDefinition, List<TaskExecutorAllocationRequest> allocationRequests, boolean isJobIdAlreadyPending) {
        // Finds best fit for N workers of the same machine def
        final Optional<Map<TaskExecutorID, TaskExecutorState>> taskExecutors = findBestFitFor(
            request, machineDefinition, allocationRequests.size());

        // Verify that the number of task executors returned matches the asked
        if (taskExecutors.isPresent() && taskExecutors.get().size() == allocationRequests.size()) {
            return taskExecutors;
        } else {
            log.warn("Not enough available TEs found for machine def {} with core count: {}, request: {}",
                machineDefinition, machineDefinition.getCpuCores(), request);

            // If there are not enough workers with the given spec then add the request the pending ones
            if (!isJobIdAlreadyPending && request.getAllocationRequests().size() > 2) {
                // Add jobId to pending requests only once
                if (pendingJobRequests.getIfPresent(request.getJobId()) == null) {
                    log.info("Adding job {} to pending requests for {} machine {}", request.getJobId(), allocationRequests.size(), machineDefinition);
                    pendingJobRequests.put(request.getJobId(), new JobRequirements(request.getGroupedByCoresCount()));
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
