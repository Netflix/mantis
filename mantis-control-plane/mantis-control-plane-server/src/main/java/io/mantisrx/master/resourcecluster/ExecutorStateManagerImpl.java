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

import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetActiveJobsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorAssignmentRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.GetClusterUsageResponseBuilder;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ExecutorStateManagerImpl implements ExecutorStateManager {
    private final Map<TaskExecutorID, TaskExecutorState> taskExecutorStateMap = new HashMap<>();

    /**
     * Cache the available executors ready to accept assignments. Note these executors' state are not strongly
     * synchronized and requires state level check when matching.
     */
    private final SortedMap<Double, Set<TaskExecutorID>> executorByCores = new ConcurrentSkipListMap<>();

    private final Cache<TaskExecutorID, TaskExecutorState> archivedState = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(24, TimeUnit.HOURS)
        .removalListener(notification ->
            log.info("Archived TaskExecutor: {} removed due to: {}", notification.getKey(), notification.getCause()))
        .build();

    @Override
    public void putIfAbsent(TaskExecutorID taskExecutorID, TaskExecutorState state) {
        this.taskExecutorStateMap.putIfAbsent(taskExecutorID, state);
        if (this.archivedState.getIfPresent(taskExecutorID) != null) {
            log.info("Reviving archived executor: {}", taskExecutorID);
            this.archivedState.invalidate(taskExecutorID);
        }

        // add to buckets. however new executors won't have valid registration at this moment and requires marking
        // again later when registration is ready.
        if (state.isAvailable() && state.getRegistration() != null) {
            log.info("Marking executor {} as available for matching.", taskExecutorID);
            double cpuCores = state.getRegistration().getMachineDefinition().getCpuCores();
            if (!this.executorByCores.containsKey(cpuCores)) {
                this.executorByCores.putIfAbsent(cpuCores, new HashSet<>());
            }

            this.executorByCores.get(cpuCores).add(taskExecutorID);
        }
    }

    @Override
    public void markAvailable(TaskExecutorID taskExecutorID) {
        if (!this.taskExecutorStateMap.containsKey(taskExecutorID)) {
            log.warn("marking invalid executor as available: {}", taskExecutorID);
            return;
        }

        TaskExecutorState taskExecutorState = this.taskExecutorStateMap.get(taskExecutorID);
        if (taskExecutorState.getRegistration() == null) {
            log.warn("marking invalid executor registration as available: {}", taskExecutorID);
            return;
        }

        double cpuCores = taskExecutorState.getRegistration().getMachineDefinition().getCpuCores();
        if (!this.executorByCores.containsKey(cpuCores)) {
            this.executorByCores.putIfAbsent(cpuCores, new HashSet<>());
        }

        this.executorByCores.get(cpuCores).add(taskExecutorID);
    }

    @Override
    public void markUnavailable(TaskExecutorID taskExecutorID) {
        if (this.taskExecutorStateMap.containsKey(taskExecutorID)) {
            TaskExecutorState taskExecutorState = this.taskExecutorStateMap.get(taskExecutorID);
            if (taskExecutorState.getRegistration() != null) {
                double cpuCores = taskExecutorState.getRegistration().getMachineDefinition().getCpuCores();
                if (this.executorByCores.containsKey(cpuCores)) {
                    this.executorByCores.get(cpuCores).remove(taskExecutorID);
                }
                return;
            }
        }

        // todo: check archive map as well?
        log.warn("invalid task executor to mark as unavailable: {}", taskExecutorID);
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
    public Optional<Pair<TaskExecutorID, TaskExecutorState>> findBestFit(TaskExecutorAssignmentRequest request) {
        // only allow allocation in the lowest CPU cores matching group.
        SortedMap<Double, Set<TaskExecutorID>> targetMap =
            this.executorByCores.tailMap(request.getAllocationRequest().getMachineDefinition().getCpuCores());

        if (targetMap.size() < 1) {
            log.warn("Cannot find any executor for request: {}", request);
            return Optional.empty();
        }
        Double targetCoreCount = targetMap.firstKey();
        log.trace("Applying assignmentReq: {} to {} cores.", request, targetCoreCount);

        return this.executorByCores.get(targetCoreCount)
            .stream()
            .filter(tid -> {
                if (!this.taskExecutorStateMap.containsKey(tid)) {
                    return false;
                }

                TaskExecutorState st = this.taskExecutorStateMap.get(tid);
                return st.isAvailable() &&
                    st.getRegistration() != null &&
                    st.getRegistration().getMachineDefinition().canFit(
                        request.getAllocationRequest().getMachineDefinition());
            })
            .findAny()
            .map(taskExecutorID -> Pair.of(taskExecutorID, this.taskExecutorStateMap.get(taskExecutorID)));
    }

    @Override
    public Set<Entry<TaskExecutorID, TaskExecutorState>> getActiveExecutorEntry() {
        return this.taskExecutorStateMap.entrySet();
    }

    @Override
    public GetClusterUsageResponse getClusterUsage(GetClusterUsageRequest req) {
        log.info("Computing cluster usage: {}", req.getClusterID());

        // default grouping is containerSkuID to usage
        Map<String, Pair<Integer, Integer>> usageByGroupKey = new HashMap<>();
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
        });

        GetClusterUsageResponseBuilder resBuilder = GetClusterUsageResponse.builder().clusterID(req.getClusterID());
        usageByGroupKey.forEach((key, value) -> resBuilder.usage(UsageByGroupKey.builder()
            .usageGroupKey(key)
            .idleCount(value.getLeft())
            .totalCount(value.getRight())
            .build()));

        GetClusterUsageResponse res = resBuilder.build();
        log.info("Usage result: {}", res);
        return res;
    }
}
