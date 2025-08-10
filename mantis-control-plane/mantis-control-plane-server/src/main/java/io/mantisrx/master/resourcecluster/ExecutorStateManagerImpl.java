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
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.master.scheduler.FitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration.TaskExecutorGroupKey;
import io.mantisrx.shaded.com.google.common.cache.Cache;
import io.mantisrx.shaded.com.google.common.cache.CacheBuilder;
import io.mantisrx.shaded.com.google.common.cache.RemovalListener;

import java.time.Duration;
import java.util.AbstractMap;
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
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

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

    @Getter
    @ToString
    class JobRequirements {
        private final Map<TaskExecutorGroupKey, Integer> groupToTaskExecutorCount;

        JobRequirements(Map<SchedulingConstraints, List<TaskExecutorAllocationRequest>> constraintsToTaskAllocationRequests) {
            this.groupToTaskExecutorCount = constraintsToTaskAllocationRequests
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    entry -> findBestFitGroupOrDefault(entry.getKey()),
                    entry -> entry.getValue().size(),
                    Integer::sum
                ));
        }

        public int getTotalWorkers() {
            return groupToTaskExecutorCount.values().stream().mapToInt(Integer::intValue).sum();
        }

        private TaskExecutorGroupKey findBestFitGroupOrDefault(SchedulingConstraints constraints) {
            Optional<TaskExecutorGroupKey> bestGroup = findBestGroup(constraints);
            if (!bestGroup.isPresent()) {
                log.warn("No fitting group found for provided constraints {}", constraints);
            }
            return bestGroup.orElse(new TaskExecutorGroupKey(constraints.getMachineDefinition(), constraints.getSizeName(), constraints.getSchedulingAttributes()));
        }
    }

    /**
     * Cache the available executors ready to accept assignments.
     */
    private final Map<TaskExecutorGroupKey, NavigableSet<TaskExecutorHolder>> executorsByGroup = new HashMap<>();

    private final FitnessCalculator fitnessCalculator;

    private final Map<String, String> schedulingAttributes;

    private final Duration schedulerLeaseExpirationDuration;

    private final Cache<TaskExecutorID, TaskExecutorState> archivedState = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(24, TimeUnit.HOURS)
        .removalListener(notification ->
            log.info("Archived TaskExecutor: {} removed due to: {}", notification.getKey(), notification.getCause()))
        .build();

    private final AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook;

    ExecutorStateManagerImpl(Map<String, String> schedulingAttributes) {
        this.schedulingAttributes = schedulingAttributes;
        this.fitnessCalculator = new CpuWeightedFitnessCalculator();
        this.schedulerLeaseExpirationDuration = Duration.ofMillis(100);
        this.availableTaskExecutorMutatorHook = null;
    }

    ExecutorStateManagerImpl(
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator,
        Duration schedulerLeaseExpirationDuration) {
        this.schedulingAttributes = schedulingAttributes;
        this.fitnessCalculator = fitnessCalculator;
        this.schedulerLeaseExpirationDuration = schedulerLeaseExpirationDuration;
        this.availableTaskExecutorMutatorHook = null;
    }

    ExecutorStateManagerImpl(Map<String, String> schedulingAttributes,
                             FitnessCalculator fitnessCalculator,
                             Duration schedulerLeaseExpirationDuration,
                             AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook) {
        this.schedulingAttributes = schedulingAttributes;
        this.fitnessCalculator = fitnessCalculator;
        this.schedulerLeaseExpirationDuration = schedulerLeaseExpirationDuration;
        this.availableTaskExecutorMutatorHook = availableTaskExecutorMutatorHook;
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
            TaskExecutorGroupKey taskExecutorGroupKey = state.getRegistration().getGroup();
            if (!this.executorsByGroup.containsKey(taskExecutorGroupKey)) {
                log.info("[executorsByGroup] adding {} from TE: {}", taskExecutorGroupKey, teHolder);
                this.executorsByGroup.putIfAbsent(
                    taskExecutorGroupKey,
                    new TreeSet<>(TaskExecutorHolder.generationFirstComparator));
            }

            log.info("Assign {} to available.", teHolder.getId());
            return this.executorsByGroup.get(taskExecutorGroupKey).add(teHolder);
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
                TaskExecutorGroupKey taskExecutorGroupKey = taskExecutorState.getRegistration().getGroup();
                if (this.executorsByGroup.containsKey(taskExecutorGroupKey)) {
                    this.executorsByGroup.get(taskExecutorGroupKey)
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

        for (Entry<SchedulingConstraints, List<TaskExecutorAllocationRequest>> entry : request.getGroupedBySchedulingConstraints().entrySet()) {
            final SchedulingConstraints schedulingConstraints = entry.getKey();
            final List<TaskExecutorAllocationRequest> allocationRequests = entry.getValue();
            Optional<Map<TaskExecutorID, TaskExecutorState>> taskExecutors = findTaskExecutorsFor(request, schedulingConstraints, allocationRequests, isJobIdAlreadyPending, bestFit);

            // Mark noResourcesAvailable if we can't find enough TEs for a given set of scheduling constraints
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
            log.warn("Not all scheduling constraints had enough workers available to fulfill the request {}", request);
            return Optional.empty();
        } else {
            // Return best fit only if there are enough available TEs for all scheduling constraints
            return Optional.of(bestFit);
        }

    }

    private Optional<Map<TaskExecutorID, TaskExecutorState>> findBestFitFor(TaskExecutorBatchAssignmentRequest request, SchedulingConstraints schedulingConstraints, Integer numWorkers, BestFit currentBestFit) {
        // only allow allocation in the lowest CPU cores matching group.
        Optional<TaskExecutorGroupKey> bestFitTeGroupKey = findBestGroup(schedulingConstraints);

        if (!bestFitTeGroupKey.isPresent()) {
            log.warn("Cannot find any matching sku for request: {}", request);
            return Optional.empty();
        }

        log.info("Applying assignment request: {} to best fit TE group {}.", request, bestFitTeGroupKey);
        if (!this.executorsByGroup.containsKey(bestFitTeGroupKey.get())) {
            log.warn("No available TE found for best fit TE group: {}, request: {}", bestFitTeGroupKey.get(), request);
            return Optional.empty();
        }

        Stream<TaskExecutorHolder> availableTEs = this.executorsByGroup.get(bestFitTeGroupKey.get())
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
                    // when a TE is returned from here to be used for scheduling, its state remain active until
                    // the scheduler trigger another message to update (lock) the state. However when large number
                    // of the requests are active at the same time on same sku, the gap between here and the message
                    // to lock the state can be large so another schedule request message can be in between and
                    // got the same set of TEs. To avoid this, a lease is added to each TE state to temporarily
                    // lock the TE to be used again. Since this is only lock between actor messages and lease
                    // duration can be short.
                    st.getLastSchedulerLeasedDuration().compareTo(this.schedulerLeaseExpirationDuration) > 0 &&
                    st.getRegistration() != null;
            });

        if(availableTaskExecutorMutatorHook != null) {
            availableTEs = availableTaskExecutorMutatorHook.mutate(availableTEs, request, schedulingConstraints);
        }


        return Optional.of(
                availableTEs
                .limit(numWorkers)
                .map(teHolder -> {
                    TaskExecutorState st = this.taskExecutorStateMap.get(teHolder.getId());
                    st.updateLastSchedulerLeased();
                    return teHolder.getId();
                })
                .collect(Collectors.toMap(
                    taskExecutorID -> taskExecutorID,
                    this.taskExecutorStateMap::get)));
    }

    /**
     * Verifies if all scheduling attributes constraints are satisfied.
     *
     * For each entry in 'schedulingAttributes':
     * - Fetch the corresponding attribute value from Task Executor scheduling attributes, if not present, default to the value from the current entry.
     * - Fetch the corresponding attribute value from Schedule Request scheduling attributes, if not present, default to the value from the current entry.
     * - Checks if these two values are equal ignoring case. If any pair is not equal, the function returns false.
     *
     * Hence, the function ensures that the TaskExecutor scheduling attributes match or satisfy the constraints required. If either the TaskExecutor registration or the scheduling request lacks an attribute, it uses the provided defaults.
     *
     * @param constraints The schedule request constraints to be satisfied.
     * @param teAssignmentAttributes The scheduling attributes of a Task Executor that needs to satisfy scheduling constraints.
     *
     * @return true if all allocation constraints are satisfied, false otherwise.
     */
    public boolean areSchedulingAttributeConstraintsSatisfied(SchedulingConstraints constraints, Map<String, String> teAssignmentAttributes) {
        Map<String, String> teAssignmentAttributesLowercased = teAssignmentAttributes.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));

        Map<String, String> constraintsAttributesLowercased = constraints.getSchedulingAttributes().entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));

        return schedulingAttributes.entrySet()
            .stream()
            .allMatch(entry -> {
                String lowerCaseKey = entry.getKey().toLowerCase();
                return teAssignmentAttributesLowercased
                    .getOrDefault(lowerCaseKey, entry.getValue())
                    .equalsIgnoreCase(constraintsAttributesLowercased
                        .getOrDefault(lowerCaseKey, entry.getValue()));
            });
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

            Optional<String> groupKeyO =
                req.getGroupKeyFunc().apply(value.getRegistration());

            if (!groupKeyO.isPresent()) {
                log.info("Empty groupKey from: {}, {}. Skip usage request.", req.getClusterID(), key);
                return;
            }

            String groupKey = groupKeyO.get();

            Pair<Integer, Integer> kvState = Pair.of(
                value.isAvailable() && !value.isDisabled() ? 1 : 0,
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
                    getPendingCountByTaskExecutorGroup(value.getRegistration().getGroup()));
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

    /**
     * Calculates the total count of pending scheduling requests for a specific Task Executor group.
     * The function does this by summing over the job requests' group-to-task executor counts.
     *
     * @param teGroup the key of the task executor group for which to calculate the pending request count.
     * @return The total count of pending requests for the provided task executor group.
     */
    private int getPendingCountByTaskExecutorGroup(TaskExecutorGroupKey teGroup) {
        return pendingJobRequests
            .asMap()
            .values()
            .stream()
            .map(req -> req.getGroupToTaskExecutorCount().getOrDefault(teGroup, 0))
            .reduce(Integer::sum)
            .orElse(0);
    }

    private Optional<Map<TaskExecutorID, TaskExecutorState>> findTaskExecutorsFor(TaskExecutorBatchAssignmentRequest request, SchedulingConstraints schedulingConstraints, List<TaskExecutorAllocationRequest> allocationRequests, boolean isJobIdAlreadyPending, BestFit currentBestFit) {
        // Finds best fit for N workers of the same scheduling constraints
        final Optional<Map<TaskExecutorID, TaskExecutorState>> taskExecutors = findBestFitFor(
            request, schedulingConstraints, allocationRequests.size(), currentBestFit);

        // Verify that the number of task executors returned matches the asked
        if (taskExecutors.isPresent() && taskExecutors.get().size() == allocationRequests.size()) {
            return taskExecutors;
        } else {
            log.warn("Not enough available TEs found for scheduling constraints {}, request: {}", schedulingConstraints, request);
            if (taskExecutors.isPresent()) {
                log.debug("Found {} Task Executors: {} for request: {} with constraints: {}",
                    taskExecutors.get().size(), taskExecutors.get(), request, schedulingConstraints);
            } else {
                log.warn("No suitable Task Executors found for request: {} with constraints: {}",
                    request, schedulingConstraints);
            }

            // If there are not enough workers with the given spec then add the request the pending ones
            if (!isJobIdAlreadyPending && request.getAllocationRequests().size() > 2) {
                // Add jobId to pending requests only once
                if (pendingJobRequests.getIfPresent(request.getJobId()) == null) {
                    log.info("Adding job {} to pending requests for {} scheduling constraints {}", request.getJobId(), allocationRequests.size(), schedulingConstraints);
                    pendingJobRequests.put(request.getJobId(), new JobRequirements(request.getGroupedBySchedulingConstraints()));
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Finds the best fit Task Executor Group Key based on requested constraints.
     *
     * First, it tries to find a best fit group by matching sizeNames. If it fails,
     * it then uses a fitness calculator to get the best fit.
     *
     * @param requestedConstraints The constraints for the scheduling request.
     * @return An Optional of the best fit Task Executor Group Key. If no suitable key is found,
     *         it returns an empty Optional.
     */
    private Optional<TaskExecutorGroupKey> findBestGroup(SchedulingConstraints requestedConstraints) {
        Optional<TaskExecutorGroupKey> bestGroupBySizeName = findBestGroupBySizeNameMatch(requestedConstraints);

        return bestGroupBySizeName.isPresent()
            ? bestGroupBySizeName
            : findBestGroupByFitnessCalculator(requestedConstraints);
    }

    /**
     * Finds the best fit Task Executor Group by matching sizeNames.
     *
     * @param requestedConstraints The constraints for the scheduling request.
     * @return An Optional of the best fit Task Executor Group Key based on sizeName and scheduling attributes matching.
     *         If no suitable key is found, it returns an empty Optional.
     */
    private Optional<TaskExecutorGroupKey> findBestGroupBySizeNameMatch(SchedulingConstraints requestedConstraints) {
        return executorsByGroup.keySet()
            .stream()
            // Filter to retain groups where sizeName is present
            .filter(group -> group.getSizeName().isPresent())
            // Filter to retain groups where the requested sizeName is also present
            .filter(group -> requestedConstraints.getSizeName().isPresent())
            // Filter to retain groups where sizeNames of group and requested constraints are equal
            .filter(group -> group.getSizeName().get().equalsIgnoreCase(requestedConstraints.getSizeName().get()))
            // Verify scheduling attribute constraints
            .filter(taskExecutorGroupKey -> areSchedulingAttributeConstraintsSatisfied(requestedConstraints,
                taskExecutorGroupKey.getSchedulingAttributes()))
            // Get highest generation group
            .max(Comparator.comparing(taskExecutorGroupKey -> {
                NavigableSet<TaskExecutorHolder> holders = executorsByGroup.get(taskExecutorGroupKey);
                if (holders.isEmpty()) {
                    return null;
                } else {
                    return holders.last().getGeneration();
                }
            }, Comparator.nullsLast(Comparator.reverseOrder())));
    }

    /**
     * Finds the best fit Task Executor Group by using a fitness calculator on machine definitions.
     *
     * Groups that match requestedConstraints and have a fitness score greater than 0 are considered.
     * Among these, the key with the highest score is returned.
     *
     * @param requestedConstraints The constraints for the scheduling request.
     * @return An Optional of the best fit Task Executor Group Key according to a fitness calculator.
     *         If no suitable key is found, it returns an empty Optional.
     */
    private Optional<TaskExecutorGroupKey> findBestGroupByFitnessCalculator(SchedulingConstraints requestedConstraints) {
        log.info("Falling back to find best group by fitness calculator for constraints: {}", requestedConstraints);
        log.debug("All present executor groups: {}", executorsByGroup.keySet());

        // Filter and sort Task Executor Group Keys based on fitness score
        final List<Map.Entry<TaskExecutorGroupKey, Double>> groupFitnessList = executorsByGroup.keySet()
            .stream()
            // Filter out if both sizeName exist and are different (ie. small vs large)
            .filter(taskExecutorGroupKey -> {
                Optional<String> teGroupSizeName = taskExecutorGroupKey.getSizeName();
                Optional<String> requestSizeName = requestedConstraints.getSizeName();

                return !(teGroupSizeName.isPresent() && requestSizeName.isPresent()
                    && !teGroupSizeName.get().equalsIgnoreCase(requestSizeName.get()));
            })
            // Verify scheduling attribute constraints
            .filter(taskExecutorGroupKey -> areSchedulingAttributeConstraintsSatisfied(requestedConstraints,
                taskExecutorGroupKey.getSchedulingAttributes()))
            // Calculate fitness score for each Task Executor Group
            .map(key -> new AbstractMap.SimpleEntry<>(
                key,
                fitnessCalculator.calculate(requestedConstraints.getMachineDefinition(), key.getMachineDefinition())
            ))
            // Filter out entries with non-positive fitness scores (aka. requested machine doesn't fit in TE)
            .filter(entry -> entry.getValue() > 0)
            // During the process of adding size metadata to an existing SKU and initiating corresponding ASG updates,
            // TEs from both new and existing ASGs are now grouped separately in the resource cluster actor, i.e.,
            // one with size and one without. Due to this, issues may arise during task migrations as it's not
            // predictable which TEs will be chosen by the scheduler. While, instead, we want to always use TEs from the latest ASGs.
            .sorted((entry1, entry2) -> {
                int fitnessComparison = Precision.compareTo(entry2.getValue(), entry1.getValue(), 0.0001);
                if (fitnessComparison != 0) {
                    return fitnessComparison;
                } else {
                    NavigableSet<TaskExecutorHolder> holders1 = executorsByGroup.get(entry1.getKey());
                    NavigableSet<TaskExecutorHolder> holders2 = executorsByGroup.get(entry2.getKey());
                    String generation1 = holders1.isEmpty() ? null : holders1.last().getGeneration();
                    String generation2 = holders2.isEmpty() ? null : holders2.last().getGeneration();
                    return Comparator.<String>nullsLast(Comparator.reverseOrder()).compare(generation1, generation2);
                }
            })
            .collect(Collectors.toList());

        if (groupFitnessList.isEmpty()) {
            log.debug("No suitable Task Executor Groups found for constraints: {}", requestedConstraints);
        } else {
            log.debug("Fitness calculation results for the Task Executor Groups:");
            for (Map.Entry<TaskExecutorGroupKey, Double> entry : groupFitnessList) {
                log.debug("TaskExecutorGroupKey: {}, Fitness Score: {}", entry.getKey(), entry.getValue());
            }
        }

        return groupFitnessList.stream().map(Map.Entry::getKey).findFirst();
    }

    /**
     * Holder class in {@link ExecutorStateManagerImpl} to wrap task executor ID with other metatdata needed during
     * scheduling e.g. generation.
     */
    @Builder
    @Value
    public static class TaskExecutorHolder {
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
