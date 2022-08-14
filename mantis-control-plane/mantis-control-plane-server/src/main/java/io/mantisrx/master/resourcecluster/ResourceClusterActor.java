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

package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status;
import akka.japi.Pair;
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.GetClusterUsageResponseBuilder;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport.Available;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport.Occupied;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.shaded.guava30.com.google.common.collect.Comparators;

/**
 * Akka actor implementation of ResourceCluster.
 * The actor is not directly exposed to other classes. Instead, the actor is exposed via {@link ResourceClusterGatewayAkkaImpl} and
 * {@link ResourceClusterAkkaImpl} classes, which pass the corresponding messages to the actor on method invocation and wait for the response
 * returned by the actor. This essentially converts the actor behavior to a request/response style pattern while still
 * keeping the benefits of the actor paradigm such as non-shared mutable data.
 */
@ToString(of = {"clusterID"})
@Slf4j
class ResourceClusterActor extends AbstractActorWithTimers {

    private final Duration heartbeatTimeout;
    private final Duration assignmentTimeout;
    private final Duration disabledTaskExecutorsCheckInterval;

    private final Map<TaskExecutorID, TaskExecutorState> taskExecutorStateMap;
    private final Clock clock;
    private final Set<TaskExecutorID> taskExecutorsReadyToPerformWork;
    private final RpcService rpcService;
    private final ClusterID clusterID;
    private final MantisJobStore mantisJobStore;
    private final Set<DisableTaskExecutorsRequest> activeDisableTaskExecutorsRequests;
    private final JobMessageRouter jobMessageRouter;

    static Props props(final ClusterID clusterID, final Duration heartbeatTimeout, Duration assignmentTimeout, Duration disabledTaskExecutorsCheckInterval, Clock clock, RpcService rpcService, MantisJobStore mantisJobStore, JobMessageRouter jobMessageRouter) {
        return Props.create(ResourceClusterActor.class, clusterID, heartbeatTimeout, assignmentTimeout, disabledTaskExecutorsCheckInterval, clock, rpcService, mantisJobStore, jobMessageRouter);
    }

    ResourceClusterActor(
        ClusterID clusterID,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        Duration disabledTaskExecutorsCheckInterval,
        Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore,
        JobMessageRouter jobMessageRouter) {
        this.clusterID = clusterID;
        this.heartbeatTimeout = heartbeatTimeout;
        this.assignmentTimeout = assignmentTimeout;
        this.disabledTaskExecutorsCheckInterval = disabledTaskExecutorsCheckInterval;

        this.clock = clock;
        this.rpcService = rpcService;
        this.jobMessageRouter = jobMessageRouter;
        this.taskExecutorStateMap = new HashMap<>();
        this.taskExecutorsReadyToPerformWork = new HashSet<>();
        this.mantisJobStore = mantisJobStore;
        this.activeDisableTaskExecutorsRequests = new HashSet<>();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        List<DisableTaskExecutorsRequest> activeRequests =
            mantisJobStore.loadAllDisableTaskExecutorsRequests(clusterID);
        for (DisableTaskExecutorsRequest request : activeRequests) {
            onNewDisableTaskExecutorsRequest(request);
        }

        timers().startPeriodicTimer(
            String.format("periodic-disabled-task-executors-test-for-%s", clusterID.getResourceID()),
            new CheckDisabledTaskExecutors("periodic"),
            disabledTaskExecutorsCheckInterval);
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(GetRegisteredTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isRegistered), self()))
                .match(GetBusyTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isBusy), self()))
                .match(GetAvailableTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isAvailable), self()))
                .match(GetDisabledTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isDisabled), self()))
                .match(GetUnregisteredTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(unregistered), self()))
                .match(GetTaskExecutorStatusRequest.class, req -> sender().tell(getTaskExecutorStatus(req.getTaskExecutorID()), self()))
                .match(GetClusterUsageRequest.class, req -> sender().tell(getClusterUsage(req), self()))
                .match(GetClusterIdleInstancesRequest.class,
                    req -> sender().tell(onGetClusterIdleInstancesRequest(req), self()))
                .match(Ack.class, ack -> log.info("Received ack from {}", sender()))

                .match(TaskExecutorAssignmentTimeout.class, this::onTaskExecutorAssignmentTimeout)
                .match(TaskExecutorRegistration.class, this::onTaskExecutorRegistration)
                .match(InitializeTaskExecutorRequest.class, this::onTaskExecutorInitialization)
                .match(TaskExecutorHeartbeat.class, this::onHeartbeat)
                .match(TaskExecutorStatusChange.class, this::onTaskExecutorStatusChange)
                .match(TaskExecutorDisconnection.class, this::onTaskExecutorDisconnection)
                .match(HeartbeatTimeout.class, this::onTaskExecutorHeartbeatTimeout)
                .match(TaskExecutorAssignmentRequest.class, this::onTaskExecutorAssignmentRequest)
                .match(ResourceOverviewRequest.class, this::onResourceOverviewRequest)
                .match(TaskExecutorInfoRequest.class, this::onTaskExecutorInfoRequest)
                .match(TaskExecutorGatewayRequest.class, this::onTaskExecutorGatewayRequest)
                .match(DisableTaskExecutorsRequest.class, this::onNewDisableTaskExecutorsRequest)
                .match(CheckDisabledTaskExecutors.class, this::findAndMarkDisabledTaskExecutors)
                .match(ExpireDisableTaskExecutorsRequest.class, this::onDisableTaskExecutorsRequestExpiry)
                .match(GetTaskExecutorWorkerMappingRequest.class, req -> sender().tell(getTaskExecutorWorkerMapping(req.getAttributes()), self()))
                .build();
    }

    private final Predicate<Entry<TaskExecutorID, TaskExecutorState>> isRegistered =
        e -> e.getValue().isRegistered();

    private final Predicate<Entry<TaskExecutorID, TaskExecutorState>> isBusy =
        e -> e.getValue().isRunningTask();

    private final Predicate<Entry<TaskExecutorID, TaskExecutorState>> unregistered =
        e -> e.getValue().isDisconnected();

    private final Predicate<Entry<TaskExecutorID, TaskExecutorState>> isAvailable =
        e -> e.getValue().isAvailable();

    private final Predicate<Entry<TaskExecutorID, TaskExecutorState>> isDisabled =
        e -> e.getValue().isDisabled();

    private GetClusterUsageResponse getClusterUsage(GetClusterUsageRequest req) {
        log.info("Computing cluster usage: {}", this.clusterID);

        // default grouping is containerSkuID to usage
        Map<String, Pair<Integer, Integer>> usageByGroupKey = new HashMap<>();
        taskExecutorStateMap.entrySet().stream()
            .forEach(kv -> {
                if (kv.getValue() == null ||
                    kv.getValue().getRegistration() == null) {
                    log.warn("Empty registration: {}, {}. Skip usage request.", this.clusterID, kv.getKey());
                    return;
                }

                Optional<String> groupKeyO =
                    req.getGroupKeyFunc().apply(kv.getValue().getRegistration());

                if (!groupKeyO.isPresent()) {
                    log.info("Empty groupKey from: {}, {}. Skip usage request.", this.clusterID, kv.getKey());
                    return;
                }

                String groupKey = groupKeyO.get();

                Pair<Integer, Integer> kvState = new Pair<>(
                    kv.getValue().isAvailable() ? 1 : 0,
                    kv.getValue().isRegistered() ? 1 : 0);


                if (usageByGroupKey.containsKey(groupKey)) {
                    Pair<Integer, Integer> prevState = usageByGroupKey.get(groupKey);
                    usageByGroupKey.put(groupKey,
                        new Pair<>(kvState.first() + prevState.first(), kvState.second() + prevState.second()));
                } else {
                    usageByGroupKey.put(groupKey, kvState);
                }
            });

        GetClusterUsageResponseBuilder resBuilder = GetClusterUsageResponse.builder().clusterID(this.clusterID);
        usageByGroupKey.entrySet().stream()
            .forEach(kv -> resBuilder.usage(UsageByGroupKey.builder()
                .usageGroupKey(kv.getKey())
                .idleCount(kv.getValue().first())
                .totalCount(kv.getValue().second())
                .build()));

        GetClusterUsageResponse res = resBuilder.build();
        log.info("Usage result: {}", res);
        return res;
    }

    private GetClusterIdleInstancesResponse onGetClusterIdleInstancesRequest(GetClusterIdleInstancesRequest req) {
        log.info("Computing idle instance list: {}", req);
        if (!req.getClusterID().equals(this.clusterID)) {
            throw new RuntimeException(String.format("Mismatch cluster ids %s, %s", req.getClusterID(), this.clusterID));
        }

        List<TaskExecutorID> instanceList = taskExecutorStateMap.entrySet().stream()
            .filter(kv -> {
                if (kv.getValue().getRegistration() == null) {
                    return false;
                }

                Optional<ContainerSkuID> skuIdO =
                    kv.getValue().getRegistration().getTaskExecutorContainerDefinitionId();
                return skuIdO.isPresent() && skuIdO.get().equals(req.getSkuId());
            })
            .filter(isAvailable)
            .map(kv -> kv.getKey())
            .limit(req.getMaxInstanceCount())
            .collect(Collectors.toList());

        GetClusterIdleInstancesResponse res = GetClusterIdleInstancesResponse.builder()
            .instanceIds(instanceList)
            .clusterId(this.clusterID)
            .skuId(req.getSkuId())
            .build();
        log.info("Return idle instance list: {}", res);
        return res;
    }

    private TaskExecutorsList getTaskExecutors(Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate) {
        return
            new TaskExecutorsList(
                taskExecutorStateMap
                    .entrySet()
                    .stream()
                    .filter(predicate)
                    .map(Entry::getKey)
                    .collect(Collectors.toList()));
    }

    private void onTaskExecutorInfoRequest(TaskExecutorInfoRequest request) {
        if (request.getTaskExecutorID() != null) {
            sender().tell(taskExecutorStateMap.get(request.getTaskExecutorID()).getRegistration(), self());
        } else {
            Optional<TaskExecutorRegistration> taskExecutorRegistration =
                taskExecutorStateMap
                    .values()
                    .stream()
                    .filter(state -> state.getRegistration() != null && state.getRegistration().getHostname().equals(request.getHostName()))
                    .findFirst()
                    .map(TaskExecutorState::getRegistration);
            if (taskExecutorRegistration.isPresent()) {
                sender().tell(taskExecutorRegistration.get(), self());
            } else {
                sender().tell(new Status.Failure(new Exception(String.format("Unknown task executor for hostname %s", request.getHostName()))), self());
            }
        }
    }

    private void onTaskExecutorGatewayRequest(TaskExecutorGatewayRequest request) {
        TaskExecutorState state = taskExecutorStateMap.get(request.getTaskExecutorID());
        if (state == null) {
            sender().tell(new Exception(), self());
        } else {
            if (state.isRegistered() && state.getGateway().isDone()) {
                sender().tell(state.getGateway().join(), self());
            } else {
                sender().tell(new Status.Failure(new Exception("")), self());
            }
        }
    }

    // custom equals function to check if the existing set already has the request under consideration.
    private boolean addNewDisableTaskExecutorsRequest(DisableTaskExecutorsRequest newRequest) {
        for (DisableTaskExecutorsRequest existing: activeDisableTaskExecutorsRequests) {
            if (existing.targetsSameTaskExecutorsAs(newRequest)) {
                return false;
            }
        }

        Preconditions.checkState(activeDisableTaskExecutorsRequests.add(newRequest), "activeDisableTaskExecutorRequests cannot contain %s", newRequest);
        return true;
    }

    private void onNewDisableTaskExecutorsRequest(DisableTaskExecutorsRequest request) {
        ActorRef sender = sender();
        if (addNewDisableTaskExecutorsRequest(request)) {
            try {
                // store the request in a persistent store in order to retrieve it if the node goes down
                mantisJobStore.storeNewDisabledTaskExecutorsRequest(request);
                // figure out the time to expire the current request
                Duration toExpiry = Comparators.max(Duration.between(clock.instant(), request.getExpiry()), Duration.ZERO);
                // setup a timer to clear it after a given period
                getTimers().startSingleTimer(
                    getExpiryKeyFor(request),
                    new ExpireDisableTaskExecutorsRequest(request),
                    toExpiry);
                findAndMarkDisabledTaskExecutors(new CheckDisabledTaskExecutors("new_request"));
                sender.tell(Ack.getInstance(), self());
            } catch (IOException e) {
                sender().tell(new Status.Failure(e), self());
            }
        } else {
            sender.tell(Ack.getInstance(), self());
        }
    }

    private String getExpiryKeyFor(DisableTaskExecutorsRequest request) {
        return "ExpireDisableTaskExecutorsRequest-" + request;
    }

    private void findAndMarkDisabledTaskExecutors(CheckDisabledTaskExecutors r) {
        log.info("Checking disabled task executors for Cluster {} because of {}", clusterID.getResourceID(), r.getReason());
        final Instant now = clock.instant();
        for (DisableTaskExecutorsRequest request: activeDisableTaskExecutorsRequests) {
            if (request.isExpired(now)) {
                self().tell(new ExpireDisableTaskExecutorsRequest(request), self());
            } else {
                // go and mark all task executors that match the filter as disabled
                taskExecutorStateMap.forEach((taskExecutorId, taskExecutorState) -> {
                    if (request.covers(taskExecutorState.getRegistration())) {
                        if (taskExecutorState.onNodeDisabled()) {
                            log.info("Marking task executor {} as disabled", taskExecutorId);
                        }
                    }
                });
            }
        }
    }

    private void onDisableTaskExecutorsRequestExpiry(ExpireDisableTaskExecutorsRequest request) {
        try {
            log.info("Expiring Disable Task Executors Request {}", request.getRequest());
            getTimers().cancel(getExpiryKeyFor(request.getRequest()));
            if (activeDisableTaskExecutorsRequests.remove(request.getRequest())) {
                mantisJobStore.deleteExpiredDisableTaskExecutorsRequest(request.getRequest());
            }
        } catch (Exception e) {
            log.error("Failed to delete expired {}", request.getRequest());
        }
    }

    private Map<TaskExecutorID, WorkerId> getTaskExecutorWorkerMapping(Map<String, String> attributes) {
        final Map<TaskExecutorID, WorkerId> result = new HashMap<>();
        taskExecutorStateMap.forEach((taskExecutorID, taskExecutorState) -> {
            if (taskExecutorState.getRegistration() != null && taskExecutorState.getRegistration().containsAttributes(attributes)) {
                if (taskExecutorState.isRunningTask()) {
                    result.put(taskExecutorID, taskExecutorState.getWorkerId());
                }
            }
        });
        return result;
    }

    private void onTaskExecutorInitialization(InitializeTaskExecutorRequest request) {
        log.info("Initializing taskExecutor {} for the resource cluster {}", request.getTaskExecutorID(), this);
        ActorRef sender = sender();
        try {
            TaskExecutorRegistration registration =
                mantisJobStore.getTaskExecutor(request.getTaskExecutorID());
            setupTaskExecutorStateIfNecessary(request.getTaskExecutorID());
            self().tell(registration, self());
            self().tell(
                new TaskExecutorStatusChange(
                    registration.getTaskExecutorID(),
                    registration.getClusterID(),
                    TaskExecutorReport.occupied(request.getWorkerId())),
                self());
            sender.tell(Ack.getInstance(), self());
        } catch (Exception e) {
            log.error("Failed to initialize taskExecutor {}; all retries exhausted", request.getTaskExecutorID(), e);
            sender.tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorRegistration(TaskExecutorRegistration registration) {
        setupTaskExecutorStateIfNecessary(registration.getTaskExecutorID());
        log.info("Request for registering on resource cluster {}: {}.", this, registration);
        try {
            final TaskExecutorID taskExecutorID = registration.getTaskExecutorID();
            final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
            boolean stateChange = state.onRegistration(registration);
            mantisJobStore.storeNewTaskExecutor(registration);
            if (stateChange) {
                if (state.isAvailable()) {
                    taskExecutorsReadyToPerformWork.add(taskExecutorID);
                }
                // check if the task executor has been marked as 'Disabled'
                for (DisableTaskExecutorsRequest request: activeDisableTaskExecutorsRequests) {
                    if (request.covers(registration)) {
                        log.info("Newly registered task executor {} was already marked for disabling because of {}", registration.getTaskExecutorID(), request);
                        state.onNodeDisabled();
                    }
                }
                updateHeartbeatTimeout(registration.getTaskExecutorID());
            }
            log.info("Successfully registered {} with the resource cluster {}", registration.getTaskExecutorID(), this);
            sender().tell(Ack.getInstance(), self());
        } catch (Exception e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onHeartbeat(TaskExecutorHeartbeat heartbeat) {
        setupTaskExecutorStateIfNecessary(heartbeat.getTaskExecutorID());
        try {
            final TaskExecutorID taskExecutorID = heartbeat.getTaskExecutorID();
            final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
            boolean stateChange = state.onHeartbeat(heartbeat);
            if (stateChange) {
                if (state.isAvailable()) {
                    taskExecutorsReadyToPerformWork.add(taskExecutorID);
                }
            }

            updateHeartbeatTimeout(heartbeat.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorStatusChange(TaskExecutorStatusChange statusChange) {
        setupTaskExecutorStateIfNecessary(statusChange.getTaskExecutorID());
        try {
            final TaskExecutorID taskExecutorID = statusChange.getTaskExecutorID();
            final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
            boolean stateChange = state.onTaskExecutorStatusChange(statusChange);
            if (stateChange) {
                if (state.isAvailable()) {
                    taskExecutorsReadyToPerformWork.add(taskExecutorID);
                } else {
                    taskExecutorsReadyToPerformWork.remove(taskExecutorID);
                }
            }

            updateHeartbeatTimeout(statusChange.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorAssignmentRequest(TaskExecutorAssignmentRequest request) {
        Optional<Entry<TaskExecutorID, TaskExecutorState>> matchedExecutor =
            taskExecutorStateMap
                .entrySet()
                .stream()
                .filter(entry -> (entry.getValue().isAvailable() &&
                    entry.getValue().getRegistration().getMachineDefinition()
                        .canFit(request.getMachineDefinition())))
                .findAny();

        if (matchedExecutor.isPresent()) {
            log.info("matched executor {} for request {}", matchedExecutor.get().getKey(), request);
            matchedExecutor.get().getValue().onAssignment(request.getWorkerId());
            // let's give some time for the assigned executor to be scheduled work. otherwise, the assigned executor
            // will be returned back to the pool.
            getTimers().startSingleTimer(
                "Assignment-" + matchedExecutor.get().getKey().toString(),
                new TaskExecutorAssignmentTimeout(matchedExecutor.get().getKey()),
                assignmentTimeout);
            sender().tell(matchedExecutor.get().getKey(), self());
        } else {
            sender().tell(new Status.Failure(new NoResourceAvailableException(
                String.format("No resource available for request %s: resource overview: %s", request,
                    getResourceOverview()))), self());
        }
    }

    private void onTaskExecutorAssignmentTimeout(TaskExecutorAssignmentTimeout request) {
        try {
            TaskExecutorState state = taskExecutorStateMap.get(request.getTaskExecutorID());
            if (state.isRunningTask()) {
                log.debug("TaskExecutor {} entered running state alraedy; no need to act", request.getTaskExecutorID());
            } else {
                boolean stateChange = state.onUnassignment();
                if (stateChange) {
                    taskExecutorsReadyToPerformWork.add(request.getTaskExecutorID());
                }
            }
        } catch (IllegalStateException e) {
            log.error("Failed to un-assign taskExecutor {}", request.getTaskExecutorID(), e);
        }
    }

    private void onResourceOverviewRequest(ResourceOverviewRequest request) {
        sender().tell(getResourceOverview(), self());
    }

    private ResourceOverview getResourceOverview() {
        long numRegistered = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isRegistered).count();
        long numAvailable = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isAvailable).count();
        long numOccupied = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isRunningTask).count();
        long numAssigned = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isAssigned).count();
        long numDisabled = taskExecutorStateMap.values().stream().filter(TaskExecutorState::isDisabled).count();

        return new ResourceOverview(numRegistered, numAvailable, numOccupied, numAssigned, numDisabled);
    }

    private TaskExecutorStatus getTaskExecutorStatus(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
        return new TaskExecutorStatus(
            state.getRegistration(),
            state.isRegistered(),
            state.isRunningTask(),
            state.isAssigned(),
            state.isDisabled(),
            state.getWorkerId(),
            state.getLastActivity().toEpochMilli());
    }

    private void onTaskExecutorDisconnection(TaskExecutorDisconnection disconnection) {
        setupTaskExecutorStateIfNecessary(disconnection.getTaskExecutorID());
        try {
            disconnectTaskExecutor(disconnection.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void disconnectTaskExecutor(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
        boolean stateChange = state.onDisconnection();
        if (stateChange) {
            taskExecutorsReadyToPerformWork.remove(taskExecutorID);
            getTimers().cancel(getHeartbeatTimerFor(taskExecutorID));
        }
    }

    private String getHeartbeatTimerFor(TaskExecutorID taskExecutorID) {
        return "Heartbeat-" + taskExecutorID.toString();
    }

    private void onTaskExecutorHeartbeatTimeout(HeartbeatTimeout timeout) {
        setupTaskExecutorStateIfNecessary(timeout.getTaskExecutorID());
        try {
            log.info("heartbeat timeout received for {}", timeout.getTaskExecutorID());
            final TaskExecutorID taskExecutorID = timeout.getTaskExecutorID();
            final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
            if (state.getLastActivity().compareTo(timeout.getLastActivity()) <= 0) {
                log.info("Disconnecting task executor {}", timeout.getTaskExecutorID());
                disconnectTaskExecutor(timeout.getTaskExecutorID());
            }

        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void setupTaskExecutorStateIfNecessary(TaskExecutorID taskExecutorID) {
        taskExecutorStateMap.putIfAbsent(taskExecutorID, TaskExecutorState.of(clock, rpcService, jobMessageRouter));
    }

    private void updateHeartbeatTimeout(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
        getTimers().startSingleTimer(
            getHeartbeatTimerFor(taskExecutorID),
            new HeartbeatTimeout(taskExecutorID, state.getLastActivity()),
            heartbeatTimeout);
    }

    @Value
    private static class HeartbeatTimeout {

        TaskExecutorID taskExecutorID;
        Instant lastActivity;
    }

    @Value
    static class TaskExecutorAssignmentRequest {
        MachineDefinition machineDefinition;
        WorkerId workerId;
        ClusterID clusterID;
    }

    @Value
    static class TaskExecutorAssignmentTimeout {
        TaskExecutorID taskExecutorID;
    }

    @Value
    static class ExpireDisableTaskExecutorsRequest {
        DisableTaskExecutorsRequest request;
    }

    @Value
    static class InitializeTaskExecutorRequest {
        TaskExecutorID taskExecutorID;
        WorkerId workerId;
    }

    @Value
    static class ResourceOverviewRequest {
        ClusterID clusterID;
    }

    @Value
    static class TaskExecutorInfoRequest {
        @Nullable
        TaskExecutorID taskExecutorID;

        @Nullable
        String hostName;

        ClusterID clusterID;
    }

    @Value
    static class TaskExecutorGatewayRequest {
        TaskExecutorID taskExecutorID;

        ClusterID clusterID;
    }

    @Value
    static class GetRegisteredTaskExecutorsRequest {
        ClusterID clusterID;
    }

    @Value
    static class GetAvailableTaskExecutorsRequest {
        ClusterID clusterID;
    }
    @Value
    static class GetDisabledTaskExecutorsRequest {
        ClusterID clusterID;
    }

    @Value
    static class GetBusyTaskExecutorsRequest {
        ClusterID clusterID;
    }

    @Value
    static class GetUnregisteredTaskExecutorsRequest {
        ClusterID clusterID;
    }

    @Value
    static class GetTaskExecutorStatusRequest {
        TaskExecutorID taskExecutorID;
        ClusterID clusterID;
    }

    @Value
    static class TaskExecutorsList {
        List<TaskExecutorID> taskExecutors;
    }

    @Value
    static class GetClusterUsageRequest {
        ClusterID clusterID;
        Function<TaskExecutorRegistration, Optional<String>> groupKeyFunc;
    }

    @Value
    private static class CheckDisabledTaskExecutors {
        String reason;
    }

    @Value
    static class GetTaskExecutorWorkerMappingRequest {
        Map<String, String> attributes;
    }

    /**
     * Represents the Availability of a given node in the resource cluster.
     * Can go from PENDING -> ASSIGNED(workerId) -> RUNNING(workerId) -> PENDING
     * in the happy path.
     */
    interface AvailabilityState {
        @Nullable
        WorkerId getWorkerId();

        AvailabilityState onAssignment(WorkerId workerId);

        AvailabilityState onUnassignment();

        AvailabilityState onTaskExecutorStatusChange(TaskExecutorReport report);

        Pending PENDING = new Pending();

        static AvailabilityState pending() {
            return PENDING;
        }

        static AvailabilityState assigned(WorkerId workerId) {
            return new Assigned(workerId);
        }

        static AvailabilityState running(WorkerId workerId) {
            return new Running(workerId);
        }

        default <T> T throwInvalidTransition() throws IllegalStateException {
            throw new IllegalStateException(
                String.format("availability state was %s when worker was unassigned", this));
        }

        default <T> T throwInvalidTransition(WorkerId workerId) throws IllegalStateException {
            throw new IllegalStateException(
                String.format("availability state was %s when workerId %s was assigned",
                    this, workerId));
        }

        default <T> T throwInvalidTransition(TaskExecutorReport report) throws IllegalStateException {
            throw new IllegalStateException(
                String.format("availability state was %s when report %s was received", this, report));
        }
    }

    @Value
    static class Pending implements AvailabilityState {
        @Override
        public WorkerId getWorkerId() {
            return null;
        }

        @Override
        public AvailabilityState onAssignment(WorkerId workerId) {
            return AvailabilityState.assigned(workerId);
        }

        @Override
        public AvailabilityState onUnassignment() {
            return this;
        }

        @Override
        public AvailabilityState onTaskExecutorStatusChange(TaskExecutorReport report) {
            if (report instanceof Available) {
                return this;
            } else if (report instanceof Occupied) {
                return AvailabilityState.running(((Occupied) report).getWorkerId());
            } else {
                return throwInvalidTransition(report);
            }
        }
    }

    @Value
    static class Assigned implements AvailabilityState {
        WorkerId workerId;

        @Override
        public AvailabilityState onAssignment(WorkerId workerId) {
            if (this.workerId.equals(workerId)) {
                return this;
            } else {
                return throwInvalidTransition(workerId);
            }
        }

        @Override
        public AvailabilityState onUnassignment() {
            return AvailabilityState.pending();
        }

        @Override
        public AvailabilityState onTaskExecutorStatusChange(TaskExecutorReport report) {
            if (report instanceof Available) {
                return this;
            } else if (report instanceof Occupied) {
                return AvailabilityState.running(workerId);
            } else {
                return throwInvalidTransition(report);
            }
        }
    }

    @Value
    static class Running implements AvailabilityState {
        WorkerId workerId;

        @Override
        public AvailabilityState onAssignment(WorkerId workerId) {
            return throwInvalidTransition(workerId);
        }

        @Override
        public AvailabilityState onUnassignment() {
            return throwInvalidTransition();
        }

        @Override
        public AvailabilityState onTaskExecutorStatusChange(TaskExecutorReport report) {
            if (report instanceof Available) {
                return AvailabilityState.pending();
            } else if (report instanceof Occupied) {
                return this;
            } else {
                return throwInvalidTransition(report);
            }
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    @AllArgsConstructor
    static class TaskExecutorState {

        enum RegistrationState {
            Registered,
            Unregistered,
        }

        private RegistrationState state;
        @Nullable
        private TaskExecutorRegistration registration;

        @Nullable
        private CompletableFuture<TaskExecutorGateway> gateway;

        // availabilityState being null here represents that we don't know about the actual state of the task executor
        // and are waiting for more information
        @Nullable
        private AvailabilityState availabilityState;
        private boolean disabled;
        // last interaction initiated by the task executor
        private Instant lastActivity;
        private final Clock clock;
        private final RpcService rpcService;
        private final JobMessageRouter jobMessageRouter;

        static TaskExecutorState of(Clock clock, RpcService rpcService, JobMessageRouter jobMessageRouter) {
            return new TaskExecutorState(
                RegistrationState.Unregistered,
                null,
                null,
                null,
                false,
                clock.instant(),
                clock,
                rpcService,
                jobMessageRouter);
        }

        boolean isRegistered() {
            return state == RegistrationState.Registered;
        }

        boolean isDisconnected() {
            return !isRegistered();
        }

        boolean isDisabled() {
            return disabled;
        }

        boolean onRegistration(TaskExecutorRegistration registration) {
            if (state == RegistrationState.Registered) {
                return false;
            } else {
                this.state = RegistrationState.Registered;
                this.registration = registration;
                this.gateway =
                    rpcService.connect(registration.getTaskExecutorAddress(), TaskExecutorGateway.class)
                        .whenComplete((gateway, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to connect to the gateway", throwable);
                            }
                        });
                updateTicker();
                return true;
            }
        }

        boolean onDisconnection() {
            if (state == RegistrationState.Unregistered) {
                return false;
            } else {
                state = RegistrationState.Unregistered;
                registration = null;
                setAvailabilityState(null);
                gateway = null;
                updateTicker();
                return true;
            }
        }

        private static AvailabilityState from(TaskExecutorReport report) {
            if (report instanceof Available) {
                return AvailabilityState.pending();
            } else if (report instanceof Occupied) {
                return AvailabilityState.running(((Occupied) report).getWorkerId());
            } else {
                throw new RuntimeException(String.format("TaskExecutorReport=%s was unexpected", report));
            }
        }

        boolean onAssignment(WorkerId workerId) throws IllegalStateException {
            if (!isRegistered()) {
                throwNotRegistered(String.format("assignment to %s", workerId));
            }

            if (this.availabilityState == null) {
                throw new IllegalStateException("availability state was null when unassignmentas was issued");
            }

            return setAvailabilityState(this.availabilityState.onAssignment(workerId));
        }

        boolean onUnassignment() throws IllegalStateException {
            if (this.availabilityState == null) {
                throw new IllegalStateException("availability state was null when unassignment was issued");
            }

            return setAvailabilityState(this.availabilityState.onUnassignment());
        }

        boolean onNodeDisabled() {
            if (!this.disabled) {
                this.disabled = true;
                if (this.availabilityState instanceof Running) {
                    jobMessageRouter.routeWorkerEvent(new WorkerOnDisabledVM(this.availabilityState.getWorkerId()));
                }
                return true;
            } else {
                return false;
            }
        }

        boolean onHeartbeat(TaskExecutorHeartbeat heartbeat) throws IllegalStateException {
            if (!isRegistered()) {
                throwNotRegistered(String.format("heartbeat %s", heartbeat));
            }

            boolean result = handleStatusChange(heartbeat.getTaskExecutorReport());
            updateTicker();
            return result;
        }

        boolean onTaskExecutorStatusChange(TaskExecutorStatusChange statusChange) {
            if (!isRegistered()) {
                throwNotRegistered(String.format("status change %s", statusChange));
            }

            boolean result = handleStatusChange(statusChange.getTaskExecutorReport());
            updateTicker();
            return result;
        }

        private boolean handleStatusChange(TaskExecutorReport report) throws IllegalStateException {
            if (availabilityState == null) {
                return setAvailabilityState(from(report));
            } else {
                return setAvailabilityState(availabilityState.onTaskExecutorStatusChange(report));
            }
        }

        private boolean setAvailabilityState(AvailabilityState newState) {
            if (this.availabilityState != newState) {
                this.availabilityState = newState;
                if (this.availabilityState instanceof Running) {
                    if (isDisabled()) {
                        jobMessageRouter.routeWorkerEvent(new WorkerOnDisabledVM(newState.getWorkerId()));
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        @Nullable
        private WorkerId getWorkerId() {
            if (this.availabilityState != null) {
                return this.availabilityState.getWorkerId();
            } else {
                return null;
            }
        }

        private void throwNotRegistered(String message) throws IllegalStateException {
            throw new IllegalStateException(
                String.format("Task Executor un-registered when it received %s", message));
        }

        private void updateTicker() {
            this.lastActivity = clock.instant();
        }

        boolean isAvailable() {
            return this.availabilityState instanceof Pending && !isDisabled();
        }

        boolean isRunningTask() {
            return this.availabilityState instanceof Running;
        }

        boolean isAssigned() {
            return this.availabilityState instanceof Assigned;
        }

        // Captures the last interaction from the task executor. Any interactions
        // that are caused from within the server do not cause an uptick.
        Instant getLastActivity() {
            return this.lastActivity;
        }

        TaskExecutorRegistration getRegistration() {
            return this.registration;
        }

        private CompletableFuture<TaskExecutorGateway> getGateway() {
            return this.gateway;
        }
    }
}
