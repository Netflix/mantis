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

import static java.util.stream.Collectors.groupingBy;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status;
import akka.japi.pf.ReceiveBuilder;
import com.netflix.spectator.api.TagList;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.master.resourcecluster.metrics.ResourceClusterActorMetrics;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.server.core.CacheJobArtifactsRequest;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.PagedActiveJobOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ConnectionFailedException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport.Available;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport.Occupied;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.server.worker.TaskExecutorGateway.TaskNotFoundException;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Comparators;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.vavr.Tuple;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.rpc.RpcService;

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

    private final ExecutorStateManager executorStateManager;
    private final Clock clock;
    private final RpcService rpcService;
    private final ClusterID clusterID;
    private final MantisJobStore mantisJobStore;
    private final Set<DisableTaskExecutorsRequest> activeDisableTaskExecutorsRequests;
    private final JobMessageRouter jobMessageRouter;

    private final ResourceClusterActorMetrics metrics;

    private final HashSet<ArtifactID> jobArtifactsToCache = new HashSet<>();

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
        this.mantisJobStore = mantisJobStore;
        this.activeDisableTaskExecutorsRequests = new HashSet<>();

        this.executorStateManager = new ExecutorStateManagerImpl();

        this.metrics = new ResourceClusterActorMetrics();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        fetchJobArtifactsToCache();

        List<DisableTaskExecutorsRequest> activeRequests =
            mantisJobStore.loadAllDisableTaskExecutorsRequests(clusterID);
        for (DisableTaskExecutorsRequest request : activeRequests) {
            onNewDisableTaskExecutorsRequest(request);
        }

        timers().startTimerWithFixedDelay(
            String.format("periodic-disabled-task-executors-test-for-%s", clusterID.getResourceID()),
            new CheckDisabledTaskExecutors("periodic"),
            disabledTaskExecutorsCheckInterval);

        timers().startTimerWithFixedDelay(
            "periodic-resource-overview-metrics-publisher",
            new PublishResourceOverviewMetricsRequest(),
            Duration.ofMinutes(1));
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(GetRegisteredTaskExecutorsRequest.class,
                    req -> {
                        sender().tell(getTaskExecutors(filterByAttrs(req).and(ExecutorStateManager.isRegistered)), self());
                    })
                .match(GetBusyTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(filterByAttrs(req).and(ExecutorStateManager.isBusy)), self()))
                .match(GetAvailableTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(filterByAttrs(req).and(ExecutorStateManager.isAvailable)), self()))
                .match(GetDisabledTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(filterByAttrs(req).and(ExecutorStateManager.isDisabled)), self()))
                .match(GetUnregisteredTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(filterByAttrs(req).and(ExecutorStateManager.unregistered)), self()))
                .match(GetActiveJobsRequest.class, this::getActiveJobs)
                .match(GetTaskExecutorStatusRequest.class, req -> sender().tell(getTaskExecutorStatus(req.getTaskExecutorID()), self()))
                .match(GetClusterUsageRequest.class,
                    req -> sender().tell(this.executorStateManager.getClusterUsage(req), self()))
                .match(GetClusterIdleInstancesRequest.class,
                    req -> sender().tell(onGetClusterIdleInstancesRequest(req), self()))
                .match(GetAssignedTaskExecutorRequest.class, this::onAssignedTaskExecutorRequest)
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
                .match(PublishResourceOverviewMetricsRequest.class, this::onPublishResourceOverviewMetricsRequest)
                .match(CacheJobArtifactsOnTaskExecutorRequest.class, this::onCacheJobArtifactsOnTaskExecutorRequest)
                .match(AddNewJobArtifactsToCacheRequest.class, this::onAddNewJobArtifactsToCacheRequest)
                .match(RemoveJobArtifactsToCacheRequest.class, this::onRemoveJobArtifactsToCacheRequest)
                .match(GetJobArtifactsToCacheRequest.class, req -> sender().tell(new ArtifactList(new ArrayList<>(jobArtifactsToCache)), self()))
                .build();
    }

    private void onAddNewJobArtifactsToCacheRequest(AddNewJobArtifactsToCacheRequest req) {
        try {
            mantisJobStore.addNewJobArtifactsToCache(req.getClusterID(), req.getArtifacts());
            jobArtifactsToCache.addAll(req.artifacts);
            sender().tell(Ack.getInstance(), self());
        } catch (IOException e) {
            log.warn("Cannot add new job artifacts {} to cache in cluster: {}", req.getArtifacts(), req.getClusterID(), e);
        }
    }

    private void onRemoveJobArtifactsToCacheRequest(RemoveJobArtifactsToCacheRequest req) {
        try {
            mantisJobStore.removeJobArtifactsToCache(req.getClusterID(), req.getArtifacts());
            req.artifacts.forEach(jobArtifactsToCache::remove);
            sender().tell(Ack.getInstance(), self());
        } catch (IOException e) {
            log.warn("Cannot remove job artifacts {} to cache in cluster: {}", req.getArtifacts(), req.getClusterID(), e);
        }
    }

    private void fetchJobArtifactsToCache() {
        try {
            mantisJobStore.getJobArtifactsToCache(clusterID)
                .stream()
                .map(ArtifactID::of)
                .forEach(jobArtifactsToCache::add);
        } catch (IOException e) {
            log.warn("Cannot refresh job artifacts to cache in cluster: {}", clusterID, e);
        }
    }

    private GetClusterIdleInstancesResponse onGetClusterIdleInstancesRequest(GetClusterIdleInstancesRequest req) {
        log.info("Computing idle instance list: {}", req);
        if (!req.getClusterID().equals(this.clusterID)) {
            throw new RuntimeException(String.format("Mismatch cluster ids %s, %s", req.getClusterID(), this.clusterID));
        }

        List<TaskExecutorID> instanceList = this.executorStateManager.getIdleInstanceList(req);

        GetClusterIdleInstancesResponse res = GetClusterIdleInstancesResponse.builder()
            .instanceIds(instanceList)
            .clusterId(this.clusterID)
            .skuId(req.getSkuId())
            .build();
        log.info("Return idle instance list: {}", res);
        return res;
    }

    private TaskExecutorsList getTaskExecutors(Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate) {
        return new TaskExecutorsList(this.executorStateManager.getTaskExecutors(predicate));
    }

    private void getActiveJobs(GetActiveJobsRequest req) {
        List<String> pagedList = this.executorStateManager.getActiveJobs(req);

        PagedActiveJobOverview res =
            new PagedActiveJobOverview(
                pagedList,
                req.getStartingIndex().orElse(0) + pagedList.size()
            );

        log.info("Returning getActiveJobs res starting at {}: {}", req.getStartingIndex(), res.getActiveJobs().size());
        sender().tell(res, self());
    }

    private void onTaskExecutorInfoRequest(TaskExecutorInfoRequest request) {
        if (request.getTaskExecutorID() != null) {
            sender().tell(this.executorStateManager.get(request.getTaskExecutorID()).getRegistration(), self());
        } else {
            Optional<TaskExecutorRegistration> taskExecutorRegistration =
                this.executorStateManager
                    .findFirst(
                        kv -> kv.getValue().getRegistration() != null &&
                            kv.getValue().getRegistration().getHostname().equals(request.getHostName()))
                    .map(Entry::getValue)
                    .map(TaskExecutorState::getRegistration);
            if (taskExecutorRegistration.isPresent()) {
                sender().tell(taskExecutorRegistration.get(), self());
            } else {
                sender().tell(new Status.Failure(new Exception(String.format("Unknown task executor for hostname %s", request.getHostName()))), self());
            }
        }
    }

    private void onAssignedTaskExecutorRequest(GetAssignedTaskExecutorRequest request) {
        Optional<TaskExecutorID> matchedTaskExecutor =
            this.executorStateManager.findFirst(
                e -> e.getValue().isRunningOrAssigned(request.getWorkerId())).map(Entry::getKey);

        if (matchedTaskExecutor.isPresent()) {
            sender().tell(matchedTaskExecutor.get(), self());
        } else {
            sender().tell(new Status.Failure(new TaskNotFoundException(request.getWorkerId())),
                self());
        }
    }

    private void onTaskExecutorGatewayRequest(TaskExecutorGatewayRequest request) {
        TaskExecutorState state = this.executorStateManager.get(request.getTaskExecutorID());
        if (state == null) {
            sender().tell(new Exception(), self());
        } else {
            try {
                if (state.isRegistered()) {
                    sender().tell(state.getGateway().join(), self());
                } else {
                    sender().tell(new Status.Failure(new Exception("")), self());
                }
            } catch (Exception e) {
                metrics.incrementCounter(
                    ResourceClusterActorMetrics.TE_CONNECTION_FAILURE,
                    TagList.create(ImmutableMap.of(
                        "resourceCluster",
                        clusterID.getResourceID(),
                        "taskExecutor",
                        request.getTaskExecutorID().getResourceId())));
                try {
                    // let's try one more time by reconnecting with the gateway.
                    sender().tell(state.reconnect().join(), self());
                } catch (Exception e1) {
                    metrics.incrementCounter(
                        ResourceClusterActorMetrics.TE_RECONNECTION_FAILURE,
                        TagList.create(ImmutableMap.of(
                            "resourceCluster",
                            clusterID.getResourceID(),
                            "taskExecutor",
                            request.getTaskExecutorID().getResourceId())));
                    sender().tell(new Status.Failure(new ConnectionFailedException(e)), self());
                }
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
        for (DisableTaskExecutorsRequest request : activeDisableTaskExecutorsRequests) {
            if (request.isExpired(now)) {
                self().tell(new ExpireDisableTaskExecutorsRequest(request), self());
            } else {
                // go and mark all task executors that match the filter as disabled
                this.executorStateManager.getActiveExecutorEntry().forEach(idAndState -> {
                    if (request.covers(idAndState.getValue().getRegistration())) {
                        if (idAndState.getValue().onNodeDisabled()) {
                            log.info("Marking task executor {} as disabled", idAndState.getKey());
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
        this.executorStateManager.getActiveExecutorEntry().forEach(idAndState -> {
            if (idAndState.getValue().getRegistration() != null && idAndState.getValue().getRegistration().containsAttributes(attributes)) {
                if (idAndState.getValue().isRunningTask()) {
                    result.put(idAndState.getKey(), idAndState.getValue().getWorkerId());
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
            final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
            boolean stateChange = state.onRegistration(registration);
            mantisJobStore.storeNewTaskExecutor(registration);
            if (stateChange) {
                if (state.isAvailable()) {
                    this.executorStateManager.markAvailable(taskExecutorID);
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
            if (!jobArtifactsToCache.isEmpty() && isJobArtifactCachingEnabled()) {
                self().tell(new CacheJobArtifactsOnTaskExecutorRequest(taskExecutorID, clusterID), self());
            }
            sender().tell(Ack.getInstance(), self());
        } catch (Exception e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onHeartbeat(TaskExecutorHeartbeat heartbeat) {
        setupTaskExecutorStateIfNecessary(heartbeat.getTaskExecutorID());
        try {
            final TaskExecutorID taskExecutorID = heartbeat.getTaskExecutorID();
            final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
            boolean stateChange = state.onHeartbeat(heartbeat);
            if (stateChange) {
                if (state.isAvailable()) {
                    this.executorStateManager.markAvailable(taskExecutorID);
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
            final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
            boolean stateChange = state.onTaskExecutorStatusChange(statusChange);
            if (stateChange) {
                if (state.isAvailable()) {
                    this.executorStateManager.markAvailable(taskExecutorID);
                } else {
                    this.executorStateManager.markUnavailable(taskExecutorID);
                }
            }

            updateHeartbeatTimeout(statusChange.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorAssignmentRequest(TaskExecutorAssignmentRequest request) {
        Optional<Pair<TaskExecutorID, TaskExecutorState>> matchedExecutor =
            this.executorStateManager.findBestFit(request);

        if (matchedExecutor.isPresent()) {
            log.info("matched executor {} for request {}", matchedExecutor.get().getKey(), request);
            matchedExecutor.get().getValue().onAssignment(request.getAllocationRequest().getWorkerId());
            // let's give some time for the assigned executor to be scheduled work. otherwise, the assigned executor
            // will be returned back to the pool.
            getTimers().startSingleTimer(
                "Assignment-" + matchedExecutor.get().getKey().toString(),
                new TaskExecutorAssignmentTimeout(matchedExecutor.get().getKey()),
                assignmentTimeout);
            sender().tell(matchedExecutor.get().getKey(), self());
        } else {
            metrics.incrementCounter(
                ResourceClusterActorMetrics.NO_RESOURCES_AVAILABLE,
                TagList.create(ImmutableMap.of(
                    "resourceCluster",
                    clusterID.getResourceID(),
                    "workerId",
                    request.getAllocationRequest().getWorkerId().getId(),
                    "jobCluster",
                    request.getAllocationRequest().getWorkerId().getJobCluster(),
                    "cpuCores",
                    String.valueOf(request.getAllocationRequest().getMachineDefinition().getCpuCores()))));
            sender().tell(new Status.Failure(new NoResourceAvailableException(
                String.format("No resource available for request %s: resource overview: %s", request,
                    getResourceOverview()))), self());
        }
    }

    private void onTaskExecutorAssignmentTimeout(TaskExecutorAssignmentTimeout request) {
        TaskExecutorState state = this.executorStateManager.get(request.getTaskExecutorID());
        if (state.isRunningTask()) {
            log.debug("TaskExecutor {} entered running state alraedy; no need to act", request.getTaskExecutorID());
        } else {
            try
            {
                boolean stateChange = state.onUnassignment();
                if (stateChange) {
                    this.executorStateManager.markAvailable(request.getTaskExecutorID());
                }
            } catch (IllegalStateException e) {
                if (state.isRegistered()) {
                    log.error("Failed to un-assign registered taskExecutor {}", request.getTaskExecutorID(), e);
                } else {
                    log.debug("Failed to un-assign unRegistered taskExecutor {}", request.getTaskExecutorID(), e);
                }
            }
        }
    }

    private void onResourceOverviewRequest(ResourceOverviewRequest request) {
        sender().tell(getResourceOverview(), self());
    }

    private void onPublishResourceOverviewMetricsRequest(PublishResourceOverviewMetricsRequest request) {
        publishResourceClusterMetricBySKU(getTaskExecutors(ExecutorStateManager.isRegistered), ResourceClusterActorMetrics.NUM_REGISTERED_TE);
        publishResourceClusterMetricBySKU(getTaskExecutors(ExecutorStateManager.isBusy), ResourceClusterActorMetrics.NUM_BUSY_TE);
        publishResourceClusterMetricBySKU(getTaskExecutors(ExecutorStateManager.isAvailable), ResourceClusterActorMetrics.NUM_AVAILABLE_TE);
        publishResourceClusterMetricBySKU(getTaskExecutors(ExecutorStateManager.isDisabled), ResourceClusterActorMetrics.NUM_DISABLED_TE);
        publishResourceClusterMetricBySKU(getTaskExecutors(ExecutorStateManager.unregistered), ResourceClusterActorMetrics.NUM_UNREGISTERED_TE);
        publishResourceClusterMetricBySKU(getTaskExecutors(ExecutorStateManager.isAssigned), ResourceClusterActorMetrics.NUM_ASSIGNED_TE);
    }

    private void publishResourceClusterMetricBySKU(TaskExecutorsList taskExecutorsList, String metricName) {
        try {
            taskExecutorsList.getTaskExecutors()
                .stream()
                .map(this::getTaskExecutorStatus)
                .filter(Objects::nonNull)
                .map(TaskExecutorStatus::getRegistration)
                .filter(Objects::nonNull)
                .filter(registration -> registration.getTaskExecutorContainerDefinitionId().isPresent() && registration.getAttributeByKey(WorkerConstants.AUTO_SCALE_GROUP_KEY).isPresent())
                .collect(groupingBy(registration -> Tuple.of(registration.getTaskExecutorContainerDefinitionId().get(), registration.getAttributeByKey(WorkerConstants.AUTO_SCALE_GROUP_KEY).get()), Collectors.counting()))
                .forEach((keys, count) -> metrics.setGauge(
                    metricName,
                    count,
                    TagList.create(ImmutableMap.of("resourceCluster", clusterID.getResourceID(), "sku", keys._1.getResourceID(), "autoScaleGroup", keys._2))));
        } catch (Exception e) {
            log.warn("Error while publishing resource cluster metrics by sku. RC: {}, Metric: {}.", clusterID.getResourceID(), metricName, e);
        }
    }

    private ResourceOverview getResourceOverview() {
        return this.executorStateManager.getResourceOverview();
    }

    private TaskExecutorStatus getTaskExecutorStatus(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
        if (state == null) {
            log.warn("Unknown executorID: {}", taskExecutorID);
            return null;
        }

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
        final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
        boolean stateChange = state.onDisconnection();
        if (stateChange) {
            this.executorStateManager.archive(taskExecutorID);
            getTimers().cancel(getHeartbeatTimerFor(taskExecutorID));
        }
    }

    private String getHeartbeatTimerFor(TaskExecutorID taskExecutorID) {
        return "Heartbeat-" + taskExecutorID.toString();
    }

    private void onTaskExecutorHeartbeatTimeout(HeartbeatTimeout timeout) {
        setupTaskExecutorStateIfNecessary(timeout.getTaskExecutorID());
        try {
            metrics.incrementCounter(
                ResourceClusterActorMetrics.HEARTBEAT_TIMEOUT,
                TagList.create(ImmutableMap.of("resourceCluster", clusterID.getResourceID(), "taskExecutorID", timeout.getTaskExecutorID().getResourceId())));
            log.info("heartbeat timeout received for {}", timeout.getTaskExecutorID());
            final TaskExecutorID taskExecutorID = timeout.getTaskExecutorID();
            final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
            if (state.getLastActivity().compareTo(timeout.getLastActivity()) <= 0) {
                log.info("Disconnecting task executor {}", timeout.getTaskExecutorID());
                disconnectTaskExecutor(timeout.getTaskExecutorID());
            }

        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void setupTaskExecutorStateIfNecessary(TaskExecutorID taskExecutorID) {
        this.executorStateManager
            .putIfAbsent(taskExecutorID, TaskExecutorState.of(clock, rpcService, jobMessageRouter));
    }

    private void updateHeartbeatTimeout(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
        getTimers().startSingleTimer(
            getHeartbeatTimerFor(taskExecutorID),
            new HeartbeatTimeout(taskExecutorID, state.getLastActivity()),
            heartbeatTimeout);
    }

    private void onCacheJobArtifactsOnTaskExecutorRequest(CacheJobArtifactsOnTaskExecutorRequest request) {
        TaskExecutorState state = this.executorStateManager.get(request.getTaskExecutorID());
        if (state != null) {
            TaskExecutorGateway gateway = state.getGateway().join();
            List<URI> artifacts = jobArtifactsToCache.stream().map(artifactID -> URI.create(artifactID.getResourceID())).collect(Collectors.toList());
            gateway.cacheJobArtifacts(new CacheJobArtifactsRequest(artifacts));
        }
    }

    @Value
    private static class HeartbeatTimeout {

        TaskExecutorID taskExecutorID;
        Instant lastActivity;
    }

    @Value
    static class TaskExecutorAssignmentRequest {
        TaskExecutorAllocationRequest allocationRequest;
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
    static class GetAssignedTaskExecutorRequest {
        WorkerId workerId;

        ClusterID clusterID;
    }

    @Value
    static class TaskExecutorGatewayRequest {
        TaskExecutorID taskExecutorID;

        ClusterID clusterID;
    }

    @Value
    static class GetRegisteredTaskExecutorsRequest implements HasAttributes {
        ClusterID clusterID;

        Map<String, String> attributes;
    }

    @Value
    @Builder
    @AllArgsConstructor // needed for build to work with custom ctor.
    static class GetActiveJobsRequest {
        ClusterID clusterID;
        Optional<Integer> startingIndex;
        Optional<Integer> pageSize;

        public GetActiveJobsRequest(ClusterID clusterID) {
            this.clusterID = clusterID;
            this.pageSize = Optional.empty();
            this.startingIndex = Optional.empty();
        }
    }

    interface HasAttributes {
        Map<String, String> getAttributes();
    }

    @Value
    static class GetAvailableTaskExecutorsRequest implements HasAttributes {
        ClusterID clusterID;

        Map<String, String> attributes;
    }
    @Value
    static class GetDisabledTaskExecutorsRequest implements HasAttributes {
        ClusterID clusterID;

        Map<String, String> attributes;
    }

    @Value
    static class GetBusyTaskExecutorsRequest implements HasAttributes {
        ClusterID clusterID;

        Map<String, String> attributes;
    }

    @Value
    static class GetUnregisteredTaskExecutorsRequest implements HasAttributes {
        ClusterID clusterID;

        Map<String, String> attributes;
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
    static class ArtifactList {
        List<ArtifactID> artifacts;
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

    @Value
    private static class PublishResourceOverviewMetricsRequest {
    }

    @Value
    static class CacheJobArtifactsOnTaskExecutorRequest {
        TaskExecutorID taskExecutorID;
        ClusterID clusterID;
    }

    @Value
    @Builder
    static class AddNewJobArtifactsToCacheRequest {
        ClusterID clusterID;
        List<ArtifactID> artifacts;
    }

    @Value
    @Builder
    static class RemoveJobArtifactsToCacheRequest {
        ClusterID clusterID;
        List<ArtifactID> artifacts;
    }

    @Value
    @Builder
    static class GetJobArtifactsToCacheRequest {
        ClusterID clusterID;
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

    private Boolean isJobArtifactCachingEnabled() {
        return true;
// TODO: fix this ->   return ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.job.artifact.caching.enabled", "false").equals("true");
    }

    private Predicate<Entry<TaskExecutorID, TaskExecutorState>> filterByAttrs(HasAttributes hasAttributes) {
        if (hasAttributes.getAttributes().isEmpty()) {
            return e -> true;
        } else {
            return e -> e.getValue().containsAttributes(hasAttributes.getAttributes());
        }
    }
}
