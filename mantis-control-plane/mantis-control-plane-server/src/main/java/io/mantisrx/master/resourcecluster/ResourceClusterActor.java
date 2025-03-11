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
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.TagList;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.scheduler.FitnessCalculator;
import io.mantisrx.server.core.CacheJobArtifactsRequest;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.PagedActiveJobOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorNotFoundException;
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
import io.mantisrx.server.worker.TaskExecutorGateway.TaskNotFoundException;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Comparators;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.vavr.Tuple;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
public class ResourceClusterActor extends AbstractActorWithTimers {
    /**
     * For ResourceClusterActor instances, we need to ensure they are always running after encountering error so that
     * TaskExecutors can still remain connected. If there is a fatal error that needs to be escalated to terminate the
     * whole system/leader you can define a fatal exception type and override its behavior to
     * SupervisorStrategy.escalate() instead.
     */
    private static SupervisorStrategy resourceClusterActorStrategy =
        new OneForOneStrategy(
            3,
            Duration.ofSeconds(60),
            DeciderBuilder
                .match(Exception.class, e -> SupervisorStrategy.restart())
                .build());

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return resourceClusterActorStrategy;
    }

    private final Duration heartbeatTimeout;
    private final Duration assignmentTimeout;
    private final Duration disabledTaskExecutorsCheckInterval;
    private final Duration schedulerLeaseExpirationDuration;

    private final ExecutorStateManager executorStateManager;
    private final Clock clock;
    private final RpcService rpcService;
    private final ClusterID clusterID;
    private final MantisJobStore mantisJobStore;
    private final Set<DisableTaskExecutorsRequest> activeDisableTaskExecutorsByAttributesRequests;
    private final Set<TaskExecutorID> disabledTaskExecutors;
    private final JobMessageRouter jobMessageRouter;

    private final ResourceClusterActorMetrics metrics;

    private final HashSet<ArtifactID> jobArtifactsToCache = new HashSet<>();

    private final int maxJobArtifactsToCache;
    private final String jobClustersWithArtifactCachingEnabled;

    private final boolean isJobArtifactCachingEnabled;

    static Props props(
        final ClusterID clusterID,
        final Duration heartbeatTimeout,
        Duration assignmentTimeout,
        Duration disabledTaskExecutorsCheckInterval,
        Duration schedulerLeaseExpirationDuration,
        Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore,
        JobMessageRouter jobMessageRouter,
        int maxJobArtifactsToCache,
        String jobClustersWithArtifactCachingEnabled,
        boolean isJobArtifactCachingEnabled,
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator,
        AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook
    ) {
        return Props.create(
            ResourceClusterActor.class,
            clusterID,
            heartbeatTimeout,
            assignmentTimeout,
            disabledTaskExecutorsCheckInterval,
            schedulerLeaseExpirationDuration,
            clock,
            rpcService,
            mantisJobStore,
            jobMessageRouter,
            maxJobArtifactsToCache,
            jobClustersWithArtifactCachingEnabled,
            isJobArtifactCachingEnabled,
            schedulingAttributes,
            fitnessCalculator,
            availableTaskExecutorMutatorHook
        ).withMailbox("akka.actor.metered-mailbox");
    }

    static Props props(
        final ClusterID clusterID,
        final Duration heartbeatTimeout,
        Duration assignmentTimeout,
        Duration disabledTaskExecutorsCheckInterval,
        Duration schedulerLeaseExpirationDuration,
        Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore,
        JobMessageRouter jobMessageRouter,
        int maxJobArtifactsToCache,
        String jobClustersWithArtifactCachingEnabled,
        boolean isJobArtifactCachingEnabled,
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator
    ) {
        return Props.create(
            ResourceClusterActor.class,
            clusterID,
            heartbeatTimeout,
            assignmentTimeout,
            disabledTaskExecutorsCheckInterval,
            schedulerLeaseExpirationDuration,
            clock,
            rpcService,
            mantisJobStore,
            jobMessageRouter,
            maxJobArtifactsToCache,
            jobClustersWithArtifactCachingEnabled,
            isJobArtifactCachingEnabled,
            schedulingAttributes,
            fitnessCalculator,
            null
        ).withMailbox("akka.actor.metered-mailbox");
    }

    ResourceClusterActor(
        ClusterID clusterID,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        Duration disabledTaskExecutorsCheckInterval,
        Duration schedulerLeaseExpirationDuration,
        Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore,
        JobMessageRouter jobMessageRouter,
        int maxJobArtifactsToCache,
        String jobClustersWithArtifactCachingEnabled,
        boolean isJobArtifactCachingEnabled,
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator,
        AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook) {
        this.clusterID = clusterID;
        this.heartbeatTimeout = heartbeatTimeout;
        this.assignmentTimeout = assignmentTimeout;
        this.disabledTaskExecutorsCheckInterval = disabledTaskExecutorsCheckInterval;
        this.schedulerLeaseExpirationDuration = schedulerLeaseExpirationDuration;
        this.isJobArtifactCachingEnabled = isJobArtifactCachingEnabled;

        this.clock = clock;
        this.rpcService = rpcService;
        this.jobMessageRouter = jobMessageRouter;
        this.mantisJobStore = mantisJobStore;
        this.activeDisableTaskExecutorsByAttributesRequests = new HashSet<>();
        this.disabledTaskExecutors = new HashSet<>();
        this.maxJobArtifactsToCache = maxJobArtifactsToCache;
        this.jobClustersWithArtifactCachingEnabled = jobClustersWithArtifactCachingEnabled;

        this.executorStateManager = new ExecutorStateManagerImpl(
            schedulingAttributes, fitnessCalculator, this.schedulerLeaseExpirationDuration, availableTaskExecutorMutatorHook);

        this.metrics = new ResourceClusterActorMetrics();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        metrics.incrementCounter(
            ResourceClusterActorMetrics.RC_ACTOR_RESTART,
            TagList.create(ImmutableMap.of(
                "resourceCluster",
                clusterID.getResourceID())));

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
                .match(GetTaskExecutorStatusRequest.class, this::getTaskExecutorStatus)
                .match(GetClusterUsageRequest.class,
                    metrics.withTracking(req ->
                        sender().tell(this.executorStateManager.getClusterUsage(req), self())))
                .match(GetClusterIdleInstancesRequest.class,
                    metrics.withTracking(req ->
                            sender().tell(onGetClusterIdleInstancesRequest(req), self())))
                .match(GetAssignedTaskExecutorRequest.class, this::onAssignedTaskExecutorRequest)
                .match(MarkExecutorTaskCancelledRequest.class, this::onMarkExecutorTaskCancelledRequest)
                .match(Ack.class, ack -> log.info("Received ack from {}", sender()))

                .match(TaskExecutorAssignmentTimeout.class, this::onTaskExecutorAssignmentTimeout)
                .match(TaskExecutorRegistration.class, metrics.withTracking(this::onTaskExecutorRegistration))
                .match(InitializeTaskExecutorRequest.class, metrics.withTracking(this::onTaskExecutorInitialization))
                .match(TaskExecutorHeartbeat.class, metrics.withTracking(this::onHeartbeat))
                .match(TaskExecutorStatusChange.class, this::onTaskExecutorStatusChange)
                .match(TaskExecutorDisconnection.class, metrics.withTracking(this::onTaskExecutorDisconnection))
                .match(HeartbeatTimeout.class, metrics.withTracking(this::onTaskExecutorHeartbeatTimeout))
                .match(TaskExecutorBatchAssignmentRequest.class, metrics.withTracking(this::onTaskExecutorBatchAssignmentRequest))
                .match(ResourceOverviewRequest.class, this::onResourceOverviewRequest)
                .match(TaskExecutorInfoRequest.class, this::onTaskExecutorInfoRequest)
                .match(TaskExecutorGatewayRequest.class, metrics.withTracking(this::onTaskExecutorGatewayRequest))
                .match(DisableTaskExecutorsRequest.class, this::onNewDisableTaskExecutorsRequest)
                .match(CheckDisabledTaskExecutors.class, this::findAndMarkDisabledTaskExecutors)
                .match(ExpireDisableTaskExecutorsRequest.class, this::onDisableTaskExecutorsRequestExpiry)
                .match(GetTaskExecutorWorkerMappingRequest.class, req -> sender().tell(getTaskExecutorWorkerMapping(req.getAttributes()), self()))
                .match(PublishResourceOverviewMetricsRequest.class, this::onPublishResourceOverviewMetricsRequest)
                .match(CacheJobArtifactsOnTaskExecutorRequest.class, metrics.withTracking(this::onCacheJobArtifactsOnTaskExecutorRequest))
                .match(AddNewJobArtifactsToCacheRequest.class, this::onAddNewJobArtifactsToCacheRequest)
                .match(RemoveJobArtifactsToCacheRequest.class, this::onRemoveJobArtifactsToCacheRequest)
                .match(GetJobArtifactsToCacheRequest.class, req -> sender().tell(new ArtifactList(new ArrayList<>(jobArtifactsToCache)), self()))
                .build();
    }



    private void onAddNewJobArtifactsToCacheRequest(AddNewJobArtifactsToCacheRequest req) {
        try {
            Set<ArtifactID> newArtifacts = new HashSet<>(req.artifacts);
            newArtifacts.removeAll(jobArtifactsToCache);

            if (!newArtifacts.isEmpty()) {
                if(jobArtifactsToCache.size() < maxJobArtifactsToCache) {
                    log.info("Storing and caching new artifacts: {}", newArtifacts);

                    jobArtifactsToCache.addAll(newArtifacts);
                    mantisJobStore.addNewJobArtifactsToCache(req.getClusterID(), ImmutableList.copyOf(jobArtifactsToCache));
                    refreshTaskExecutorJobArtifactCache();
                } else {
                    log.warn("Cannot enable caching for artifacts {}. Max number ({}) of job artifacts to cache reached.", newArtifacts, maxJobArtifactsToCache);

                    metrics.incrementCounter(
                        ResourceClusterActorMetrics.MAX_JOB_ARTIFACTS_TO_CACHE_REACHED,
                        TagList.create(ImmutableMap.of(
                            "resourceCluster",
                            clusterID.getResourceID())));
                }
            }
            sender().tell(Ack.getInstance(), self());
        } catch (IOException e) {
            log.warn("Cannot add new job artifacts {} to cache in cluster: {}", req.getArtifacts(), req.getClusterID(), e);
        }
    }

    private void refreshTaskExecutorJobArtifactCache() {
        // TODO: implement rate control to confirm we are not overwhelming the TEs with excessive caching requests
        getTaskExecutors(ExecutorStateManager.isAvailable).getTaskExecutors().forEach(taskExecutorID ->
            self().tell(new CacheJobArtifactsOnTaskExecutorRequest(taskExecutorID, clusterID), self()));
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
            TaskExecutorState state =
                    this.executorStateManager.getIncludeArchived(request.getTaskExecutorID());
            if (state != null && state.getRegistration() != null) {
                sender().tell(state.getRegistration(), self());
            } else {
                sender().tell(new Status.Failure(new Exception(String.format("No task executor state for %s",
                        request.getTaskExecutorID()))), self());
            }
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

    private void onMarkExecutorTaskCancelledRequest(MarkExecutorTaskCancelledRequest request) {
        Optional<Entry<TaskExecutorID, TaskExecutorState>> matchedTaskExecutor =
            this.executorStateManager.findFirst(e -> e.getValue().isRunningOrAssigned(request.getWorkerId()));

        if (matchedTaskExecutor.isPresent()) {
            log.info("Setting executor {} to cancelled workerID: {}", matchedTaskExecutor.get().getKey(), request);
            matchedTaskExecutor.get().getValue().setCancelledWorkerOnTask(request.getWorkerId());
            sender().tell(Ack.getInstance(), self());
        } else {
            log.info("Cannot find executor to mark worker {} as cancelled", request);
            sender().tell(new Status.Failure(new TaskNotFoundException(request.getWorkerId())), self());
        }
    }

    private void onTaskExecutorGatewayRequest(TaskExecutorGatewayRequest request) {
        TaskExecutorState state = this.executorStateManager.get(request.getTaskExecutorID());
        if (state == null) {
            sender().tell(new NullPointerException("Null TaskExecutorState for: " + request.getTaskExecutorID()), self());
        } else {
            try {
                if (state.isRegistered()) {
                    sender().tell(state.getGatewayAsync(), self());
                } else {
                    sender().tell(
                        new Status.Failure(new IllegalStateException("Unregistered TaskExecutor: " + request.getTaskExecutorID())),
                        self());
                }
            } catch (Exception e) {
                log.error("onTaskExecutorGatewayRequest error: {}", request, e);
                metrics.incrementCounter(
                    ResourceClusterActorMetrics.TE_CONNECTION_FAILURE,
                    TagList.create(ImmutableMap.of(
                        "resourceCluster",
                        clusterID.getResourceID(),
                        "taskExecutor",
                        request.getTaskExecutorID().getResourceId())));
            }
        }
    }

    // custom equals function to check if the existing set already has the request under consideration.
    private boolean addNewDisableTaskExecutorsRequest(DisableTaskExecutorsRequest newRequest) {
        if (newRequest.isRequestByAttributes()) {
            log.info("Req with attributes {}", newRequest);
            for (DisableTaskExecutorsRequest existing: activeDisableTaskExecutorsByAttributesRequests) {
                if (existing.targetsSameTaskExecutorsAs(newRequest)) {
                    return false;
                }
            }

            Preconditions.checkState(activeDisableTaskExecutorsByAttributesRequests.add(newRequest), "activeDisableTaskExecutorRequests cannot contain %s", newRequest);
            return true;
        } else if (newRequest.getTaskExecutorID().isPresent() && !disabledTaskExecutors.contains(newRequest.getTaskExecutorID().get())) {
            log.info("Req with id {}", newRequest);
            disabledTaskExecutors.add(newRequest.getTaskExecutorID().get());
            return true;
        }
        log.info("No Req {}", newRequest);
        return false;
    }

    private void onNewDisableTaskExecutorsRequest(DisableTaskExecutorsRequest request) {
        ActorRef sender = sender();
        if (addNewDisableTaskExecutorsRequest(request)) {
            try {
                log.info("New req to add {}", request);
                // store the request in a persistent store in order to retrieve it if the node goes down
                mantisJobStore.storeNewDisabledTaskExecutorsRequest(request);
                // figure out the time to expire the current request
                Duration toExpiry = Comparators.max(Duration.between(clock.instant(), request.getExpiry()), Duration.ZERO);
                // setup a timer to clear it after a given period
                getTimers().startSingleTimer(
                    getExpiryKeyFor(request),
                    new ExpireDisableTaskExecutorsRequest(request),
                    toExpiry);
                findAndMarkDisabledTaskExecutorsFor(request);
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

    private void findAndMarkDisabledTaskExecutorsFor(DisableTaskExecutorsRequest request) {
        if (request.isRequestByAttributes()) {
            findAndMarkDisabledTaskExecutors(new CheckDisabledTaskExecutors("new_request"));
        } else if (request.getTaskExecutorID().isPresent()) {
            final TaskExecutorID taskExecutorID = request.getTaskExecutorID().get();
            final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
            if (state == null) {
                // If the TE is unknown by mantis, delete it from state
                disabledTaskExecutors.remove(taskExecutorID);
                self().tell(new ExpireDisableTaskExecutorsRequest(request), self());
            } else {
                log.info("Marking task executor {} as disabled", taskExecutorID);
                state.onNodeDisabled();
            }
        }
    }

    private void findAndMarkDisabledTaskExecutors(CheckDisabledTaskExecutors r) {
        log.info(
            "Checking disabled task executors for Cluster {} because of {}. Current disabled request size: {}",
            clusterID.getResourceID(), r.getReason(), activeDisableTaskExecutorsByAttributesRequests.size());
        final Instant now = clock.instant();
        for (DisableTaskExecutorsRequest request : activeDisableTaskExecutorsByAttributesRequests) {
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
            if (activeDisableTaskExecutorsByAttributesRequests.remove(request.getRequest()) || (request.getRequest().getTaskExecutorID().isPresent() && disabledTaskExecutors.remove(request.getRequest().getTaskExecutorID().get()))) {
                mantisJobStore.deleteExpiredDisableTaskExecutorsRequest(request.getRequest());
            }

            // also re-enable the node if the state is still valid.
            if (request.getRequest().getTaskExecutorID().isPresent()) {
                final TaskExecutorState state = this.executorStateManager.get(
                    request.getRequest().getTaskExecutorID().get());
                if (state != null) {
                    state.onNodeEnabled();
                }
            }
        } catch (Exception e) {
            log.error("Failed to delete expired {}", request.getRequest(), e);
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
                    this.executorStateManager.tryMarkAvailable(taskExecutorID);
                }
                // check if the task executor has been marked as 'Disabled'
                if (isTaskExecutorDisabled(registration)) {
                    log.info("Newly registered task executor {} was already marked for disabling.", registration.getTaskExecutorID());
                    state.onNodeDisabled();
                }
                updateHeartbeatTimeout(registration.getTaskExecutorID());
            }
            log.info("Successfully registered {} with the resource cluster {}", registration.getTaskExecutorID(), this);
            if (!jobArtifactsToCache.isEmpty() && isJobArtifactCachingEnabled) {
                self().tell(new CacheJobArtifactsOnTaskExecutorRequest(taskExecutorID, clusterID), self());
            }
            sender().tell(Ack.getInstance(), self());
        } catch (Exception e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private boolean isTaskExecutorDisabled(TaskExecutorRegistration registration) {
        for (DisableTaskExecutorsRequest request: activeDisableTaskExecutorsByAttributesRequests) {
            if (request.covers(registration)) {
                return true;
            }
        }
        return disabledTaskExecutors.contains(registration.getTaskExecutorID());
    }

    private void onHeartbeat(TaskExecutorHeartbeat heartbeat) {
        log.debug("Received heartbeat {} from task executor {}", heartbeat, heartbeat.getTaskExecutorID());
        setupTaskExecutorStateIfNecessary(heartbeat.getTaskExecutorID());
        try {
            final TaskExecutorID taskExecutorID = heartbeat.getTaskExecutorID();
            final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
            if (state.getRegistration() == null || !state.isRegistered()) {
                TaskExecutorRegistration registration = this.mantisJobStore.getTaskExecutor(heartbeat.getTaskExecutorID());
                if (registration != null) {
                    log.debug("Found registration {} for task executor {}", registration, heartbeat.getTaskExecutorID());
                    Preconditions.checkState(state.onRegistration(registration));

                    // check if the task executor has been marked as 'Disabled'
                    if (isTaskExecutorDisabled(registration)) {
                        log.info("Reconnected task executor {} was already marked for disabling.", registration.getTaskExecutorID());
                        state.onNodeDisabled();
                    }
                } else {
//                  TODO(sundaram): add a metric
                    log.warn("Received heartbeat from unknown task executor {}", heartbeat.getTaskExecutorID());
                    sender().tell(new Status.Failure(new TaskExecutorNotFoundException(taskExecutorID)), self());
                    return;
                }
            } else {
                log.debug("Found registration {} for registered task executor {}",
                    state.getRegistration(), heartbeat.getTaskExecutorID());
            }
            boolean stateChange = state.onHeartbeat(heartbeat);
            if (stateChange && state.isAvailable()) {
                this.executorStateManager.tryMarkAvailable(taskExecutorID);
            }

            updateHeartbeatTimeout(heartbeat.getTaskExecutorID());
            log.debug("Successfully processed heartbeat {} from task executor {}", heartbeat, heartbeat.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (Exception e) {
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
                    this.executorStateManager.tryMarkAvailable(taskExecutorID);
                } else {
                    this.executorStateManager.tryMarkUnavailable(taskExecutorID);
                }
            }

            updateHeartbeatTimeout(statusChange.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorBatchAssignmentRequest(TaskExecutorBatchAssignmentRequest request) {
        Optional<BestFit> matchedExecutors = this.executorStateManager.findBestFit(request);

        if (matchedExecutors.isPresent()) {
            log.info("Matched all executors {} for request {}", matchedExecutors.get(), request);
            matchedExecutors.get().getBestFit().forEach((allocationRequest, taskExecutorToState) -> assignTaskExecutor(
                allocationRequest, taskExecutorToState.getLeft(), taskExecutorToState.getRight(), request));
            sender().tell(new TaskExecutorsAllocation(matchedExecutors.get().getRequestToTaskExecutorMap()), self());
        } else {
            request.allocationRequests.forEach(req -> metrics.incrementCounter(
                ResourceClusterActorMetrics.NO_RESOURCES_AVAILABLE,
                createTagListFrom(req)));
            sender().tell(new Status.Failure(new NoResourceAvailableException(
                String.format("No resource available for request %s: resource overview: %s", request,
                    getResourceOverview()))), self());
        }
    }

    private void assignTaskExecutor(TaskExecutorAllocationRequest allocationRequest, TaskExecutorID taskExecutorID, TaskExecutorState taskExecutorState, TaskExecutorBatchAssignmentRequest request) {
        if(shouldCacheJobArtifacts(allocationRequest)) {
            self().tell(new AddNewJobArtifactsToCacheRequest(clusterID, Collections.singletonList(allocationRequest.getJobMetadata().getJobArtifact())), self());
        }

        taskExecutorState.onAssignment(allocationRequest.getWorkerId());
        // let's give some time for the assigned executor to be scheduled work. otherwise, the assigned executor
        // will be returned back to the pool.
        getTimers().startSingleTimer(
            "Assignment-" + taskExecutorID.toString(),
            new TaskExecutorAssignmentTimeout(taskExecutorID),
            assignmentTimeout);
    }

    private void onTaskExecutorAssignmentTimeout(TaskExecutorAssignmentTimeout request) {
        TaskExecutorState state = this.executorStateManager.get(request.getTaskExecutorID());
        if (state == null) {
            log.error("TaskExecutor lost during task assignment: {}", request);
        }
        else if (state.isRunningTask()) {
            log.debug("TaskExecutor {} entered running state already; no need to act", request.getTaskExecutorID());
        } else {
            try
            {
                boolean stateChange = state.onUnassignment();
                if (stateChange) {
                    this.executorStateManager.tryMarkAvailable(request.getTaskExecutorID());
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
                .map(this::getTaskExecutorState)
                .filter(Objects::nonNull)
                .map(TaskExecutorState::getRegistration)
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

    private void getTaskExecutorStatus(GetTaskExecutorStatusRequest req) {
        TaskExecutorID taskExecutorID = req.getTaskExecutorID();
        final TaskExecutorState state = this.executorStateManager.get(taskExecutorID);
        if (state == null) {
            log.info("Unknown executorID: {}", taskExecutorID);
            getSender().tell(
                new Status.Failure(new TaskExecutorNotFoundException(taskExecutorID)),
                self());
        }
        else {
            getSender().tell(
                new TaskExecutorStatus(
                    state.getRegistration(),
                    state.isRegistered(),
                    state.isRunningTask(),
                    state.isAssigned(),
                    state.isDisabled(),
                    state.getWorkerId(),
                    state.getLastActivity().toEpochMilli(),
                    state.getCancelledWorkerId()),
                self());
        }
    }

    @Nullable
    private TaskExecutorState getTaskExecutorState(TaskExecutorID taskExecutorID) {
        return this.executorStateManager.get(taskExecutorID);
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
            .trackIfAbsent(taskExecutorID, TaskExecutorState.of(clock, rpcService, jobMessageRouter));
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
        if (state != null && state.isRegistered()) {
            try {
                // TODO(fdichiara): store URI directly to avoid remapping for each TE
                state.getGatewayAsync()
                    .thenComposeAsync(taskExecutorGateway ->
                        taskExecutorGateway.cacheJobArtifacts(new CacheJobArtifactsRequest(
                            jobArtifactsToCache
                                .stream()
                                .map(artifactID -> URI.create(artifactID.getResourceID()))
                                .collect(Collectors.toList()))))
                    .whenComplete((res, throwable) -> {
                        if (throwable != null) {
                            log.error("failed to cache artifact on {}", request.getTaskExecutorID(), throwable);
                        }
                        else {
                            log.debug("Acked from cacheJobArtifacts for {}", request.getTaskExecutorID());
                        }
                    });
            } catch (Exception ex) {
                log.warn("Failed to cache job artifacts in task executor {}", request.getTaskExecutorID(), ex);
            }
        }
        else {
            log.debug("no valid TE state for CacheJobArtifactsOnTaskExecutorRequest: {}", request);
        }
    }

    /**
     * Artifact is added to the list of artifacts if it's the first worker of the first stage
     * (this is to reduce the work in master) and if the job cluster is enabled (via config
     * for now)
     */
    private boolean shouldCacheJobArtifacts(TaskExecutorAllocationRequest allocationRequest) {
        final WorkerId workerId = allocationRequest.getWorkerId();
        final boolean isFirstWorkerOfFirstStage = allocationRequest.getStageNum() == 1 && workerId.getWorkerIndex() == 0;
        if (isFirstWorkerOfFirstStage) {
            final Set<String> jobClusters = getJobClustersWithArtifactCachingEnabled();
            return jobClusters.contains(workerId.getJobCluster());
        }
        return false;
    }

    private Set<String> getJobClustersWithArtifactCachingEnabled() {
        return new HashSet<>(Arrays.asList(jobClustersWithArtifactCachingEnabled.split(",")));
    }

    /**
     * Creates a list of tags from the provided TaskExecutorAllocationRequest.
     * The list includes resource cluster, workerId, jobCluster, and either sizeName or cpuCores and memoryMB
     * based on whether sizeName is present in the request's constraints.
     *
     * @param req The task executor allocation request from which the tag list will be generated.
     *
     * @return An iterable list of tags created from the task executor allocation request.
     */
    private Iterable<Tag> createTagListFrom(TaskExecutorAllocationRequest req) {
        // Basic tags that will always be included
        ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.<String, String>builder()
            .put("resourceCluster", clusterID.getResourceID())
            .put("workerId", req.getWorkerId().getId())
            .put("jobCluster", req.getWorkerId().getJobCluster());

        // Add the sizeName tag if it exists, otherwise add the cpuCores and memoryMB tags
        if (req.getConstraints().getSizeName().isPresent()) {
            tagsBuilder.put("sizeName", req.getConstraints().getSizeName().get());
        } else {
            tagsBuilder.put("cpuCores", String.valueOf(req.getConstraints().getMachineDefinition().getCpuCores()))
                .put("memoryMB", String.valueOf(req.getConstraints().getMachineDefinition().getMemoryMB()));
        }

        return TagList.create(tagsBuilder.build());
    }

    @Value
    static class HeartbeatTimeout {

        TaskExecutorID taskExecutorID;
        Instant lastActivity;
    }

    @Value
    public static class TaskExecutorBatchAssignmentRequest {
        Set<TaskExecutorAllocationRequest> allocationRequests;
        ClusterID clusterID;

        public Map<SchedulingConstraints, List<TaskExecutorAllocationRequest>> getGroupedBySchedulingConstraints() {
            return allocationRequests
                .stream()
                .collect(Collectors.groupingBy(TaskExecutorAllocationRequest::getConstraints));
        }

        public String getJobId() {
            return allocationRequests.iterator().next().getWorkerId().getJobId();
        }
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
    static class TaskExecutorsAllocation {
        Map<TaskExecutorAllocationRequest, TaskExecutorID> allocations;
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
    static class MarkExecutorTaskCancelledRequest {
        ClusterID clusterID;
        WorkerId workerId;
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


    @Value
    static class BestFit {
        Map<TaskExecutorAllocationRequest, Pair<TaskExecutorID, TaskExecutorState>> bestFit;
        Set<TaskExecutorID> taskExecutorIDSet;

        public BestFit() {
            this.bestFit = new HashMap<>();
            this.taskExecutorIDSet = new HashSet<>();
        }

        public void add(TaskExecutorAllocationRequest request, Pair<TaskExecutorID, TaskExecutorState> taskExecutorStatePair) {
            bestFit.put(request, taskExecutorStatePair);
            taskExecutorIDSet.add(taskExecutorStatePair.getLeft());
        }

        public boolean contains(TaskExecutorID taskExecutorID) {
            return taskExecutorIDSet.contains(taskExecutorID);
        }

        public Map<TaskExecutorAllocationRequest, TaskExecutorID> getRequestToTaskExecutorMap() {
            return bestFit
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Entry::getKey,
                    e -> e.getValue().getKey()
                ));
        }
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

    private Predicate<Entry<TaskExecutorID, TaskExecutorState>> filterByAttrs(HasAttributes hasAttributes) {
        if (hasAttributes.getAttributes().isEmpty()) {
            return e -> true;
        } else {
            return e -> e.getValue().containsAttributes(hasAttributes.getAttributes());
        }
    }
}
