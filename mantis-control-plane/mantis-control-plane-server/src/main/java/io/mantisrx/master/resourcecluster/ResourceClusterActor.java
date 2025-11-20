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

import static akka.pattern.Patterns.pipe;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.ReceiveBuilder;
import com.netflix.spectator.api.TagList;
import io.mantisrx.common.Ack;
import io.mantisrx.common.akka.MantisActorSupervisorStrategy;
import io.mantisrx.master.resourcecluster.ExecutorStateManagerActor.RefreshTaskExecutorJobArtifactCache;
import io.mantisrx.master.resourcecluster.ExecutorStateManagerActor.UpdateDisabledState;
import io.mantisrx.master.resourcecluster.ExecutorStateManagerActor.UpdateJobArtifactsToCache;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.scheduler.FitnessCalculator;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
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
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Comparators;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.rpc.RpcService;
import akka.pattern.Patterns;
import scala.Option;
import scala.compat.java8.FutureConverters;

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
    @Override
    public SupervisorStrategy supervisorStrategy() {
        return MantisActorSupervisorStrategy.getInstance().create();
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

    private final String reservationRegistryActorName;
    private ActorRef reservationRegistryActor;

    private final String executorStateManagerActorName;
    private ActorRef executorStateManagerActor;

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
        this.reservationRegistryActorName = buildReservationRegistryActorName(clusterID);
        this.executorStateManagerActorName = buildExecutorStateManagerActorName(clusterID);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        metrics.incrementCounter(
            ResourceClusterActorMetrics.RC_ACTOR_RESTART,
            TagList.create(ImmutableMap.of(
                "resourceCluster",
                clusterID.getResourceID())));

        Option<ActorRef> existingRegistry = getContext().child(reservationRegistryActorName);
        if (existingRegistry.isDefined()) {
            reservationRegistryActor = existingRegistry.get();
        } else {
            Props registryProps = ReservationRegistryActor.props(this.clusterID, clock, null, null, metrics);
            reservationRegistryActor = getContext().actorOf(registryProps, reservationRegistryActorName);
        }

        Option<ActorRef> existingExecutorStateManager = getContext().child(executorStateManagerActorName);
        if (existingExecutorStateManager.isDefined()) {
            executorStateManagerActor = existingExecutorStateManager.get();
        } else {
            if (!(executorStateManager instanceof ExecutorStateManagerImpl)) {
                throw new IllegalStateException("ExecutorStateManager is not an instance of ExecutorStateManagerImpl");
            }
            Props esmProps = ExecutorStateManagerActor.props(
                (ExecutorStateManagerImpl) executorStateManager,
                clock,
                rpcService,
                jobMessageRouter,
                mantisJobStore,
                heartbeatTimeout,
                assignmentTimeout,
                clusterID,
                isJobArtifactCachingEnabled,
                jobClustersWithArtifactCachingEnabled,
                metrics);
            executorStateManagerActor = getContext().actorOf(esmProps, executorStateManagerActorName);
        }

        syncExecutorDisabledState();
        syncExecutorJobArtifactsCache();

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
                .match(UpsertReservation.class, this::forwardToReservationRegistry)
                .match(CancelReservation.class, this::forwardToReservationRegistry)
                .match(GetPendingReservationsView.class, this::forwardToReservationRegistry)
                .match(MarkReady.class, this::forwardToReservationRegistry)
                .match(GetRegisteredTaskExecutorsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetBusyTaskExecutorsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetAvailableTaskExecutorsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetDisabledTaskExecutorsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetUnregisteredTaskExecutorsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetActiveJobsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetTaskExecutorStatusRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetClusterUsageRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetClusterIdleInstancesRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(GetAssignedTaskExecutorRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(MarkExecutorTaskCancelledRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(Ack.class, ack -> log.info("Received ack from {}", sender()))

                .match(TaskExecutorRegistration.class, metrics.withTracking(this::forwardToExecutorStateManager))
                .match(InitializeTaskExecutorRequest.class, metrics.withTracking(this::forwardToExecutorStateManager))
                .match(TaskExecutorHeartbeat.class, metrics.withTracking(this::forwardToExecutorStateManager))
                .match(TaskExecutorStatusChange.class, metrics.withTracking(this::forwardToExecutorStateManager))
                .match(TaskExecutorDisconnection.class, metrics.withTracking(this::forwardToExecutorStateManager))
                .match(TaskExecutorBatchAssignmentRequest.class, metrics.withTracking(this::forwardToExecutorStateManager))
                .match(ResourceOverviewRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(TaskExecutorInfoRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(TaskExecutorGatewayRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(DisableTaskExecutorsRequest.class, this::onNewDisableTaskExecutorsRequest)
                .match(CheckDisabledTaskExecutors.class, req -> {
                    log.info(
                        "Checking disabled task executors for Cluster {} because of {}. Current disabled request size: {}",
                        clusterID.getResourceID(), req.getReason(), activeDisableTaskExecutorsByAttributesRequests.size());
                    forwardToExecutorStateManager(req);
                })
                .match(ExpireDisableTaskExecutorsRequest.class, this::onDisableTaskExecutorsRequestExpiry)
                .match(GetTaskExecutorWorkerMappingRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(PublishResourceOverviewMetricsRequest.class,
                    metrics.withTracking(this::forwardToExecutorStateManager))
                .match(CacheJobArtifactsOnTaskExecutorRequest.class, metrics.withTracking(req ->
                    pipe(
                        FutureConverters.toJava(Patterns.ask(
                            executorStateManagerActor,
                            req,
                            assignmentTimeout.toMillis())),
                        getContext().dispatcher())
                        .to(sender(), self())))
                .match(AddNewJobArtifactsToCacheRequest.class, this::onAddNewJobArtifactsToCacheRequest)
                .match(RemoveJobArtifactsToCacheRequest.class, this::onRemoveJobArtifactsToCacheRequest)
                .match(GetJobArtifactsToCacheRequest.class, req -> sender().tell(new ArtifactList(new ArrayList<>(jobArtifactsToCache)), self()))
                .build();
    }

    private void forwardToReservationRegistry(Object message) {
        if (reservationRegistryActor == null) {
            log.warn("Reservation registry actor not initialized; dropping {}", message);
            sender().tell(new Status.Failure(new IllegalStateException("reservation registry not available")), self());
            return;
        }
        // TODO (reservation-registry): job actor and scheduler interactions will route through this bridge.
        reservationRegistryActor.forward(message, getContext());
    }

    private void forwardToExecutorStateManager(Object message) {
        if (executorStateManagerActor == null) {
            log.warn("ExecutorStateManagerActor not initialized; dropping {}", message);
            sender().tell(new Status.Failure(new IllegalStateException("executor state manager actor not available")), self());
            return;
        }
        executorStateManagerActor.forward(message, getContext());
    }


    private void syncExecutorJobArtifactsCache() {
        if (executorStateManagerActor == null) {
            return;
        }
        executorStateManagerActor.tell(
            new UpdateJobArtifactsToCache(new HashSet<>(jobArtifactsToCache)),
            self());
    }

    private void syncExecutorDisabledState() {
        if (executorStateManagerActor == null) {
            return;
        }
        executorStateManagerActor.tell(
            new UpdateDisabledState(
                new HashSet<>(activeDisableTaskExecutorsByAttributesRequests),
                new HashSet<>(disabledTaskExecutors)),
            self());
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
                    syncExecutorJobArtifactsCache();
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
        if (executorStateManagerActor == null) {
            log.warn("ExecutorStateManagerActor not initialized; skipping artifact cache refresh");
            return;
        }
        executorStateManagerActor.tell(new RefreshTaskExecutorJobArtifactCache(), self());
    }

    private void onRemoveJobArtifactsToCacheRequest(RemoveJobArtifactsToCacheRequest req) {
        try {
            mantisJobStore.removeJobArtifactsToCache(req.getClusterID(), req.getArtifacts());
            req.artifacts.forEach(jobArtifactsToCache::remove);
            syncExecutorJobArtifactsCache();
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
            syncExecutorJobArtifactsCache();
        } catch (IOException e) {
            log.warn("Cannot refresh job artifacts to cache in cluster: {}", clusterID, e);
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
            syncExecutorDisabledState();
            return true;
        } else if (newRequest.getTaskExecutorID().isPresent() && !disabledTaskExecutors.contains(newRequest.getTaskExecutorID().get())) {
            log.info("Req with id {}", newRequest);
            disabledTaskExecutors.add(newRequest.getTaskExecutorID().get());
            syncExecutorDisabledState();
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
            forwardToExecutorStateManager(new CheckDisabledTaskExecutors("new_request"));
        } else if (request.getTaskExecutorID().isPresent()) {
            forwardToExecutorStateManager(new CheckDisabledTaskExecutors("targeted_request"));
        }
    }

    private void onDisableTaskExecutorsRequestExpiry(ExpireDisableTaskExecutorsRequest request) {
        try {
            log.debug("Expiring Disable Task Executors Request {}", request.getRequest());
            getTimers().cancel(getExpiryKeyFor(request.getRequest()));
            boolean removed = activeDisableTaskExecutorsByAttributesRequests.remove(request.getRequest()) ||
                (request.getRequest().getTaskExecutorID().isPresent() && disabledTaskExecutors.remove(request.getRequest().getTaskExecutorID().get()));
            if (removed) {
                syncExecutorDisabledState();
                mantisJobStore.deleteExpiredDisableTaskExecutorsRequest(request.getRequest());
            }

            forwardToExecutorStateManager(request);
            forwardToExecutorStateManager(new CheckDisabledTaskExecutors("expiry"));
        } catch (Exception e) {
            log.error("Failed to delete expired {}", request.getRequest(), e);
        }
    }

    /**
     * Artifact is added to the list of artifacts if it's the first worker of the first stage
     * (this is to reduce the work in master) and if the job cluster is enabled (via config
     * for now)
     */
    /**
     * Creates a list of tags from the provided TaskExecutorAllocationRequest.
     * The list includes resource cluster, workerId, jobCluster, and either sizeName or cpuCores and memoryMB
     * based on whether sizeName is present in the request's constraints.
     *
     * @return An iterable list of tags created from the task executor allocation request.
     */
    @Value
    static class HeartbeatTimeout {

        TaskExecutorID taskExecutorID;
        Instant lastActivity;
    }

    @Value
    public static class TaskExecutorBatchAssignmentRequest {
        Set<TaskExecutorAllocationRequest> allocationRequests;
        ClusterID clusterID;
        Reservation reservation;

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
        Reservation reservation;
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
    static class CheckDisabledTaskExecutors {
        String reason;
    }

    @Value
    static class GetTaskExecutorWorkerMappingRequest {
        Map<String, String> attributes;
    }

    @Value
    static class PublishResourceOverviewMetricsRequest {
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
     * Reservation value objects shared with {@link ReservationRegistryActor} for tracking outstanding scheduling
     * requests.
     */
    @Value
    @Builder
    static class ReservationKey {
        String jobId;
        int stageNumber;
    }

    @Value
    @Builder
    static class ReservationPriority implements Comparable<ReservationPriority>{
        public enum PriorityType {
            REPLACE,
            SCALE,
            NEW_JOB
        }

        PriorityType type;
        int tier;
        long timestamp;

        @Override
        public int compareTo(ReservationPriority other) {
            // 1. First, compare by PriorityType (REPLACE < SCALE < NEW_JOB)
            // Enums compareTo method uses their ordinal (declaration order)
            int typeComparison = this.type.compareTo(other.type);
            if (typeComparison != 0) {
                return typeComparison;
            }

            // 2. If types are equal, compare by level (ascending)
            int levelComparison = Integer.compare(this.tier, other.tier);
            if (levelComparison != 0) {
                return levelComparison;
            }

            // 3. If types and levels are equal, compare by value (ascending)
            return Long.compare(this.timestamp, other.timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReservationPriority priority = (ReservationPriority) o;
            return tier == priority.tier &&
                timestamp == priority.timestamp &&
                type == priority.type;
        }

        /**
         * Generates a hash code based on type, level, and value.
         */
        @Override
        public int hashCode() {
            return Objects.hash(type, tier, timestamp);
        }
    }

    @Value
    @Builder(toBuilder = true)
    static class Reservation {
        ReservationKey key;
        SchedulingConstraints schedulingConstraints;
        String canonicalConstraintKey;
        Set<WorkerId> requestedWorkers;
        Set<TaskExecutorAllocationRequest> allocationRequests;
        int stageTargetSize;
        ReservationPriority priority;

        boolean hasSameShape(Reservation other) {
            return other != null
                && Objects.equals(key, other.key)
                && Objects.equals(canonicalConstraintKey, other.canonicalConstraintKey)
                && Objects.equals(requestedWorkers, other.requestedWorkers)
                && stageTargetSize == other.stageTargetSize;
        }

        int getRequestedWorkersCount() {
            return requestedWorkers != null ? requestedWorkers.size() : 0;
        }

        static Reservation fromUpsertReservation(UpsertReservation upsert, String canonicalConstraintKey) {
            return Reservation.builder()
                .key(upsert.getReservationKey())
                .schedulingConstraints(upsert.getSchedulingConstraints())
                .canonicalConstraintKey(canonicalConstraintKey)
                .requestedWorkers(upsert.getAllocationRequests() != null ?
                    upsert.getAllocationRequests().stream().map(TaskExecutorAllocationRequest::getWorkerId).collect(Collectors.toSet())
                    : Collections.emptySet())
                .allocationRequests(upsert.getAllocationRequests() != null ? upsert.getAllocationRequests() : Collections.emptySet())
                .stageTargetSize(upsert.getStageTargetSize())
                .priority(upsert.getPriority())
                .build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Reservation that = (Reservation) o;
            return stageTargetSize == that.stageTargetSize
                && Objects.equals(key, that.key)
                && Objects.equals(canonicalConstraintKey, that.canonicalConstraintKey)
                && Objects.equals(priority, that.priority)
                && Objects.equals(requestedWorkers, that.requestedWorkers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, canonicalConstraintKey, priority, stageTargetSize, requestedWorkers);
        }
    }

    @Value
    @Builder
    static class PendingReservationsView {
        boolean ready;
        @Builder.Default
        Map<String, PendingReservationGroupView> groups = Collections.emptyMap();
    }

    @Value
    @Builder
    static class PendingReservationGroupView {
        String canonicalConstraintKey;
        int reservationCount;
        int totalRequestedWorkers;
        @Builder.Default
        List<ReservationRegistryActor.ReservationSnapshot> reservations = Collections.emptyList();
    }

    @Value
    @Builder
    static class CancelReservation {
        ReservationKey reservationKey;
    }

    @Value
    static class CancelReservationAck {
        ReservationKey reservationKey;
        boolean cancelled;
    }

    @Value
    @Builder
    static class UpsertReservation {
        ReservationKey reservationKey;
        SchedulingConstraints schedulingConstraints;
        Set<TaskExecutorAllocationRequest>  allocationRequests;
        int stageTargetSize;
        ReservationPriority priority;
    }

    enum MarkReady {
        INSTANCE
    }

    enum ProcessReservationsTick {
        INSTANCE
    }

    enum ForceProcessReservationsTick {
        INSTANCE
    }

    enum GetPendingReservationsView {
        INSTANCE
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

    private static String buildReservationRegistryActorName(ClusterID clusterID) {
        String resourceId = clusterID != null ? clusterID.getResourceID() : "";
        if (resourceId == null) {
            resourceId = "";
        }
        String sanitized = resourceId.replaceAll("[^a-zA-Z0-9-_]", "_");
        if (sanitized.isEmpty()) {
            sanitized = "default";
        } else if (sanitized.charAt(0) == '$') {
            sanitized = "_" + sanitized.substring(1);
        }
        return "reservationRegistry-" + sanitized;
    }

    private static String buildExecutorStateManagerActorName(ClusterID clusterID) {
        String resourceId = clusterID != null ? clusterID.getResourceID() : "";
        if (resourceId == null) {
            resourceId = "";
        }
        String sanitized = resourceId.replaceAll("[^a-zA-Z0-9-_]", "_");
        if (sanitized.isEmpty()) {
            sanitized = "default";
        } else if (sanitized.charAt(0) == '$') {
            sanitized = "_" + sanitized.substring(1);
        }
        return "executorStateManager-" + sanitized;
    }
}
