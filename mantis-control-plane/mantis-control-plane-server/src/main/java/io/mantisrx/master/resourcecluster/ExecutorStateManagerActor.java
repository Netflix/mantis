package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.actor.Status;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.TagList;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.NoResourceAvailableException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorNotFoundException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.master.resourcecluster.TaskExecutorState;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.AddNewJobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ArtifactList;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.BestFit;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CacheJobArtifactsOnTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CheckDisabledTaskExecutors;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ExpireDisableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetActiveJobsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetAssignedTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetAvailableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetBusyTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetDisabledTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetJobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetRegisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorStatusRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorWorkerMappingRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetUnregisteredTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.HeartbeatTimeout;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.InitializeTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.MarkExecutorTaskCancelledRequest;
import io.mantisrx.server.master.resourcecluster.PagedActiveJobOverview;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PublishResourceOverviewMetricsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.RemoveJobArtifactsToCacheRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ResourceOverviewRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorAssignmentTimeout;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorInfoRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorsAllocation;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorsList;
import io.mantisrx.master.resourcecluster.ResourceClusterActorMetrics;
import io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.scheduler.FitnessCalculator;
import io.mantisrx.server.core.CacheJobArtifactsRequest;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.WorkerId;
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
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.server.worker.TaskExecutorGateway.TaskNotFoundException;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.rpc.RpcService;


/**
 * [Changes TODO]
 * - add a child actor "AssignmentHandlerActor" which takes props of clusterID/jobMessageRouter/assignmentTimeout
 * - in child actor AssignmentHandlerActor support behavior "onTaskExecutorAssignementRequest" which gets a message
 * from the parent actor and contains the future to connect to TE gateway and TE registration info. The actual logic
 * is the same as onAssignedScheduleRequestEvent in ResourceClusterAwareSchedulerActor and the retry should be
 * treated the same.
 * - on successful assignment, route message to JobActor to notify worker starting state.
 */

/**
 * Akka actor wrapper around {@link ExecutorStateManager}. The actor provides an asynchronous façade that mirrors the
 * current synchronous API so callers can be migrated incrementally. Once fully integrated, the actor will be the single
 * owner of executor state mutations as described in {@code plan-reservation-registry-v3.md}.
 */
@Slf4j
public class ExecutorStateManagerActor extends AbstractActorWithTimers {

    @Value
    static class UpdateDisabledState {
        Set<DisableTaskExecutorsRequest> attributeRequests;
        Set<TaskExecutorID> disabledExecutors;
    }

    @Value
    static class UpdateJobArtifactsToCache {
        Set<ArtifactID> artifacts;
    }

    static class RefreshTaskExecutorJobArtifactCache { }

    private final ExecutorStateManagerImpl delegate;
    private final Clock clock;
    private final RpcService rpcService;
    private final JobMessageRouter jobMessageRouter;
    private final MantisJobStore mantisJobStore;
    private final Duration heartbeatTimeout;
    private final Duration assignmentTimeout;
    private final ClusterID clusterID;
    private final boolean isJobArtifactCachingEnabled;
    private final ResourceClusterActorMetrics metrics;
    private final String jobClustersWithArtifactCachingEnabled;
    private final Set<DisableTaskExecutorsRequest> activeDisableTaskExecutorsByAttributesRequests;
    private final Set<TaskExecutorID> disabledTaskExecutors;
    private final Set<ArtifactID> jobArtifactsToCache;

    public static Props props(
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator,
        Duration schedulerLeaseExpirationDuration,
        @Nullable AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook,
        Clock clock,
        RpcService rpcService,
        JobMessageRouter jobMessageRouter,
        MantisJobStore mantisJobStore,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        ClusterID clusterID,
        boolean isJobArtifactCachingEnabled,
        String jobClustersWithArtifactCachingEnabled,
        ResourceClusterActorMetrics metrics
    ) {
        Objects.requireNonNull(schedulingAttributes, "schedulingAttributes");
        Objects.requireNonNull(fitnessCalculator, "fitnessCalculator");
        Objects.requireNonNull(schedulerLeaseExpirationDuration, "schedulerLeaseExpirationDuration");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(rpcService, "rpcService");
        Objects.requireNonNull(jobMessageRouter, "jobMessageRouter");
        Objects.requireNonNull(mantisJobStore, "mantisJobStore");
        Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        Objects.requireNonNull(assignmentTimeout, "assignmentTimeout");
        Objects.requireNonNull(clusterID, "clusterID");
        Objects.requireNonNull(jobClustersWithArtifactCachingEnabled, "jobClustersWithArtifactCachingEnabled");
        Objects.requireNonNull(metrics, "metrics");
        return Props.create(
            ExecutorStateManagerActor.class,
            schedulingAttributes,
            fitnessCalculator,
            schedulerLeaseExpirationDuration,
            availableTaskExecutorMutatorHook,
            clock,
            rpcService,
            jobMessageRouter,
            mantisJobStore,
            heartbeatTimeout,
            assignmentTimeout,
            clusterID,
            isJobArtifactCachingEnabled,
            jobClustersWithArtifactCachingEnabled,
            metrics
        )
            .withMailbox("akka.actor.metered-mailbox");
    }

    public static Props props(
        ExecutorStateManagerImpl delegate,
        Clock clock,
        RpcService rpcService,
        JobMessageRouter jobMessageRouter,
        MantisJobStore mantisJobStore,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        ClusterID clusterID,
        boolean isJobArtifactCachingEnabled,
        String jobClustersWithArtifactCachingEnabled,
        ResourceClusterActorMetrics metrics
    ) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(rpcService, "rpcService");
        Objects.requireNonNull(jobMessageRouter, "jobMessageRouter");
        Objects.requireNonNull(mantisJobStore, "mantisJobStore");
        Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        Objects.requireNonNull(assignmentTimeout, "assignmentTimeout");
        Objects.requireNonNull(clusterID, "clusterID");
        Objects.requireNonNull(jobClustersWithArtifactCachingEnabled, "jobClustersWithArtifactCachingEnabled");
        Objects.requireNonNull(metrics, "metrics");
        return Props.create(
            ExecutorStateManagerActor.class,
            delegate,
            clock,
            rpcService,
            jobMessageRouter,
            mantisJobStore,
            heartbeatTimeout,
            assignmentTimeout,
            clusterID,
            isJobArtifactCachingEnabled,
            jobClustersWithArtifactCachingEnabled,
            metrics
        )
            .withMailbox("akka.actor.metered-mailbox");
    }

    ExecutorStateManagerActor(
        Map<String, String> schedulingAttributes,
        FitnessCalculator fitnessCalculator,
        Duration schedulerLeaseExpirationDuration,
        @Nullable AvailableTaskExecutorMutatorHook availableTaskExecutorMutatorHook,
        Clock clock,
        RpcService rpcService,
        JobMessageRouter jobMessageRouter,
        MantisJobStore mantisJobStore,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        ClusterID clusterID,
        boolean isJobArtifactCachingEnabled,
        String jobClustersWithArtifactCachingEnabled,
        ResourceClusterActorMetrics metrics
    ) {
        this(
            new ExecutorStateManagerImpl(
                schedulingAttributes,
                fitnessCalculator,
                schedulerLeaseExpirationDuration,
                availableTaskExecutorMutatorHook),
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
    }

    ExecutorStateManagerActor(
        ExecutorStateManagerImpl delegate,
        Clock clock,
        RpcService rpcService,
        JobMessageRouter jobMessageRouter,
        MantisJobStore mantisJobStore,
        Duration heartbeatTimeout,
        Duration assignmentTimeout,
        ClusterID clusterID,
        boolean isJobArtifactCachingEnabled,
        String jobClustersWithArtifactCachingEnabled,
        ResourceClusterActorMetrics metrics
    ) {
        this.delegate = delegate;
        this.clock = clock;
        this.rpcService = rpcService;
        this.jobMessageRouter = jobMessageRouter;
        this.mantisJobStore = mantisJobStore;
        this.heartbeatTimeout = heartbeatTimeout;
        this.assignmentTimeout = assignmentTimeout;
        this.clusterID = clusterID;
        this.isJobArtifactCachingEnabled = isJobArtifactCachingEnabled;
        this.jobClustersWithArtifactCachingEnabled = jobClustersWithArtifactCachingEnabled;
        this.metrics = metrics;
        this.activeDisableTaskExecutorsByAttributesRequests = new HashSet<>();
        this.disabledTaskExecutors = new HashSet<>();
        this.jobArtifactsToCache = new HashSet<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(InitializeTaskExecutorRequest.class, this::onTaskExecutorInitialization)
            .match(TaskExecutorRegistration.class, this::onTaskExecutorRegistration)
            .match(TaskExecutorHeartbeat.class, this::onHeartbeat)
            .match(TaskExecutorStatusChange.class, this::onTaskExecutorStatusChange)
            .match(TaskExecutorBatchAssignmentRequest.class, this::onTaskExecutorBatchAssignmentRequest)
            .match(TaskExecutorAssignmentTimeout.class, this::onTaskExecutorAssignmentTimeout)
            .match(TaskExecutorDisconnection.class, this::onTaskExecutorDisconnection)
            .match(HeartbeatTimeout.class, this::onTaskExecutorHeartbeatTimeout)
            .match(CacheJobArtifactsOnTaskExecutorRequest.class, this::onCacheJobArtifactsOnTaskExecutorRequest)
            .match(TaskExecutorInfoRequest.class, this::onTaskExecutorInfoRequest)
            .match(TaskExecutorGatewayRequest.class, this::onTaskExecutorGatewayRequest)
            .match(GetTaskExecutorStatusRequest.class, this::onGetTaskExecutorStatus)
            .match(GetRegisteredTaskExecutorsRequest.class, req -> onGetTaskExecutors(req, ExecutorStateManager.isRegistered))
            .match(GetBusyTaskExecutorsRequest.class, req -> onGetTaskExecutors(req, ExecutorStateManager.isBusy))
            .match(GetAvailableTaskExecutorsRequest.class, req -> onGetTaskExecutors(req, ExecutorStateManager.isAvailable))
            .match(GetDisabledTaskExecutorsRequest.class, req -> onGetTaskExecutors(req, ExecutorStateManager.isDisabled))
            .match(GetUnregisteredTaskExecutorsRequest.class, req -> onGetTaskExecutors(req, ExecutorStateManager.unregistered))
            .match(GetActiveJobsRequest.class, this::onGetActiveJobs)
            .match(GetClusterUsageRequest.class, this::onGetClusterUsage)
            .match(GetClusterIdleInstancesRequest.class, this::onGetClusterIdleInstancesRequest)
            .match(GetAssignedTaskExecutorRequest.class, this::onGetAssignedTaskExecutorRequest)
            .match(MarkExecutorTaskCancelledRequest.class, this::onMarkExecutorTaskCancelledRequest)
            .match(ResourceOverviewRequest.class, this::onResourceOverviewRequest)
            .match(GetTaskExecutorWorkerMappingRequest.class, this::onGetTaskExecutorWorkerMappingRequest)
            .match(PublishResourceOverviewMetricsRequest.class, this::onPublishResourceOverviewMetricsRequest)
            .match(AddNewJobArtifactsToCacheRequest.class, this::onAddNewJobArtifactsToCacheRequest)
            .match(RemoveJobArtifactsToCacheRequest.class, this::onRemoveJobArtifactsToCacheRequest)
            .match(GetJobArtifactsToCacheRequest.class, this::onGetJobArtifactsToCacheRequest)
            .match(RefreshTaskExecutorJobArtifactCache.class, refresh -> refreshTaskExecutorJobArtifactCache())
            .match(CheckDisabledTaskExecutors.class, this::onCheckDisabledTaskExecutors)
            .match(ExpireDisableTaskExecutorsRequest.class, this::onDisableTaskExecutorsRequestExpiry)
            .match(UpdateDisabledState.class, this::onUpdateDisabledState)
            .match(UpdateJobArtifactsToCache.class, this::onUpdateJobArtifactsToCache)
            .build();
    }

    private void onTaskExecutorInitialization(InitializeTaskExecutorRequest request) {
        log.info("Initializing taskExecutor {} for the resource cluster {}", request.getTaskExecutorID(), clusterID);
        try {
            TaskExecutorRegistration registration =
                mantisJobStore.getTaskExecutor(request.getTaskExecutorID());
            if (registration == null) {
                sender().tell(new Status.Failure(new TaskExecutorNotFoundException(request.getTaskExecutorID())), self());
                return;
            }
            handleTaskExecutorRegistration(registration);
            self().tell(
                new TaskExecutorStatusChange(
                    registration.getTaskExecutorID(),
                    registration.getClusterID(),
                    TaskExecutorReport.occupied(request.getWorkerId())),
                self());
            sender().tell(Ack.getInstance(), self());
        } catch (Exception e) {
            log.error("Failed to initialize taskExecutor {}; all retries exhausted", request.getTaskExecutorID(), e);
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorRegistration(TaskExecutorRegistration registration) {
        try {
            handleTaskExecutorRegistration(registration);
            sender().tell(Ack.getInstance(), self());
        } catch (Exception e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void handleTaskExecutorRegistration(TaskExecutorRegistration registration) throws Exception {
        setupTaskExecutorStateIfNecessary(registration.getTaskExecutorID());
        log.info("Request for registering on resource cluster {}: {}.", clusterID, registration);
        final TaskExecutorID taskExecutorID = registration.getTaskExecutorID();
        final TaskExecutorState state = this.delegate.get(taskExecutorID);
        boolean stateChange = state.onRegistration(registration);
        mantisJobStore.storeNewTaskExecutor(registration);
        if (stateChange) {
            if (state.isAvailable()) {
                this.delegate.tryMarkAvailable(taskExecutorID);
            }
            if (isTaskExecutorDisabled(registration)) {
                log.info("Newly registered task executor {} was already marked for disabling.", registration.getTaskExecutorID());
                state.onNodeDisabled();
            }
            updateHeartbeatTimeout(registration.getTaskExecutorID());
        }
        log.info("Successfully registered {} with the resource cluster {}", registration.getTaskExecutorID(), clusterID);
        if (!jobArtifactsToCache.isEmpty() && isJobArtifactCachingEnabled) {
            self().tell(new CacheJobArtifactsOnTaskExecutorRequest(taskExecutorID, clusterID), self());
        }
    }

    private void onHeartbeat(TaskExecutorHeartbeat heartbeat) {
        log.debug("Received heartbeat {} from task executor {}", heartbeat, heartbeat.getTaskExecutorID());
        setupTaskExecutorStateIfNecessary(heartbeat.getTaskExecutorID());
        try {
            final TaskExecutorID taskExecutorID = heartbeat.getTaskExecutorID();
            final TaskExecutorState state = this.delegate.get(taskExecutorID);
            if (state.getRegistration() == null || !state.isRegistered()) {
                TaskExecutorRegistration registration = this.mantisJobStore.getTaskExecutor(heartbeat.getTaskExecutorID());
                if (registration != null) {
                    log.debug("Found registration {} for task executor {}", registration, heartbeat.getTaskExecutorID());
                    Preconditions.checkState(state.onRegistration(registration));

                    if (isTaskExecutorDisabled(registration)) {
                        log.info("Reconnected task executor {} was already marked for disabling.", registration.getTaskExecutorID());
                        state.onNodeDisabled();
                    }
                } else {
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
                this.delegate.tryMarkAvailable(taskExecutorID);
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
            final TaskExecutorState state = this.delegate.get(taskExecutorID);
            boolean stateChange = state.onTaskExecutorStatusChange(statusChange);
            if (stateChange) {
                if (state.isAvailable()) {
                    this.delegate.tryMarkAvailable(taskExecutorID);
                } else {
                    this.delegate.tryMarkUnavailable(taskExecutorID);
                }
            }

            updateHeartbeatTimeout(statusChange.getTaskExecutorID());
            sender().tell(Ack.getInstance(), self());
        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void onTaskExecutorBatchAssignmentRequest(TaskExecutorBatchAssignmentRequest request) {
        Optional<BestFit> matchedExecutors = this.delegate.findBestFit(request);

        if (matchedExecutors.isPresent()) {
            log.info("Matched all executors {} for request {}", matchedExecutors.get(), request);
            matchedExecutors.get().getBestFit().forEach((allocationRequest, taskExecutorToState) -> assignTaskExecutor(
                allocationRequest, taskExecutorToState.getLeft(), taskExecutorToState.getRight(), request));
            sender().tell(new TaskExecutorsAllocation(matchedExecutors.get().getRequestToTaskExecutorMap()), self());
        } else {
            request.getAllocationRequests().forEach(req -> metrics.incrementCounter(
                ResourceClusterActorMetrics.NO_RESOURCES_AVAILABLE,
                createTagListFrom(req)));
            sender().tell(new Status.Failure(new NoResourceAvailableException(
                String.format("No resource available for request %s: resource overview: %s", request,
                    getResourceOverview()))), self());
        }
    }

    private void assignTaskExecutor(TaskExecutorAllocationRequest allocationRequest, TaskExecutorID taskExecutorID, TaskExecutorState taskExecutorState, TaskExecutorBatchAssignmentRequest request) {
        if(shouldCacheJobArtifacts(allocationRequest)) {
            getContext().parent().tell(new AddNewJobArtifactsToCacheRequest(clusterID, Collections.singletonList(allocationRequest.getJobMetadata().getJobArtifact())), self());
        }

        taskExecutorState.onAssignment(allocationRequest.getWorkerId());
        getTimers().startSingleTimer(
            "Assignment-" + taskExecutorID.toString(),
            new TaskExecutorAssignmentTimeout(taskExecutorID),
            assignmentTimeout);
    }

    private void onTaskExecutorAssignmentTimeout(TaskExecutorAssignmentTimeout request) {
        TaskExecutorState state = this.delegate.get(request.getTaskExecutorID());
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
                    this.delegate.tryMarkAvailable(request.getTaskExecutorID());
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
        final TaskExecutorState state = this.delegate.get(taskExecutorID);
        boolean stateChange = state.onDisconnection();
        if (stateChange) {
            this.delegate.archive(taskExecutorID);
            getTimers().cancel(getHeartbeatTimerFor(taskExecutorID));
        }
    }

    private void onTaskExecutorHeartbeatTimeout(HeartbeatTimeout timeout) {
        setupTaskExecutorStateIfNecessary(timeout.getTaskExecutorID());
        try {
            metrics.incrementCounter(
                ResourceClusterActorMetrics.HEARTBEAT_TIMEOUT,
                TagList.create(ImmutableMap.of("resourceCluster", clusterID.getResourceID(), "taskExecutorID", timeout.getTaskExecutorID().getResourceId())));
            log.info("heartbeat timeout received for {}", timeout.getTaskExecutorID());
            final TaskExecutorID taskExecutorID = timeout.getTaskExecutorID();
            final TaskExecutorState state = this.delegate.get(taskExecutorID);
            if (state.getLastActivity().compareTo(timeout.getLastActivity()) <= 0) {
                log.info("Disconnecting task executor {}", timeout.getTaskExecutorID());
                disconnectTaskExecutor(timeout.getTaskExecutorID());
            }

        } catch (IllegalStateException e) {
            sender().tell(new Status.Failure(e), self());
        }
    }

    private void setupTaskExecutorStateIfNecessary(TaskExecutorID taskExecutorID) {
        this.delegate.trackIfAbsent(
            taskExecutorID,
            TaskExecutorState.of(clock, rpcService, jobMessageRouter));
    }

    private void updateHeartbeatTimeout(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = this.delegate.get(taskExecutorID);
        getTimers().startSingleTimer(
            getHeartbeatTimerFor(taskExecutorID),
            new HeartbeatTimeout(taskExecutorID, state.getLastActivity()),
            heartbeatTimeout);
    }

    private String getHeartbeatTimerFor(TaskExecutorID taskExecutorID) {
        return "Heartbeat-" + taskExecutorID;
    }

    private boolean isTaskExecutorDisabled(TaskExecutorRegistration registration) {
        for (DisableTaskExecutorsRequest request : activeDisableTaskExecutorsByAttributesRequests) {
            if (request.covers(registration)) {
                return true;
            }
        }
        return disabledTaskExecutors.contains(registration.getTaskExecutorID());
    }

    private void onCacheJobArtifactsOnTaskExecutorRequest(CacheJobArtifactsOnTaskExecutorRequest request) {
        TaskExecutorState state = this.delegate.get(request.getTaskExecutorID());
        if (state != null && state.isRegistered()) {
            try {
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
                        } else {
                            log.debug("Acked from cacheJobArtifacts for {}", request.getTaskExecutorID());
                        }
                    });
            } catch (Exception ex) {
                log.warn("Failed to cache job artifacts in task executor {}", request.getTaskExecutorID(), ex);
            }
        } else {
            log.debug("no valid TE state for CacheJobArtifactsOnTaskExecutorRequest: {}", request);
        }
        sender().tell(Ack.getInstance(), self());
    }

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

    private void onTaskExecutorGatewayRequest(TaskExecutorGatewayRequest request) {
        TaskExecutorState state = this.delegate.get(request.getTaskExecutorID());
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

    private void onTaskExecutorInfoRequest(TaskExecutorInfoRequest request) {
        if (request.getTaskExecutorID() != null) {
            TaskExecutorState state =
                this.delegate.getIncludeArchived(request.getTaskExecutorID());
            if (state != null && state.getRegistration() != null) {
                sender().tell(state.getRegistration(), self());
            } else {
                sender().tell(new Status.Failure(new Exception(String.format("No task executor state for %s",
                    request.getTaskExecutorID()))), self());
            }
        } else {
            Optional<TaskExecutorRegistration> taskExecutorRegistration =
                this.delegate
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

    private void onGetTaskExecutorStatus(GetTaskExecutorStatusRequest req) {
        TaskExecutorID taskExecutorID = req.getTaskExecutorID();
        final TaskExecutorState state = this.delegate.get(taskExecutorID);
        if (state == null) {
            log.info("Unknown executorID: {}", taskExecutorID);
            sender().tell(
                new Status.Failure(new TaskExecutorNotFoundException(taskExecutorID)),
                self());
        } else {
            sender().tell(
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

    private void onGetTaskExecutors(ResourceClusterActor.HasAttributes request,
                                    Predicate<Entry<TaskExecutorID, TaskExecutorState>> predicate) {
        Predicate<Entry<TaskExecutorID, TaskExecutorState>> combined = filterByAttrs(request).and(predicate);
        sender().tell(new TaskExecutorsList(this.delegate.getTaskExecutors(combined)), self());
    }

    private Predicate<Entry<TaskExecutorID, TaskExecutorState>> filterByAttrs(ResourceClusterActor.HasAttributes hasAttributes) {
        if (hasAttributes.getAttributes().isEmpty()) {
            return e -> true;
        } else {
            return e -> e.getValue().containsAttributes(hasAttributes.getAttributes());
        }
    }

    private void onGetActiveJobs(GetActiveJobsRequest req) {
        List<String> pagedList = this.delegate.getActiveJobs(req);

        PagedActiveJobOverview res =
            new PagedActiveJobOverview(
                pagedList,
                req.getStartingIndex().orElse(0) + pagedList.size()
            );

        sender().tell(res, self());
    }

    private void onGetClusterUsage(GetClusterUsageRequest req) {
        sender().tell(this.delegate.getClusterUsage(req), self());
    }

    private void onGetClusterIdleInstancesRequest(GetClusterIdleInstancesRequest req) {
        if (!req.getClusterID().equals(this.clusterID)) {
            sender().tell(new Status.Failure(
                new IllegalArgumentException(String.format("Mismatch cluster ids %s, %s", req.getClusterID(), this.clusterID))),
                self());
            return;
        }

        List<TaskExecutorID> instanceList = this.delegate.getIdleInstanceList(req);

        GetClusterIdleInstancesResponse res = GetClusterIdleInstancesResponse.builder()
            .instanceIds(instanceList)
            .clusterId(this.clusterID)
            .skuId(req.getSkuId())
            .build();
        sender().tell(res, self());
    }

    private void onGetAssignedTaskExecutorRequest(GetAssignedTaskExecutorRequest request) {
        Optional<TaskExecutorID> matchedTaskExecutor =
            this.delegate.findFirst(
                e -> e.getValue().isRunningOrAssigned(request.getWorkerId())).map(Entry::getKey);

        if (matchedTaskExecutor.isPresent()) {
            sender().tell(matchedTaskExecutor.get(), self());
        } else {
            sender().tell(new Status.Failure(new TaskNotFoundException(request.getWorkerId())), self());
        }
    }

    private void onMarkExecutorTaskCancelledRequest(MarkExecutorTaskCancelledRequest request) {
        Optional<Entry<TaskExecutorID, TaskExecutorState>> matchedTaskExecutor =
            this.delegate.findFirst(e -> e.getValue().isRunningOrAssigned(request.getWorkerId()));

        if (matchedTaskExecutor.isPresent()) {
            log.info("Setting executor {} to cancelled workerID: {}", matchedTaskExecutor.get().getKey(), request);
            matchedTaskExecutor.get().getValue().setCancelledWorkerOnTask(request.getWorkerId());
            sender().tell(Ack.getInstance(), self());
        } else {
            log.info("Cannot find executor to mark worker {} as cancelled", request);
            sender().tell(new Status.Failure(new TaskNotFoundException(request.getWorkerId())), self());
        }
    }

    private void onResourceOverviewRequest(ResourceOverviewRequest request) {
        sender().tell(getResourceOverview(), self());
    }

    private void onGetTaskExecutorWorkerMappingRequest(GetTaskExecutorWorkerMappingRequest request) {
        sender().tell(getTaskExecutorWorkerMapping(request.getAttributes()), self());
    }

    private void onPublishResourceOverviewMetricsRequest(PublishResourceOverviewMetricsRequest request) {
        publishResourceClusterMetricBySKU(
            new TaskExecutorsList(this.delegate.getTaskExecutors(ExecutorStateManager.isRegistered)),
            ResourceClusterActorMetrics.NUM_REGISTERED_TE);
        publishResourceClusterMetricBySKU(
            new TaskExecutorsList(this.delegate.getTaskExecutors(ExecutorStateManager.isBusy)),
            ResourceClusterActorMetrics.NUM_BUSY_TE);
        publishResourceClusterMetricBySKU(
            new TaskExecutorsList(this.delegate.getTaskExecutors(ExecutorStateManager.isAvailable)),
            ResourceClusterActorMetrics.NUM_AVAILABLE_TE);
        publishResourceClusterMetricBySKU(
            new TaskExecutorsList(this.delegate.getTaskExecutors(ExecutorStateManager.isDisabled)),
            ResourceClusterActorMetrics.NUM_DISABLED_TE);
        publishResourceClusterMetricBySKU(
            new TaskExecutorsList(this.delegate.getTaskExecutors(ExecutorStateManager.unregistered)),
            ResourceClusterActorMetrics.NUM_UNREGISTERED_TE);
        publishResourceClusterMetricBySKU(
            new TaskExecutorsList(this.delegate.getTaskExecutors(ExecutorStateManager.isAssigned)),
            ResourceClusterActorMetrics.NUM_ASSIGNED_TE);
        sender().tell(Ack.getInstance(), self());
    }

    private void publishResourceClusterMetricBySKU(TaskExecutorsList taskExecutorsList, String metricName) {
        try {
            taskExecutorsList.getTaskExecutors()
                .stream()
                .map(this.delegate::get)
                .filter(Objects::nonNull)
                .map(TaskExecutorState::getRegistration)
                .filter(Objects::nonNull)
                .filter(registration -> registration.getTaskExecutorContainerDefinitionId().isPresent() && registration.getAttributeByKey(WorkerConstants.AUTO_SCALE_GROUP_KEY).isPresent())
                .collect(Collectors.groupingBy(
                    registration -> new AbstractMap.SimpleEntry<>(
                        registration.getTaskExecutorContainerDefinitionId().get(),
                        registration.getAttributeByKey(WorkerConstants.AUTO_SCALE_GROUP_KEY).get()),
                    Collectors.counting()))
                .forEach((keys, count) -> metrics.setGauge(
                    metricName,
                    count,
                    TagList.create(ImmutableMap.of(
                        "resourceCluster",
                        clusterID.getResourceID(),
                        "sku",
                        keys.getKey().getResourceID(),
                        "autoScaleGroup",
                        keys.getValue()))));
        } catch (Exception e) {
            log.warn("Error while publishing resource cluster metrics by sku. RC: {}, Metric: {}.", clusterID.getResourceID(), metricName, e);
        }
    }

    private void onAddNewJobArtifactsToCacheRequest(AddNewJobArtifactsToCacheRequest req) {
        jobArtifactsToCache.addAll(req.getArtifacts());
        refreshTaskExecutorJobArtifactCache();
        sender().tell(Ack.getInstance(), self());
    }

    private void onRemoveJobArtifactsToCacheRequest(RemoveJobArtifactsToCacheRequest req) {
        req.getArtifacts().forEach(jobArtifactsToCache::remove);
        sender().tell(Ack.getInstance(), self());
    }

    private void onGetJobArtifactsToCacheRequest(GetJobArtifactsToCacheRequest req) {
        sender().tell(new ArtifactList(new ArrayList<>(jobArtifactsToCache)), self());
    }

    private void onUpdateDisabledState(UpdateDisabledState update) {
        this.activeDisableTaskExecutorsByAttributesRequests.clear();
        this.activeDisableTaskExecutorsByAttributesRequests.addAll(update.getAttributeRequests());
        this.disabledTaskExecutors.clear();
        this.disabledTaskExecutors.addAll(update.getDisabledExecutors());
    }

    private void onUpdateJobArtifactsToCache(UpdateJobArtifactsToCache update) {
        this.jobArtifactsToCache.clear();
        this.jobArtifactsToCache.addAll(update.getArtifacts());
    }

    private void onCheckDisabledTaskExecutors(CheckDisabledTaskExecutors request) {
        final Instant now = clock.instant();
        for (DisableTaskExecutorsRequest disableRequest : activeDisableTaskExecutorsByAttributesRequests) {
            if (disableRequest.isExpired(now)) {
                self().tell(new ExpireDisableTaskExecutorsRequest(disableRequest), self());
            } else {
                this.delegate.getActiveExecutorEntry().forEach(idAndState -> {
                    if (disableRequest.covers(idAndState.getValue().getRegistration())) {
                        idAndState.getValue().onNodeDisabled();
                    }
                });
            }
        }

        for (TaskExecutorID taskExecutorID : disabledTaskExecutors) {
            TaskExecutorState state = this.delegate.get(taskExecutorID);
            if (state != null) {
                state.onNodeDisabled();
            }
        }
    }

    private void onDisableTaskExecutorsRequestExpiry(ExpireDisableTaskExecutorsRequest request) {
        try {
            if (request.getRequest().getTaskExecutorID().isPresent()) {
                final TaskExecutorID taskExecutorID = request.getRequest().getTaskExecutorID().get();
                final TaskExecutorState state = this.delegate.get(taskExecutorID);
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
        this.delegate.getActiveExecutorEntry().forEach(idAndState -> {
            if (idAndState.getValue().getRegistration() != null && idAndState.getValue().getRegistration().containsAttributes(attributes)) {
                if (idAndState.getValue().isRunningTask()) {
                    result.put(idAndState.getKey(), idAndState.getValue().getWorkerId());
                }
            }
        });
        return result;
    }

    private void refreshTaskExecutorJobArtifactCache() {
        this.delegate.getTaskExecutors(ExecutorStateManager.isAvailable)
            .forEach(taskExecutorID ->
                self().tell(new CacheJobArtifactsOnTaskExecutorRequest(taskExecutorID, clusterID), self()));
    }

    private Iterable<Tag> createTagListFrom(TaskExecutorAllocationRequest req) {
        ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.<String, String>builder()
            .put("resourceCluster", clusterID.getResourceID())
            .put("workerId", req.getWorkerId().getId())
            .put("jobCluster", req.getWorkerId().getJobCluster());

        if (req.getConstraints().getSizeName().isPresent()) {
            tagsBuilder.put("sizeName", req.getConstraints().getSizeName().get());
        } else {
            tagsBuilder.put("cpuCores", String.valueOf(req.getConstraints().getMachineDefinition().getCpuCores()))
                .put("memoryMB", String.valueOf(req.getConstraints().getMachineDefinition().getMemoryMB()));
        }

        return TagList.create(tagsBuilder.build());
    }

    private ResourceOverview getResourceOverview() {
        return this.delegate.getResourceOverview();
    }
}
