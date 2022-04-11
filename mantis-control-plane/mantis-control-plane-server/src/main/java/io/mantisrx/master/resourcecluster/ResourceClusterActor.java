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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.Status;
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.common.Ack;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
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
import io.mantisrx.server.worker.TaskExecutorGateway;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
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
class ResourceClusterActor extends AbstractActor {

    private final Duration heartbeatTimeout;

    private final Map<TaskExecutorID, TaskExecutorState> taskExecutorStateMap;
    private final Clock clock;
    private final Set<TaskExecutorID> taskExecutorsReadyToPerformWork;
    private final RpcService rpcService;
    private final ClusterID clusterID;
    private final MantisJobStore mantisJobStore;

    static Props props(final ClusterID clusterID, final Duration heartbeatTimeout, Clock clock, RpcService rpcService) {
        return Props.create(ResourceClusterActor.class, clusterID, heartbeatTimeout, clock, rpcService);
    }

    ResourceClusterActor(
        ClusterID clusterID,
        Duration heartbeatTimeout,
        Clock clock,
        RpcService rpcService,
        MantisJobStore mantisJobStore) {
        this.clusterID = clusterID;
        this.heartbeatTimeout = heartbeatTimeout;
        this.clock = clock;
        this.rpcService = rpcService;
        this.taskExecutorStateMap = new HashMap<>();
        this.taskExecutorsReadyToPerformWork = new HashSet<>();
        this.mantisJobStore = mantisJobStore;
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(GetRegisteredTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isRegistered), self()))
                .match(GetBusyTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isBusy), self()))
                .match(GetAvailableTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(isAvailable), self()))
                .match(GetUnregisteredTaskExecutorsRequest.class, req -> sender().tell(getTaskExecutors(unregistered), self()))
                .match(GetTaskExecutorStatusRequest.class, req -> sender().tell(getTaskExecutorStatus(req.getTaskExecutorID()), self()))

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
        log.info("Request for registering {} with the resource cluster {}", registration.getTaskExecutorID(), this);
        try {
            final TaskExecutorID taskExecutorID = registration.getTaskExecutorID();
            final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
            boolean stateChange = state.onRegistration(registration);
            mantisJobStore.storeNewTaskExecutor(registration);
            if (stateChange) {
                if (state.isAvailable()) {
                    taskExecutorsReadyToPerformWork.add(taskExecutorID);
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
            sender().tell(matchedExecutor.get().getKey(), self());
        } else {
            sender().tell(new Status.Failure(new NoResourceAvailableException(
                String.format("No resource available for request %s: resource overview: %s", request,
                    getResourceOverview()))), self());
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

        return new ResourceOverview(numRegistered, numAvailable, numOccupied, numAssigned);
    }

    private TaskExecutorStatus getTaskExecutorStatus(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
        return new TaskExecutorStatus(
            state.getRegistration(),
            state.isRegistered(),
            state.isRunningTask(),
            state.isAssigned(),
            state.getWorkerId(),
            state.getLastActivity());
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
            state.setNextHeartbeatChecker(null);
        }
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
        taskExecutorStateMap.putIfAbsent(taskExecutorID, TaskExecutorState.of(clock, rpcService));
    }

    private void updateHeartbeatTimeout(TaskExecutorID taskExecutorID) {
        final TaskExecutorState state = taskExecutorStateMap.get(taskExecutorID);
        final Cancellable nextHeartbeatChecker =
            context()
                .system()
                .scheduler()
                .scheduleOnce(
                    heartbeatTimeout,
                    self(),
                    new HeartbeatTimeout(taskExecutorID, state.getLastActivity()),
                    getContext().getDispatcher(),
                    self());
        state.setNextHeartbeatChecker(nextHeartbeatChecker);
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

    @SuppressWarnings("UnusedReturnValue")
    @AllArgsConstructor
    static class TaskExecutorState {

        enum RegistrationState {
            Registered,
            Unregistered,
        }

        enum AvailabilityState {
            Pending,
            Assigned,
            Running,
        }

        private RegistrationState state;
        @Nullable
        private TaskExecutorRegistration registration;

        @Nullable
        private CompletableFuture<TaskExecutorGateway> gateway;

        @Nullable
        private AvailabilityState availabilityState;
        @Nullable
        private WorkerId workerId;
        @Nullable
        private Cancellable nextHeartbeatChecker;
        private Instant lastActivity;
        private final Clock clock;
        private final RpcService rpcService;

        static TaskExecutorState of(Clock clock, RpcService rpcService) {
            return new TaskExecutorState(
                RegistrationState.Unregistered,
                null,
                null,
                null,
                null,
                null,
                clock.instant(),
                clock,
                rpcService);
        }

        boolean isRegistered() {
            return state == RegistrationState.Registered;
        }

        boolean isDisconnected() {
            return !isRegistered();
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
                workerId = null;
                availabilityState = null;
                gateway = null;
                updateTicker();
                return true;
            }
        }

        private static AvailabilityState from(TaskExecutorReport report) {
            if (report instanceof Available) {
                return AvailabilityState.Pending;
            } else if (report instanceof Occupied) {
                return AvailabilityState.Running;
            } else {
                throw new RuntimeException(String.format("TaskExecutorReport=%s was unexpected", report));
            }
        }

        boolean onAssignment(WorkerId workerId) throws IllegalStateException {
            if (!isRegistered()) {
                throwNotRegistered(String.format("assignment to %s", workerId));
            }

            if (this.availabilityState == null) {
                throwInvalidTransition(workerId);
            } else {
                switch (this.availabilityState) {
                    case Pending:
                        this.workerId = workerId;
                        this.availabilityState = AvailabilityState.Assigned;
                        return true;
                    case Assigned:
                        if (!this.workerId.equals(workerId)) {
                            throwInvalidTransition(workerId);
                        } else {
                            return false;
                        }
                    default:
                        throwInvalidTransition(workerId);
                }
            }
            return false;
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
                availabilityState = from(report);
                return true;
            } else {
                switch (availabilityState) {
                    case Pending:
                        if (report instanceof Available) {
                            return false;
                        } else if (report instanceof Occupied) {
                            throwInvalidTransition(report);
                        }
                    case Assigned:
                        if (report instanceof Available) {
                            return false;
                        } else if (report instanceof Occupied) {
                            if (((Occupied) report).getWorkerId().equals(workerId)) {
                                this.availabilityState = AvailabilityState.Running;
                                return true;
                            } else {
                                throwInvalidTransition(report);
                            }
                        }
                    case Running:
                        if (report instanceof Available) {
                            this.workerId = null;
                            this.availabilityState = AvailabilityState.Pending;
                            return true;
                        } else if (report instanceof Occupied) {
                            if (!((Occupied) report).getWorkerId().equals(workerId)) {
                                throwInvalidTransition(report);
                            } else {
                                return false;
                            }
                        }
                }
            }
            return false;
        }

        @Nullable
        private WorkerId getWorkerId() {
            return this.workerId;
        }

        private void throwNotRegistered(String message) throws IllegalStateException {
            throw new IllegalStateException(
                String.format("Task Executor un-registered when it received %s", message));
        }

        private void throwInvalidTransition(TaskExecutorReport report) throws IllegalStateException {
            throw new IllegalStateException(
                String.format("availability state was %s, workerId was %s when report %s was received",
                    this.availabilityState, this.workerId, report));
        }

        private void throwInvalidTransition(WorkerId workerId) throws IllegalStateException {
            throw new IllegalStateException(
                String.format("availability state was %s, workerId was %s when workerId %s was assigned",
                    this.availabilityState, this.workerId, workerId));
        }

        private void setNextHeartbeatChecker(@Nullable Cancellable nextHeartbeatChecker) {
            if (this.nextHeartbeatChecker != null) {
                this.nextHeartbeatChecker.cancel();
            }

            this.nextHeartbeatChecker = nextHeartbeatChecker;
        }

        private void updateTicker() {
            this.lastActivity = clock.instant();
        }

        boolean isAvailable() {
            return this.availabilityState == AvailabilityState.Pending;
        }

        boolean isRunningTask() {
            return this.availabilityState == AvailabilityState.Running;
        }

        boolean isAssigned() {
            return this.availabilityState == AvailabilityState.Assigned;
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
