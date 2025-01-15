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

import io.mantisrx.master.resourcecluster.ResourceClusterActor.Assigned;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.AvailabilityState;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.Pending;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.Running;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport.Available;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport.Occupied;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.resourcecluster.TaskExecutorTaskCancelledException;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import io.mantisrx.server.worker.TaskExecutorGateway;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.rpc.RpcService;

@SuppressWarnings("UnusedReturnValue")
@AllArgsConstructor
@Slf4j
class TaskExecutorState {

    enum RegistrationState {
        Registered,
        Unregistered,
    }

    private RegistrationState state;
    @Nullable
    private TaskExecutorRegistration registration;

    // availabilityState being null here represents that we don't know about the actual state of the task executor
    // and are waiting for more information
    @Nullable
    private AvailabilityState availabilityState;
    private boolean disabled;

    // last interaction initiated by the task executor
    private Instant lastActivity;

    // last interaction time when this instance was leased by the scheduler in findBestFit.
    private Instant lastSchedulerLeased;
    private final Clock clock;
    private final RpcService rpcService;
    private final JobMessageRouter jobMessageRouter;

    // isTaskCancelled: this state is to mark the current assigned worker has been cancelled and this executor need to
    // stop the task and re-register.
    @Nullable
    private WorkerId cancelledWorkerOnTask;

    static TaskExecutorState of(Clock clock, RpcService rpcService, JobMessageRouter jobMessageRouter) {
        return new TaskExecutorState(
            RegistrationState.Unregistered,
            null,
            null,
            false,
            clock.instant(),
            Instant.MIN,
            clock,
            rpcService,
            jobMessageRouter,
            null);
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

    @Nullable
    WorkerId getCancelledWorkerId() {
        return this.cancelledWorkerOnTask;
    }

    void setCancelledWorkerOnTask(WorkerId cancelledWorkerOnTask) {
        this.cancelledWorkerOnTask = cancelledWorkerOnTask;
    }

    boolean onRegistration(TaskExecutorRegistration registration) {
        if (state == RegistrationState.Registered) {
            return false;
        } else {
            this.state = RegistrationState.Registered;
            this.registration = registration;
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

    boolean onNodeEnabled() {
        if (this.disabled) {
            this.disabled = false;
            return true;
        } else {
            return false;
        }
    }

    boolean onHeartbeat(TaskExecutorHeartbeat heartbeat)
        throws IllegalStateException, TaskExecutorTaskCancelledException {
        if (!isRegistered()) {
            throwNotRegistered(String.format("heartbeat %s", heartbeat));
        }

        TaskExecutorReport report = heartbeat.getTaskExecutorReport();
        if (this.cancelledWorkerOnTask != null) {
            if (report instanceof Occupied && ((Occupied) report).getWorkerId().equals(this.cancelledWorkerOnTask)) {
                log.warn("{} cancelled, request cancel on heartbeat.", this.cancelledWorkerOnTask);
                throw new TaskExecutorTaskCancelledException(
                    String.format(
                        "heartbeat from %s has cancelled task %s",
                        heartbeat.getTaskExecutorID(),
                        this.cancelledWorkerOnTask),
                    this.cancelledWorkerOnTask);
            } else {
                log.info("{} cancelled but executor is no longer occupied by it.", this.cancelledWorkerOnTask);
                this.cancelledWorkerOnTask = null;
            }
        }

        boolean result = handleStatusChange(report);
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
    protected WorkerId getWorkerId() {
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

    boolean isRunningOrAssigned(WorkerId workerId) {
        return this.getWorkerId() != null && this.getWorkerId().equals(workerId);
    }

    // Captures the last interaction from the task executor. Any interactions
    // that are caused from within the server do not cause an uptick.
    Instant getLastActivity() {
        return this.lastActivity;
    }

    Duration getLastSchedulerLeasedDuration()
    {
        return Duration.between(this.lastSchedulerLeased, this.clock.instant());
    }

    void updateLastSchedulerLeased()
    {
        this.lastSchedulerLeased = this.clock.instant();
    }

    TaskExecutorRegistration getRegistration() {
        return this.registration;
    }

    protected CompletableFuture<TaskExecutorGateway> getGatewayAsync() {
        if (this.registration == null || this.state == RegistrationState.Unregistered) {
            throw new IllegalStateException("TE is unregistered");
        }

        // [Note] here the gateway connection is re-created every time it's requested to avoid corrupted state that
        // can block the connection to TE.
        // To be able to store and re-use the gateway, we probably need to make a chain of callbacks so taht the TE
        // is only marked as available after this gateway connection is successfully established with proper retry
        // loops (since the TE only register once and take the ack as success on API response).
        return rpcService.connect(registration.getTaskExecutorAddress(), TaskExecutorGateway.class)
                .whenComplete((gateway, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to connect to the gateway", throwable);
                    }
                });
    }

    boolean containsAttributes(Map<String, String> attributes) {
        return registration != null && registration.containsAttributes(attributes);
    }
}
