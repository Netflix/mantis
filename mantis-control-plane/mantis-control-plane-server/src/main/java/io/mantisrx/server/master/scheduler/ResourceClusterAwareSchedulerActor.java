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

package io.mantisrx.server.master.scheduler;

import static akka.pattern.Patterns.pipe;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.japi.pf.ReceiveBuilder;
import com.netflix.spectator.api.Tag;
import io.mantisrx.common.Ack;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.Timer;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.server.worker.TaskExecutorGateway.TaskNotFoundException;
import io.mantisrx.shaded.com.google.common.base.Throwables;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.ExceptionUtils;

@Slf4j
class ResourceClusterAwareSchedulerActor extends AbstractActorWithTimers {

    private final ResourceCluster resourceCluster;
    private final ExecuteStageRequestFactory executeStageRequestFactory;
    private final JobMessageRouter jobMessageRouter;
    private final int maxScheduleRetries;
    private final int maxCancelRetries;
    private final Duration intervalBetweenRetries;
    private final Timer schedulingLatency;
    private final Counter schedulingFailures;
    private final Counter batchSchedulingFailures;
    private final Counter connectionFailures;

    public static Props props(
        int maxScheduleRetries,
        int maxCancelRetries,
        Duration intervalBetweenRetries,
        final ResourceCluster resourceCluster,
        final ExecuteStageRequestFactory executeStageRequestFactory,
        final JobMessageRouter jobMessageRouter,
        final MetricsRegistry metricsRegistry) {
        return Props.create(ResourceClusterAwareSchedulerActor.class, maxScheduleRetries, maxCancelRetries, intervalBetweenRetries, resourceCluster, executeStageRequestFactory,
            jobMessageRouter, metricsRegistry);
    }

    public ResourceClusterAwareSchedulerActor(
        int maxScheduleRetries,
        int maxCancelRetries,
        Duration intervalBetweenRetries,
        ResourceCluster resourceCluster,
        ExecuteStageRequestFactory executeStageRequestFactory,
        JobMessageRouter jobMessageRouter,
        MetricsRegistry metricsRegistry) {
        this.resourceCluster = resourceCluster;
        this.executeStageRequestFactory = executeStageRequestFactory;
        this.jobMessageRouter = jobMessageRouter;
        this.maxScheduleRetries = maxScheduleRetries;
        this.intervalBetweenRetries = intervalBetweenRetries;
        this.maxCancelRetries = maxCancelRetries;
        final String metricsGroup = "ResourceClusterAwareSchedulerActor";
        final Metrics metrics =
            new Metrics.Builder()
                .id(metricsGroup, Tag.of("resourceCluster", resourceCluster.getName()))
                .addTimer("schedulingLatency")
                .addCounter("batchSchedulingFailures")
                .addCounter("schedulingFailures")
                .addCounter("connectionFailures")
                .build();
        metricsRegistry.registerAndGet(metrics);
        this.schedulingLatency = metrics.getTimer("schedulingLatency");
        this.schedulingFailures = metrics.getCounter("schedulingFailures");
        this.batchSchedulingFailures = metrics.getCounter("batchSchedulingFailures");
        this.connectionFailures = metrics.getCounter("connectionFailures");

    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
            // batch schedule requests
            .match(BatchScheduleRequestEvent.class, this::onBatchScheduleRequestEvent)
            .match(AssignedBatchScheduleRequestEvent.class, this::onAssignedBatchScheduleRequestEvent)
            .match(FailedToBatchScheduleRequestEvent.class, this::onFailedToBatchScheduleRequestEvent)
            .match(CancelBatchRequestEvent.class, this::onCancelBatchRequestEvent)

            // single schedule request
            .match(ScheduleRequestEvent.class, this::onScheduleRequestEvent)
            .match(InitializeRunningWorkerRequestEvent.class, this::onInitializeRunningWorkerRequest)
            .match(CancelRequestEvent.class, this::onCancelRequestEvent)
            .match(AssignedScheduleRequestEvent.class, this::onAssignedScheduleRequestEvent)
            .match(FailedToScheduleRequestEvent.class, this::onFailedScheduleRequestEvent)
            .match(SubmittedScheduleRequestEvent.class, this::onSubmittedScheduleRequestEvent)
            .match(FailedToSubmitScheduleRequestEvent.class, this::onFailedToSubmitScheduleRequestEvent)
            .match(RetryCancelRequestEvent.class, this::onRetryCancelRequestEvent)
            .match(Noop.class, this::onNoop)
            .match(Ack.class, ack -> log.debug("Received ack from {}", sender()))
            .match(Failure.class, failure -> log.error("Received failure from {}: {}", sender(), failure))
            .build();
    }

    private void onBatchScheduleRequestEvent(BatchScheduleRequestEvent event) {
        log.info("Received batch schedule request event: {}", event);

        // If the size of the batch request is 1 we'll handle it as the "old" schedule request
        if (event.getRequest().getScheduleRequests().size() == 1) {
            final ScheduleRequestEvent scheduleRequestEvent = ScheduleRequestEvent.of(event);
            self().tell(scheduleRequestEvent, self());
        } else {
            if (event.isRetry()) {
                log.info("Retrying Batch Schedule Request {}, attempt {}", event.getRequest(),
                    event.getAttempt());
            }

            CompletableFuture<Object> assignedFuture =
                resourceCluster
                    .getTaskExecutorsFor(event.getTaskExecutorAllocationRequests())
                    .<Object>thenApply(event::onAssignment)
                    .exceptionally(event::onFailure);

            pipe(assignedFuture, getContext().getDispatcher()).to(self());
        }
    }

    private void onAssignedBatchScheduleRequestEvent(AssignedBatchScheduleRequestEvent event) {
        // Each and every allocation of a batch schedule request will be handled separately (ie. assignment, retry etc etc)
        event
                .getAllocations()
                .forEach((key, value) -> {
                    final ScheduleRequest scheduleRequest = event.getScheduleRequestEvent().getAllocationRequestScheduleRequestMap().get(key);
                    final ScheduleRequestEvent scheduleRequestEvent = ScheduleRequestEvent.of(scheduleRequest, event.getScheduleRequestEvent().eventTime);
                    final AssignedScheduleRequestEvent assignedScheduleRequestEvent = new AssignedScheduleRequestEvent(scheduleRequestEvent, value);

                    self().tell(assignedScheduleRequestEvent, self());
                });
    }

    private void onFailedToBatchScheduleRequestEvent(FailedToBatchScheduleRequestEvent event) {
        batchSchedulingFailures.increment();
        Duration timeout = Duration.ofMillis(intervalBetweenRetries.toMillis());
        log.warn("BatchScheduleRequest failed to allocate resource: {}; Retrying in {} because of ",
            event.getScheduleRequestEvent(), timeout, event.getThrowable());

        getTimers().startSingleTimer(
            getBatchSchedulingQueueKeyFor(event.getScheduleRequestEvent().getJobId()),
            event.onRetry(),
            timeout);
    }

    private void onScheduleRequestEvent(ScheduleRequestEvent event) {
        if (event.isRetry()) {
            log.info("Retrying Schedule Request {}, attempt {}", event.getRequest(),
                event.getAttempt());
        }

        CompletableFuture<Object> assignedFuture =
            resourceCluster
                .getTaskExecutorsFor(
                    Collections.singleton(TaskExecutorAllocationRequest.of(
                        event.getRequest().getWorkerId(), event.getRequest().getSchedulingConstraints(), event.getRequest().getJobMetadata(), event.getRequest().getStageNum())))
                .<Object>thenApply(allocation -> event.onAssignment(allocation.values().stream().findFirst().get()))
                .exceptionally(event::onFailure);

        pipe(assignedFuture, getContext().getDispatcher()).to(self());
    }

    private void onInitializeRunningWorkerRequest(InitializeRunningWorkerRequestEvent request) {
        resourceCluster.initializeTaskExecutor(
            request.getTaskExecutorID(),
            request.getScheduleRequest().getWorkerId());
    }

    private void onAssignedScheduleRequestEvent(AssignedScheduleRequestEvent event) {
        try {
            CompletableFuture<TaskExecutorGateway> gatewayFut = resourceCluster.getTaskExecutorGateway(event.getTaskExecutorID());
            TaskExecutorRegistration info = resourceCluster.getTaskExecutorInfo(event.getTaskExecutorID()).join();

            if (gatewayFut != null && info != null) {
                CompletionStage<Object> ackFuture =
                    gatewayFut
                        .thenComposeAsync(gateway ->
                            gateway
                                .submitTask(
                                    executeStageRequestFactory.of(
                                        event.getScheduleRequestEvent().getRequest(),
                                        info))
                                .<Object>thenApply(
                                    dontCare -> new SubmittedScheduleRequestEvent(
                                        event.getScheduleRequestEvent(),
                                        event.getTaskExecutorID()))
                                .exceptionally(
                                    throwable ->
                                        new FailedToSubmitScheduleRequestEvent(
                                            event.getScheduleRequestEvent(),
                                            event.getTaskExecutorID(),
                                            ExceptionUtils.stripCompletionException(throwable))
                                    )
                                .whenCompleteAsync((res, err) ->
                                {
                                    if (err == null) {
                                        log.debug("[Submit Task] finish with {}", res);
                                    }
                                    else {
                                        log.error("[Submit Task] fail: {}", event.getTaskExecutorID(), err);
                                    }
                                })

                        )
                        .exceptionally(
                            // Note: throwable is the wrapped completable error (inside is akka rpc actor selection
                            // error).
                            // On this error, we want to:
                            // 1) trigger rpc service reconnection (to fix the missing action).
                            // 2) re-schedule worker node with delay (to avoid a fast loop to exhaust idle TE pool).
                            throwable ->
                                event.getScheduleRequestEvent().onFailure(throwable)
                        );
                pipe(ackFuture, getContext().getDispatcher()).to(self());
            }
        } catch (Exception e) {
            // we are not able to get the gateway, which either means the node is not great or some transient network issue
            // we will retry the request
            log.warn(
                "Failed to submit task with the task executor {}; Resubmitting the request",
                event.getTaskExecutorID(), e);
            self().tell(event.getScheduleRequestEvent().onFailure(e), self());
        }
    }

    private void onFailedScheduleRequestEvent(FailedToScheduleRequestEvent event) {
        schedulingFailures.increment();
        if (event.getAttempt() >= this.maxScheduleRetries) {
            log.error("Failed to submit the request {} because of ", event.getScheduleRequestEvent(), event.getThrowable());
        } else {
            // honor the readyAt attribute from schedule request's rate limiter.
            Duration timeout = Duration.ofMillis(
                Math.max(
                    event.getScheduleRequestEvent().getRequest().getReadyAt() - Instant.now().toEpochMilli(),
                    intervalBetweenRetries.toMillis()));
            log.error("Failed to submit the request {}; Retrying in {} because of ",
                event.getScheduleRequestEvent(), timeout, event.getThrowable());
            getTimers().startSingleTimer(
                getSchedulingQueueKeyFor(event.getScheduleRequestEvent().getRequest().getWorkerId()),
                event.onRetry(),
                timeout);
        }
    }

    private void onSubmittedScheduleRequestEvent(SubmittedScheduleRequestEvent event) {
        log.debug("[Submit Task]: receive SubmittedScheduleRequestEvent: {}", event);
        final TaskExecutorID taskExecutorID = event.getTaskExecutorID();
        try {
            final TaskExecutorRegistration info = resourceCluster.getTaskExecutorInfo(taskExecutorID)
                .join();
            boolean success = //todo this return state is not wired to actual processing
                jobMessageRouter.routeWorkerEvent(new WorkerLaunched(
                    event.getEvent().getRequest().getWorkerId(),
                    event.getEvent().getRequest().getStageNum(),
                    info.getHostname(),
                    taskExecutorID.getResourceId(),
                    Optional.ofNullable(info.getClusterID().getResourceID()),
                    Optional.of(info.getClusterID()),
                    info.getWorkerPorts()));
            final Duration latency =
                Duration.between(event.getEvent().getEventTime(), Clock.systemDefaultZone().instant());
            schedulingLatency.record(latency.toNanos(), TimeUnit.NANOSECONDS);

            if (!success) {
                log.error(
                    "Routing message to jobMessageRouter was never expected to fail but it has failed to event {}",
                    event);
            }
        } catch (Exception ex) {
            log.warn("Failed to route message due to error in getting TaskExecutor info: {}", taskExecutorID, ex);
            self().tell(event.onFailure(ex), self());
        }
    }

    private void onFailedToSubmitScheduleRequestEvent(FailedToSubmitScheduleRequestEvent event) {
        log.error("Failed to submit schedule request event {}", event, event.getThrowable());
        jobMessageRouter.routeWorkerEvent(new WorkerLaunchFailed(
            event.getScheduleRequestEvent().getRequest().getWorkerId(),
            event.getScheduleRequestEvent().getRequest().getStageNum(),
            Throwables.getStackTraceAsString(event.throwable)));
    }

    private void onCancelRequestEvent(CancelRequestEvent event) {
        try {
            log.info("onCancelRequestEvent {}", event);
            getTimers().cancel(getSchedulingQueueKeyFor(event.getWorkerId()));
            final TaskExecutorID taskExecutorID =
                resourceCluster.getTaskExecutorAssignedFor(event.getWorkerId()).join();

            CompletableFuture<Object> cancelFuture =
                resourceCluster.getTaskExecutorGateway(taskExecutorID)
                    .thenComposeAsync(gateway ->
                        gateway
                            .cancelTask(event.getWorkerId())
                            .<Object>thenApply(dontCare -> Noop.getInstance())
                            .exceptionally(exception -> {
                                Throwable actual =
                                    ExceptionUtils.stripCompletionException(
                                        ExceptionUtils.stripExecutionException(exception));
                                // no need to retry if the TaskExecutor does not know about the task anymore.
                                if (actual instanceof TaskNotFoundException) {
                                    return Noop.getInstance();
                                } else {
                                    return event.onFailure(actual);
                                }
                            }));

            pipe(cancelFuture, context().dispatcher()).to(self());
        } catch (Exception e) {
            Throwable throwable =
                ExceptionUtils.stripCompletionException(ExceptionUtils.stripExecutionException(e));
            if (!(throwable instanceof TaskNotFoundException)) {
                // something failed and its not TaskNotFoundException
                // which implies this is still a valid request
                self().tell(event.onFailure(throwable), self());
            } else {
                log.info("Failed to cancel task {} as no matching executor could be found", event.getWorkerId());
            }
        }
    }

    private void onRetryCancelRequestEvent(RetryCancelRequestEvent event) {
        // mark target as cancelled in resource cluster actor
        CompletableFuture<Ack> markCancelledFuture =
            this.resourceCluster.markTaskExecutorWorkerCancelled(event.getWorkerId());
        pipe(markCancelledFuture, context().dispatcher()).to(self());

        if (event.getActualEvent().getAttempt() < maxCancelRetries) {
            context().system()
                .scheduler()
                .scheduleOnce(
                    Duration.ofMinutes(1),
                    self(), // received
                    event.onRetry(), // event
                    getContext().getDispatcher(), // executor
                    self()); // sender
        } else {
            log.error("Exhausted number of retries for cancel request {}", event.getActualEvent(),
                event.getCurrentFailure());
        }
    }

    private void onCancelBatchRequestEvent(CancelBatchRequestEvent event) {
        getTimers().cancel(getBatchSchedulingQueueKeyFor(event.getJobId()));
    }

    private void onNoop(Noop event) {
    }

    @Value
    static class BatchScheduleRequestEvent {
        BatchScheduleRequest request;
        int attempt;
        @Nullable
        Throwable previousFailure;
        Instant eventTime;
        Map<TaskExecutorAllocationRequest, ScheduleRequest> allocationRequestScheduleRequestMap;

        BatchScheduleRequestEvent(BatchScheduleRequest request, int attempt, Throwable previousFailure, Instant eventTime) {
            this.request = request;
            this.attempt = attempt;
            this.previousFailure = previousFailure;
            this.eventTime = eventTime;
            this.allocationRequestScheduleRequestMap = request
                .getScheduleRequests()
                .stream()
                .map(req -> Pair.of(req, TaskExecutorAllocationRequest.of(req.getWorkerId(), req.getSchedulingConstraints(), req.getJobMetadata(), req.getStageNum())))
                .collect(Collectors.toMap(Pair::getRight, Pair::getLeft));
        }

        boolean isRetry() {
            return attempt > 1;
        }

        static BatchScheduleRequestEvent of(BatchScheduleRequest request) {
            return new BatchScheduleRequestEvent(request, 1, null, Clock.systemDefaultZone().instant());
        }

        AssignedBatchScheduleRequestEvent onAssignment(Map<TaskExecutorAllocationRequest, TaskExecutorID> allocations) {
            return new AssignedBatchScheduleRequestEvent(this, allocations);
        }

        FailedToBatchScheduleRequestEvent onFailure(Throwable throwable) {
            return new FailedToBatchScheduleRequestEvent(this, this.attempt, throwable);
        }

        Set<TaskExecutorAllocationRequest> getTaskExecutorAllocationRequests() {
            return allocationRequestScheduleRequestMap.keySet();
        }

        String getJobId() {
            return request.getScheduleRequests().get(0).getJobMetadata().getJobId();
        }
    }

    @Value
    static class ScheduleRequestEvent {

        ScheduleRequest request;
        int attempt;
        @Nullable
        Throwable previousFailure;
        Instant eventTime;

        boolean isRetry() {
            return attempt > 1;
        }

        static ScheduleRequestEvent of(BatchScheduleRequestEvent request) {
            return new ScheduleRequestEvent(request.getRequest().getScheduleRequests().get(0), 1, null, Clock.systemDefaultZone().instant());
        }

        static ScheduleRequestEvent of(ScheduleRequest request, Instant eventTime) {
            return new ScheduleRequestEvent(request, 1, null, eventTime);
        }

        FailedToScheduleRequestEvent onFailure(Throwable throwable) {
            return new FailedToScheduleRequestEvent(
                this, this.attempt, ExceptionUtils.stripCompletionException(throwable));
        }

        AssignedScheduleRequestEvent onAssignment(TaskExecutorID taskExecutorID) {
            return new AssignedScheduleRequestEvent(this, taskExecutorID);
        }
    }

    @Value
    static class InitializeRunningWorkerRequestEvent {
        ScheduleRequest scheduleRequest;
        TaskExecutorID taskExecutorID;
    }

    @Value
    private static class FailedToScheduleRequestEvent {

        ScheduleRequestEvent scheduleRequestEvent;
        int attempt;
        Throwable throwable;

        private ScheduleRequestEvent onRetry() {
            return new ScheduleRequestEvent(
                scheduleRequestEvent.getRequest(),
                attempt + 1,
                this.throwable,
                scheduleRequestEvent.getEventTime());
        }
    }

    @Value
    private static class AssignedBatchScheduleRequestEvent {

        BatchScheduleRequestEvent scheduleRequestEvent;
        Map<TaskExecutorAllocationRequest, TaskExecutorID> allocations;
    }

    @Value
    private static class FailedToBatchScheduleRequestEvent {

        BatchScheduleRequestEvent scheduleRequestEvent;
        int attempt;
        Throwable throwable;

        private BatchScheduleRequestEvent onRetry() {
            return new BatchScheduleRequestEvent(
                scheduleRequestEvent.getRequest(),
                attempt + 1,
                this.throwable,
                scheduleRequestEvent.getEventTime());
        }
    }

    @Value
    private static class AssignedScheduleRequestEvent {

        ScheduleRequestEvent scheduleRequestEvent;
        TaskExecutorID taskExecutorID;
    }

    @Value
    private static class SubmittedScheduleRequestEvent {

        ScheduleRequestEvent event;
        TaskExecutorID taskExecutorID;

        FailedToScheduleRequestEvent onFailure(Throwable throwable) {
            return new FailedToScheduleRequestEvent(
                this.event, 1, ExceptionUtils.stripCompletionException(throwable));
        }
    }

    @Value
    private static class FailedToSubmitScheduleRequestEvent {

        ScheduleRequestEvent scheduleRequestEvent;
        TaskExecutorID taskExecutorID;
        Throwable throwable;
    }

    @Value
    static class CancelRequestEvent {

        WorkerId workerId;
        int attempt;
        Throwable previousFailure;

        static CancelRequestEvent of(WorkerId workerId) {
            return new CancelRequestEvent(workerId, 1, null);
        }

        RetryCancelRequestEvent onFailure(Throwable throwable) {
            return new RetryCancelRequestEvent(this, throwable);
        }
    }

    @Value
    static class CancelBatchRequestEvent {
        String jobId;

        static CancelBatchRequestEvent of(String jobId) {
            return new CancelBatchRequestEvent(jobId);
        }
    }

    @Value
    private static class RetryCancelRequestEvent {

        CancelRequestEvent actualEvent;
        Throwable currentFailure;

        CancelRequestEvent onRetry() {
            return new CancelRequestEvent(actualEvent.getWorkerId(),
                actualEvent.getAttempt() + 1, currentFailure);
        }

        WorkerId getWorkerId() {
            return actualEvent.getWorkerId();
        }
    }

    @Value(staticConstructor = "getInstance")
    private static class Noop {

    }

    private String getSchedulingQueueKeyFor(WorkerId workerId) {
        return "Retry-Schedule-Request-For" + workerId.toString();
    }

    private String getBatchSchedulingQueueKeyFor(String jobId) {
        return "Retry-Batch-Schedule-Request-For" + jobId;
    }
}
