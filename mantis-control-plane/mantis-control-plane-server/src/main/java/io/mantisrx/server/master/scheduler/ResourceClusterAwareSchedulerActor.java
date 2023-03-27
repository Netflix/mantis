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
import akka.japi.pf.ReceiveBuilder;
import com.netflix.spectator.api.Tag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.Timer;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.server.worker.TaskExecutorGateway.TaskNotFoundException;
import io.mantisrx.shaded.com.google.common.base.Throwables;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
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
                .addCounter("schedulingFailures")
                .build();
        metricsRegistry.registerAndGet(metrics);
        this.schedulingLatency = metrics.getTimer("schedulingLatency");
        this.schedulingFailures = metrics.getCounter("schedulingFailures");
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
            .match(ScheduleRequestEvent.class, this::onScheduleRequestEvent)
            .match(InitializeRunningWorkerRequestEvent.class, this::onInitializeRunningWorkerRequest)
            .match(CancelRequestEvent.class, this::onCancelRequestEvent)
            .match(AssignedScheduleRequestEvent.class, this::onAssignedScheduleRequestEvent)
            .match(FailedToScheduleRequestEvent.class, this::onFailedScheduleRequestEvent)
            .match(SubmittedScheduleRequestEvent.class, this::onSubmittedScheduleRequestEvent)
            .match(FailedToSubmitScheduleRequestEvent.class, this::onFailedToSubmitScheduleRequestEvent)
            .match(RetryCancelRequestEvent.class, this::onRetryCancelRequestEvent)
            .match(Noop.class, this::onNoop)
            .build();
    }

    private void onScheduleRequestEvent(ScheduleRequestEvent event) {
        if (event.isRetry()) {
            log.info("Retrying Schedule Request {}, attempt {}", event.getRequest(),
                event.getAttempt());
        }

        CompletableFuture<Object> assignedFuture =
            resourceCluster
                .getTaskExecutorFor(event.request.getMachineDefinition(),
                    event.request.getWorkerId())
                .<Object>thenApply(event::onAssignment)
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
            TaskExecutorGateway gateway =
                resourceCluster.getTaskExecutorGateway(event.getTaskExecutorID()).join();

            TaskExecutorRegistration info =
                resourceCluster.getTaskExecutorInfo(event.getTaskExecutorID()).join();

            CompletableFuture<Object> ackFuture =
                gateway
                    .submitTask(executeStageRequestFactory.of(event.getScheduleRequestEvent().getRequest(), info))
                    .<Object>thenApply(
                        dontCare -> new SubmittedScheduleRequestEvent(event.getScheduleRequestEvent(),
                            event.getTaskExecutorID()))
                    .exceptionally(
                        throwable -> new FailedToSubmitScheduleRequestEvent(event.getScheduleRequestEvent(),
                            event.getTaskExecutorID(), throwable));

            pipe(ackFuture, getContext().getDispatcher()).to(self());
        } catch (Exception e) {
            log.error("Failed here", e);
        }
    }

    private void onFailedScheduleRequestEvent(FailedToScheduleRequestEvent event) {
        schedulingFailures.increment();
        if (event.getAttempt() >= this.maxScheduleRetries) {
            log.error("Failed to submit the request {} because of ", event.getScheduleRequestEvent(), event.getThrowable());
        } else {
            log.error("Failed to submit the request {}; Retrying in {} because of ", event.getScheduleRequestEvent(), event.getThrowable());
            getTimers().startSingleTimer(
                getSchedulingQueueKeyFor(event.getScheduleRequestEvent().getRequest().getWorkerId()),
                event.onRetry(),
                intervalBetweenRetries);
        }
    }

    private void onSubmittedScheduleRequestEvent(SubmittedScheduleRequestEvent event) {
        final TaskExecutorID taskExecutorID = event.getTaskExecutorID();
        final TaskExecutorRegistration info = resourceCluster.getTaskExecutorInfo(taskExecutorID)
            .join();
        boolean success =
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
            final TaskExecutorGateway gateway =
                resourceCluster.getTaskExecutorGateway(taskExecutorID).join();

            CompletableFuture<Object> cancelFuture =
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
                    });

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

    private void onNoop(Noop event) {
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

        static ScheduleRequestEvent of(ScheduleRequest request) {
            return new ScheduleRequestEvent(request, 1, null, Clock.systemDefaultZone().instant());
        }

        FailedToScheduleRequestEvent onFailure(Throwable throwable) {
            return new FailedToScheduleRequestEvent(this, this.attempt, throwable);
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
    private static class AssignedScheduleRequestEvent {

        ScheduleRequestEvent scheduleRequestEvent;
        TaskExecutorID taskExecutorID;
    }

    @Value
    private static class SubmittedScheduleRequestEvent {

        ScheduleRequestEvent event;
        TaskExecutorID taskExecutorID;
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
    private static class RetryCancelRequestEvent {

        CancelRequestEvent actualEvent;
        Throwable currentFailure;

        CancelRequestEvent onRetry() {
            return new CancelRequestEvent(actualEvent.getWorkerId(),
                actualEvent.getAttempt() + 1, currentFailure);
        }
    }

    @Value(staticConstructor = "getInstance")
    private static class Noop {

    }

    private String getSchedulingQueueKeyFor(WorkerId workerId) {
        return "Retry-Schedule-Request-For" + workerId.toString();
    }
}
