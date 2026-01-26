package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.pattern.Patterns;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.util.ExceptionUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import scala.compat.java8.FutureConverters;

@Slf4j
public class AssignmentHandlerActor extends AbstractActorWithTimers {

    private final ClusterID clusterID;
    private final JobMessageRouter jobMessageRouter;
    private final Duration assignmentTimeout;
    private final int maxAssignmentRetries;
    private final Duration intervalBetweenRetries;
    private final ExecuteStageRequestFactory executeStageRequestFactory;

    public static Props props(
        ClusterID clusterID,
        JobMessageRouter jobMessageRouter,
        Duration assignmentTimeout,
        ExecuteStageRequestFactory executeStageRequestFactory
    ) {
        return props(clusterID, jobMessageRouter, assignmentTimeout, executeStageRequestFactory, 3, assignmentTimeout);
    }

    public static Props props(
        ClusterID clusterID,
        JobMessageRouter jobMessageRouter,
        Duration assignmentTimeout,
        ExecuteStageRequestFactory executeStageRequestFactory,
        int maxAssignmentRetries,
        Duration intervalBetweenRetries
    ) {
        Objects.requireNonNull(clusterID, "clusterID");
        Objects.requireNonNull(jobMessageRouter, "jobMessageRouter");
        Objects.requireNonNull(assignmentTimeout, "assignmentTimeout");
        Objects.requireNonNull(executeStageRequestFactory, "executeStageRequestFactory");
        return Props.create(
            AssignmentHandlerActor.class,
            clusterID,
            jobMessageRouter,
            assignmentTimeout,
            executeStageRequestFactory,
            maxAssignmentRetries,
            intervalBetweenRetries
        );
    }

    AssignmentHandlerActor(
        ClusterID clusterID,
        JobMessageRouter jobMessageRouter,
        Duration assignmentTimeout,
        ExecuteStageRequestFactory executeStageRequestFactory,
        int maxAssignmentRetries,
        Duration intervalBetweenRetries
    ) {
        this.clusterID = clusterID;
        this.jobMessageRouter = jobMessageRouter;
        this.assignmentTimeout = assignmentTimeout;
        this.executeStageRequestFactory = executeStageRequestFactory;
        this.maxAssignmentRetries = maxAssignmentRetries;
        this.intervalBetweenRetries = intervalBetweenRetries;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(TaskExecutorAssignmentRequest.class, this::onTaskExecutorAssignmentRequest)
            .match(TaskExecutorAssignmentSucceededEvent.class, this::onAssignmentSucceeded)
            .match(TaskExecutorAssignmentFailedEvent.class, this::onAssignmentFailed)
            .match(ScheduleRetryWithFreshGateway.class, this::onScheduleRetryWithFreshGateway)
            .build();
    }

    private void onTaskExecutorAssignmentRequest(TaskExecutorAssignmentRequest request) {
        log.info("Received task executor assignment request: {} (attempt {}/{})",
            request, request.getAttempt(), maxAssignmentRetries);
        try {
            TaskExecutorRegistration registration = request.getRegistration();
            // Use the gateway future from the request
            CompletableFuture<TaskExecutorGateway> gatewayFut = request.getGatewayFuture();

            CompletableFuture<Object> ackFuture =
                gatewayFut
                    .<Object>thenComposeAsync(gateway -> {
                        log.debug("Successfully obtained gateway for task executor {}",
                            registration.getTaskExecutorID());
                        return gateway
                            .submitTask(
                                executeStageRequestFactory.of(
                                    registration,
                                    request.getAllocationRequest()))
                            .<Object>thenApplyAsync(
                                dontCare -> {
                                    log.debug("[Submit Task] succeeded for {}", registration.getTaskExecutorID());
                                    return new TaskExecutorAssignmentSucceededEvent(request);
                                })
                            .exceptionally(
                                throwable -> {
                                    log.error("[Submit Task] failed for {}: {}",
                                        registration.getTaskExecutorID(), throwable.getMessage());
                                    return new TaskExecutorAssignmentFailedEvent(
                                        request, ExceptionUtils.stripCompletionException(throwable));
                                });
                    })
                    .exceptionally(throwable -> {
                        log.warn("Failed to obtain gateway for task executor {}",
                            registration.getTaskExecutorID(), throwable);
                        return new TaskExecutorAssignmentFailedEvent(
                            request,
                            ExceptionUtils.stripCompletionException(throwable));
                    })
                    .toCompletableFuture()
                    .orTimeout(
                        assignmentTimeout.toMillis(),
                        java.util.concurrent.TimeUnit.MILLISECONDS)
                    .exceptionally(throwable -> {
                        if (throwable instanceof java.util.concurrent.TimeoutException) {
                            log.warn("Assignment timeout for task executor {} after {}ms",
                                registration.getTaskExecutorID(), assignmentTimeout.toMillis());
                            return new TaskExecutorAssignmentFailedEvent(
                                request,
                                throwable);
                        }
                        return new TaskExecutorAssignmentFailedEvent(
                            request,
                            ExceptionUtils.stripCompletionException(throwable));
                    });

            akka.pattern.Patterns.pipe(ackFuture, getContext().getDispatcher()).to(self());
        } catch (Exception e) {
            log.error("Exception during task executor assignment for {}",
                request.getRegistration().getTaskExecutorID(), e);
            self().tell(new TaskExecutorAssignmentFailedEvent(request, e), self());
        }
    }

    private void onAssignmentSucceeded(TaskExecutorAssignmentSucceededEvent event) {
        TaskExecutorAssignmentRequest request = event.getRequest();
        TaskExecutorRegistration registration = request.getRegistration();
        log.info("Task executor assignment succeeded for {}", registration.getTaskExecutorID());
    }

    private void onAssignmentFailed(TaskExecutorAssignmentFailedEvent event) {
        TaskExecutorAssignmentRequest request = event.getRequest();
        TaskExecutorRegistration registration = request.getRegistration();

        log.warn("Task executor assignment failed for {} (attempt {}/{}: {})",
            registration.getTaskExecutorID(),
            request.getAttempt(),
            maxAssignmentRetries,
            event.getThrowable().getMessage());

        if (request.getAttempt() >= maxAssignmentRetries) {
            log.error("Assignment failed for {} after {} attempts, giving up",
                registration.getTaskExecutorID(), maxAssignmentRetries);

            // Send assignmentFailure event to parent after max retries
            getContext().parent().tell(new TaskExecutorAssignmentFailAndTerminate(
                registration.getTaskExecutorID(),
                request.getAllocationRequest(),
                event.getThrowable(),
                request.getAttempt()
            ), self());
        } else {
            log.info("Retrying assignment for {} in {} (attempt {}/{})",
                registration.getTaskExecutorID(), intervalBetweenRetries, request.getAttempt(), maxAssignmentRetries);

            // Request a fresh gateway future from the parent actor to avoid reusing a failed future
            TaskExecutorGatewayRequest gatewayRequest = new TaskExecutorGatewayRequest(
                registration.getTaskExecutorID(),
                clusterID
            );

            CompletableFuture<Object> gatewayFutureResponse = FutureConverters.toJava(
                Patterns.ask(getContext().parent(), gatewayRequest, assignmentTimeout.toMillis())
            ).toCompletableFuture();

            // Pipe the result back to self as a message to schedule the retry
            CompletableFuture<ScheduleRetryWithFreshGateway> retryScheduleFuture = gatewayFutureResponse
                .thenApply(result -> {
                    if (result instanceof CompletableFuture) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<TaskExecutorGateway> gatewayFuture =
                            (CompletableFuture<TaskExecutorGateway>) result;
                        return new ScheduleRetryWithFreshGateway(request, gatewayFuture);
                    } else if (result instanceof Throwable) {
                        log.error("Failed to get fresh gateway for retry: {}", result);
                        // Create a failed future to avoid reusing the old one
                        CompletableFuture<TaskExecutorGateway> failedFuture = new CompletableFuture<>();
                        failedFuture.completeExceptionally((Throwable) result);
                        return new ScheduleRetryWithFreshGateway(request, failedFuture);
                    } else {
                        log.error("Unexpected response type when requesting gateway: {}", result.getClass());
                        CompletableFuture<TaskExecutorGateway> failedFuture = new CompletableFuture<>();
                        failedFuture.completeExceptionally(
                            new RuntimeException("Unexpected response type: " + result.getClass()));
                        return new ScheduleRetryWithFreshGateway(request, failedFuture);
                    }
                })
                .exceptionally(throwable -> {
                    log.error("Exception while requesting fresh gateway for retry: {}", throwable.getMessage(), throwable);
                    // Fallback: create retry request with a new failed future
                    // This ensures we don't reuse the old failed future
                    CompletableFuture<TaskExecutorGateway> failedFuture = new CompletableFuture<>();
                    failedFuture.completeExceptionally(throwable);
                    return new ScheduleRetryWithFreshGateway(request, failedFuture);
                });

            Patterns.pipe(retryScheduleFuture, getContext().getDispatcher()).to(self());
        }
    }

    private void onScheduleRetryWithFreshGateway(ScheduleRetryWithFreshGateway message) {
        TaskExecutorAssignmentRequest originalRequest = message.getOriginalRequest();
        TaskExecutorRegistration registration = originalRequest.getRegistration();

        TaskExecutorAssignmentRequest retryRequest = originalRequest.onRetry(message.getFreshGatewayFuture());

        getTimers().startSingleTimer(
            getRetryTimerKeyFor(registration.getTaskExecutorID()),
            retryRequest,
            intervalBetweenRetries
        );
    }

    private String getRetryTimerKeyFor(TaskExecutorID taskExecutorID) {
        return "Retry-" + taskExecutorID.getResourceId();
    }

    // Event classes

    @Value
    public static class TaskExecutorAssignmentRequest {
        TaskExecutorAllocationRequest allocationRequest;
        TaskExecutorID taskExecutorID;
        TaskExecutorRegistration registration;
        CompletableFuture<TaskExecutorGateway> gatewayFuture;
        int attempt;

        /*
        Deprecated field.
         */
        @Deprecated
        @Nullable
        Throwable previousFailure;
        Instant requestTime;

        @JsonCreator
        public TaskExecutorAssignmentRequest(
            @JsonProperty("allocationRequest") TaskExecutorAllocationRequest allocationRequest,
            @JsonProperty("taskExecutorID") TaskExecutorID taskExecutorID,
            @JsonProperty("registration") TaskExecutorRegistration registration,
            @JsonProperty("gatewayFuture") CompletableFuture<TaskExecutorGateway> gatewayFuture,
            @JsonProperty("attempt") int attempt,
            @JsonProperty("previousFailure") @Nullable Throwable previousFailure,
            @JsonProperty("requestTime") Instant requestTime
        ) {
            this.allocationRequest = allocationRequest;
            this.taskExecutorID = taskExecutorID;
            this.registration = registration;
            this.gatewayFuture = gatewayFuture;
            this.attempt = attempt;
            this.previousFailure = previousFailure;
            this.requestTime = requestTime;
        }

        public static TaskExecutorAssignmentRequest of(
            TaskExecutorAllocationRequest allocationRequest,
            TaskExecutorID taskExecutorID,
            TaskExecutorRegistration registration,
            CompletableFuture<TaskExecutorGateway> gatewayFuture
        ) {
            return new TaskExecutorAssignmentRequest(
                allocationRequest,
                taskExecutorID,
                registration,
                gatewayFuture,
                1,
                null,
                Instant.now()
            );
        }

        /**
         * Creates a retry request with a fresh gateway future to avoid reusing a failed future.
         * This prevents race conditions where a failed gateway future from a previous attempt
         * would cause all retries to immediately fail.
         *
         * @param freshGatewayFuture A new gateway future obtained from TaskExecutorState.getGatewayAsync()
         * @return A new TaskExecutorAssignmentRequest with incremented attempt count and fresh gateway future
         */
        public TaskExecutorAssignmentRequest onRetry(CompletableFuture<TaskExecutorGateway> freshGatewayFuture) {
            return new TaskExecutorAssignmentRequest(
                allocationRequest,
                taskExecutorID,
                registration,
                freshGatewayFuture,  // Use fresh future instead of reusing the old one
                attempt + 1,
                null,
                requestTime
            );
        }

        public boolean isRetry() {
            return attempt > 1;
        }

        public io.mantisrx.server.core.domain.WorkerId getWorkerId() {
            return allocationRequest.getWorkerId();
        }

        public int getStageNum() {
            return allocationRequest.getStageNum();
        }
    }

    @Value
    private static class TaskExecutorAssignmentSucceededEvent {
        TaskExecutorAssignmentRequest request;
    }

    @Value
    private static class TaskExecutorAssignmentFailedEvent {
        TaskExecutorAssignmentRequest request;
        Throwable throwable;
    }

    @Value
    public static class TaskExecutorAssignmentFailAndTerminate {
        TaskExecutorID taskExecutorID;
        TaskExecutorAllocationRequest allocationRequest;
        Throwable throwable;
        int attemptCount;
    }

    @Value
    private static class ScheduleRetryWithFreshGateway {
        TaskExecutorAssignmentRequest originalRequest;
        CompletableFuture<TaskExecutorGateway> freshGatewayFuture;
    }
}
