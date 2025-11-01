package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status;
import io.mantisrx.common.Ack;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.util.ExceptionUtils;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssignmentHandlerActor extends AbstractActorWithTimers {

    private final ClusterID clusterID;
    private final JobMessageRouter jobMessageRouter;
    private final Duration assignmentTimeout;
    private final int maxAssignmentRetries;
    private final Duration intervalBetweenRetries;

    public static Props props(
        ClusterID clusterID,
        JobMessageRouter jobMessageRouter,
        Duration assignmentTimeout
    ) {
        return props(clusterID, jobMessageRouter, assignmentTimeout, 3, Duration.ofSeconds(1));
    }

    public static Props props(
        ClusterID clusterID,
        JobMessageRouter jobMessageRouter,
        Duration assignmentTimeout,
        int maxAssignmentRetries,
        Duration intervalBetweenRetries
    ) {
        Objects.requireNonNull(clusterID, "clusterID");
        Objects.requireNonNull(jobMessageRouter, "jobMessageRouter");
        Objects.requireNonNull(assignmentTimeout, "assignmentTimeout");
        return Props.create(
            AssignmentHandlerActor.class,
            clusterID,
            jobMessageRouter,
            assignmentTimeout,
            maxAssignmentRetries,
            intervalBetweenRetries
        );
    }

    AssignmentHandlerActor(
        ClusterID clusterID,
        JobMessageRouter jobMessageRouter,
        Duration assignmentTimeout,
        int maxAssignmentRetries,
        Duration intervalBetweenRetries
    ) {
        this.clusterID = clusterID;
        this.jobMessageRouter = jobMessageRouter;
        this.assignmentTimeout = assignmentTimeout;
        this.maxAssignmentRetries = maxAssignmentRetries;
        this.intervalBetweenRetries = intervalBetweenRetries;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(TaskExecutorAssignmentRequest.class, this::onTaskExecutorAssignmentRequest)
            .match(AssignmentTimeoutEvent.class, this::onAssignmentTimeout)
            .match(TaskExecutorAssignmentSucceededEvent.class, this::onAssignmentSucceeded)
            .match(TaskExecutorAssignmentFailedEvent.class, this::onAssignmentFailed)
            .build();
    }

    private void onTaskExecutorAssignmentRequest(TaskExecutorAssignmentRequest request) {
        log.debug("Received task executor assignment request: {}", request);
        try {
            TaskExecutorRegistration registration = request.getRegistration();
            TaskExecutorAllocationRequest allocationRequest = request.getAllocationRequest();
            
            // Use the gateway future from the request
            CompletableFuture<TaskExecutorGateway> gatewayFut = request.getGatewayFuture();

            CompletionStage<Object> ackFuture =
                gatewayFut
                    .thenComposeAsync(gateway -> {
                        log.debug("Successfully obtained gateway for task executor {}", 
                            registration.getTaskExecutorID());
                        
                        // For now, just test the connection. In a full implementation,
                        // this would submit an ExecuteStageRequest to the gateway
                        return CompletableFuture.completedFuture(
                            new TaskExecutorAssignmentSucceededEvent(request));
                    })
                    .exceptionally(throwable -> {
                        log.warn("Failed to obtain gateway for task executor {}",
                            registration.getTaskExecutorID(), throwable);
                        return new TaskExecutorAssignmentFailedEvent(
                            request,
                            ExceptionUtils.stripCompletionException(throwable));
                    });

            akka.pattern.Patterns.pipe(ackFuture, getContext().getDispatcher()).to(self());

            // Start assignment timeout timer
            getTimers().startSingleTimer(
                getAssignmentTimerKeyFor(registration.getTaskExecutorID()),
                new AssignmentTimeoutEvent(request),
                assignmentTimeout
            );
        } catch (Exception e) {
            log.error("Exception during task executor assignment for {}", 
                request.getRegistration().getTaskExecutorID(), e);
            self().tell(new TaskExecutorAssignmentFailedEvent(request, e), self());
        }
    }

    private void onAssignmentTimeout(AssignmentTimeoutEvent event) {
        TaskExecutorID taskExecutorID = event.getRequest().getRegistration().getTaskExecutorID();
        log.warn("Assignment timeout for task executor {}", taskExecutorID);
        
        // Send TaskExecutorAssignmentTimeout to parent to trigger unassignment
        getContext().parent().tell(new TaskExecutorAssignmentTimeout(taskExecutorID), self());
        
        // Also trigger the failure handling
        self().tell(new TaskExecutorAssignmentFailedEvent(
            event.getRequest(), 
            new RuntimeException("Assignment timeout")), self());
    }

    private void onAssignmentSucceeded(TaskExecutorAssignmentSucceededEvent event) {
        TaskExecutorAssignmentRequest request = event.getRequest();
        TaskExecutorRegistration registration = request.getRegistration();
        
        log.info("Task executor assignment succeeded for {}", registration.getTaskExecutorID());
        
        // Cancel the timeout timer since assignment succeeded
        getTimers().cancel(getAssignmentTimerKeyFor(registration.getTaskExecutorID()));
        
        try {
            // Route worker launched event to job message router
            boolean success = jobMessageRouter.routeWorkerEvent(new WorkerLaunched(
                request.getWorkerId(),
                request.getStageNum(),
                registration.getHostname(),
                registration.getTaskExecutorID().getResourceId(),
                Optional.ofNullable(registration.getClusterID().getResourceID()),
                Optional.of(registration.getClusterID()),
                registration.getWorkerPorts()
            ));
            
            if (success) {
                log.debug("Successfully routed WorkerLaunched event for {}", request.getWorkerId());
            } else {
                log.warn("Failed to route WorkerLaunched event for {}", request.getWorkerId());
            }
            
            // Notify parent actor of successful assignment
            getContext().parent().tell(Ack.getInstance(), self());
            
        } catch (Exception e) {
            log.error("Error routing WorkerLaunched event for {}", request.getWorkerId(), e);
            getContext().parent().tell(new Status.Failure(e), self());
        }
    }

    private void onAssignmentFailed(TaskExecutorAssignmentFailedEvent event) {
        TaskExecutorAssignmentRequest request = event.getRequest();
        TaskExecutorRegistration registration = request.getRegistration();
        
        log.error("Task executor assignment failed for {} (attempt {}/{}: {})", 
            registration.getTaskExecutorID(), 
            request.getAttempt(),
            maxAssignmentRetries,
            event.getThrowable().getMessage());
        
        // Cancel the timeout timer
        getTimers().cancel(getAssignmentTimerKeyFor(registration.getTaskExecutorID()));
        
        if (request.getAttempt() >= maxAssignmentRetries) {
            log.error("Assignment failed for {} after {} attempts, giving up",
                registration.getTaskExecutorID(), maxAssignmentRetries);
            
            // Send TaskExecutorAssignmentTimeout to parent to trigger unassignment
            getContext().parent().tell(new TaskExecutorAssignmentTimeout(registration.getTaskExecutorID()), self());
            getContext().parent().tell(new Status.Failure(event.getThrowable()), self());
        } else {
            log.info("Retrying assignment for {} in {}", 
                registration.getTaskExecutorID(), intervalBetweenRetries);
            
            TaskExecutorAssignmentRequest retryRequest = request.onRetry();
            getTimers().startSingleTimer(
                getRetryTimerKeyFor(registration.getTaskExecutorID()),
                retryRequest,
                intervalBetweenRetries
            );
        }
    }

    private String getAssignmentTimerKeyFor(TaskExecutorID taskExecutorID) {
        return "Assignment-" + taskExecutorID.getResourceId();
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

        public TaskExecutorAssignmentRequest onRetry() {
            return new TaskExecutorAssignmentRequest(
                allocationRequest,
                taskExecutorID,
                registration,
                gatewayFuture,
                attempt + 1,
                previousFailure,
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
    private static class AssignmentTimeoutEvent {
        TaskExecutorAssignmentRequest request;
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
    public static class TaskExecutorAssignmentTimeout {
        TaskExecutorID taskExecutorID;
    }
}