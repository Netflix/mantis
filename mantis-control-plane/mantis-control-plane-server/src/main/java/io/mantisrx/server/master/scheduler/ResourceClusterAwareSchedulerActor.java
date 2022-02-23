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

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.ExecuteStageRequestUtils;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.base.Throwables;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ResourceClusterAwareSchedulerActor extends AbstractActor {

  private final ResourceCluster resourceCluster;
  private final ExecuteStageRequestUtils executeStageRequestUtils;
  private final JobMessageRouter jobMessageRouter;
  private final int maxCancelRetries;

  public static Props props(
      final ResourceCluster resourceCluster,
      final ExecuteStageRequestUtils executeStageRequestUtils,
      final JobMessageRouter jobMessageRouter) {
    return Props.create(ResourceClusterAwareSchedulerActor.class, resourceCluster, executeStageRequestUtils,
        jobMessageRouter);
  }

  public ResourceClusterAwareSchedulerActor(ResourceCluster resourceCluster,
      ExecuteStageRequestUtils executeStageRequestUtils,
      JobMessageRouter jobMessageRouter) {
    this.resourceCluster = resourceCluster;
    this.executeStageRequestUtils = executeStageRequestUtils;
    this.jobMessageRouter = jobMessageRouter;
    this.maxCancelRetries = 3;
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
        .match(ScheduleRequestEvent.class, this::onScheduleRequestEvent)
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
      log.info("Retrying Schedule Request {}, attempt {}", event.getScheduleRequest(),
          event.getAttempt());
    }

    CompletableFuture<Object> assignedFuture =
        resourceCluster
            .getTaskExecutorFor(event.scheduleRequest.getMachineDefinition(),
                event.scheduleRequest.getWorkerId())
            .<Object>thenApply(
                taskExecutorID1 -> new AssignedScheduleRequestEvent(event.getScheduleRequest(),
                    taskExecutorID1))
            .exceptionally(event::onFailure);

    pipe(assignedFuture, getContext().getDispatcher()).to(self());
  }

  private void onAssignedScheduleRequestEvent(AssignedScheduleRequestEvent event) {
    try {
      TaskExecutorGateway gateway =
          resourceCluster.getTaskExecutorGateway(event.getTaskExecutorID()).join();

      TaskExecutorRegistration info =
          resourceCluster.getTaskExecutorInfo(event.getTaskExecutorID()).join();

      CompletableFuture<Object> ackFuture =
          gateway
              .submitTask(executeStageRequestUtils.of(event.getScheduleRequest(), info))
              .<Object>thenApply(
                  dontCare -> new SubmittedScheduleRequestEvent(event.getScheduleRequest(),
                      event.getTaskExecutorID()))
              .exceptionally(
                  throwable -> new FailedToSubmitScheduleRequestEvent(event.getScheduleRequest(),
                      event.getTaskExecutorID(), throwable));

      pipe(ackFuture, getContext().getDispatcher()).to(self());
    } catch (Exception e) {
      log.error("Failed here", e);
    }
  }

  private void onFailedScheduleRequestEvent(FailedToScheduleRequestEvent event) {
    log.error("Failed to submit the request {}; Retrying", event.getScheduleRequest(),
        event.getThrowable());
    context()
        .system()
        .scheduler()
        .scheduleOnce(
            Duration.ofMinutes(1), // when to retry
            self(), // receiver
            event.onRetry(), // event to send
            context().dispatcher(),
            self());  // sender
  }

  private void onSubmittedScheduleRequestEvent(SubmittedScheduleRequestEvent event) {
    final TaskExecutorID taskExecutorID = event.getTaskExecutorID();
    final TaskExecutorRegistration info = resourceCluster.getTaskExecutorInfo(taskExecutorID)
        .join();
    boolean success =
        jobMessageRouter.routeWorkerEvent(new WorkerLaunched(
            event.getScheduleRequest().getWorkerId(),
            event.getScheduleRequest().getStageNum(),
            info.getTaskExecutorAddress(),
            taskExecutorID.getResourceId(),
            Optional.ofNullable(info.getClusterID().getResourceID()),
            info.getWorkerPorts()));

    if (!success) {
      log.error(
          "Routing message to jobMessageRouter was never expected to fail but it has failed to event {}",
          event);
    }
  }

  private void onFailedToSubmitScheduleRequestEvent(FailedToSubmitScheduleRequestEvent event) {
    log.error("Failed to submit schedule request event {}", event, event.getThrowable());
    jobMessageRouter.routeWorkerEvent(new WorkerLaunchFailed(event.scheduleRequest.getWorkerId(),
        event.scheduleRequest.getStageNum(),
        Throwables.getStackTraceAsString(event.throwable)));
  }

  private void onCancelRequestEvent(CancelRequestEvent event) {
    final TaskExecutorID taskExecutorID =
        resourceCluster.getTaskExecutorInfo(event.getHostName()).join().getTaskExecutorID();
    final TaskExecutorGateway gateway =
        resourceCluster.getTaskExecutorGateway(taskExecutorID).join();

    CompletableFuture<Object> cancelFuture =
        gateway
            .cancelTask(event.getWorkerId())
            .<Object>thenApply(dontCare -> Noop.getInstance())
            .exceptionally(event::onFailure);

    pipe(cancelFuture, context().dispatcher()).to(self());
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

    ScheduleRequest scheduleRequest;
    int attempt;
    @Nullable
    Throwable previousFailure;

    boolean isRetry() {
      return attempt > 1;
    }

    static ScheduleRequestEvent of(ScheduleRequest request) {
      return new ScheduleRequestEvent(request, 1, null);
    }

    FailedToScheduleRequestEvent onFailure(Throwable throwable) {
      return new FailedToScheduleRequestEvent(this.scheduleRequest, this.attempt, throwable);
    }
  }

  @Value
  private static class FailedToScheduleRequestEvent {

    ScheduleRequest scheduleRequest;
    int attempt;
    Throwable throwable;

    private ScheduleRequestEvent onRetry() {
      return new ScheduleRequestEvent(this.scheduleRequest, attempt + 1, this.throwable);
    }
  }

  @Value
  private static class AssignedScheduleRequestEvent {

    ScheduleRequest scheduleRequest;
    TaskExecutorID taskExecutorID;
  }

  @Value
  private static class SubmittedScheduleRequestEvent {

    ScheduleRequest scheduleRequest;
    TaskExecutorID taskExecutorID;
  }

  @Value
  private static class FailedToSubmitScheduleRequestEvent {

    ScheduleRequest scheduleRequest;
    TaskExecutorID taskExecutorID;
    Throwable throwable;
  }

  @Value
  static class CancelRequestEvent {

    WorkerId workerId;
    String hostName;
    int attempt;
    Throwable previousFailure;

    static CancelRequestEvent of(WorkerId workerId, String hostName) {
      return new CancelRequestEvent(workerId, hostName, 1, null);
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
      return new CancelRequestEvent(actualEvent.getWorkerId(), actualEvent.getHostName(),
          actualEvent.getAttempt() + 1, currentFailure);
    }
  }

  @Value(staticConstructor = "getInstance")
  private static class Noop {

  }
}
