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
package io.mantisrx.server.worker;

import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.metrics.MetricsFactory;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.subjects.PublishSubject;

@Slf4j
@RequiredArgsConstructor
public class Task extends AbstractIdleService implements TaskPayload {

  @Delegate(types = TaskPayload.class)
  private final ExecuteStageRequest executeStageRequest;

  private final WorkerConfiguration config;

  private final List<Service> mantisServices = new ArrayList<>();

  private final MantisMasterGateway masterMonitor;

  /**
   * The classpaths used by this task.
   */
  private final Collection<URL> requiredClasspaths;

  private final ClassLoaderHandle classLoaderHandle;

  private final Function<Status, CompletableFuture<Ack>> updateTaskExecutionStatusFunction;

  private final SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory;

  @Override
  public void startUp() {
    try {
      doRun();
    } catch (Exception e) {
      log.error("Failed executing the task {}", executeStageRequest.getExecutionAttemptID(), e);
    }
  }

  public void doRun() throws Exception {
    // shared state
    PublishSubject<WrappedExecuteStageRequest> executeStageSubject = PublishSubject.create();
    // the wrapped observable tracks the status of the ExecuteStageRequest that was passed in the previous
    // observable.
    PublishSubject<Observable<Status>> tasksStatusSubject = PublishSubject.create();
    PublishSubject<VirtualMachineTaskStatus> vmTaskStatusSubject = PublishSubject.create();

    mantisServices.add(MetricsFactory.newMetricsServer(config, executeStageRequest));
    mantisServices.add(MetricsFactory.newMetricsPublisher(config, executeStageRequest));
    WorkerMetricsClient workerMetricsClient = new WorkerMetricsClient(masterMonitor);

    mantisServices.add(
        new BaseReportStatusService(tasksStatusSubject) {
          @Override
          public CompletableFuture<Ack> updateTaskExecutionStatus(Status status) {
            return updateTaskExecutionStatusFunction.apply(status);
          }
        });
    mantisServices.add(new ExecuteStageRequestService(
        executeStageSubject,
        tasksStatusSubject,
        new WorkerExecutionOperationsNetworkStage(
            vmTaskStatusSubject,
            masterMonitor,
            config,
            workerMetricsClient, sinkSubscriptionStateHandlerFactory),
        getJobProviderClass(), classLoaderHandle, requiredClasspaths, null));

    // first of all, get a user-code classloader
    // this may involve downloading the job's JAR files and/or classes
//    log.info("Loading JAR files for task {}.", this);
//    DownloadJob.run(executeStageRequest.getJobJarUrl(), executeStageRequest.getJobName(), "");

    log.info("Starting Mantis Worker for task {}", this);
    for (Service service : mantisServices) {
      log.info("Starting service: " + service.getClass().getName());
      try {
        service.start();
      } catch (Throwable e) {
        log.error(String.format("Failed to start service %s: %s", service, e.getMessage()), e);
        throw e;
      }
    }

    // now that all the services have been started, let's submit the request
    executeStageSubject.onNext(new WrappedExecuteStageRequest(PublishSubject.create(), executeStageRequest));
  }

  @Override
  public void shutDown() {
    log.info("Attempting to cancel task {}", this);
    for (Service service : mantisServices) {
      log.info("Stopping service: " + service.getClass().getName());
      try {
        service.shutdown();
      } catch (Throwable e) {
        log.error(String.format("Failed to stop service %s: %s", service, e.getMessage()), e);
        throw e;
      }
    }
  }

  private Optional<String> getJobProviderClass() {
    return executeStageRequest.getNameOfJobProviderClass();
  }
}
