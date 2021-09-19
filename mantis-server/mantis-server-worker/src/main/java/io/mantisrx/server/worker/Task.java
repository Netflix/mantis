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

import io.mantisrx.common.metrics.netty.MantisNettyEventsListenerFactory;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.metrics.MetricsFactory;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.RxNetty;
import rx.Observable;
import rx.subjects.PublishSubject;

@Slf4j
@RequiredArgsConstructor
public class Task implements Runnable, TaskPayload {

  @Delegate(types = TaskPayload.class)
  private final ExecuteStageRequest executeStageRequest;

  private final WorkerConfiguration config;

  private final List<Service> mantisServices;

  private final MasterDescription initialMasterDescription;

  /**
   * The classpaths used by this task.
   */
  private final Collection<URL> requiredClasspaths;

  private final ClassLoaderHandle classLoaderHandle;

  private UserCodeClassLoader userCodeClassLoader;

  @Override
  public void run() {
    try {
      doRun();
    } catch (Exception e) {
      log.error("Failed executing the task {}", executeStageRequest.getExecutionAttemptID());
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
    WorkerMetricsClient workerMetricsClient = new WorkerMetricsClient(config);

    // services split out by local/non-local mode
    if (config.isLocalMode()) {
            /* To run MantisWorker locally in IDE, use VirualMachineWorkerServiceLocalImpl instead
            WorkerTopologyInfo.Data workerData = new WorkerTopologyInfo.Data(data.getJobName(), data.getJobId(),
                data.getWorkerIndex(), data.getWorkerNumber(), data.getStageNumber(), data.getNumStages(), -1, -1, data.getMetricsPort());
            mantisServices.add(new VirtualMachineWorkerServiceLocalImpl(workerData, executeStageSubject, vmTaskStatusSubject));
            */
      mantisServices.add(
          new ReportStatusServiceHttpImpl(new LocalMasterMonitor(initialMasterDescription),
              tasksStatusSubject));
      mantisServices.add(new ExecuteStageRequestService(
          executeStageSubject,
          tasksStatusSubject,
          new WorkerExecutionOperationsNetworkStage(
              vmTaskStatusSubject,
              new MantisMasterClientApi(new LocalMasterMonitor(initialMasterDescription)),
              config,
              workerMetricsClient),
          getJobProviderClass(), classLoaderHandle, requiredClasspaths, null));
    } else {
      CuratorService curatorService = new CuratorService(config, initialMasterDescription);
      curatorService.start();
      MasterMonitor masterMonitor = curatorService.getMasterMonitor();
      mantisServices.add(new ReportStatusServiceHttpImpl(masterMonitor, tasksStatusSubject));
      mantisServices.add(new ExecuteStageRequestService(
          executeStageSubject,
          tasksStatusSubject,
          new WorkerExecutionOperationsNetworkStage(vmTaskStatusSubject,
              new MantisMasterClientApi(masterMonitor), config, workerMetricsClient),
          getJobProviderClass(), classLoaderHandle, requiredClasspaths, null));
    }

    // first of all, get a user-code classloader
    // this may involve downloading the job's JAR files and/or classes
    log.info("Loading JAR files for task {}.", this);
    DownloadJob.run(executeStageRequest.getJobJarUrl(), executeStageRequest.getJobName(), "");

    log.info("Starting Mantis Worker for task {}", this);
    RxNetty.useMetricListenersFactory(new MantisNettyEventsListenerFactory());
    for (Service service : mantisServices) {
      log.info("Starting service: " + service.getClass().getName());
      try {
        service.start();
      } catch (Throwable e) {
        log.error(String.format("Failed to start service %s: %s", service, e.getMessage()), e);
        throw e;
      }
    }
  }

  public void cancelExecution() {
    log.info("Attempting to cancel task {} ({}/{}).", getExecutionAttemptID(), getWorkerIndex(),
        executeStageRequest.getTotalNumWorkers());
  }

  private Optional<String> getJobProviderClass() {
    return executeStageRequest.getNameOfJobProviderClass();
  }
}
