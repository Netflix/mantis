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

import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.loader.ITask;
import io.mantisrx.runtime.loader.SinkSubscriptionStateHandler;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.metrics.MetricsFactory;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.UserCodeClassLoader;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

@Slf4j
public class Task extends AbstractIdleService implements ITask {

    private WrappedExecuteStageRequest wrappedExecuteStageRequest;

    private WorkerConfiguration config;

    private final List<Service> mantisServices = new ArrayList<>();

    private MantisMasterGateway masterMonitor;

    private UserCodeClassLoader userCodeClassLoader;

    private SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory;

    private final PublishSubject<Observable<Status>> tasksStatusSubject;

    private final PublishSubject<VirtualMachineTaskStatus> vmTaskStatusSubject = PublishSubject.create();

    // hostname from which the task is run from
    private Optional<String> hostname = Optional.empty();

    private Optional<Job> mantisJob = Optional.empty();

    private ExecuteStageRequest executeStageRequest;

    public Task() {
        this.tasksStatusSubject = PublishSubject.create();
    }

    public Task(PublishSubject<Observable<Status>> tasksStatusSubject) {
        this.tasksStatusSubject = tasksStatusSubject;
    }


    @Override
    public void initialize(WrappedExecuteStageRequest wrappedExecuteStageRequest,
                           WorkerConfiguration config,
                           MantisMasterGateway masterMonitor,
                           UserCodeClassLoader userCodeClassLoader,
                           SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory,
                           Optional<String> hostname) {
        this.wrappedExecuteStageRequest = wrappedExecuteStageRequest;
        this.executeStageRequest = wrappedExecuteStageRequest.getRequest();
        this.config = config;
        this.masterMonitor = masterMonitor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.sinkSubscriptionStateHandlerFactory = sinkSubscriptionStateHandlerFactory;
        this.hostname = hostname;
    }

    public void setJob(Optional<Job> job) {
        this.mantisJob = job;
    }

    @Override
    public void startUp() throws Exception {
        try {
            log.info("Starting current task {}", this);
            doRun();
        } catch (Exception e) {
            log.error("Failed executing the task {}", executeStageRequest, e);
            throw e;
        }
    }

    public void doRun() throws Exception {
        // shared state
        PublishSubject<WrappedExecuteStageRequest> executeStageSubject = PublishSubject.create();

        mantisServices.add(MetricsFactory.newMetricsServer(config, executeStageRequest));
        mantisServices.add(MetricsFactory.newMetricsPublisher(config, executeStageRequest));
        WorkerMetricsClient workerMetricsClient = new WorkerMetricsClient(masterMonitor);

        mantisServices.add(new ExecuteStageRequestService(
            executeStageSubject,
            tasksStatusSubject,
            new WorkerExecutionOperationsNetworkStage(
                vmTaskStatusSubject,
                masterMonitor,
                config,
                workerMetricsClient,
                sinkSubscriptionStateHandlerFactory,
                hostname),
            getJobProviderClass(),
            userCodeClassLoader,
            mantisJob));

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
        executeStageSubject.onNext(wrappedExecuteStageRequest);
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
        closeUserCodeClassLoader();
    }

    private Optional<String> getJobProviderClass() {
        return executeStageRequest.getNameOfJobProviderClass();
    }

    public Observable<Status> getStatus() {
        return tasksStatusSubject
            .flatMap((Func1<Observable<Status>, Observable<Status>>) status -> status);
    }

    public Observable<VirtualMachineTaskStatus> getVMStatus() {
        return vmTaskStatusSubject;
    }

    public WorkerId getWorkerId() {
        return executeStageRequest.getWorkerId();
    }

    private void closeUserCodeClassLoader() {
        if (userCodeClassLoader != null) {
            if (userCodeClassLoader.asClassLoader() instanceof Closeable) {
                try {
                    ((Closeable) userCodeClassLoader.asClassLoader()).close();
                } catch (IOException ex) {
                    log.error("Failed to close user class loader successfully", ex);
                }
            }
        }
    }
}
