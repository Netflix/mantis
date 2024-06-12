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

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.loader.RuntimeTask;
import io.mantisrx.runtime.loader.SinkSubscriptionStateHandler;
import io.mantisrx.runtime.loader.config.MetricsCollector;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.runtime.loader.config.WorkerConfigurationUtils;
import io.mantisrx.runtime.loader.config.WorkerConfigurationWritable;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import io.mantisrx.server.core.metrics.MetricsFactory;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.TaskStatusUpdateHandler;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.UserCodeClassLoader;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

@Slf4j
public class RuntimeTaskImpl extends AbstractIdleService implements RuntimeTask {

    private WrappedExecuteStageRequest wrappedExecuteStageRequest;

    private WorkerConfiguration config;

    private final List<Service> mantisServices = new ArrayList<>();

    // HA service instance on TaskExecutor path.
    private HighAvailabilityServices highAvailabilityServices;

    private TaskStatusUpdateHandler taskStatusUpdateHandler;

    private MantisMasterGateway masterMonitor;

    private UserCodeClassLoader userCodeClassLoader;

    private SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory;

    private final PublishSubject<Observable<Status>> tasksStatusSubject;

    private Optional<Job> mantisJob = Optional.empty();

    private ExecuteStageRequest executeStageRequest;

    public RuntimeTaskImpl() {
        this.tasksStatusSubject = PublishSubject.create();
    }

    public RuntimeTaskImpl(PublishSubject<Observable<Status>> tasksStatusSubject) {
        this.tasksStatusSubject = tasksStatusSubject;
    }

    @Override
    public void initialize(
        String executeStageRequestString, // request string + publishSubject replace?
        String workerConfigurationString, // config string
        UserCodeClassLoader userCodeClassLoader) {

        try {
            log.info("Creating runtimeTaskImpl.");
            log.info("runtimeTaskImpl workerConfigurationString: {}", workerConfigurationString);
            log.info("runtimeTaskImpl executeStageRequestString: {}", executeStageRequestString);
            JsonSerializer ser = new JsonSerializer();
            WorkerConfigurationWritable configWritable =
                WorkerConfigurationUtils.stringToWorkerConfiguration(workerConfigurationString);
            this.config = configWritable;
            ExecuteStageRequest executeStageRequest =
                ser.fromJSON(executeStageRequestString, ExecuteStageRequest.class);
            this.wrappedExecuteStageRequest =
                new WrappedExecuteStageRequest(PublishSubject.create(), executeStageRequest);

            configWritable.setMetricsCollector(createMetricsCollector(this.config.getMetricsCollectorClassName()));
        } catch (IOException | InvocationTargetException | IllegalAccessException | ClassNotFoundException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        this.highAvailabilityServices = HighAvailabilityServicesUtil.createHAServices(config);
        this.executeStageRequest = wrappedExecuteStageRequest.getRequest();
        this.masterMonitor = this.highAvailabilityServices.getMasterClientApi();
        this.userCodeClassLoader = userCodeClassLoader;
        this.sinkSubscriptionStateHandlerFactory =
            SinkSubscriptionStateHandler.Factory.forEphemeralJobsThatNeedToBeKilledInAbsenceOfSubscriber(
                this.highAvailabilityServices.getMasterClientApi(),
                Clock.systemDefaultZone());

        // link task status to status updateHandler
        this.taskStatusUpdateHandler = TaskStatusUpdateHandler.forReportingToGateway(masterMonitor);
        this.getStatus().observeOn(Schedulers.io())
            .subscribe(status -> this.taskStatusUpdateHandler.onStatusUpdate(status));
    }

    private static MetricsCollector createMetricsCollector(String name)
        throws InvocationTargetException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException {
        Class<?> metricsCollectorClass = Class.forName(name);
        log.info("Picking {} metrics collector", metricsCollectorClass.getName());
        Method metricsCollectorFactory = metricsCollectorClass.getMethod("valueOf", Properties.class);
        return (MetricsCollector) metricsCollectorFactory.invoke(null, System.getProperties());
    }

    /**
     * Initialize path used by Mesos driver. This is not part of the RuntimeTask interface but only invoked directly
     * via mesos startup.
     */
    protected void initialize(
        WrappedExecuteStageRequest wrappedExecuteStageRequest,
        WorkerConfiguration config,
        MantisMasterGateway masterMonitor,
        UserCodeClassLoader userCodeClassLoader,
        SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory) {

        log.info("initialize RuntimeTaskImpl on injected ExecuteStageRequest: {}",
            wrappedExecuteStageRequest.getRequest());
        this.wrappedExecuteStageRequest = wrappedExecuteStageRequest;
        this.executeStageRequest = wrappedExecuteStageRequest.getRequest();
        this.config = config;
        this.masterMonitor = masterMonitor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.sinkSubscriptionStateHandlerFactory = sinkSubscriptionStateHandlerFactory;

        // link task status to status updateHandler
        this.taskStatusUpdateHandler = TaskStatusUpdateHandler.forReportingToGateway(masterMonitor);
        this.getStatus().observeOn(Schedulers.io())
            .subscribe(status -> this.taskStatusUpdateHandler.onStatusUpdate(status));
    }

    public void setJob(Optional<Job> job) {
        this.mantisJob = job;
    }

    @Override
    protected void startUp() throws Exception {
        try {
            log.info("Starting current task {}", this);
            if (this.highAvailabilityServices != null && !this.highAvailabilityServices.isRunning()) {
                this.highAvailabilityServices.startAsync().awaitRunning();
            }
            doRun();
        } catch (Exception e) {
            log.error("Failed executing the task {}", executeStageRequest, e);
            throw e;
        }
    }

    private void doRun() throws Exception {
        // shared state
        PublishSubject<WrappedExecuteStageRequest> executeStageSubject = PublishSubject.create();

        mantisServices.add(MetricsFactory.newMetricsServer(config, executeStageRequest));

        // [TODO:andyz] disable noOp publisher for now. Need to fix the full publisher injection.
        // mantisServices.add(MetricsFactory.newMetricsPublisher(config, executeStageRequest));
        WorkerMetricsClient workerMetricsClient = new WorkerMetricsClient(masterMonitor);

        mantisServices.add(new ExecuteStageRequestService(
            executeStageSubject,
            tasksStatusSubject,
            new WorkerExecutionOperationsNetworkStage(
                masterMonitor,
                config,
                workerMetricsClient,
                sinkSubscriptionStateHandlerFactory,
                userCodeClassLoader.asClassLoader()),
            getJobProviderClass(),
            userCodeClassLoader,
            mantisJob));

        log.info("Starting Mantis Worker for task {}", this);
        for (Service service : mantisServices) {
            log.info("Starting service: " + service.getClass().getName());
            try {
                service.start();
            } catch (Throwable e) {
                log.error("Failed to start service {}", service, e);
                throw e;
            }
        }

        // now that all the services have been started, let's submit the request
        executeStageSubject.onNext(wrappedExecuteStageRequest);
    }

    @Override
    protected void shutDown() {
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

    protected Observable<Status> getStatus() {
        return tasksStatusSubject
            .flatMap((Func1<Observable<Status>, Observable<Status>>) status -> status);
    }

    public String getWorkerId() {
        return executeStageRequest.getWorkerId().getId();
    }
}
