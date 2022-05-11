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

import com.mantisrx.common.utils.ListenerCallQueue;
import com.mantisrx.common.utils.Services;
import com.netflix.spectator.api.Tag;
import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.netty.MantisNettyEventsListenerFactory;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.ResourceLeaderConnection;
import io.mantisrx.server.master.client.ResourceLeaderConnection.ResourceLeaderChangeListener;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.worker.SinkSubscriptionStateHandler.Factory;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractScheduledService;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.State;
import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.RxNetty;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * TaskExecutor implements the task executor gateway which provides capabilities to
 * 1). start a stage task from the mantis master
 * 2). cancel a stage task from the mantis master
 * 3). take a thread dump to see which threads are active
 */
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    @Getter
    private final TaskExecutorID taskExecutorID;
    @Getter
    private final ClusterID clusterID;
    private final WorkerConfiguration workerConfiguration;
    private final HighAvailabilityServices highAvailabilityServices;
    private final ClassLoaderHandle classLoaderHandle;
    private final SinkSubscriptionStateHandler.Factory subscriptionStateHandlerFactory;
    private final TaskExecutorRegistration taskExecutorRegistration;
    private final CompletableFuture<Void> startFuture = new CompletableFuture<>();
    private final ExecutorService ioExecutor;
    private final ListenerCallQueue<Listener> listeners = new ListenerCallQueue<>();
    private final MetricsRegistry metricsRegistry;

    // the reason the MantisMasterGateway field is not final is because we expect the HighAvailabilityServices
    // to be started before we can get the MantisMasterGateway
    private MantisMasterGateway masterMonitor;
    private ResourceLeaderConnection<ResourceClusterGateway> resourceClusterGatewaySupplier;
    private TaskStatusUpdateHandler taskStatusUpdateHandler;
    // represents the current connection to the resource manager.
    private ResourceManagerGatewayCxn currentResourceManagerCxn;
    private TaskExecutorReport currentReport;
    private Task currentTask;
    private Subscription currentTaskStatusSubscription;
    private int resourceManagerCxnIdx;
    private Throwable previousFailure;

    public TaskExecutor(
        RpcService rpcService,
        WorkerConfiguration workerConfiguration,
        HighAvailabilityServices highAvailabilityServices,
        ClassLoaderHandle classLoaderHandle,
        Factory subscriptionStateHandlerFactory, MetricsRegistry metricsRegistry) {
        super(rpcService, RpcServiceUtils.createRandomName("worker"));

        // this is the task executor ID that will be used for the rest of the JVM process
        this.taskExecutorID =
            Optional.ofNullable(workerConfiguration.getTaskExecutorId())
                .map(TaskExecutorID::of)
                .orElseGet(TaskExecutorID::generate);
        this.clusterID = ClusterID.of(workerConfiguration.getClusterId());
        this.workerConfiguration = workerConfiguration;
        this.highAvailabilityServices = highAvailabilityServices;
        this.classLoaderHandle = classLoaderHandle;
        this.subscriptionStateHandlerFactory = subscriptionStateHandlerFactory;
        this.metricsRegistry = metricsRegistry;
        WorkerPorts workerPorts = new WorkerPorts(workerConfiguration.getMetricsPort(),
            workerConfiguration.getDebugPort(), workerConfiguration.getConsolePort(),
            workerConfiguration.getCustomPort(),
            workerConfiguration.getSinkPort());
        MachineDefinition machineDefinition = new MachineDefinition(
            Hardware.getNumberCPUCores(),
            Hardware.getSizeOfPhysicalMemory(),
            Hardware.getSizeOfDisk(),
            workerPorts.getNumberOfPorts());
        String hostName = workerConfiguration.getExternalAddress();
        this.taskExecutorRegistration =
            new TaskExecutorRegistration(
                taskExecutorID, clusterID, getAddress(), hostName, workerPorts, machineDefinition);
        this.ioExecutor =
            Executors.newFixedThreadPool(
                Hardware.getNumberCPUCores(),
                new ExecutorThreadFactory("taskexecutor-io"));
        this.resourceManagerCxnIdx = 0;
    }

    @Override
    protected void onStart() {
        try {
            startTaskExecutorServices();
            startFuture.complete(null);
        } catch (Throwable throwable) {
            log.error("Fatal error occurred in starting TaskExecutor {}", getAddress(), throwable);
            startFuture.completeExceptionally(throwable);
            throw throwable;
        }
    }

    private void startTaskExecutorServices() {
        validateRunsInMainThread();

        masterMonitor = highAvailabilityServices.getMasterClientApi();
        taskStatusUpdateHandler = TaskStatusUpdateHandler.forReportingToGateway(masterMonitor);
        RxNetty.useMetricListenersFactory(new MantisNettyEventsListenerFactory());
        resourceClusterGatewaySupplier =
            highAvailabilityServices.connectWithResourceManager(clusterID);
        resourceClusterGatewaySupplier.register(new ResourceManagerChangeListener());
        establishNewResourceManagerCxnSync();
    }

    public CompletableFuture<Void> awaitRunning() {
        return startFuture;
    }

    private void establishNewResourceManagerCxnSync() {
        // check if we are running on the main thread first
        validateRunsInMainThread();
        Preconditions.checkArgument(
            this.currentResourceManagerCxn == null,
            String.format("resource manager connection already exists %s",
                this.currentResourceManagerCxn));

        ResourceManagerGatewayCxn cxn = newResourceManagerCxn();
        setResourceManagerCxn(cxn);
        cxn.startAsync().awaitRunning();
    }

    private CompletableFuture<Void> establishNewResourceManagerCxnAsync() {
        // check if we are running on the main thread first
        validateRunsInMainThread();
        if (this.currentResourceManagerCxn != null) {
            return CompletableFutures.exceptionallyCompletedFuture(
                new Exception(String.format("resource manager connection already exists %s",
                    this.currentResourceManagerCxn)));
        }

        ResourceManagerGatewayCxn cxn = newResourceManagerCxn();
        setResourceManagerCxn(cxn);
        return Services.startAsync(cxn, getIOExecutor()).handleAsync((dontCare, throwable) -> {
            if (throwable != null) {
                log.error("Failed to create a connection; Retrying", throwable);
                // let's first verify if the cxn that failed to start is still the current cxn
                if (this.currentResourceManagerCxn == cxn) {
                    this.currentResourceManagerCxn = null;
                    scheduleRunAsync(this::establishNewResourceManagerCxnAsync,
                        workerConfiguration.heartbeatInternalInMs(), TimeUnit.MILLISECONDS);
                }
                // since we have handled this issue, we don't need to propagate it further.
                return null;
            } else {
                return dontCare;
            }
        }, getMainThreadExecutor());
    }

    private CompletableFuture<Void> reestablishResourceManagerCxnAsync() {
        // check if we are running on the main thread first
        validateRunsInMainThread();
        CompletableFuture<Void> previousCxn;
        if (currentResourceManagerCxn != null) {
            previousCxn = Services.stopAsync(currentResourceManagerCxn, getIOExecutor());
        } else {
            previousCxn = CompletableFuture.completedFuture(null);
        }
        // clear the connection so that no one else will try to stop the same connection again
        TaskExecutor.this.currentResourceManagerCxn = null;

        return previousCxn
            // ignoring any closing issues for the time being
            .exceptionally(throwable -> {
                log.error("Closing the previous connection failed; Ignoring the error", throwable);
                return null;
            })
            .thenComposeAsync(dontCare -> {
                // only establish a new connection if there is none already
                // It could be the case that someone already went ahead and created a connection ahead of
                // us in which case we can resort to a no-op.
                if (this.currentResourceManagerCxn == null) {
                    return establishNewResourceManagerCxnAsync();
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }, getMainThreadExecutor());
    }

    private void setResourceManagerCxn(ResourceManagerGatewayCxn cxn) {
        // check if we are running on the main thread first since we are operating on
        // shared mutable state
        validateRunsInMainThread();
        Preconditions.checkArgument(this.currentResourceManagerCxn == null,
            "existing connection already set");
        cxn.addListener(new Service.Listener() {
            @Override
            public void failed(Service.State from, Throwable failure) {
                if (from.ordinal() == State.RUNNING.ordinal()) {
                    log.error("Connection with the resource manager failed; Retrying", failure);
                    clearResourceManagerCxn();
                    scheduleRunAsync(TaskExecutor.this::establishNewResourceManagerCxnAsync,
                        workerConfiguration.getHeartbeatInterval());
                }
            }
        }, getMainThreadExecutor());
        this.currentResourceManagerCxn = cxn;
    }

    private void clearResourceManagerCxn() {
        validateRunsInMainThread();
        TaskExecutor.this.currentResourceManagerCxn = null;
    }

    private ResourceManagerGatewayCxn newResourceManagerCxn() {
        validateRunsInMainThread();
        ResourceClusterGateway resourceManagerGateway = resourceClusterGatewaySupplier.getCurrent();

        // let's register ourselves with the resource manager
        return new ResourceManagerGatewayCxn(
            resourceManagerCxnIdx++,
            taskExecutorRegistration,
            resourceManagerGateway,
            workerConfiguration.getHeartbeatInterval(),
            workerConfiguration.getHeartbeatTimeout(),
            this::getCurrentReport,
            workerConfiguration.getTolerableConsecutiveHeartbeatFailures(),
            metricsRegistry);
    }

    private ExecutorService getIOExecutor() {
        return this.ioExecutor;
    }

    private CompletableFuture<TaskExecutorReport> getCurrentReport(Time timeout) {
        return callAsync(() -> {
            if (this.currentTask == null) {
                return TaskExecutorReport.available();
            } else {
                return TaskExecutorReport.occupied(currentTask.getWorkerId());
            }
        }, timeout);
    }

    @Slf4j
    @ToString(of = "gateway")
    @RequiredArgsConstructor
    static class ResourceManagerGatewayCxn extends AbstractScheduledService {

        private final int idx;
        private final TaskExecutorRegistration taskExecutorRegistration;
        private final ResourceClusterGateway gateway;
        private final Time heartBeatInterval;
        private final Time heartBeatTimeout;
        private final Time timeout = Time.of(1000, TimeUnit.MILLISECONDS);
        private final Function<Time, CompletableFuture<TaskExecutorReport>> currentReportSupplier;
        private final int tolerableConsecutiveHeartbeatFailures;
        private final Counter newRegistrationsTracker;
        private final Counter numSucceededRegistrations;
        private final Counter numFailedRegistrations;
        private final Gauge lastHeartbeatTracker;
        private final Gauge numFailedHeartbeatsTracker;

        private int numFailedHeartbeats = 0;

        public ResourceManagerGatewayCxn(
            int idx,
            TaskExecutorRegistration taskExecutorRegistration,
            ResourceClusterGateway gateway,
            Time heartBeatInterval,
            Time heartBeatTimeout,
            Function<Time, CompletableFuture<TaskExecutorReport>> currentReportSupplier,
            int tolerableConsecutiveHeartbeatFailures,
            MetricsRegistry metricsRegistry) {
            this.idx = idx;
            this.taskExecutorRegistration = taskExecutorRegistration;
            this.gateway = gateway;
            this.heartBeatInterval = heartBeatInterval;
            this.heartBeatTimeout = heartBeatTimeout;
            this.currentReportSupplier = currentReportSupplier;
            this.tolerableConsecutiveHeartbeatFailures = tolerableConsecutiveHeartbeatFailures;
            Metrics metrics =
                new Metrics.Builder()
                    .id("ResourceManagerGatewayCxn", Tag.of("idx", String.valueOf(idx)))
                    .addCounter("numRegistrations")
                    .addCounter("numSuccessfulRegistrations")
                    .addCounter("numFailedRegistrations")
                    .addGauge("lastSuccessfulHeartbeat")
                    .addGauge("numFailedHeartbeats")
                    .build();
            metricsRegistry.registerAndGet(metrics);

            this.newRegistrationsTracker = metrics.getCounter("numRegistrations");
            this.numSucceededRegistrations = metrics.getCounter("numSuccessfulRegistrations");
            this.numFailedRegistrations = metrics.getCounter("numFailedRegistrations");
            this.lastHeartbeatTracker = metrics.getGauge("lastSuccessfulHeartbeat");
            this.numFailedHeartbeatsTracker = metrics.getGauge("numFailedHeartbeats");
        }

        @Override
        protected String serviceName() {
            return "ResourceManagerGatewayCxn-" + idx;
        }

        @Override
        protected Scheduler scheduler() {
            return Scheduler.newFixedDelaySchedule(
                0,
                heartBeatInterval.getSize(),
                heartBeatInterval.getUnit());
        }

        @Override
        public void startUp() throws Exception {
            log.info("Trying to register with resource manager {}", gateway);
            try {
                newRegistrationsTracker.increment();
                gateway
                    .registerTaskExecutor(taskExecutorRegistration)
                    .get(heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());
                numSucceededRegistrations.increment();
            } catch (Exception e) {
                // the registration may or may not have succeeded. Since we don't know let's just
                // do the disconnection just to be safe.
                log.error("Registration to gateway {} has failed; Disconnecting now to be safe", gateway,
                    e);
                numFailedRegistrations.increment();
                try {
                    gateway.disconnectTaskExecutor(
                            new TaskExecutorDisconnection(taskExecutorRegistration.getTaskExecutorID(),
                                taskExecutorRegistration.getClusterID()))
                        .get(2 * heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());
                } catch (Exception inner) {
                    log.error("Disconnection has also failed", inner);
                }
                throw e;
            }
        }

        @Override
        public void runOneIteration() throws Exception {
            try {
                currentReportSupplier.apply(timeout)
                    .thenComposeAsync(report -> {
                        return gateway.heartBeatFromTaskExecutor(
                            new TaskExecutorHeartbeat(taskExecutorRegistration.getTaskExecutorID(),
                                taskExecutorRegistration.getClusterID(), report));
                    })
                    .get(heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());

                log.debug("Heartbeat to resource manager {} successful", gateway);
                lastHeartbeatTracker.set(Instant.now().toEpochMilli());
                // the heartbeat was successful, let's reset the counter.
                numFailedHeartbeats = 0;
            } catch (Exception e) {
                log.error("Failed to send heartbeat to gateway {}", gateway, e);
                numFailedHeartbeatsTracker.increment();
                // increase the number of failed heartbeats by 1
                numFailedHeartbeats += 1;
                if (numFailedHeartbeats > tolerableConsecutiveHeartbeatFailures) {
                    throw e;
                } else {
                    log.info("Ignoring heartbeat failure to gateway {} due to failed heartbeats {} <= {}",
                        gateway, numFailedHeartbeats, tolerableConsecutiveHeartbeatFailures);
                }
            } finally {
                numFailedHeartbeatsTracker.set(numFailedHeartbeats);
            }
        }

        @Override
        public void shutDown() throws Exception {
            gateway
                .disconnectTaskExecutor(
                    new TaskExecutorDisconnection(taskExecutorRegistration.getTaskExecutorID(),
                        taskExecutorRegistration.getClusterID()))
                .get(heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());
        }
    }

    private class ResourceManagerChangeListener implements
        ResourceLeaderChangeListener<ResourceClusterGateway> {

        @Override
        public void onResourceLeaderChanged(ResourceClusterGateway previousResourceLeader,
                                            ResourceClusterGateway newResourceLeader) {
            runAsync(TaskExecutor.this::reestablishResourceManagerCxnAsync);
        }
    }

    @VisibleForTesting
    <T> CompletableFuture<T> callInMainThread(Callable<CompletableFuture<T>> tSupplier,
                                              Time timeout) {
        return this.callAsync(tSupplier, timeout).thenCompose(t -> t);
    }

    @Override
    public CompletableFuture<Ack> submitTask(ExecuteStageRequest request) {

        log.info("Received request {} for execution", request);
        if (currentTask != null) {
            if (currentTask.getWorkerId().equals(request.getWorkerId())) {
                return CompletableFuture.completedFuture(Ack.getInstance());
            } else {
                return CompletableFutures.exceptionallyCompletedFuture(
                    new TaskAlreadyRunningException(currentTask.getWorkerId()));
            }
        }

        // todo(sundaram): rethink on how this needs to be handled better.
        // The publishsubject is today used to communicate the failure to start the request in a timely fashion.
        // Seems very convoluted.
        WrappedExecuteStageRequest wrappedRequest =
            new WrappedExecuteStageRequest(PublishSubject.create(), request);

        Task task = new Task(
            wrappedRequest,
            workerConfiguration,
            masterMonitor,
            classLoaderHandle,
            subscriptionStateHandlerFactory,
            Optional.of(getHostname()),
            Optional.empty());

        setCurrentTask(task);

        scheduleRunAsync(this::startCurrentTask, 0, TimeUnit.MILLISECONDS);
        return CompletableFuture.completedFuture(Ack.getInstance());
    }

    private void startCurrentTask() {
        validateRunsInMainThread();

        if (currentTask.state().equals(State.NEW)) {
            listeners.enqueue(getTaskStartingEvent(currentTask));
            getIOExecutor().execute(listeners::dispatch);

            CompletableFuture<Void> currentTaskSuccessfullyStartFuture =
                Services.startAsync(currentTask, getIOExecutor());

            currentTaskSuccessfullyStartFuture
                .whenCompleteAsync((dontCare, throwable) -> {
                    if (throwable != null) {
                        // okay failed to start task successfully
                        // lets stop it
                        Task task = currentTask;
                        setCurrentTask(null);
                        setPreviousFailure(throwable);
                        listeners.enqueue(getTaskFailedEvent(task, throwable));
                        getIOExecutor().execute(listeners::dispatch);
                    }
                }, getMainThreadExecutor());
        }
    }

    private void setCurrentTask(@Nullable Task task) {
        validateRunsInMainThread();

        this.currentTask = task;
        if (task == null) {
            if (currentTaskStatusSubscription != null) {
                currentTaskStatusSubscription.unsubscribe();
            }

            setStatus(TaskExecutorReport.available());
        } else {
            currentTaskStatusSubscription =
                task
                    .getStatus()
                    .observeOn(Schedulers.from(getMainThreadExecutor()))
                    .subscribe(this::updateExecutionStatus);
            setStatus(TaskExecutorReport.occupied(task.getWorkerId()));
        }
    }

    private void setPreviousFailure(Throwable throwable) {
        validateRunsInMainThread();

        this.previousFailure = throwable;
    }

    // tries to update the local state and the resource manager responsible for the task executor
    // so that it's aware of the task executor state correctly.
    // any failure to update the resource manager is not propagated upwards and instead is silently
    // ignored with just an exception log. this is okay because the resource manager anyways gets periodic
    // heartbeats which also contain the status of the task executor.
    private void setStatus(TaskExecutorReport newReport) {
        validateRunsInMainThread();

        this.currentReport = newReport;
        try {
            Preconditions.checkState(currentResourceManagerCxn != null,
                "currentResourceManagerCxn was not expected to be null");
            currentResourceManagerCxn.gateway.notifyTaskExecutorStatusChange(
                    new TaskExecutorStatusChange(taskExecutorID, clusterID, newReport))
                .whenCompleteAsync((ack, throwable) -> {
                    if (throwable != null) {
                        log.warn("Failed to update the status {}", newReport, throwable);
                    }
                }, getIOExecutor());
        } catch (Exception e) {
            log.warn("Failed to update the status {}", newReport, e);
        }
    }

    @Override
    public CompletableFuture<Ack> cancelTask(WorkerId workerId) {
        if (this.currentTask == null) {
            return CompletableFutures.exceptionallyCompletedFuture(new TaskNotFoundException(workerId));
        } else if (!this.currentTask.getWorkerId().equals(workerId)) {
            log.error("my current worker id is {} while expected worker id is {}", currentTask.getWorkerId(), workerId);
            return CompletableFutures.exceptionallyCompletedFuture(new TaskNotFoundException(workerId));
        } else {
            scheduleRunAsync(this::stopCurrentTask, 0, TimeUnit.MILLISECONDS);
            return CompletableFuture.completedFuture(Ack.getInstance());
        }
    }

    private CompletableFuture<Void> stopCurrentTask() {
        validateRunsInMainThread();
        if (this.currentTask != null) {
            try {
                if (this.currentTask.state().ordinal() <= State.RUNNING.ordinal()) {
                    listeners.enqueue(getTaskCancellingEvent(currentTask));
                    CompletableFuture<Void> stopTaskFuture =
                        Services.stopAsync(this.currentTask, getIOExecutor());

                    return stopTaskFuture
                        .whenCompleteAsync((dontCare, throwable) -> {
                            Task t = this.currentTask;
                            setCurrentTask(null);
                            if (throwable != null) {
                                setPreviousFailure(throwable);
                            }
                            listeners.enqueue(getTaskCancelledEvent(t, throwable));
                            getIOExecutor().execute(listeners::dispatch);
                        }, getMainThreadExecutor());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            } catch (Exception e) {
                log.error("stopping current task failed", e);
                return CompletableFutures.exceptionallyCompletedFuture(e);
            } finally {
                getIOExecutor().execute(listeners::dispatch);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> stopResourceManager() {
        validateRunsInMainThread();

        final CompletableFuture<Void> currentResourceManagerCxnCompletionFuture;
        if (currentResourceManagerCxn != null) {
            currentResourceManagerCxnCompletionFuture = Services.stopAsync(currentResourceManagerCxn,
                getIOExecutor());
        } else {
            currentResourceManagerCxnCompletionFuture = CompletableFuture.completedFuture(null);
        }
        return currentResourceManagerCxnCompletionFuture;
    }

    @Override
    public CompletableFuture<String> requestThreadDump() {
        return CompletableFuture.completedFuture(JvmUtils.createThreadDumpAsString());
    }

    CompletableFuture<Boolean> isRegistered(Time timeout) {
        return callAsync(() -> this.currentResourceManagerCxn != null, timeout);
    }

    protected void updateExecutionStatus(Status status) {
        taskStatusUpdateHandler.onStatusUpdate(status);
    }

    @Override
    protected CompletableFuture<Void> onStop() {
        validateRunsInMainThread();

        final CompletableFuture<Void> runningTaskCompletionFuture = stopCurrentTask();

        return runningTaskCompletionFuture
            .handleAsync((dontCare, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to stop the task successfully", throwable);
                }
                return stopResourceManager();
            }, getMainThreadExecutor())
            .thenCompose(Function.identity())
            .whenCompleteAsync((dontCare, throwable) -> {
                try {
                    classLoaderHandle.close();
                } catch (Exception e) {
                    log.error("Failed to close classloader handle correctly", e);
                }
            }, getIOExecutor());
    }

    public final void addListener(Listener listener, Executor executor) {
        synchronized (listeners) {
            listeners.addListener(listener, executor);
        }
    }

    /**
     * Listener interface that allows one to listen on various events happening in the TaskExecutor.
     *
     * Some of these events include:
     *   -> when a task has started to be executed by the TaskExecutor
     *   -> when a task has failed to be started
     *   -> when a task is currently being cancelled
     *   -> when a cancellation is complete
     */
    public interface Listener {
        void onTaskStarting(Task task);

        void onTaskFailed(Task task, Throwable throwable);

        void onTaskCancelling(Task task);

        void onTaskCancelled(Task task, @Nullable Throwable throwable);

        static Listener noop() {
            return new Listener() {
                @Override
                public void onTaskStarting(Task task) {
                }

                @Override
                public void onTaskFailed(Task task, Throwable throwable) {
                }

                @Override
                public void onTaskCancelling(Task task) {
                }

                @Override
                public void onTaskCancelled(Task task, @Nullable Throwable throwable) {
                }
            };
        }
    }

    private static ListenerCallQueue.Event<Listener> getTaskStartingEvent(Task task) {
        return new ListenerCallQueue.Event<Listener>() {
            @Override
            public void call(Listener listener) {
                listener.onTaskStarting(task);
            }

            @Override
            public String toString() {
                return "starting()";
            }
        };
    }

    private static ListenerCallQueue.Event<Listener> getTaskCancellingEvent(Task task) {
        return new ListenerCallQueue.Event<Listener>() {
            @Override
            public void call(Listener listener) {
                listener.onTaskCancelling(task);
            }

            @Override
            public String toString() {
                return "cancelling()";
            }
        };
    }

    private static ListenerCallQueue.Event<Listener> getTaskFailedEvent(Task task, Throwable throwable) {
        return new ListenerCallQueue.Event<Listener>() {
            @Override
            public void call(Listener listener) {
                listener.onTaskFailed(task, throwable);
            }

            @Override
            public String toString() {
                return "failed()";
            }
        };
    }

    private static ListenerCallQueue.Event<Listener> getTaskCancelledEvent(Task task, @Nullable Throwable throwable) {
        return new ListenerCallQueue.Event<Listener>() {
            @Override
            public void call(Listener listener) {
                listener.onTaskCancelled(task, throwable);
            }

            @Override
            public String toString() {
                return "cancelled()";
            }
        };
    }
}
