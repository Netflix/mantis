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
package io.mantisrx.server.agent;

import com.mantisrx.common.utils.ListenerCallQueue;
import com.mantisrx.common.utils.Services;
import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.Ack;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.netty.MantisNettyEventsListenerFactory;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.loader.ClassLoaderHandle;
import io.mantisrx.runtime.loader.RuntimeTask;
import io.mantisrx.runtime.loader.TaskFactory;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.runtime.loader.config.WorkerConfigurationUtils;
import io.mantisrx.server.agent.utils.DurableBooleanState;
import io.mantisrx.server.core.CacheJobArtifactsRequest;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.utils.ConfigUtils;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.ResourceLeaderConnection;
import io.mantisrx.server.master.client.TaskStatusUpdateHandler;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.server.worker.client.SseWorkerConnection;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.State;
import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.RxNetty;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import rx.Subscription;
import rx.subjects.PublishSubject;

/**
 * TaskExecutor implements the task executor gateway which provides capabilities to
 * 1). start a stage task from the mantis master
 * 2). cancel a stage task from the mantis master
 * 3). take a thread dump to see which threads are active
 */
@Slf4j
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {
    private static final String LEGACY_REGISTRATION_STATE_FILE = "rmCxnState.txt";
    private static final String REGISTRATION_STATE_FILE =
        "rmCxnState-accepted-task-reservation-v1.txt";

    @Getter
    private final TaskExecutorID taskExecutorID;
    @Getter
    private final ClusterID clusterID;
    private final WorkerConfiguration workerConfiguration;
    private final MantisPropertiesLoader dynamicPropertiesLoader;
    private final LongDynamicProperty rpcCallTimeoutMsDp;
    private final HighAvailabilityServices highAvailabilityServices;
    private final ClassLoaderHandle classLoaderHandle;
    private final TaskExecutorRegistration taskExecutorRegistration;
    private final CompletableFuture<Void> startFuture = new CompletableFuture<>();
    private final ExecutorService ioExecutor;
    private final ExecutorService runtimeTaskExecutor;
    private final ListenerCallQueue<Listener> listeners = new ListenerCallQueue<>();

    // the reason the MantisMasterGateway field is not final is because we expect the HighAvailabilityServices
    // to be started before we can get the MantisMasterGateway
    private MantisMasterGateway masterMonitor;
    private ResourceLeaderConnection<ResourceClusterGateway> resourceClusterGatewaySupplier;
    private TaskStatusUpdateHandler taskStatusUpdateHandler;
    // represents the current connection to the resource manager.
    private ResourceManagerGatewayCxn currentResourceManagerCxn;
    private TaskExecutorReport currentReport;
    private Subscription currentTaskStatusSubscription;
    private int resourceManagerCxnIdx;
    private Throwable previousFailure;

    private final TaskFactory taskFactory;

    private final AtomicReference<TaskSlot> taskSlot = new AtomicReference<>();
    private volatile boolean stopping;
    private final DurableBooleanState registeredState;

    private static final class TaskSlot {
        private final ExecuteStageRequest request;
        private final WorkerId workerId;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean cleanupStarted = new AtomicBoolean();
        private final CompletableFuture<Void> terminated = new CompletableFuture<>();
        @Nullable
        private volatile RuntimeTask task;
        @Nullable
        private volatile UserCodeClassLoader userCodeClassLoader;
        private volatile boolean prepared;

        private TaskSlot(ExecuteStageRequest request) {
            this.request = request;
            this.workerId = request.getWorkerId();
        }
    }

    @VisibleForTesting
    public TaskExecutor(
        RpcService rpcService,
        WorkerConfiguration workerConfiguration,
        HighAvailabilityServices highAvailabilityServices,
        ClassLoaderHandle classLoaderHandle) {
        this(rpcService,
            workerConfiguration,
            new DefaultMantisPropertiesLoader(System.getProperties()),
            highAvailabilityServices,
            classLoaderHandle,
            null
        );
    }

    public TaskExecutor(
        RpcService rpcService,
        WorkerConfiguration workerConfiguration,
        MantisPropertiesLoader propertiesLoader,
        HighAvailabilityServices highAvailabilityServices,
        ClassLoaderHandle classLoaderHandle,
        @Nullable TaskFactory taskFactory) {
        super(rpcService, RpcServiceUtils.createRandomName("worker"));

        // this is the task executor ID that will be used for the rest of the JVM process
        this.taskExecutorID =
            Optional.ofNullable(workerConfiguration.getTaskExecutorId())
                .map(TaskExecutorID::of)
                .orElseGet(TaskExecutorID::generate);
        this.clusterID = ClusterID.of(workerConfiguration.getClusterId());
        this.workerConfiguration = workerConfiguration;
        this.dynamicPropertiesLoader = propertiesLoader;
        this.highAvailabilityServices = highAvailabilityServices;
        this.classLoaderHandle = classLoaderHandle;
        WorkerPorts workerPorts = new WorkerPorts(workerConfiguration.getMetricsPort(),
            workerConfiguration.getDebugPort(), workerConfiguration.getConsolePort(),
            workerConfiguration.getCustomPort(),
            workerConfiguration.getSinkPort());
        MachineDefinition machineDefinition = MachineDefinitionUtils.from(workerConfiguration, workerPorts);
        String hostName = workerConfiguration.getExternalAddress();

        Map<String, String> taskExecutorAttributes =
            workerConfiguration
                .getTaskExecutorAttributes()
                .entrySet()
                .stream()
                .collect(Collectors.<Map.Entry<String, String>, String, String>toMap(
                    kv -> kv.getKey().toLowerCase(),
                    kv -> kv.getValue().toLowerCase()));
        taskExecutorAttributes.put(
            TaskExecutorRegistration.ACCEPTED_TASK_RESERVATION_ATTRIBUTE,
            Boolean.TRUE.toString());

        this.taskExecutorRegistration =
            TaskExecutorRegistration.builder()
                .machineDefinition(machineDefinition)
                .taskExecutorID(taskExecutorID)
                .clusterID(clusterID)
                .hostname(hostName)
                .taskExecutorAddress(getAddress())
                .workerPorts(workerPorts)
                .taskExecutorAttributes(ImmutableMap.copyOf(taskExecutorAttributes))
                .build();
        log.info("Starting executor registration: {}", this.taskExecutorRegistration);

        this.ioExecutor =
            Executors.newCachedThreadPool(
                new ExecutorThreadFactory("taskexecutor-io"));

        this.runtimeTaskExecutor =
            Executors.newCachedThreadPool(
                new ExecutorThreadFactory("taskexecutor-runtime"));

        this.resourceManagerCxnIdx = 0;
        this.taskFactory = taskFactory == null ? new SingleTaskOnlyFactory() : taskFactory;
        File registrationStore = workerConfiguration.getRegistrationStoreDir();
        DurableBooleanState legacyRegisteredState = new DurableBooleanState(
            new File(registrationStore, LEGACY_REGISTRATION_STATE_FILE).getAbsolutePath());
        try {
            legacyRegisteredState.setState(false);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to reset legacy registration marker", e);
        }
        this.registeredState = new DurableBooleanState(
            new File(registrationStore, REGISTRATION_STATE_FILE).getAbsolutePath());
        this.rpcCallTimeoutMsDp =
            ConfigUtils.getDynamicPropertyLong("heartbeatTimeoutMs", WorkerConfiguration.class,
                workerConfiguration.heartbeatTimeoutMs(), this.dynamicPropertiesLoader);
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

        // setup netty client listeners for metrics.
        MantisNettyEventsListenerFactory mantisNettyEventsListenerFactory = new MantisNettyEventsListenerFactory();
        RxNetty.useMetricListenersFactory(mantisNettyEventsListenerFactory);
        SseWorkerConnection.useMetricListenersFactory(mantisNettyEventsListenerFactory);
        resourceClusterGatewaySupplier =
            highAvailabilityServices.connectWithResourceManager(clusterID);
        resourceClusterGatewaySupplier.register((oldGateway, newGateway) -> TaskExecutor.this.runAsync(() -> setNewResourceClusterGateway(newGateway)));
        establishNewResourceManagerCxnSync();
    }

    public CompletableFuture<Void> awaitRunning() {
        return startFuture;
    }

    private void setNewResourceClusterGateway(ResourceClusterGateway gateway) {
        // check if we are running on the main thread first
        validateRunsInMainThread();
        Preconditions.checkArgument(
            this.currentResourceManagerCxn != null,
            String.format("resource manager connection does not exist %s",
                this.currentResourceManagerCxn));

        log.info("Setting new resource cluster gateway {}", gateway);
        this.currentResourceManagerCxn.setGateway(gateway);
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

    private void setResourceManagerCxn(ResourceManagerGatewayCxn cxn) {
        // check if we are running on the main thread first since we are operating on
        // shared mutable state
        validateRunsInMainThread();
        Preconditions.checkArgument(this.currentResourceManagerCxn == null,
            "existing connection already set");
        cxn.addListener(new Service.Listener() {
            @Override
            public void failed(Service.State from, Throwable failure) {
                if (from.ordinal() == Service.State.RUNNING.ordinal()) {
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
        LongDynamicProperty heartbeatIntervalDp =
            ConfigUtils.getDynamicPropertyLong("heartbeatInternalInMs", WorkerConfiguration.class,
                workerConfiguration.heartbeatInternalInMs(), this.dynamicPropertiesLoader);

        log.info("Starting ResourceManagerGatewayCxn with interval {} from default {} and timeout {}.",
            heartbeatIntervalDp.getValue(),
            workerConfiguration.heartbeatInternalInMs(),
            this.rpcCallTimeoutMsDp.getValue());

        return new ResourceManagerGatewayCxn(
            resourceManagerCxnIdx++,
            taskExecutorRegistration,
            resourceManagerGateway,
            heartbeatIntervalDp,
            this.rpcCallTimeoutMsDp,
            this,
            workerConfiguration.getTolerableConsecutiveHeartbeatFailures(),
            workerConfiguration.heartbeatRetryInitialDelayMs(),
            workerConfiguration.heartbeatRetryMaxDelayMs(),
            workerConfiguration.registrationRetryInitialDelayMillis(),
            workerConfiguration.registrationRetryMultiplier(),
            workerConfiguration.registrationRetryRandomizationFactor(),
            workerConfiguration.registrationRetryMaxAttempts(),
            registeredState);
    }

    private ExecutorService getIOExecutor() {
        return this.ioExecutor;
    }

    private ExecutorService getRuntimeExecutor() {
        return this.runtimeTaskExecutor;
    }

    CompletableFuture<TaskExecutorReport> getCurrentReport() {
        return callAsync(() -> {
            TaskSlot slot = this.taskSlot.get();
            if (slot == null) {
                return TaskExecutorReport.available();
            } else {
                return TaskExecutorReport.occupied(slot.workerId);
            }
        }, Time.milliseconds(this.rpcCallTimeoutMsDp.getValue()));
    }

    @VisibleForTesting
    <T> CompletableFuture<T> callInMainThread(Callable<CompletableFuture<T>> tSupplier,
                                              Time timeout) {
        return this.callAsync(tSupplier, timeout).thenCompose(t -> t);
    }

    @Override
    public CompletableFuture<Ack> submitTask(ExecuteStageRequest request) {

        log.info("Received request {} for execution", request);
        TaskSlot newSlot;
        while (true) {
            TaskSlot existingSlot = taskSlot.get();
            if (existingSlot != null) {
                if (existingSlot.workerId.equals(request.getWorkerId())) {
                    return CompletableFuture.completedFuture(Ack.getInstance());
                }
                return CompletableFutures.exceptionallyCompletedFuture(
                    new TaskAlreadyRunningException(existingSlot.workerId));
            }

            newSlot = new TaskSlot(request);
            if (taskSlot.compareAndSet(null, newSlot)) {
                break;
            }
        }

        setStatus(TaskExecutorReport.occupied(newSlot.workerId));
        final TaskSlot slotToPrepare = newSlot;
        try {
            getIOExecutor().execute(() -> prepareTask(slotToPrepare));
        } catch (RuntimeException e) {
            clearTaskSlot(newSlot);
            return CompletableFutures.exceptionallyCompletedFuture(e);
        }
        return CompletableFuture.completedFuture(Ack.getInstance());
    }

    @Override
    public CompletableFuture<Ack> cacheJobArtifacts(CacheJobArtifactsRequest request) {

        log.info("Received request {} for downloading artifact", request);
        getIOExecutor().execute(() -> this.classLoaderHandle.cacheJobArtifacts(request.getArtifacts()));
        return CompletableFuture.completedFuture(Ack.getInstance());
    }

    private void prepareTask(TaskSlot slot) {
        try {
            slot.userCodeClassLoader = this.taskFactory.getUserCodeClassLoader(
                slot.request, classLoaderHandle);
            ClassLoader cl = slot.userCodeClassLoader.asClassLoader();
            // There should only be 1 task implementation provided by mantis-server-worker.
            JsonSerializer ser = new JsonSerializer();
            String executeRequest = ser.toJson(slot.request);
            String configString = ser.toJson(WorkerConfigurationUtils.toWritable(workerConfiguration));
            slot.task = this.taskFactory.getRuntimeTaskInstance(slot.request, cl);

            slot.task.initialize(
                executeRequest,
                configString,
                slot.userCodeClassLoader);

            slot.prepared = true;
            scheduleRunAsync(() -> onTaskPrepared(slot), 0, TimeUnit.MILLISECONDS);
        } catch (Throwable ex) {
            slot.prepared = true;
            log.error("Failed to submit task, request: {}", slot.request, ex);
            final Status failedStatus = new Status(slot.request.getJobId(), slot.request.getStage(), slot.request.getWorkerIndex(), slot.request.getWorkerNumber(),
                Status.TYPE.INFO, "stage " + slot.request.getStage() + " worker index=" + slot.request.getWorkerIndex() + " number=" + slot.request.getWorkerNumber() + " failed during initialization",
                MantisJobState.Failed);
            updateExecutionStatus(failedStatus);
            scheduleRunAsync(() -> onTaskPreparationFailed(slot, ex), 0, TimeUnit.MILLISECONDS);
        }
        finally {
            getIOExecutor().execute(listeners::dispatch);
        }
    }

    private void onTaskPrepared(TaskSlot slot) {
        validateRunsInMainThread();
        if (taskSlot.get() != slot || slot.cancelled.get() || stopping) {
            disposeTaskSlot(slot, true);
            return;
        }
        startTask(slot);
    }

    private void onTaskPreparationFailed(TaskSlot slot, Throwable throwable) {
        validateRunsInMainThread();
        if (!slot.cancelled.get()) {
            listeners.enqueue(getTaskFailedEvent(slot.task, throwable));
            getIOExecutor().execute(listeners::dispatch);
        }
        disposeTaskSlot(slot, slot.cancelled.get());
    }

    private void startTask(TaskSlot slot) {
        validateRunsInMainThread();
        RuntimeTask task = slot.task;
        if (task == null) {
            onTaskPreparationFailed(slot, new IllegalStateException("Prepared task was null"));
            return;
        }

        if (task.state().equals(State.NEW)) {
            listeners.enqueue(getTaskStartingEvent(task));
            getIOExecutor().execute(listeners::dispatch);

            CompletableFuture<Void> startFuture = Services.startAsync(task, getRuntimeExecutor());

            startFuture
                .whenCompleteAsync((dontCare, throwable) -> {
                    if (throwable != null) {
                        log.error("TaskExecutor failed to start", throwable);
                        setPreviousFailure(throwable);
                        listeners.enqueue(getTaskFailedEvent(task, throwable));
                        getIOExecutor().execute(listeners::dispatch);
                        disposeTaskSlot(slot, false);
                    }
                }, getMainThreadExecutor());
        }
    }

    private CompletableFuture<Void> disposeTaskSlot(TaskSlot slot, boolean cancellation) {
        validateRunsInMainThread();
        if (!slot.cleanupStarted.compareAndSet(false, true)) {
            return slot.terminated;
        }

        RuntimeTask task = slot.task;
        if (cancellation && task != null) {
            listeners.enqueue(getTaskCancellingEvent(task));
            getIOExecutor().execute(listeners::dispatch);
        }

        CompletableFuture<Void> stopFuture;
        try {
            stopFuture = task != null && task.state().ordinal() <= Service.State.RUNNING.ordinal()
                ? Services.stopAsync(task, getRuntimeExecutor())
                : CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            stopFuture = CompletableFutures.exceptionallyCompletedFuture(e);
        }

        stopFuture.whenCompleteAsync((ignored, throwable) -> {
            if (throwable != null) {
                setPreviousFailure(throwable);
            }

            getIOExecutor().execute(() -> {
                closeTaskClassLoader(slot);
                scheduleRunAsync(
                    () -> finishTaskCleanup(slot, task, cancellation, throwable),
                    0,
                    TimeUnit.MILLISECONDS);
            });
        }, getMainThreadExecutor());
        return slot.terminated;
    }

    private void finishTaskCleanup(
        TaskSlot slot,
        @Nullable RuntimeTask task,
        boolean cancellation,
        @Nullable Throwable cleanupFailure) {
        validateRunsInMainThread();
        if (cancellation && task != null) {
            listeners.enqueue(getTaskCancelledEvent(task, cleanupFailure));
            getIOExecutor().execute(listeners::dispatch);
        }
        clearTaskSlot(slot);
        if (cleanupFailure == null) {
            slot.terminated.complete(null);
        } else {
            slot.terminated.completeExceptionally(cleanupFailure);
        }
    }

    private void clearTaskSlot(TaskSlot slot) {
        if (taskSlot.compareAndSet(slot, null) && !stopping) {
            setStatus(TaskExecutorReport.available());
        }
    }

    private void closeTaskClassLoader(TaskSlot slot) {
        UserCodeClassLoader userCodeClassLoader = slot.userCodeClassLoader;
        if (userCodeClassLoader == null) {
            return;
        }
        ClassLoader classLoader = userCodeClassLoader.asClassLoader();
        if (classLoader instanceof Closeable) {
            try {
                ((Closeable) classLoader).close();
            } catch (IOException e) {
                log.warn("Failed to close classloader for {}", slot.workerId, e);
            }
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
            currentResourceManagerCxn.getGateway().notifyTaskExecutorStatusChange(
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
        log.info("TaskExecutor cancelTask requested for {}", workerId);
        TaskSlot slot = taskSlot.get();
        if (slot == null) {
            return CompletableFutures.exceptionallyCompletedFuture(new TaskNotFoundException(workerId));
        } else if (!slot.workerId.equals(workerId)) {
            log.error("my current worker id is {} while expected worker id is {}", slot.workerId, workerId);
            return CompletableFutures.exceptionallyCompletedFuture(new TaskNotFoundException(workerId));
        }

        slot.cancelled.set(true);
        scheduleRunAsync(() -> {
            if (slot.prepared) {
                disposeTaskSlot(slot, true);
            }
        }, 0, TimeUnit.MILLISECONDS);
        return CompletableFuture.completedFuture(Ack.getInstance());
    }

    private CompletableFuture<Void> stopCurrentTask() {
        log.info("TaskExecutor stopCurrentTask.");
        validateRunsInMainThread();
        TaskSlot slot = taskSlot.get();
        if (slot == null) {
            return CompletableFuture.completedFuture(null);
        }
        slot.cancelled.set(true);
        return slot.prepared ? disposeTaskSlot(slot, true) : slot.terminated;
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

    @Override
    public CompletableFuture<Boolean> isRegistered() {
        return callAsync(() -> this.currentResourceManagerCxn != null && this.currentResourceManagerCxn.isRegistered(),
            Time.milliseconds(this.rpcCallTimeoutMsDp.getValue()));
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

        log.info("TaskExecutor onStop.");
        stopping = true;
        final CompletableFuture<Void> runningTaskCompletionFuture =
            stopCurrentTask()
                .thenApply(ignored -> (Void) null)
                .orTimeout(this.rpcCallTimeoutMsDp.getValue(), TimeUnit.MILLISECONDS);

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
        void onTaskStarting(RuntimeTask task);

        void onTaskFailed(RuntimeTask task, Throwable throwable);

        void onTaskCancelling(RuntimeTask task);

        void onTaskCancelled(RuntimeTask task, @Nullable Throwable throwable);

        static Listener noop() {
            return new Listener() {
                @Override
                public void onTaskStarting(RuntimeTask task) {
                }

                @Override
                public void onTaskFailed(RuntimeTask task, Throwable throwable) {
                }

                @Override
                public void onTaskCancelling(RuntimeTask task) {
                }

                @Override
                public void onTaskCancelled(RuntimeTask task, @Nullable Throwable throwable) {
                }
            };
        }
    }

    private static ListenerCallQueue.Event<Listener> getTaskStartingEvent(RuntimeTask task) {
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

    private static ListenerCallQueue.Event<Listener> getTaskCancellingEvent(RuntimeTask task) {
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

    private static ListenerCallQueue.Event<Listener> getTaskFailedEvent(RuntimeTask task, Throwable throwable) {
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

    private static ListenerCallQueue.Event<Listener> getTaskCancelledEvent(RuntimeTask task, @Nullable Throwable throwable) {
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
