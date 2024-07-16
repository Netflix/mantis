/*
 * Copyright 2019 Netflix, Inc.
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

import static io.mantisrx.common.SystemParameters.JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM;
import static io.mantisrx.server.core.utils.StatusConstants.STATUS_MESSAGE_FORMAT;

import com.mantisrx.common.utils.Closeables;
import com.netflix.spectator.api.Registry;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.netty.MantisNettyEventsListenerFactory;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.WorkerMap;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.runtime.executor.PortSelector;
import io.mantisrx.runtime.executor.StageExecutors;
import io.mantisrx.runtime.executor.WorkerConsumer;
import io.mantisrx.runtime.executor.WorkerConsumerRemoteObservable;
import io.mantisrx.runtime.executor.WorkerPublisherRemoteObservable;
import io.mantisrx.runtime.lifecycle.Lifecycle;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.loader.SinkSubscriptionStateHandler;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.runtime.parameter.ParameterUtils;
import io.mantisrx.runtime.parameter.Parameters;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.ServiceRegistry;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.Status.TYPE;
import io.mantisrx.server.core.StatusPayloads;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.SseWorkerConnection;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.server.worker.jobmaster.AutoScaleMetricsConfig;
import io.mantisrx.server.worker.jobmaster.JobAutoscalerManager;
import io.mantisrx.server.worker.jobmaster.JobMasterService;
import io.mantisrx.server.worker.jobmaster.JobMasterStageConfig;
import io.mantisrx.shaded.com.google.common.base.Splitter;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.reactivex.mantis.remote.observable.RemoteRxServer;
import io.reactivex.mantis.remote.observable.RxMetrics;
import io.reactivex.mantis.remote.observable.ToDeltaEndpointInjector;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import mantis.io.reactivex.netty.RxNetty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

// stage that actually calls custom stage method
public class WorkerExecutionOperationsNetworkStage implements WorkerExecutionOperations {

    private static final Logger logger = LoggerFactory.getLogger(WorkerExecutionOperationsNetworkStage.class);
    private final WorkerConfiguration config;
    private final WorkerMetricsClient workerMetricsClient;
    private final AtomicReference<Heartbeat> heartbeatRef = new AtomicReference<>();
    private final SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory;
    private final MantisMasterGateway mantisMasterApi;
    private int connectionsPerEndpoint = 2;
    private boolean lookupSpectatorRegistry = true;
    private SinkSubscriptionStateHandler subscriptionStateHandler;
    private Action0 onSinkSubscribe = null;
    private Action0 onSinkUnsubscribe = null;
    private final List<Closeable> closeables = new ArrayList<>();
    private final ScheduledExecutorService scheduledExecutorService;
    private final ClassLoader classLoader;
    private Observer<Status> jobStatusObserver;

    public WorkerExecutionOperationsNetworkStage(
        MantisMasterGateway mantisMasterApi,
        WorkerConfiguration config,
        WorkerMetricsClient workerMetricsClient,
        SinkSubscriptionStateHandler.Factory sinkSubscriptionStateHandlerFactory,
        ClassLoader classLoader) {
        this.mantisMasterApi = mantisMasterApi;
        this.config = config;
        this.workerMetricsClient = workerMetricsClient;
        this.sinkSubscriptionStateHandlerFactory = sinkSubscriptionStateHandlerFactory;
        this.classLoader = classLoader;

        String connectionsPerEndpointStr =
            ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.worker.connectionsPerEndpoint", "2");
        if (connectionsPerEndpointStr != null && !connectionsPerEndpointStr.equals("2")) {
            connectionsPerEndpoint = Integer.parseInt(connectionsPerEndpointStr);
        }

        String locateSpectatorRegistry =
            ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.worker.locate.spectator.registry", "true");
        lookupSpectatorRegistry = Boolean.valueOf(locateSpectatorRegistry);
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * Converts a JobSchedulingInfo object to a simple WorkerMap to be used from within the context.
     * Static for easier testing.
     *
     * @param jobName
     * @param jobId
     * @param durationType
     * @param js
     *
     * @return
     */
    static WorkerMap convertJobSchedulingInfoToWorkerMap(String jobName, String jobId, MantisJobDurationType durationType, JobSchedulingInfo js) {
        Map<Integer, List<WorkerInfo>> stageToWorkerInfoMap = new HashMap<>();
        WorkerMap workerMap = new WorkerMap(stageToWorkerInfoMap);
        if (jobName == null || jobName.isEmpty() || jobId == null || jobId.isEmpty()) {
            logger.warn("Job name/jobId cannot be null in convertJobSchedulingInfoToWorkerMap");
            return workerMap;
        }

        if (js == null || js.getWorkerAssignments() == null) {
            logger.warn("JobSchedulingInfo or workerAssignments cannot be null in convertJobSchedulingInfoToWorkerMap");
            return workerMap;
        }
        try {
            Map<Integer, WorkerAssignments> workerAssignments = js.getWorkerAssignments();

            Iterator<Map.Entry<Integer, WorkerAssignments>> entryIterator = workerAssignments.entrySet().iterator();
            while (entryIterator.hasNext()) {

                Map.Entry<Integer, WorkerAssignments> next = entryIterator.next();
                int stageNo = next.getKey();
                WorkerAssignments workerAssignmentsForStage = next.getValue();
                Map<Integer, WorkerHost> hosts = workerAssignmentsForStage.getHosts();

                if (hosts != null) {

                    List<WorkerInfo> workerInfoList = hosts.values().stream().map((workerHost) -> {

                        return generateWorkerInfo(jobName, jobId, stageNo, workerHost.getWorkerIndex(), workerHost.getWorkerNumber(), durationType, workerHost.getHost(), workerHost);
                    }).collect(Collectors.toList());
                    stageToWorkerInfoMap.put(stageNo, workerInfoList);
                }

            }
            workerMap = new WorkerMap(stageToWorkerInfoMap);
        } catch (Exception e) {
            logger.warn("Exception converting JobSchedulingInfo " + js + " to worker Map " + e.getMessage());
            return workerMap;
        }
        return workerMap;
    }

    private static WorkerInfo generateWorkerInfo(String jobName, String jobId, int stageNumber, int workerIndex, int workerNumber, MantisJobDurationType durationType, String host, WorkerHost workerHost) {


        int sinkPort = Optional.ofNullable(workerHost.getPort()).map(ports -> (ports.size() >= 1 ? ports.get(0) : -1)).orElse(-1);
        WorkerPorts wPorts = new WorkerPorts(workerHost.getMetricsPort(), 65534, 65535, workerHost.getCustomPort(), sinkPort);
        return generateWorkerInfo(jobName, jobId, stageNumber, workerIndex, workerNumber, durationType, host, wPorts);

    }

    private static WorkerInfo generateWorkerInfo(String jobName, String jobId, int stageNumber, int workerIndex, int workerNumber, MantisJobDurationType durationType, String host, WorkerPorts workerPorts) {


        return new WorkerInfo(jobName, jobId, stageNumber, workerIndex, workerNumber, durationType, host, workerPorts);

    }

    private static Context generateContext(Parameters parameters, ServiceLocator serviceLocator, WorkerInfo workerInfo,
                                           MetricsRegistry metricsRegistry, Action0 completeAndExitAction, Observable<WorkerMap> workerMapObservable, ClassLoader classLoader) {

        return new Context(parameters, serviceLocator, workerInfo, metricsRegistry, completeAndExitAction, workerMapObservable, classLoader);

    }

    private Closeable startSendingHeartbeats(final Observer<Status> jobStatusObserver, long heartbeatIntervalSecs) {
        heartbeatRef.get().setPayload(String.valueOf(StatusPayloads.Type.SubscriptionState), "false");
        Future<?> heartbeatFuture = scheduledExecutorService.scheduleWithFixedDelay(
            () -> jobStatusObserver.onNext(heartbeatRef.get().getCurrentHeartbeatStatus()),
            heartbeatIntervalSecs,
            heartbeatIntervalSecs,
            TimeUnit.SECONDS);
        // start heartbeat payload setter for incoming data drops
        DataDroppedPayloadSetter droppedPayloadSetter = new DataDroppedPayloadSetter(heartbeatRef.get());
        droppedPayloadSetter.start(heartbeatIntervalSecs);

        ResourceUsagePayloadSetter usagePayloadSetter = new ResourceUsagePayloadSetter(heartbeatRef.get(), config);
        usagePayloadSetter.start(heartbeatIntervalSecs);

        return Closeables.combine(() -> heartbeatFuture.cancel(false), droppedPayloadSetter, usagePayloadSetter);
    }

    /**
     * Converts JobSchedulingInfo to a simpler WorkerMap object to be used within Context
     *
     * @param selfSchedulingInfo
     * @param jobName
     * @param jobId
     * @param durationType
     *
     * @return
     */
    private Observable<WorkerMap> createWorkerMapObservable(Observable<JobSchedulingInfo> selfSchedulingInfo, String jobName, String jobId, MantisJobDurationType durationType) {

        return selfSchedulingInfo
                .filter(jobSchedulingInfo -> (jobSchedulingInfo != null && jobSchedulingInfo.getWorkerAssignments() != null && !jobSchedulingInfo.getWorkerAssignments().isEmpty()))
                .map((jssi) -> convertJobSchedulingInfoToWorkerMap(jobName, jobId, durationType, jssi));

    }

    private Observable<Integer> createSourceStageTotalWorkersObservable(Observable<JobSchedulingInfo> selfSchedulingInfo) {

        return selfSchedulingInfo
                .filter(jobSchedulingInfo -> (jobSchedulingInfo != null &&
                        jobSchedulingInfo.getWorkerAssignments() != null &&
                        !jobSchedulingInfo.getWorkerAssignments().isEmpty()))
                .map((JobSchedulingInfo schedulingInfo) -> {
                    final Map<Integer, WorkerAssignments> workerAssignmentsMap = schedulingInfo.getWorkerAssignments();
                    final int stageNum = 1;
                    final WorkerAssignments workerAssignments = workerAssignmentsMap.get(stageNum);
                    return workerAssignments.getNumWorkers();
                });


    }

    private void signalStarted(RunningWorker rw) {
        rw.signalStarted();
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Override
    public void executeStage(final ExecutionDetails setup) throws IOException {

        ExecuteStageRequest executionRequest = setup.getExecuteStageRequest().getRequest();

        jobStatusObserver = setup.getStatus();

        // Initialize the schedulingInfo observable for current job and mark it shareable to be reused by anyone interested in this data.
        //Observable<JobSchedulingInfo> selfSchedulingInfo = mantisMasterApi.schedulingChanges(executionRequest.getJobId()).switchMap((e) -> Observable.just(e).repeatWhen(x -> x.delay(5 , TimeUnit.SECONDS))).subscribeOn(Schedulers.io()).share();

        // JobSchedulingInfo has metadata around which stage runs on which set of workers
        Observable<JobSchedulingInfo> selfSchedulingInfo =
            mantisMasterApi.schedulingChanges(executionRequest.getJobId())
                .subscribeOn(Schedulers.io())
                .replay(1)
                .refCount()
                .doOnSubscribe(() -> logger.info("mantisApi schedulingChanges subscribe"))
                .doOnUnsubscribe(() -> logger.info("mantisApi schedulingChanges stream unsub."))
                .doOnError(e -> logger.warn("mantisApi schedulingChanges stream error:", e))
                .doOnCompleted(() -> logger.info("mantisApi schedulingChanges stream completed."));
        // represents datastructure that has the current worker information and what it represents in the overall operator DAG
        WorkerInfo workerInfo = generateWorkerInfo(executionRequest.getJobName(), executionRequest.getJobId(),
                executionRequest.getStage(), executionRequest.getWorkerIndex(),
                executionRequest.getWorkerNumber(), executionRequest.getDurationType(), "host", executionRequest.getWorkerPorts());

        // observable that represents the number of workers for the source stage
        final Observable<Integer> sourceStageTotalWorkersObs = createSourceStageTotalWorkersObservable(selfSchedulingInfo);
        RunningWorker.Builder rwBuilder = new RunningWorker.Builder()
                .job(setup.getMantisJob())
                .schedulingInfo(executionRequest.getSchedulingInfo())
                .stageTotalWorkersObservable(sourceStageTotalWorkersObs)
                .jobName(executionRequest.getJobName())
                .stageNum(executionRequest.getStage())
                .workerIndex(executionRequest.getWorkerIndex())
                .workerNum(executionRequest.getWorkerNumber())
                .totalStages(executionRequest.getTotalNumStages())
                .metricsPort(executionRequest.getMetricsPort())
                .ports(executionRequest.getPorts().iterator())
                .jobStatusObserver(setup.getStatus())
                .requestSubject(setup.getExecuteStageRequest().getRequestSubject())
                .workerInfo(workerInfo)
                .hasJobMaster(executionRequest.getHasJobMaster())
                .jobId(executionRequest.getJobId());

        if (executionRequest.getStage() == 0) {
            rwBuilder = rwBuilder.stage(new JobMasterStageConfig("jobmasterconfig"));
        } else {
            rwBuilder = rwBuilder.stage((StageConfig) setup.getMantisJob()
                    .getStages().get(executionRequest.getStage() - 1));
        }
        final RunningWorker rw = rwBuilder.build();

        if (rw.getStageNum() == rw.getTotalStagesNet()) {
            // set up subscription state handler only for sink (last) stage
            setupSubscriptionStateHandler(setup.getExecuteStageRequest().getRequest());
        }

        logger.info("Running worker info: " + rw);

        rw.signalStartedInitiated();

        try {

            logger.info(">>>>>>>>>>>>>>>>Calling lifecycle.startup()");
            Lifecycle lifecycle = rw.getJob().getLifecycle();
            lifecycle.startup();
            ServiceLocator serviceLocator = lifecycle.getServiceLocator();

            if (lookupSpectatorRegistry) {
                try {
                    final Registry spectatorRegistry = serviceLocator.service(Registry.class);
                    SpectatorRegistryFactory.setRegistry(spectatorRegistry);
                } catch (Throwable t) {
                    logger.error("failed to init spectator registry using service locator, falling back to {}",
                            SpectatorRegistryFactory.getRegistry().getClass().getCanonicalName());
                }
            }

            // Ensure netty clients' listeners are set. This is redundant to the settings in TaskExecutor to ensure
            // the integration at runtime level.
            MantisNettyEventsListenerFactory mantisNettyEventsListenerFactory = new MantisNettyEventsListenerFactory();
            RxNetty.useMetricListenersFactory(mantisNettyEventsListenerFactory);
            SseWorkerConnection.useMetricListenersFactory(mantisNettyEventsListenerFactory);

            // create job context
            Parameters parameters = ParameterUtils
                    .createContextParameters(rw.getJob().getParameterDefinitions(),
                            setup.getParameters());
            final Context context = generateContext(parameters, serviceLocator, workerInfo, MetricsRegistry.getInstance(),
                    () -> {
                        rw.signalCompleted();
                        // wait for completion signal to go to the master and us getting killed. Upon timeout, exit.
                        try {Thread.sleep(60000);} catch (InterruptedException ie) {
                            logger.warn("Unexpected exception sleeping: " + ie.getMessage());
                        }
                        System.exit(0);
                    }, createWorkerMapObservable(selfSchedulingInfo, executionRequest.getJobName(), executionRequest.getJobId(), executionRequest.getDurationType()),
                classLoader
            );
            //context.setPrevStageCompletedObservable(createPrevStageCompletedObservable(selfSchedulingInfo, rw.getJobId(), rw.getStageNum()));

            rw.setContext(context);
            // setup heartbeats
            heartbeatRef.set(new Heartbeat(rw.getJobId(),
                    rw.getStageNum(), rw.getWorkerIndex(), rw.getWorkerNum(), config.getTaskExecutorHostName()));
            final double networkMbps = executionRequest.getSchedulingInfo().forStage(rw.getStageNum()).getMachineDefinition().getNetworkMbps();
            Closeable heartbeatCloseable = startSendingHeartbeats(rw.getJobStatus(),
                executionRequest.getHeartbeatIntervalSecs());
            closeables.add(heartbeatCloseable);

            // execute stage
            if (rw.getStageNum() == 0) {
                logger.info("JobId: " + rw.getJobId() + ", executing Job Master");

                final AutoScaleMetricsConfig autoScaleMetricsConfig = new AutoScaleMetricsConfig();

                // Temporary workaround to enable auto-scaling by custom metric in Job Master. This will be revisited to get the entire autoscaling config
                // for a job as a System parameter in the JobMaster
                final String autoScaleMetricString = (String) parameters.get(JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM, "");
                if (!Strings.isNullOrEmpty(autoScaleMetricString)) {
                    final List<String> tokens = Splitter.on("::").omitEmptyStrings().trimResults().splitToList(autoScaleMetricString);
                    if (tokens.size() == 3) {
                        final String metricGroup = tokens.get(0);
                        final String metricName = tokens.get(1);
                        final String algo = tokens.get(2);
                        try {
                            final AutoScaleMetricsConfig.AggregationAlgo aggregationAlgo = AutoScaleMetricsConfig.AggregationAlgo.valueOf(algo);
                            logger.info("registered UserDefined auto scale metric {}:{} algo {}", metricGroup, metricName, aggregationAlgo);
                            autoScaleMetricsConfig.addUserDefinedMetric(metricGroup, metricName, aggregationAlgo);
                        } catch (IllegalArgumentException e) {
                            final String errorMsg = String.format("ERROR: Invalid algorithm value %s for param %s (algo should be one of %s)",
                                    autoScaleMetricsConfig, JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM,
                                    Arrays.stream(AutoScaleMetricsConfig.AggregationAlgo.values()).map(a -> a.name()).collect(Collectors.toList()));
                            logger.error(errorMsg);
                            throw new RuntimeException(errorMsg);
                        }
                    } else {
                        final String errorMsg = String.format("ERROR: Invalid value %s for param %s", autoScaleMetricString, JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM);
                        logger.error(errorMsg);
                        throw new RuntimeException(errorMsg);
                    }
                } else {
                    logger.info("param {} is null or empty", JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM);
                }

                JobAutoscalerManager jobAutoscalerManager = getJobAutoscalerManagerInstance(serviceLocator);
                JobMasterService jobMasterService = new JobMasterService(rw.getJobId(), rw.getSchedulingInfo(),
                        workerMetricsClient, autoScaleMetricsConfig, mantisMasterApi, rw.getContext(), rw.getOnCompleteCallback(), rw.getOnErrorCallback(), rw.getOnTerminateCallback(), jobAutoscalerManager);
                jobMasterService.start();
                closeables.add(jobMasterService::shutdown);

                signalStarted(rw);
                // block until worker terminates
                rw.waitUntilTerminate();
            } else if (rw.getStageNum() == 1 && rw.getTotalStagesNet() == 1) {
                logger.info("JobId: " + rw.getJobId() + ", single stage job, executing entire job");
                // single stage, execute entire job on this machine
                PortSelector portSelector = new PortSelector() {
                    @Override
                    public int acquirePort() {
                        return rw.getPorts().next();
                    }
                };
                RxMetrics rxMetrics = new RxMetrics();
                closeables.add(StageExecutors.executeSingleStageJob(rw.getJob().getSource(), rw.getStage(),
                        rw.getJob().getSink(), portSelector, rxMetrics, rw.getContext(),
                        rw.getOnTerminateCallback(), rw.getWorkerIndex(),
                        rw.getSourceStageTotalWorkersObservable(),
                        onSinkSubscribe, onSinkUnsubscribe,
                        rw.getOnCompleteCallback(), rw.getOnErrorCallback()));
                signalStarted(rw);
                // block until worker terminates
                rw.waitUntilTerminate();
            } else {
                logger.info("JobId: " + rw.getJobId() + ", executing a multi-stage job, stage: " + rw.getStageNum());
                if (rw.getStageNum() == 1) {

                    // execute source stage
                    String remoteObservableName = rw.getJobId() + "_" + rw.getStageNum();

                    StageSchedulingInfo currentStageSchedulingInfo = rw.getSchedulingInfo().forStage(1);
                    WorkerPublisherRemoteObservable publisher
                            = new WorkerPublisherRemoteObservable<>(rw.getPorts().next(),
                            remoteObservableName, numWorkersAtStage(selfSchedulingInfo, rw.getJobId(), rw.getStageNum() + 1),
                            rw.getJobName());

                    closeables.add(StageExecutors.executeSource(rw.getWorkerIndex(), rw.getJob().getSource(),
                            rw.getStage(), publisher, rw.getContext(), rw.getSourceStageTotalWorkersObservable()));

                    logger.info("JobId: " + rw.getJobId() + " stage: " + rw.getStageNum() + ", serving remote observable for source with name: " + remoteObservableName);
                    RemoteRxServer server = publisher.getServer();
                    RxMetrics rxMetrics = server.getMetrics();
                    MetricsRegistry.getInstance().registerAndGet(rxMetrics.getCountersAndGauges());

                    signalStarted(rw);
                    logger.info("JobId: " + rw.getJobId() + " stage: " + rw.getStageNum() + ", blocking until source observable completes");
                    server.blockUntilServerShutdown();
                } else {
                    // execute intermediate stage or last stage plus sink
                    executeNonSourceStage(selfSchedulingInfo, rw);
                }
            }
            logger.info("Calling lifecycle.shutdown()");
            lifecycle.shutdown();
        } catch (Throwable t) {
            logger.warn("Error during executing stage; shutting down.", t);
            rw.signalFailed(t);
            shutdownStage();
        }
    }

    private JobAutoscalerManager getJobAutoscalerManagerInstance(ServiceLocator serviceLocator) {
        final JobAutoscalerManager autoscalerManager = serviceLocator.service(JobAutoscalerManager.class);
        return Optional.ofNullable(autoscalerManager).orElse(JobAutoscalerManager.DEFAULT);
    }

    private void setupSubscriptionStateHandler(ExecuteStageRequest executeStageRequest) {
        final SinkSubscriptionStateHandler subscriptionStateHandler =
                sinkSubscriptionStateHandlerFactory.apply(executeStageRequest);
        onSinkSubscribe = () -> {
            // TODO remove this line to set heartbeat payloads when master has upgraded to having jobMaster design
            heartbeatRef.get().setPayload(StatusPayloads.Type.SubscriptionState.toString(), Boolean.toString(true));
            subscriptionStateHandler.onSinkSubscribed();
        };
        onSinkUnsubscribe = () -> {
            // TODO remove this line to set heartbeat payloads when master has upgraded to having jobMaster design
            heartbeatRef.get().setPayload(StatusPayloads.Type.SubscriptionState.toString(), Boolean.toString(false));
            subscriptionStateHandler.onSinkUnsubscribed();
        };

        this.subscriptionStateHandler = subscriptionStateHandler;
        try {
            this.subscriptionStateHandler.startAsync().awaitRunning(Duration.of(5, ChronoUnit.SECONDS));
        } catch (TimeoutException e) {
            logger.error("Failed to start subscriptionStateHandler: ", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    private void executeNonSourceStage(Observable<JobSchedulingInfo> selfSchedulingInfo, final RunningWorker rw) {
        {
            // execute either intermediate (middle) stage or last+sink
            StageConfig previousStageExecuting = (StageConfig) rw.getJob().getStages()
                    .get(rw.getStageNum() - 2); // note, stages are zero indexed

            StageSchedulingInfo previousSchedulingInfo = rw.getSchedulingInfo().forStage(rw.getStageNum() - 1);
            int numInstanceAtPreviousStage = previousSchedulingInfo.getNumberOfInstances();
            AtomicBoolean acceptSchedulingChanges = new AtomicBoolean(true);
            WorkerConsumer consumer = connectToObservableAtPreviousStages(selfSchedulingInfo, rw.getJobId(), rw.getStageNum() - 1,
                    numInstanceAtPreviousStage, previousStageExecuting, acceptSchedulingChanges,
                    rw.getJobStatus(), rw.getStageNum(), rw.getWorkerIndex(), rw.getWorkerNum());
            final int workerPort = rw.getPorts().next();

            if (rw.getStageNum() == rw.getTotalStagesNet()) {
                // last+sink stage
                logger.info(
                    "JobId: {}, executing sink stage: {}, signaling started", rw.getJobId(), rw.getStageNum());
                rw.getJobStatus().onNext(new Status(rw.getJobId(), rw.getStageNum(), rw.getWorkerIndex(),
                        rw.getWorkerNum(),
                        TYPE.INFO, String.format(STATUS_MESSAGE_FORMAT, rw.getStageNum(), rw.getWorkerIndex(), rw.getWorkerNum(), "running"),
                        MantisJobState.Started));

                PortSelector portSelector = new PortSelector() {
                    @Override
                    public int acquirePort() {
                        return workerPort;
                    }
                };
                RxMetrics rxMetrics = new RxMetrics();
                MetricsRegistry.getInstance().registerAndGet(rxMetrics.getCountersAndGauges());
                final CountDownLatch blockUntilComplete = new CountDownLatch(1);
                Action0 countDownLatch = new Action0() {
                    @Override
                    public void call() {
                        blockUntilComplete.countDown();
                    }
                };
                closeables.add(StageExecutors.executeSink(consumer, rw.getStage(),
                        rw.getJob().getSink(), portSelector, rxMetrics,
                        rw.getContext(), countDownLatch, onSinkSubscribe, onSinkUnsubscribe,
                        rw.getOnCompleteCallback(), rw.getOnErrorCallback()));
                // block until completes
                try {
                    blockUntilComplete.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                acceptSchedulingChanges.set(false);
            } else {
                // intermediate stage
                logger.info("JobId: " + rw.getJobId() + ", executing intermediate stage: " + rw.getStageNum());


                int stageNumToExecute = rw.getStageNum();
                String jobId = rw.getJobId();
                String remoteObservableName = jobId + "_" + stageNumToExecute;

                WorkerPublisherRemoteObservable publisher
                        = new WorkerPublisherRemoteObservable<>(workerPort, remoteObservableName,
                        numWorkersAtStage(selfSchedulingInfo, rw.getJobId(), rw.getStageNum() + 1), rw.getJobName());
                closeables.add(StageExecutors.executeIntermediate(consumer, rw.getStage(), publisher,
                        rw.getContext()));
                RemoteRxServer server = publisher.getServer();

                logger.info("JobId: " + jobId + " stage: " + stageNumToExecute + ", serving intermediate remote observable with name: " + remoteObservableName);
                RxMetrics rxMetrics = server.getMetrics();
                MetricsRegistry.getInstance().registerAndGet(rxMetrics.getCountersAndGauges());
                // send running signal only after server is started
                signalStarted(rw);
                logger.info("JobId: " + jobId + " stage: " + stageNumToExecute + ", blocking until intermediate observable completes");
                server.blockUntilServerShutdown();
                acceptSchedulingChanges.set(false);
            }
        }
    }

    private Observable<Integer> numWorkersAtStage(Observable<JobSchedulingInfo> selfSchedulingInfo, String jobId, final int stageNum) {
        //return mantisMasterApi.schedulingChanges(jobId)
        return selfSchedulingInfo
                .distinctUntilChanged((prevJobSchedInfo, currentJobSchedInfo) -> (!prevJobSchedInfo.equals(currentJobSchedInfo)) ? false : true)
                .flatMap((Func1<JobSchedulingInfo, Observable<WorkerAssignments>>) schedulingChange -> {
                    Map<Integer, WorkerAssignments> assignments = schedulingChange.getWorkerAssignments();
                    if (assignments != null && !assignments.isEmpty()) {
                        return Observable.from(assignments.values());
                    } else {
                        return Observable.empty();
                    }
                })
                .filter(assignments -> (assignments.getStage() == stageNum))
                .map(assignments -> {
                    return assignments.getNumWorkers() * connectionsPerEndpoint; // scale by numConnections
                }).share();

    }

    @SuppressWarnings( {"rawtypes"})
    private WorkerConsumer connectToObservableAtPreviousStages(Observable<JobSchedulingInfo> selfSchedulingInfo, final String jobId, final int previousStageNum,
                                                               int numInstanceAtPreviousStage, final StageConfig previousStage, final AtomicBoolean acceptSchedulingChanges,
                                                               final Observer<Status> jobStatusObserver, final int stageNumToExecute, final int workerIndex, final int workerNumber) {
        logger.info("Watching for scheduling changes");

        //Observable<List<Endpoint>> schedulingUpdates = mantisMasterApi.schedulingChanges(jobId)
        Observable<List<Endpoint>> schedulingUpdates = selfSchedulingInfo
                .flatMap((Func1<JobSchedulingInfo, Observable<WorkerAssignments>>) schedulingChange -> {
                    Map<Integer, WorkerAssignments> assignments = schedulingChange.getWorkerAssignments();
                    if (assignments != null && !assignments.isEmpty()) {
                        return Observable.from(assignments.values());
                    } else {
                        return Observable.empty();
                    }
                })
                .filter(assignments -> (assignments.getStage() == previousStageNum) &&
                        acceptSchedulingChanges.get())
                .map(assignments -> {
                    List<Endpoint> endpoints = new LinkedList<>();
                    for (WorkerHost host : assignments.getHosts().values()) {
                        if (host.getState() == MantisJobState.Started) {
                            logger.info("Received scheduling update from master, connect request for host: " + host.getHost() + " port: " + host.getPort() + " state: " + host.getState() +
                                    " adding: " + connectionsPerEndpoint + " connections to host");
                            for (int i = 1; i <= connectionsPerEndpoint; i++) {
                                final String endpointId = "stage_" + stageNumToExecute + "_index_" + Integer.toString(workerIndex) + "_partition_" + i;
                                logger.info("Adding endpoint to endpoint injector to be considered for add, with id: " + endpointId);
                                endpoints.add(new Endpoint(host.getHost(), host.getPort().get(0),
                                        endpointId));
                            }
                        }
                    }
                    return endpoints;
                })
                .filter(t1 -> (t1.size() > 0));
        String name = jobId + "_" + previousStageNum;

        return new WorkerConsumerRemoteObservable(name,
                new ToDeltaEndpointInjector(schedulingUpdates));
    }

    @Override
    public void shutdownStage() throws IOException  {
        if (jobStatusObserver != null) {
            final Heartbeat heartbeat = heartbeatRef.get();
            final Status status = new Status(heartbeat.getJobId(), heartbeat.getStageNumber(), heartbeat.getWorkerIndex(), heartbeat.getWorkerNumber(),
                Status.TYPE.INFO, String.format(STATUS_MESSAGE_FORMAT, heartbeat.getStageNumber(), heartbeat.getWorkerIndex(), heartbeat.getWorkerNumber(), "shutdown"),
                MantisJobState.Failed);
            jobStatusObserver.onNext(status);
        }
        if (subscriptionStateHandler != null) {
            try {
                subscriptionStateHandler.stopAsync();
            } catch (Exception e) {
                logger.error("Failed to stop subscription state handler successfully", e);
            } finally {
                subscriptionStateHandler = null;
            }
        }

        Closeables.combine(closeables).close();
        scheduledExecutorService.shutdownNow();
        logger.info("Shutdown completed");
    }
}
