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

package io.mantisrx.runtime.executor;

import io.mantisrx.common.MantisProperties;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.MetricsServer;
import io.mantisrx.common.metrics.netty.MantisNettyEventsListenerFactory;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MachineDefinitions;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.SinkHolder;
import io.mantisrx.runtime.SourceHolder;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.WorkerMap;
import io.mantisrx.runtime.command.CommandException;
import io.mantisrx.runtime.command.ValidateJob;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.runtime.lifecycle.Lifecycle;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.ParameterUtils;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivex.mantis.remote.observable.EndpointChange.Type;
import io.reactivex.mantis.remote.observable.EndpointInjector;
import io.reactivex.mantis.remote.observable.RxMetrics;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import mantis.io.reactivex.netty.RxNetty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;


public class LocalJobExecutorNetworked {

    private static final Logger logger = LoggerFactory.getLogger(LocalJobExecutorNetworked.class);
    private static final int numPartitions = 1;
    private static final Action0 nullAction = () -> System.exit(0);

    private LocalJobExecutorNetworked() {}

    @SuppressWarnings("rawtypes")
    private static void startSource(int index, int port, int workersAtNextStage, SourceHolder source, StageConfig stage,
                                    Context context, Observable<Integer> stageWorkersObservable) {
        logger.debug("Creating source publisher on port " + port);
        WorkerPublisherRemoteObservable publisher
                = new WorkerPublisherRemoteObservable<>(port,
                null, Observable.just(workersAtNextStage * numPartitions), null); // name is set to null, defaul
        // to start job
        StageExecutors.executeSource(index, source, stage, publisher, context, stageWorkersObservable);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    private static void startIntermediate(int[] previousStagePorts,
                                          int port, StageConfig stage, Context context, int workerIndex,
                                          int workersAtNextStage, int stageNumber,
                                          int workersAtPreviousStage) {
        if (logger.isDebugEnabled()) {
            StringBuilder portsToString = new StringBuilder();
            for (int previousPort : previousStagePorts) {
                portsToString.append(previousPort + " ");
            }
            logger.debug("Creating intermediate consumer connecting to publishers on ports " + portsToString);
        }

        Observable<Set<Endpoint>> endpoints = staticEndpoints(previousStagePorts, stageNumber, workerIndex, numPartitions);
        WorkerConsumerRemoteObservable intermediateConsumer
                = new WorkerConsumerRemoteObservable(null, // name=null local
                staticInjector(endpoints));
        logger.debug("Creating intermediate publisher on port " + port);
        WorkerPublisherRemoteObservable intermediatePublisher
                = new WorkerPublisherRemoteObservable<>(port,
                null, Observable.just(workersAtNextStage * numPartitions), null); // name is null for local
        StageExecutors.executeIntermediate(intermediateConsumer, stage, intermediatePublisher,
                context);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    private static void startSink(StageConfig previousStage, int[] previousStagePorts, StageConfig stage, PortSelector portSelector,
                                  SinkHolder sink, Context context,
                                  Action0 sinkObservableCompletedCallback,
                                  Action0 sinkObservableTerminatedCompletedCallback,
                                  Action1<Throwable> sinkObservableErrorCallback,
                                  int stageNumber,
                                  int workerIndex,
                                  int workersAtPreviousStage) {
        if (logger.isDebugEnabled()) {
            StringBuilder portsToString = new StringBuilder();
            for (int previousPort : previousStagePorts) {
                portsToString.append(previousPort + " ");
            }
            logger.debug("Creating sink consumer connecting to publishers on ports " + portsToString);
        }

        Observable<Set<Endpoint>> endpoints = staticEndpoints(previousStagePorts, stageNumber, workerIndex, numPartitions);
        WorkerConsumerRemoteObservable sinkConsumer
                = new WorkerConsumerRemoteObservable(null, // name=null for local
                staticInjector(endpoints));

        StageExecutors.executeSink(sinkConsumer, stage, sink, portSelector,
                new RxMetrics(), context, sinkObservableTerminatedCompletedCallback,
                null, null,
                sinkObservableCompletedCallback, sinkObservableErrorCallback);
    }

    public static Map<String, Object> checkAndGetParameters(Map<String, ParameterDefinition<?>> parameterDefinitions,
                                                            Parameter... parameters) throws IllegalArgumentException {
        Map<String, Parameter> indexedParameters = new HashMap<>();
        for (Parameter parameter : parameters) {
            indexedParameters.put(parameter.getName(), parameter);
        }
        return ParameterUtils.checkThenCreateState(parameterDefinitions,
                indexedParameters);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public static void execute(Job job, Parameter... parameters) throws IllegalMantisJobException {
        List<StageConfig> stages = job.getStages();
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder();
        for (@SuppressWarnings("unused") StageConfig stage : stages) {
            builder.singleWorkerStage(MachineDefinitions.micro());
        }
        builder.numberOfStages(stages.size());
        execute(job, builder.build(), parameters);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public static void execute(Job job, SchedulingInfo schedulingInfo, Parameter... parameters) throws IllegalMantisJobException {
        // validate job
        try {
            new ValidateJob(job).execute();
        } catch (CommandException e) {
            throw new IllegalMantisJobException(e);
        }

        // execute job
        List<StageConfig> stages = job.getStages();
        final SourceHolder source = job.getSource();
        final SinkHolder sink = job.getSink();
        final PortSelector portSelector = new PortSelectorInRange(8000, 9000);

        // register netty metrics
        RxNetty.useMetricListenersFactory(new MantisNettyEventsListenerFactory());
        // start our metrics server
        MetricsServer metricsServer = new MetricsServer(portSelector.acquirePort(), 1, Collections.EMPTY_MAP);
        metricsServer.start();

        Lifecycle lifecycle = job.getLifecycle();
        lifecycle.startup();

        // create job context
        Map parameterDefinitions = job.getParameterDefinitions();
        final String user = Optional.ofNullable(MantisProperties.getProperty("USER")).orElse(
            "userUnknown");
        String jobId = String.format("localJob-%s-%d", user, (int) (Math.random() * 10000));
        logger.info("jobID {}", jobId);
        final ServiceLocator serviceLocator = lifecycle.getServiceLocator();


        int numInstances = schedulingInfo.forStage(1).getNumberOfInstances();
        BehaviorSubject<Integer> workersInStageOneObservable = BehaviorSubject.create(numInstances);
        BehaviorSubject<WorkerMap> workerMapObservable = BehaviorSubject.create();

        if (stages.size() == 1) {
            // single stage job
            final StageConfig stage = stages.get(0);


            // use latch to wait for all instances to complete
            final CountDownLatch waitUntilAllCompleted = new CountDownLatch(numInstances);
            Action0 countDownLatchOnComplete = new Action0() {
                @Override
                public void call() {
                    waitUntilAllCompleted.countDown();
                }
            };
            Action0 nullOnCompleted = new Action0() {
                @Override
                public void call() {}
            };
            Action1<Throwable> nullOnError = new Action1<Throwable>() {
                @Override
                public void call(Throwable t) {}
            };

            Map<Integer, List<WorkerInfo>> workerInfoMap = new HashMap<>();
            List<WorkerInfo> workerInfoList = new ArrayList<>();


            // run for num of instances
            for (int i = 0; i < numInstances; i++) {

                WorkerPorts workerPorts = new WorkerPorts(portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort());
                WorkerInfo workerInfo = new WorkerInfo(jobId, jobId, 1, i, i + 1, MantisJobDurationType.Perpetual, "localhost", workerPorts);
                workerInfoList.add(workerInfo);
                Context context = new Context(
                        ParameterUtils.createContextParameters(parameterDefinitions,
                                parameters),
                        lifecycle.getServiceLocator(),
                        //new WorkerInfo(jobId, jobId, 1, i, i, MantisJobDurationType.Perpetual, "localhost", new ArrayList<>(),-1,-1),
                        workerInfo,
                        MetricsRegistry.getInstance(), () -> {
                    System.exit(0);
                }, workerMapObservable,
                    Thread.currentThread().getContextClassLoader());

                // workers for stage 1
                workerInfoMap.put(1, workerInfoList);

                workerMapObservable.onNext(new WorkerMap(workerInfoMap));


                StageExecutors.executeSingleStageJob(source, stage, sink, () -> workerInfo.getWorkerPorts().getSinkPort(), new RxMetrics(),
                        context, countDownLatchOnComplete, i, workersInStageOneObservable, null, null, nullOnCompleted,
                        nullOnError);
            }


            // wait for all instances to complete
            try {
                waitUntilAllCompleted.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            // multi-stage job
            int workerNumber = 0;
            // start source stages
            StageConfig currentStage = stages.get(0);
            StageConfig previousStage = null;
            StageSchedulingInfo currentStageScalingInfo = schedulingInfo.forStage(1);
            StageSchedulingInfo nextStageScalingInfo = schedulingInfo.forStage(2);
            int[] previousPorts = new int[currentStageScalingInfo.getNumberOfInstances()]; // num ports

            Map<Integer, List<WorkerInfo>> workerInfoMap = new HashMap<>();
            List<WorkerInfo> workerInfoList = new ArrayList<>();

            for (int i = 0; i < currentStageScalingInfo.getNumberOfInstances(); i++) {
                WorkerPorts workerPorts = new WorkerPorts(portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort());
                WorkerInfo workerInfo = new WorkerInfo(jobId, jobId, 1, i, i + 1, MantisJobDurationType.Perpetual, "localhost", workerPorts);
                workerInfoList.add(workerInfo);

                //int sourcePort = portSelector.acquirePort();
                int sourcePort = workerInfo.getWorkerPorts().getSinkPort();
                previousPorts[i] = sourcePort;
                Context context = new Context(
                        ParameterUtils.createContextParameters(parameterDefinitions,
                                parameters),
                        serviceLocator, workerInfo,
                        MetricsRegistry.getInstance(), nullAction, workerMapObservable,
                    Thread.currentThread().getContextClassLoader());

                startSource(i, sourcePort, nextStageScalingInfo.getNumberOfInstances(),
                        job.getSource(), currentStage, context, workersInStageOneObservable);
            }
            // workers for stage 1
            workerInfoMap.put(1, workerInfoList);

            workerMapObservable.onNext(new WorkerMap(workerInfoMap));


            // start intermediate stages, all but last stage
            for (int i = 1; i < stages.size() - 1; i++) {
                previousStage = currentStage;
                StageSchedulingInfo previousStageScalingInfo = schedulingInfo.forStage(i);
                currentStageScalingInfo = schedulingInfo.forStage(i + 1); // stages indexed starting at 1
                currentStage = stages.get(i);
                nextStageScalingInfo = schedulingInfo.forStage(i + 2); // stages indexed starting at 1
                int[] currentPorts = new int[currentStageScalingInfo.getNumberOfInstances()];
                workerInfoList = new ArrayList<>();
                for (int j = 0; j < currentStageScalingInfo.getNumberOfInstances(); j++) {

                    WorkerPorts workerPorts = new WorkerPorts(portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort());
                    WorkerInfo workerInfo = new WorkerInfo(jobId, jobId, i + 1, j, workerNumber++, MantisJobDurationType.Perpetual, "localhost", workerPorts);
                    workerInfoList.add(workerInfo);
                    //int port = portSelector.acquirePort();
                    int port = workerInfo.getWorkerPorts().getSinkPort();
                    currentPorts[j] = port;

                    Context context = new Context(
                            ParameterUtils.createContextParameters(parameterDefinitions,
                                    parameters),
                            serviceLocator, workerInfo,
                            MetricsRegistry.getInstance(), nullAction, workerMapObservable,
                        Thread.currentThread().getContextClassLoader());


                    startIntermediate(previousPorts, port, currentStage, context, j,
                            nextStageScalingInfo.getNumberOfInstances(), i, previousStageScalingInfo.getNumberOfInstances());
                }
                // workers for current stage
                workerInfoMap.put(i + 1, workerInfoList);
                workerMapObservable.onNext(new WorkerMap(workerInfoMap));

                previousPorts = currentPorts;
            }

            // start sink stage
            StageSchedulingInfo previousStageScalingInfo = schedulingInfo.forStage(stages.size() - 1);
            previousStage = stages.get(stages.size() - 2);
            currentStage = stages.get(stages.size() - 1);
            currentStageScalingInfo = schedulingInfo.forStage(stages.size());
            numInstances = currentStageScalingInfo.getNumberOfInstances();
            // use latch to wait for all instances to complete

            final CountDownLatch waitUntilAllCompleted = new CountDownLatch(numInstances);
            Action0 countDownLatchOnTerminated = new Action0() {
                @Override
                public void call() {
                    waitUntilAllCompleted.countDown();
                }
            };
            Action0 nullOnCompleted = new Action0() {
                @Override
                public void call() {}
            };
            Action1<Throwable> nullOnError = new Action1<Throwable>() {
                @Override
                public void call(Throwable t) {}
            };
            workerInfoList = new ArrayList<>();
            for (int i = 0; i < numInstances; i++) {
                WorkerPorts workerPorts = new WorkerPorts(portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort(), portSelector.acquirePort());
                WorkerInfo workerInfo = new WorkerInfo(jobId, jobId, stages.size(), i, workerNumber++, MantisJobDurationType.Perpetual, "localhost", workerPorts);
                workerInfoList.add(workerInfo);

                Context context = new Context(
                        ParameterUtils.createContextParameters(parameterDefinitions,
                                parameters),
                        serviceLocator, workerInfo,
                        MetricsRegistry.getInstance(), nullAction, workerMapObservable,
                    Thread.currentThread().getContextClassLoader());


                startSink(previousStage, previousPorts, currentStage, () -> workerInfo.getWorkerPorts().getSinkPort(), sink,
                        context, countDownLatchOnTerminated,
                        nullOnCompleted, nullOnError, stages.size(), i, previousStageScalingInfo.getNumberOfInstances());
            }
            workerInfoMap.put(stages.size(), workerInfoList);
            workerMapObservable.onNext(new WorkerMap(workerInfoMap));
            // wait for all instances to complete
            try {
                waitUntilAllCompleted.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        lifecycle.shutdown();
        metricsServer.shutdown();
    }

    private static Observable<Set<Endpoint>> staticEndpoints(final int[] ports, final int stageNum, final int workerIndex,
                                                             final int numPartitions) {
        return Observable.create(new OnSubscribe<Set<Endpoint>>() {
            @Override
            public void call(Subscriber<? super Set<Endpoint>> subscriber) {
                Set<Endpoint> endpoints = new HashSet<>();
                for (int i = 0; i < ports.length; i++) {
                    int port = ports[i];
                    for (int j = 1; j <= numPartitions; j++) {
                        Endpoint endpoint = new Endpoint("localhost", port,
                                "stage_" + stageNum + "_index_" + workerIndex + "_partition_" + j);
                        logger.info("adding static endpoint:" + endpoint);
                        endpoints.add(endpoint);
                    }
                }
                subscriber.onNext(endpoints);
                subscriber.onCompleted();
            }
        });
    }

    private static EndpointInjector staticInjector(final Observable<Set<Endpoint>> endpointsToAdd) {
        return new EndpointInjector() {
            @Override
            public Observable<EndpointChange> deltas() {
                return endpointsToAdd.flatMap(new Func1<Set<Endpoint>, Observable<EndpointChange>>() {
                    @Override
                    public Observable<EndpointChange> call(Set<Endpoint> t1) {
                        return
                                Observable.from(t1)
                                        .map(new Func1<Endpoint, EndpointChange>() {
                                            @Override
                                            public EndpointChange call(Endpoint t1) {
                                                logger.info("injected endpoint:" + t1);
                                                return
                                                        new EndpointChange(Type.add, new Endpoint(t1.getHost(), t1.getPort(), t1.getSlotId()));
                                            }
                                        });

                    }
                });
            }
        };
    }


}
