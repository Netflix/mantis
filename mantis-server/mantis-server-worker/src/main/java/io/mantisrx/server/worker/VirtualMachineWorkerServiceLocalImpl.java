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

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MachineDefinitions;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.WorkerTopologyInfo;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus.TYPE;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.mesos.MesosExecutorDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;


/* Local impl to fake a task launch from mesos to allow running a MantisWorker in IDE for development */
public class VirtualMachineWorkerServiceLocalImpl extends BaseService implements VirtualMachineWorkerService {

    private static final Logger logger = LoggerFactory.getLogger(VirtualMachineWorkerServiceLocalImpl.class);
    private final WorkerTopologyInfo.Data workerInfo;
    private MesosExecutorDriver mesosDriver;
    private ExecutorService executor;
    private Observer<WrappedExecuteStageRequest> executeStageRequestObserver;
    private Observable<VirtualMachineTaskStatus> vmTaskStatusObservable;

    public VirtualMachineWorkerServiceLocalImpl(final WorkerTopologyInfo.Data workerInfo,
                                                Observer<WrappedExecuteStageRequest> executeStageRequestObserver,
                                                Observable<VirtualMachineTaskStatus> vmTaskStatusObservable) {
        this.workerInfo = workerInfo;
        this.executeStageRequestObserver = executeStageRequestObserver;
        this.vmTaskStatusObservable = vmTaskStatusObservable;
        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "vm_worker_mesos_executor_thread");
                t.setDaemon(true);
                return t;
            }
        });
    }


    private WrappedExecuteStageRequest createExecuteStageRequest() throws MalformedURLException {

        // TODO make ExecuteStageRequest params configurable
        final long timeoutToReportStartSec = 5;
        final URL jobJarUrl = new URL("file:/Users/nmahilani/Projects/Mantis/mantis-sdk/examples/sine-function/build/distributions/sine-function-1.0.zip");
        final List<Integer> ports = Arrays.asList(31015, 31013, 31014);
        final List<Parameter> params = Collections.singletonList(new Parameter("useRandom", "true"));
        final int numInstances = 1;

        //                        new MachineDefinition(2, 300, 200, 1024, 2), true));
        final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
        final StageSchedulingInfo stage0SchedInfo = StageSchedulingInfo.builder()
                .numberOfInstances(numInstances)
                .machineDefinition(MachineDefinitions.micro())
                .build();
        final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
                .numberOfInstances(numInstances)
                .machineDefinition(new MachineDefinition(2, 300, 200, 1024, 2))
                .scalingPolicy(new StageScalingPolicy(1, 1, 5, 1, 1, 30,
                    Collections.singletonMap(StageScalingPolicy.ScalingReason.Memory,
                        new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, 15.0, 25.0, new StageScalingPolicy.RollingCount(1, 2)))))
                .scalable(true)
                .build();

        //        schedulingInfoMap.put(0, stage0SchedInfo);
        schedulingInfoMap.put(1, stage1SchedInfo);

        final SchedulingInfo schedInfo = new SchedulingInfo(schedulingInfoMap);

        final ExecuteStageRequest executeStageRequest = new ExecuteStageRequest(workerInfo.getJobName(), workerInfo.getJobId(), workerInfo.getWorkerIndex(), workerInfo.getWorkerNumber(),
                jobJarUrl, workerInfo.getStageNumber(), workerInfo.getNumStages(), ports, timeoutToReportStartSec, workerInfo.getMetricsPort(), params, schedInfo, MantisJobDurationType.Transient,
                0, 0L, 0L, new WorkerPorts(Arrays.asList(7151, 7152, 7153, 7154, 7155)), Optional.empty(),
                "user");

        return new WrappedExecuteStageRequest(PublishSubject.<Boolean>create(), executeStageRequest);
    }

    private void setupRequestFailureHandler(long waitSeconds, Observable<Boolean> requestObservable,
                                            final Action0 errorHandler) {
        requestObservable
                .buffer(waitSeconds, TimeUnit.SECONDS, 1)
                .take(1)
                .subscribe(new Observer<List<Boolean>>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        logger.error("onError called for request failure handler");
                        errorHandler.call();
                    }

                    @Override
                    public void onNext(List<Boolean> booleans) {
                        logger.info("onNext called for request failure handler with items: " +
                                ((booleans == null) ? "-1" : booleans.size()));
                        if ((booleans == null) || booleans.isEmpty())
                            errorHandler.call();
                    }
                });
    }

    @Override
    public void start() {
        logger.info("Starting VirtualMachineWorkerServiceLocalImpl");
        Schedulers.newThread().createWorker().schedule(new Action0() {
            @Override
            public void call() {
                try {
                    WrappedExecuteStageRequest request = null;
                    request = createExecuteStageRequest();
                    setupRequestFailureHandler(request.getRequest().getTimeoutToReportStart(), request.getRequestSubject(),
                            new Action0() {
                                @Override
                                public void call() {
                                    logger.error("launch error");
                                }
                            });
                    logger.info("onNext'ing WrappedExecuteStageRequest: {}", request.toString());
                    executeStageRequestObserver.onNext(request);
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
        }, 2, TimeUnit.SECONDS);


        // subscribe to vm task updates on current thread
        vmTaskStatusObservable.subscribe(new Action1<VirtualMachineTaskStatus>() {
            @Override
            public void call(VirtualMachineTaskStatus vmTaskStatus) {
                TYPE type = vmTaskStatus.getType();
                if (type == TYPE.COMPLETED) {
                    logger.info("Got COMPLETED state for " + vmTaskStatus.getTaskId());
                } else if (type == TYPE.STARTED) {
                    logger.info("Would send RUNNING state to mesos, worker started for " + vmTaskStatus.getTaskId());
                }
            }
        });
    }

    @Override
    public void shutdown() {
        logger.info("Unregistering Mantis Worker with Mesos executor callbacks");
        mesosDriver.stop();
        executor.shutdown();
    }

    @Override
    public void enterActiveMode() {}

}
