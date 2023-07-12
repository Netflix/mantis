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

package io.mantisrx.server.worker.mesos;

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import io.mantisrx.server.worker.VirtualMachineWorkerService;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus.TYPE;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;


public class VirualMachineWorkerServiceMesosImpl extends BaseService implements VirtualMachineWorkerService {

    private static final Logger logger = LoggerFactory.getLogger(VirualMachineWorkerServiceMesosImpl.class);
    private MesosExecutorDriver mesosDriver;
    private final ExecutorService executor;
    private final Observer<WrappedExecuteStageRequest> executeStageRequestObserver;
    private final Observable<VirtualMachineTaskStatus> vmTaskStatusObservable;

    public VirualMachineWorkerServiceMesosImpl(Observer<WrappedExecuteStageRequest> executeStageRequestObserver,
                                               Observable<VirtualMachineTaskStatus> vmTaskStatusObservable) {
        this.executeStageRequestObserver = executeStageRequestObserver;
        this.vmTaskStatusObservable = vmTaskStatusObservable;
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "vm_worker_mesos_executor_thread");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        logger.info("Registering Mantis Worker with Mesos executor callbacks");
        mesosDriver = new MesosExecutorDriver(new MesosExecutorCallbackHandler(executeStageRequestObserver));
        // launch driver on background thread
        logger.info("launch driver on background thread");
        executor.execute(() -> {
            try {
                mesosDriver.run();
            } catch (Exception e) {
                logger.error("Failed to register Mantis Worker with Mesos executor callbacks", e);
            }
        });
        // subscribe to vm task updates on current thread
        logger.info("subscribe to vm task updates on current thread");
        vmTaskStatusObservable.subscribe(vmTaskStatus -> {
            Protos.Status status;
            TYPE type = vmTaskStatus.getType();
            switch (type) {
                case COMPLETED:
                     status = mesosDriver.sendStatusUpdate(TaskStatus.newBuilder()
                        .setTaskId(TaskID.newBuilder().setValue(vmTaskStatus.getTaskId()).build())
                        .setState(TaskState.TASK_FINISHED).build());
                    logger.info("Sent COMPLETED state to mesos, driver status={}", status);
                    break;
                case STARTED:
                    status = mesosDriver.sendStatusUpdate(TaskStatus.newBuilder()
                        .setTaskId(TaskID.newBuilder().setValue(vmTaskStatus.getTaskId()).build())
                        .setState(TaskState.TASK_RUNNING).build());
                    logger.info("Sent RUNNING state to mesos, driver status={}", status);
                    break;
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
