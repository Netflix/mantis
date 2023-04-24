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

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.subjects.PublishSubject;


public class MesosExecutorCallbackHandler implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(MesosExecutorCallbackHandler.class);
    private final Observer<WrappedExecuteStageRequest> executeStageRequestObserver;
    private final JsonSerializer serializer = new JsonSerializer();

    public MesosExecutorCallbackHandler(Observer<WrappedExecuteStageRequest> executeStageRequestObserver) {
        this.executeStageRequestObserver = executeStageRequestObserver;
    }

    @Override
    public void disconnected(ExecutorDriver arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void error(ExecutorDriver arg0, String msg) {
        // TODO Auto-generated method stub
        logger.error(msg);
    }

    @Override
    public void frameworkMessage(ExecutorDriver arg0, byte[] arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void killTask(ExecutorDriver arg0, TaskID task) {
        logger.info("Executor going to kill task {}", task.getValue());
        executeStageRequestObserver.onCompleted();
        waitAndExit();
    }

    private void waitAndExit() {
        // Allow some time for clean up and the completion report to be sent out before exiting.
        // Until we define a better way to exit than to assume that the time we wait here is
        // sufficient before a hard exit, we will live with it.
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(2000);} catch (InterruptedException ie) {}
            System.exit(0);
        });
        t.setDaemon(true);
        t.start();
    }

    private WrappedExecuteStageRequest createExecuteStageRequest(TaskInfo task) {
        // TODO
        try {
            byte[] jsonBytes = task.getData().toByteArray();
            logger.info("Received request {}", new String(jsonBytes));
            return new WrappedExecuteStageRequest(
                    PublishSubject.create(),
                    serializer.fromJson(jsonBytes, ExecuteStageRequest.class));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private void sendLaunchError(ExecutorDriver driver, final TaskInfo task) {
        driver.sendStatusUpdate(Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_FAILED).build());
        waitAndExit();
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
                        logger.info("onNext called for request failure handler with items: {}",
                            (booleans == null) ? "-1" : booleans.size());
                        if ((booleans == null) || booleans.isEmpty())
                            errorHandler.call();
                    }
                });
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        WrappedExecuteStageRequest request = createExecuteStageRequest(task);
        logger.info("Worker for task [{}] with startTimeout={}",
            task.getTaskId().getValue(), request.getRequest().getTimeoutToReportStart());

        setupRequestFailureHandler(request.getRequest().getTimeoutToReportStart(), request.getRequestSubject(),
            () -> sendLaunchError(driver, task));
        executeStageRequestObserver.onNext(request);

    }

    @Override
    public void registered(ExecutorDriver arg0, ExecutorInfo arg1,
                           FrameworkInfo arg2, SlaveInfo arg3) {
        // TODO Auto-generated method stub

    }

    @Override
    public void reregistered(ExecutorDriver arg0, SlaveInfo arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown(ExecutorDriver arg0) {
        // TODO Auto-generated method stub
    }

}
