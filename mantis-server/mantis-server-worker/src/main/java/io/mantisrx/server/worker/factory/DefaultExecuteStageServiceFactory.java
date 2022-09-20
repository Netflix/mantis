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

package io.mantisrx.server.worker.factory;

import io.mantisrx.runtime.Job;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.ClassLoaderHandle;
import io.mantisrx.server.worker.ExecuteStageRequestService;
import io.mantisrx.server.worker.SinkSubscriptionStateHandler.Factory;
import io.mantisrx.server.worker.WorkerExecutionOperations;
import io.mantisrx.server.worker.WorkerExecutionOperationsNetworkStage;
import io.mantisrx.server.worker.WrappedExecuteStageRequest;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.server.worker.mesos.VirtualMachineTaskStatus;
import java.util.Optional;
import rx.Observable;
import rx.Observer;

public class DefaultExecuteStageServiceFactory implements ExecuteStageServiceFactory {
    @Override
    public ExecuteStageService getExecuteStageService(
        ExecuteStageRequest executeStageRequest,
        Observable<WrappedExecuteStageRequest> executeStageRequestObservable,
        Observer<Observable<Status>> tasksStatusObserver,
        WorkerExecutionOperations executionOperations,
        ClassLoaderHandle classLoaderHandle,
        Optional<String> jobProviderClass,
        Optional<Job> mantisJob) {
        return new ExecuteStageRequestService(
            executeStageRequestObservable,
            tasksStatusObserver,
            executionOperations,
            classLoaderHandle,
            jobProviderClass,
            mantisJob);
    }

    @Override
    public ExecuteStageService getExecuteStageService(
        ExecuteStageRequest executeStageRequest,
        Observable<WrappedExecuteStageRequest> executeStageRequestObservable,
        Observer<Observable<Status>> tasksStatusObserver,
        Observer<VirtualMachineTaskStatus> vmTaskStatusObserver,
        MantisMasterGateway mantisMasterApi,
        WorkerConfiguration config,
        WorkerMetricsClient workerMetricsClient,
        Factory sinkSubscriptionStateHandlerFactory,
        Optional<String> hostname,
        ClassLoaderHandle classLoaderHandle,
        Optional<String> jobProviderClass,
        Optional<Job> mantisJob) {
        return new ExecuteStageRequestService(
            executeStageRequestObservable,
            tasksStatusObserver,
            new WorkerExecutionOperationsNetworkStage(
                vmTaskStatusObserver,
                mantisMasterApi,
                config,
                workerMetricsClient,
                sinkSubscriptionStateHandlerFactory,
                hostname
            ),
            classLoaderHandle,
            jobProviderClass,
            mantisJob);
    }
}
