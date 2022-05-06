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

import com.mantisrx.common.utils.Services;
import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.master.client.ClassLoaderHandle;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.client.SinkSubscriptionStateHandler;
import io.mantisrx.server.master.client.config.WorkerConfiguration;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

/**
 * TaskExecutorStarter class represents the starting point for a task executor.
 * Use the {@link TaskExecutorStarterBuilder} to build {@link TaskExecutorStarter}.
 * Once the service is build, start and stop it during the lifecycle of your runtime framework such as spring.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class TaskExecutorStarter extends AbstractIdleService {
    private final TaskExecutor taskExecutor;
    private final HighAvailabilityServices highAvailabilityServices;

    @Override
    protected void startUp() {
        highAvailabilityServices.startAsync().awaitRunning();

        taskExecutor.start();
    }

    @Override
    protected void shutDown() throws Exception {
        taskExecutor
            .closeAsync()
            .exceptionally(throwable -> null)
            .thenCompose(dontCare -> Services.stopAsync(highAvailabilityServices,
                MoreExecutors.directExecutor()))
            .get();
    }

    public static TaskExecutorStarterBuilder builder(WorkerConfiguration workerConfiguration) {
        return new TaskExecutorStarterBuilder(workerConfiguration);
    }

    @SuppressWarnings("unused")
    public static class TaskExecutorStarterBuilder {
        private final WorkerConfiguration workerConfiguration;
        private Configuration configuration;
        @Nullable
        private RpcSystem rpcSystem;
        @Nullable
        private RpcService rpcService;
        @Nullable
        private ClassLoaderHandle classLoaderHandle;
        private final HighAvailabilityServices highAvailabilityServices;
        @Nullable
        private SinkSubscriptionStateHandler.Factory sinkSubscriptionHandlerFactory;
        private final List<Tuple2<TaskExecutor.Listener, Executor>> listeners = new ArrayList<>();

        private TaskExecutorStarterBuilder(WorkerConfiguration workerConfiguration) {
            this.workerConfiguration = workerConfiguration;
            this.configuration = new Configuration();
            this.highAvailabilityServices = HighAvailabilityServicesUtil.createHAServices(workerConfiguration);
        }

        public TaskExecutorStarterBuilder configuration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public TaskExecutorStarterBuilder rpcSystem(RpcSystem rpcSystem) {
            Preconditions.checkNotNull(rpcSystem);
            this.rpcSystem = rpcSystem;
            return this;
        }

        private RpcSystem getRpcSystem() {
            if (this.rpcSystem == null) {
                return MantisAkkaRpcSystemLoader.getInstance();
            } else {
                return this.rpcSystem;
            }
        }

        public TaskExecutorStarterBuilder rpcService(RpcService rpcService) {
            Preconditions.checkNotNull(rpcService);
            this.rpcService = rpcService;
            return this;
        }

        private RpcService getRpcService() throws Exception {
            if (this.rpcService == null) {
                return RpcUtils.createRemoteRpcService(
                    getRpcSystem(),
                    configuration,
                    workerConfiguration.getExternalAddress(),
                    workerConfiguration.getExternalPortRange(),
                    workerConfiguration.getBindAddress(),
                    Optional.ofNullable(workerConfiguration.getBindPort()));
            } else {
                return this.rpcService;
            }
        }

        public TaskExecutorStarterBuilder classLoaderHandle(ClassLoaderHandle classLoaderHandle) {
            this.classLoaderHandle = classLoaderHandle;
            return this;
        }

        private ClassLoaderHandle getClassLoaderHandle() throws Exception {
            if (this.classLoaderHandle == null) {
                return new BlobStoreAwareClassLoaderHandle(
                    BlobStore.forHadoopFileSystem(
                        workerConfiguration.getBlobStoreArtifactDir(),
                        workerConfiguration.getLocalStorageDir()));
            } else {
                return this.classLoaderHandle;
            }
        }

        public TaskExecutorStarterBuilder sinkSubscriptionHandlerFactory(SinkSubscriptionStateHandler.Factory sinkSubscriptionHandlerFactory) {
            this.sinkSubscriptionHandlerFactory = sinkSubscriptionHandlerFactory;
            return this;
        }

        public TaskExecutorStarterBuilder addListener(TaskExecutor.Listener listener, Executor executor) {
            this.listeners.add(Tuple.of(listener, executor));
            return this;
        }

        private SinkSubscriptionStateHandler.Factory getSinkSubscriptionHandlerFactory() {
            if (this.sinkSubscriptionHandlerFactory == null) {
                return SinkSubscriptionStateHandler.Factory.forEphemeralJobsThatNeedToBeKilledInAbsenceOfSubscriber(
                    highAvailabilityServices.getMasterClientApi(),
                    Clock.systemDefaultZone());
            } else {
                return this.sinkSubscriptionHandlerFactory;
            }
        }

        public TaskExecutorStarter build() throws Exception {
            final TaskExecutor taskExecutor =
                new TaskExecutor(
                    getRpcService(),
                    workerConfiguration,
                    highAvailabilityServices,
                    getClassLoaderHandle(),
                    getSinkSubscriptionHandlerFactory());

            for (Tuple2<TaskExecutor.Listener, Executor> listener : listeners) {
                taskExecutor.addListener(listener._1(), listener._2());
            }

            return new TaskExecutorStarter(taskExecutor, highAvailabilityServices);
        }
    }
}
