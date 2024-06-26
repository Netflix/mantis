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
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.runtime.loader.ClassLoaderHandle;
import io.mantisrx.runtime.loader.TaskFactory;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import io.vavr.Tuple;
import io.vavr.Tuple2;
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
    private final RpcSystem rpcSystem;

    @Override
    protected void startUp() {
        // RxJava 1.x’s  rx.ring-buffer.size property stores the size of any in-memory ring buffers that RxJava
        // uses when an Observable cannot keep up with rate of event emissions. The ring buffer size is explicitly
        // set to a low number (8) because different platforms have different defaults for this value. While the
        // Java Virtual Machine (JVM) default is 128 items per ring buffer, Android has a much lower limit of 16.
        // 1024 is a well-tested value in Netflix on many production use cases.
        // source: From https://www.uber.com/blog/rxjava-backpressure/
        System.setProperty("rx.ring-buffer.size", "1024");

        highAvailabilityServices.startAsync().awaitRunning();

        taskExecutor.start();
        try {
            taskExecutor.awaitRunning().get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void shutDown() throws Exception {
        taskExecutor
            .closeAsync()
            .exceptionally(throwable -> null)
            .thenCompose(dontCare -> Services.stopAsync(highAvailabilityServices,
                MoreExecutors.directExecutor()))
            .thenRunAsync(rpcSystem::close)
            .get();
    }

    public TaskExecutor getTaskExecutor() {
        return this.taskExecutor;
    }

    public static TaskExecutorStarterBuilder builder(WorkerConfiguration workerConfiguration) {
        return new TaskExecutorStarterBuilder(workerConfiguration);
    }

    @SuppressWarnings("unused")
    public static class TaskExecutorStarterBuilder {
        private final WorkerConfiguration workerConfiguration;
        private Configuration configuration;
        private MantisPropertiesLoader propertiesLoader;
        @Nullable
        private RpcSystem rpcSystem;
        @Nullable
        private RpcService rpcService;
        @Nullable
        private ClassLoaderHandle classLoaderHandle;
        private final HighAvailabilityServices highAvailabilityServices;
        @Nullable
        private TaskFactory taskFactory;

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

        public TaskExecutorStarterBuilder taskFactory(TaskFactory taskFactory) {
            this.taskFactory = taskFactory;
            return this;
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

        public TaskExecutorStarterBuilder addListener(TaskExecutor.Listener listener, Executor executor) {
            this.listeners.add(Tuple.of(listener, executor));
            return this;
        }

        public TaskExecutorStarterBuilder propertiesLoader(MantisPropertiesLoader propertiesLoader) {
            this.propertiesLoader = propertiesLoader;
            return this;
        }

        public TaskExecutorStarter build() throws Exception {
            final TaskExecutor taskExecutor =
                new TaskExecutor(
                    getRpcService(),
                    Preconditions.checkNotNull(workerConfiguration, "WorkerConfiguration for TaskExecutor is null"),
                    Preconditions.checkNotNull(propertiesLoader, "propertiesLoader for TaskExecutor is null"),
                    highAvailabilityServices,
                    getClassLoaderHandle(),
                    this.taskFactory
                );

            for (Tuple2<TaskExecutor.Listener, Executor> listener : listeners) {
                taskExecutor.addListener(listener._1(), listener._2());
            }

            return new TaskExecutorStarter(taskExecutor, highAvailabilityServices, getRpcSystem());
        }
    }
}
