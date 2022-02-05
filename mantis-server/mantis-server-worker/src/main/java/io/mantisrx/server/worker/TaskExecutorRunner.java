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

import com.google.common.util.concurrent.AbstractIdleService;
import com.mantisrx.common.utils.Services;
import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

@Slf4j
public class TaskExecutorRunner {

  public TaskExecutorServices createRemoteTaskExecutor(
      WorkerConfiguration workerConfiguration,
      Configuration configuration) throws Exception {
    FileSystem.initialize();
    RpcSystem rpcSystem =
        MantisAkkaRpcSystemLoader.load(configuration);
    RpcService rpcService =
        RpcUtils.createRemoteRpcService(
            rpcSystem,
            configuration,
            workerConfiguration.getExternalAddress(),
            workerConfiguration.getExternalPortRange(),
            workerConfiguration.getBindAddress(),
            Optional.ofNullable(workerConfiguration.getBindPort()));

    HighAvailabilityServices highAvailabilityServices =
        HighAvailabilityServicesUtil.createHAServices(workerConfiguration);

    final TaskExecutor taskExecutor = new TaskExecutor(
        rpcService,
        workerConfiguration,
        highAvailabilityServices,
        new DefaultClassLoaderHandle(
            BlobStoreFactory.get(workerConfiguration.getClusterStorageDir(),
                workerConfiguration.getLocalStorageDir()),
            workerConfiguration.getAlwaysParentFirstLoaderPatterns()),
        executeStageRequest -> new SubscriptionStateHandlerImpl(
            executeStageRequest.getJobId(),
            highAvailabilityServices.getMasterClientApi(),
            executeStageRequest.getSubscriptionTimeoutSecs(),
            executeStageRequest.getMinRuntimeSecs()));

    return new TaskExecutorServices(taskExecutor, highAvailabilityServices);
  }

  @RequiredArgsConstructor
  public static class TaskExecutorServices extends AbstractIdleService {

    private final TaskExecutor taskExecutor;
    private final HighAvailabilityServices highAvailabilityServices;

    @Override
    protected void startUp() throws Exception {
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
  }
}
