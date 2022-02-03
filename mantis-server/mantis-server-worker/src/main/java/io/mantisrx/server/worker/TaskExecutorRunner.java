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

import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

@Slf4j
public class TaskExecutorRunner {

  public static int runTaskExecutor(
      WorkerConfiguration workerConfiguration,
      Configuration configuration) {

    try {
      FileSystem.initialize();
      RpcSystem rpcSystem = MantisAkkaRpcSystemLoader.load(configuration);
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

      highAvailabilityServices.startAsync().awaitRunning();

      final ClusterID clusterID = ClusterID.of(workerConfiguration.getClusterId());

      final TaskExecutor taskExecutor = new TaskExecutor(
          rpcService,
          workerConfiguration,
          highAvailabilityServices.getMasterClientApi(),
          new DefaultClassLoaderHandle(
              BlobStoreFactory.get(workerConfiguration.getClusterStorageDir(),
                  workerConfiguration.getLocalStorageDir()),
              workerConfiguration.getAlwaysParentFirstLoaderPatterns()),
          executeStageRequest -> new SubscriptionStateHandlerImpl(
              executeStageRequest.getJobId(),
              highAvailabilityServices.getMasterClientApi(),
              executeStageRequest.getSubscriptionTimeoutSecs(),
              executeStageRequest.getMinRuntimeSecs()),
          highAvailabilityServices.connectWithResourceManager(clusterID));

      taskExecutor.start();
      return 0;
    } catch (Exception e) {
      log.error("Failed to start", e);
      return -1;
    }
  }
}
