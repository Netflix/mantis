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
package io.mantisrx.server.master.client;

import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGatewayClient;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

public class HighAvailabilityServicesUtil {

  public static HighAvailabilityServices createHAServices(CoreConfiguration configuration) {
    if (configuration.isLocalMode()) {
      return null;
    } else {
      return new ZkHighAvailabilityServices(configuration);
    }
  }

  private static class ZkHighAvailabilityServices extends AbstractIdleService implements
      HighAvailabilityServices {

    private final CuratorService curatorService;
    private final Duration startTimeout;
    private final AtomicInteger rmConnections = new AtomicInteger(0);

    public ZkHighAvailabilityServices(CoreConfiguration configuration) {
      curatorService = new CuratorService(configuration, null);
      this.startTimeout = Duration.ofMinutes(1);
    }

    @Override
    protected void startUp() throws Exception {
      curatorService.start();
      boolean started = curatorService.awaitRunning(startTimeout);
      if (!started) {
        throw new Exception("Did not start in time");
      }
    }

    @Override
    protected void shutDown() throws Exception {
      curatorService.shutdown();
    }

    @Override
    public MantisMasterGateway getMasterClientApi() {
      return new MantisMasterClientApi(curatorService.getMasterMonitor());
    }

    @Override
    public Observable<MasterDescription> getMasterDescription() {
      return curatorService.getMasterMonitor().getMasterObservable();
    }

    @Override
    public ResourceLeaderConnection<ResourceClusterGateway> connectWithResourceManager(
        ClusterID clusterID) {
      return new ResourceLeaderConnection<ResourceClusterGateway>() {
        final MasterMonitor masterMonitor = curatorService.getMasterMonitor();

        ResourceClusterGateway currentResourceClusterGateway =
            new ResourceClusterGatewayClient(clusterID, masterMonitor.getLatestMaster());

        final String nameFormat =
            "ResourceClusterGatewayCxn (" + rmConnections.getAndIncrement() + ")-%d";
        final Scheduler scheduler =
            Schedulers
                .from(
                    Executors
                        .newSingleThreadExecutor(
                            new ThreadFactoryBuilder().setNameFormat(nameFormat).build()));

        final List<Subscription> subscriptions = new ArrayList<>();

        @Override
        public ResourceClusterGateway getCurrent() {
          return currentResourceClusterGateway;
        }

        @Override
        public void register(ResourceLeaderChangeListener<ResourceClusterGateway> changeListener) {
          Subscription subscription = masterMonitor
              .getMasterObservable()
              .observeOn(scheduler)
              .subscribe(nextDescription -> {
                ResourceClusterGateway previous = currentResourceClusterGateway;
                currentResourceClusterGateway =
                    new ResourceClusterGatewayClient(clusterID, nextDescription);
                changeListener.onResourceLeaderChanged(previous, currentResourceClusterGateway);
              });

          subscriptions.add(subscription);
        }
      };
    }
  }
}
