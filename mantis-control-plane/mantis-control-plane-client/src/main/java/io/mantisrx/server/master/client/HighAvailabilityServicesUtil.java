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

import com.typesafe.config.Config;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.core.zookeeper.ZookeeperSettings;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGatewayClient;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * HighAvailabilityServicesUtil helps you create HighAvailabilityServices instance based on the core configuration.
 */
@Slf4j
public class HighAvailabilityServicesUtil {
  private final static AtomicReference<HighAvailabilityClientServices> HAServiceInstanceRef = new AtomicReference<>();

  public static HighAvailabilityClientServices createHAServices(Config config) {
    String mode = config.getString("mantis.highAvailability.mode");
    if (mode.equals("local")) {
      throw new UnsupportedOperationException();
    } else if (mode.equalsIgnoreCase("zookeeper")) {
      if (HAServiceInstanceRef.get() == null) {
          HAServiceInstanceRef.compareAndSet(null, new ZkHighAvailabilityClientServices(config.getConfig("mantis.highAvailability.zookeeper")));
      }

      return HAServiceInstanceRef.get();
    } else {
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Zookeeper based implementation of HighAvailabilityServices that finds the various leader instances
   * through metadata stored on zookeeper.
   */
  private static class ZkHighAvailabilityClientServices extends AbstractIdleService implements
      HighAvailabilityClientServices {

    private final CuratorService curatorService;
    private final AtomicInteger rmConnections = new AtomicInteger(0);

    public ZkHighAvailabilityClientServices(Config config) {
      curatorService = new CuratorService(new ZookeeperSettings(config));
    }

    @Override
    protected void startUp() throws Exception {
      curatorService.start();
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
    public MasterMonitor getMasterMonitor() {
      return curatorService.getMasterMonitor();
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
                log.info("nextDescription={}", nextDescription);
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
