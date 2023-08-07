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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGatewayClient;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
  private final static AtomicReference<HighAvailabilityServices> HAServiceInstanceRef = new AtomicReference<>();

    public static HighAvailabilityServices createHAServices(CoreConfiguration configuration) {
    if (configuration.isLocalMode()) {
      log.warn("HA service running in local mode. This is only valid in local test.");
      if (HAServiceInstanceRef.get() == null) {
          String[] parts = configuration.getZkConnectionString().split(":");
          if (parts.length != 2) {
              throw new RuntimeException(
                  "invalid local mode connection string: " + configuration.getZkConnectionString());
          }

          int apiPort = Integer.parseInt(parts[1]);
          HAServiceInstanceRef.compareAndSet(null, new LocalHighAvailabilityServices(
              new MasterDescription(
                  parts[0],
                  "127.0.0.1",
                  apiPort,
                  apiPort,
                  apiPort,
                  "api/postjobstatus",
                  apiPort + 6,
                  System.currentTimeMillis())));
      }
    }
    else {
      if (HAServiceInstanceRef.get() == null) {
          HAServiceInstanceRef.compareAndSet(null, new ZkHighAvailabilityServices(configuration, new SimpleMeterRegistry()));
      }
    }

    return HAServiceInstanceRef.get();
  }

  private static class LocalHighAvailabilityServices extends AbstractIdleService implements HighAvailabilityServices {
    private final MasterMonitor masterMonitor;

    public LocalHighAvailabilityServices(MasterDescription masterDescription) {
        this.masterMonitor = new LocalMasterMonitor(masterDescription);
    }

    @Override
    public MantisMasterGateway getMasterClientApi() {
        return new MantisMasterClientApi(this.masterMonitor);
    }

    @Override
    public MasterMonitor getMasterMonitor() {
        return this.masterMonitor;
    }

    @Override
    public ResourceLeaderConnection<ResourceClusterGateway> connectWithResourceManager(ClusterID clusterID) {
        return new ResourceLeaderConnection<ResourceClusterGateway>() {
            final MasterMonitor masterMonitor = LocalHighAvailabilityServices.this.masterMonitor;

            @Override
            public ResourceClusterGateway getCurrent() {
                return new ResourceClusterGatewayClient(clusterID, masterMonitor.getLatestMaster());
            }

            @Override
            public void register(ResourceLeaderChangeListener<ResourceClusterGateway> changeListener) {
            }
        };
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }
  }

  /**
   * Zookeeper based implementation of HighAvailabilityServices that finds the various leader instances
   * through metadata stored on zookeeper.
   */
  private static class ZkHighAvailabilityServices extends AbstractIdleService implements
      HighAvailabilityServices {

    private final CuratorService curatorService;
    private final Counter resourceLeaderChangeCounter;
    private final Counter resourceLeaderAlreadyRegisteredCounter;
    private final AtomicInteger rmConnections = new AtomicInteger(0);

    public ZkHighAvailabilityServices(CoreConfiguration configuration, MeterRegistry meterRegistry) {
      curatorService = new CuratorService(configuration, meterRegistry);
        String groupName = "ZkHighAvailabilityServices";
        resourceLeaderChangeCounter = meterRegistry.counter(groupName + "_resourceLeaderChangeCounter");
        resourceLeaderAlreadyRegisteredCounter = meterRegistry.counter(groupName + "_resourceLeaderAlreadyRegisteredCounter");
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

                if (nextDescription.equals(((ResourceClusterGatewayClient)currentResourceClusterGateway).getMasterDescription())) {
                    resourceLeaderAlreadyRegisteredCounter.increment();
                    return;
                }
                ResourceClusterGateway previous = currentResourceClusterGateway;
                currentResourceClusterGateway = new ResourceClusterGatewayClient(clusterID, nextDescription);

                resourceLeaderChangeCounter.increment();
                changeListener.onResourceLeaderChanged(previous, currentResourceClusterGateway);
              });

          subscriptions.add(subscription);
        }
      };
    }
  }
}
