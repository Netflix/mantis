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

import com.mantisrx.common.utils.Services;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.ILeaderMonitorFactory;
import io.mantisrx.server.core.master.LocalLeaderFactory;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.master.ZookeeperLeaderMonitorFactory;
import io.mantisrx.server.core.master.ZookeeperMasterMonitor;
import io.mantisrx.server.core.utils.ConfigUtils;
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
                  System.currentTimeMillis()),
              configuration));
      }
    }
    else {
      if (HAServiceInstanceRef.get() == null) {
          HAServiceInstanceRef.compareAndSet(null, new HighAvailabilityServicesImpl(configuration));
      }
    }

    return HAServiceInstanceRef.get();
  }

  // This getter is used in situations where the context does not know the core configuration.  For example, this
  // is used to create a MantisClient when configuring a JobSource, where a job instance does not know how Mantis
  // is configured.
  // Note that in this context, the agent should have configured HighAvailabilityServices.
  public static HighAvailabilityServices get() {
      if (HAServiceInstanceRef.get() == null) {
          throw new RuntimeException("HighAvailabilityServices have not been initialized");
      }
      return HAServiceInstanceRef.get();
  }

  private static class LocalHighAvailabilityServices extends AbstractIdleService implements HighAvailabilityServices {
    private final MasterMonitor masterMonitor;
    private final CoreConfiguration configuration;

    public LocalHighAvailabilityServices(MasterDescription masterDescription, CoreConfiguration configuration) {
        this.masterMonitor = new LocalMasterMonitor(masterDescription);
        this.configuration = configuration;
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
                return new ResourceClusterGatewayClient(clusterID, masterMonitor.getLatestMaster(), configuration);
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
    private static class HighAvailabilityServicesImpl extends AbstractIdleService implements
        HighAvailabilityServices {

        private final MasterMonitor masterMonitor;
        private final Counter resourceLeaderChangeCounter;
        private final Counter resourceLeaderAlreadyRegisteredCounter;
        private final Counter resourceLeaderIsEmptyCounter;
        private final AtomicInteger rmConnections = new AtomicInteger(0);
        private final CoreConfiguration configuration;

        public HighAvailabilityServicesImpl(CoreConfiguration configuration) {
            this.configuration = configuration;
            final ILeaderMonitorFactory factory = ConfigUtils.createInstance(configuration.getLeaderMonitorFactoryName(), ILeaderMonitorFactory.class);
            if(factory instanceof LocalLeaderFactory) {
                log.warn("using default non-local Zookeeper leader monitoring you should set: "+
                    "mantis.leader.monitor.factory=io.mantisrx.server.core.master.ZookeeperLeaderMonitorFactory");
                masterMonitor = new ZookeeperLeaderMonitorFactory().createLeaderMonitor(configuration);
            } else {
                masterMonitor = factory.createLeaderMonitor(configuration);
            }
            // this for backward compatibility, but should be modified to "HighAvailabilityServices" in the future
            final String metricsGroup = masterMonitor instanceof ZookeeperMasterMonitor ? "ZkHighAvailabilityServices" :
                "HighAvailabilityServices";

            final Metrics metrics = MetricsRegistry.getInstance().registerAndGet(new Metrics.Builder()
                .name(metricsGroup)
                .addCounter("resourceLeaderChangeCounter")
                .addCounter("resourceLeaderAlreadyRegisteredCounter")
                .addCounter("resourceLeaderIsEmptyCounter")
                .build());
            resourceLeaderChangeCounter = metrics.getCounter("resourceLeaderChangeCounter");
            resourceLeaderAlreadyRegisteredCounter = metrics.getCounter("resourceLeaderAlreadyRegisteredCounter");
            resourceLeaderIsEmptyCounter = metrics.getCounter("resourceLeaderIsEmptyCounter");

        }

        @Override
        protected void startUp() throws Exception {
            masterMonitor.start();
        }

        @Override
        protected void shutDown() throws Exception {
            masterMonitor.shutdown();
        }

        @Override
        public MantisMasterGateway getMasterClientApi() {
            return new MantisMasterClientApi(masterMonitor);
        }

        @Override
        public MasterMonitor getMasterMonitor() {
            return masterMonitor;
        }

        @Override
        public ResourceLeaderConnection<ResourceClusterGateway> connectWithResourceManager(
            ClusterID clusterID) {
            return new ResourceLeaderConnection<ResourceClusterGateway>() {
                ResourceClusterGateway currentResourceClusterGateway =
                    new ResourceClusterGatewayClient(clusterID, masterMonitor.getLatestMaster(), configuration);

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

                            // We do not want to update if the master is set to null.  This is usually due to a newly
                            // initialized master monitor.
                            if (nextDescription.equals(MasterDescription.MASTER_NULL)) {
                                resourceLeaderIsEmptyCounter.increment();
                                return;
                            } else if (nextDescription.equals(((ResourceClusterGatewayClient)currentResourceClusterGateway).getMasterDescription())) {
                                resourceLeaderAlreadyRegisteredCounter.increment();
                                return;
                            }
                            ResourceClusterGateway previous = currentResourceClusterGateway;
                            currentResourceClusterGateway = new ResourceClusterGatewayClient(clusterID, nextDescription, configuration);

                            resourceLeaderChangeCounter.increment();
                            changeListener.onResourceLeaderChanged(previous, currentResourceClusterGateway);
                        });

                    subscriptions.add(subscription);
                }
            };
        }
    }
}
