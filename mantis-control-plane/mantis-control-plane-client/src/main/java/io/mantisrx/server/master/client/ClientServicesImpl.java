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

import io.mantisrx.server.core.highavailability.LeaderRetrievalService;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.master.MasterMonitorImpl;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGatewayClient;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Zookeeper based implementation of HighAvailabilityServices that finds the various leader instances
 * through metadata stored on zookeeper.
 */
@Slf4j
public class ClientServicesImpl implements ClientServices {

    private final AtomicInteger rmConnections = new AtomicInteger(0);
    private final MasterMonitor masterMonitor;

    public ClientServicesImpl(LeaderRetrievalService leaderRetrievalService) {
        this.masterMonitor = new MasterMonitorImpl(leaderRetrievalService);
    }

    @Override
    public void close() throws IOException {
        masterMonitor.close();
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
