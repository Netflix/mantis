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

package io.mantisrx.server.master.scheduler;

import akka.actor.ActorSystem;
import io.mantisrx.common.Ack;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class MantisSchedulerFactoryImpl implements MantisSchedulerFactory {

    private final ActorSystem actorSystem;
    private final ResourceClusters resourceClusters;
    private final ExecuteStageRequestFactory executeStageRequestFactory;
    private final JobMessageRouter jobMessageRouter;
    private final MasterConfiguration masterConfiguration;
    private final MetricsRegistry metricsRegistry;
    private final Map<ClusterID, MantisScheduler> actorRefMap = new ConcurrentHashMap<>();

    @Override
    public MantisScheduler forClusterID(ClusterID clusterID) {
        if (clusterID == null || Strings.isNullOrEmpty(clusterID.getResourceID())) {
            throw new RuntimeException("Invalid clusterID for MantisScheduler");
        }
        return actorRefMap.computeIfAbsent(clusterID, this::createScheduler);
    }

    private MantisScheduler createScheduler(ClusterID clusterID) {
        log.info("Creating scheduler for cluster: {} (reservationEnabled={})",
            clusterID.getResourceID(),
            masterConfiguration.isReservationSchedulingEnabled());

        if (masterConfiguration.isReservationSchedulingEnabled()) {
            return new ResourceClusterReservationAwareScheduler(
                resourceClusters.getClusterFor(clusterID));
        } else {
            return new ResourceClusterAwareScheduler(actorSystem.actorOf(
                ResourceClusterAwareSchedulerActor.props(
                    masterConfiguration.getSchedulerMaxRetries(),
                    masterConfiguration.getSchedulerMaxRetries(),
                    masterConfiguration.getSchedulerIntervalBetweenRetries(),
                    resourceClusters.getClusterFor(clusterID),
                    executeStageRequestFactory,
                    jobMessageRouter,
                    metricsRegistry),
                "scheduler-for-" + clusterID.getResourceID()),
                masterConfiguration.getSchedulerHandlesAllocationRetries());
        }
    }

    /**
     * Mark all reservation registries as ready after master initialization.
     * Should be called after all jobs have been recovered.
     *
     * @return Future that completes when all registries are ready
     */
    public CompletableFuture<Ack> markAllRegistriesReady() {
        if (masterConfiguration.isReservationSchedulingEnabled()) {
            log.info("Marking all reservation registries as ready");
            return resourceClusters.markAllRegistriesReady();
        } else {
            log.debug("Reservation scheduling disabled, skipping markAllRegistriesReady");
            return CompletableFuture.completedFuture(Ack.getInstance());
        }
    }
}
