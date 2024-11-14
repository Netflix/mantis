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
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.util.Map;
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
        if (clusterID != null) {
            if (Strings.isNullOrEmpty(clusterID.getResourceID())) {
                log.error("Received empty resource id: {}", clusterID);
                throw new RuntimeException("Empty resourceID in clusterID for MantisScheduler");
            }

            return
                actorRefMap.computeIfAbsent(
                    clusterID,
                    (cid) -> {
                        log.info("Created scheduler actor for cluster: {}",
                            clusterID.getResourceID());
                        return new ResourceClusterAwareScheduler(actorSystem.actorOf(
                            ResourceClusterAwareSchedulerActor.props(
                                masterConfiguration.getSchedulerMaxRetries(),
                                masterConfiguration.getSchedulerMaxRetries(),
                                masterConfiguration.getSchedulerIntervalBetweenRetries(),
                                resourceClusters.getClusterFor(cid),
                                executeStageRequestFactory,
                                jobMessageRouter,
                                metricsRegistry),
                            "scheduler-for-" + cid.getResourceID()),
                            masterConfiguration.getSchedulerHandlesAllocationRetries());
                    });
        } else {
            log.error("Scheduler gets unexpected null clusterID");
            throw new RuntimeException("invalid null clusterID.");
        }
    }
}
