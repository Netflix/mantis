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


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.SchedulingService;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MantisSchedulerFactoryImpl implements MantisSchedulerFactory {
  private final ActorSystem actorSystem;
  private final ResourceClusters resourceClusters;
  private final ExecuteStageRequestFactory executeStageRequestFactory;
  private final JobMessageRouter jobMessageRouter;
  private final SchedulingService mesosSchedulingService;
  private final MasterConfiguration masterConfiguration;
  private final Map<ClusterID, ActorRef> actorRefMap = new HashMap<>();

  @Override
  public MantisScheduler forJob(JobDefinition jobDefinition) {
    Optional<ClusterID> clusterIDOptional = jobDefinition.getResourceCluster();

    if (clusterIDOptional.isPresent()) {
      ActorRef resourceClusterAwareSchedulerActor =
          actorRefMap.compute(clusterIDOptional.get(), (dontCare, oldRef) -> {
            if (oldRef != null) {
              return oldRef;
            } else {
              return actorSystem.actorOf(
                  ResourceClusterAwareSchedulerActor.props(
                      masterConfiguration.getSchedulerMaxRetries(),
                      masterConfiguration.getSchedulerMaxRetries(),
                      masterConfiguration.getSchedulerIntervalBetweenRetries(),
                      resourceClusters.getClusterFor(clusterIDOptional.get()),
                      executeStageRequestFactory, jobMessageRouter),
                  "scheduler-for-" + clusterIDOptional.get().getResourceID());
            }
          });
      return new ResourceClusterAwareScheduler(resourceClusterAwareSchedulerActor);
    } else {
      return mesosSchedulingService;
    }
  }
}
