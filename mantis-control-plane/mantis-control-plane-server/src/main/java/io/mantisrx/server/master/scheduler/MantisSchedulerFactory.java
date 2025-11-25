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

import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Factory for the Mantis Scheduler based on the JobDefinition
 */
@FunctionalInterface
public interface MantisSchedulerFactory {
    default MantisScheduler forJob(JobDefinition jobDefinition) {
        Optional<ClusterID> clusterIDOptional = jobDefinition.getResourceCluster();
        return forClusterID(clusterIDOptional.orElse(null));
    }

    /**
     * returns the MantisScheduler based on the ClusterID.
     *
     * @param clusterID cluster ID for which the mantisscheduler is requested.
     * @return MantisScheduler corresponding to the ClusterID.
     */
    MantisScheduler forClusterID(@Nullable ClusterID clusterID);

    // todo add api to mark registry ready when init success on JobClustersManagerActor.
}
