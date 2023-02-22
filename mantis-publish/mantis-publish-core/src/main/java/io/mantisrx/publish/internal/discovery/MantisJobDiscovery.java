/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.publish.internal.discovery;


import io.mantisrx.discovery.proto.AppJobClustersMap;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import java.util.Map;
import java.util.Optional;

public interface MantisJobDiscovery {

    /**
     * Look up streamName to Mantis Job Cluster mappings for an app.
     *
     * @param app name of application
     *
     * @return AppJobClustersMap stream name to job cluster mappings
     */
    Optional<AppJobClustersMap> getJobClusterMappings(String app);

    /**
     * Look up the current set of Mantis Workers for given Mantis Job Cluster.
     *
     * @param jobCluster name of job cluster
     *
     * @return JobDiscoveryInfo worker locations for the most recent JobID of the job cluster
     */
    Optional<JobDiscoveryInfo> getCurrentJobWorkers(String jobCluster);

    /**
     * List of Job Clusters per stream configured to receive data for an app.
     *
     * @return Stream name to Job cluster mapping
     */
    Map<String, String> getStreamNameToJobClusterMapping(String app);

    /**
     * Look up the job cluster for a given app and stream.
     *
     * @param app upstream application which is producing events
     * @param stream the stream which the upstream application is producing to
     *
     * @return Job Cluster string
     */
    String getJobCluster(String app, String stream);
}
