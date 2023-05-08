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

package io.mantisrx.master.api.akka.route.v1;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.common.metrics.spectator.MetricId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Sets;
import java.util.Set;

public class HttpRequestMetrics {
    public enum HttpVerb {
        GET,
        POST,
        PUT,
        DELETE
    }

    public static class Endpoints {
        public static final String JOB_ARTIFACTS = "api.v1.jobArtifacts";
        public static final String JOB_ARTIFACTS_NAMES = "api.v1.jobArtifacts.names";
        public static final String JOB_CLUSTERS = "api.v1.jobClusters";
        public static final String JOB_CLUSTER_INSTANCE = "api.v1.jobClusters.instance";
        public static final String JOB_CLUSTER_INSTANCE_LATEST_JOB_DISCOVERY_INFO = "api.v1.jobClusters.instance.latestJobDiscoveryInfo";
        public static final String JOB_CLUSTER_INSTANCE_ACTION_UPDATE_ARTIFACT = "api.v1.jobClusters.instance.actions.updateArtifact";

        public static final String JOB_CLUSTER_INSTANCE_SCHEDULING_INFO_UPDATE = "api.v1.jobClusters.instance.actions.updateSchedulingInfo";
        public static final String JOB_CLUSTER_INSTANCE_ACTION_UPDATE_SLA = "api.v1.jobClusters.instance.actions.updateSla";
        public static final String JOB_CLUSTER_INSTANCE_ACTION_UPDATE_MIGRATION_STRATEGY = "api.v1.jobClusters.instance.actions.updateMigrationStrategy";
        public static final String JOB_CLUSTER_INSTANCE_ACTION_UPDATE_LABEL = "api.v1.jobClusters.instance.actions.updateLabel";
        public static final String JOB_CLUSTER_INSTANCE_ACTION_ENABLE_CLUSTER = "api.v1.jobClusters.instance.actions.enableCluster";
        public static final String JOB_CLUSTER_INSTANCE_ACTION_DISABLE_CLUSTER = "api.v1.jobClusters.instance.actions.disableCluster";
        public static final String JOBS = "api.v1.jobs";
        public static final String JOB_CLUSTER_INSTANCE_JOBS = "api.v1.jobClusters.instance.jobs";
        public static final String JOB_INSTANCE = "api.v1.jobs.instance";
        public static final String JOB_INSTANCE_ARCHIVED_WORKERS = "api.v1.jobs.instance.archivedWorkers";
        public static final String JOB_CLUSTER_INSTANCE_JOB_INSTANCE = "api.v1.jobClusters.instance.jobs.instance";
        public static final String JOB_CLUSTER_INSTANCE_JOB_INSTANCE_ARCHIVED = "api.v1.jobClusters.instance.jobs.instance.archived";
        public static final String JOBS_ACTION_QUICKSUBMIT = "api.v1.jobs.actions.quickSubmit";
        public static final String JOBS_ACTION_POST_JOB_STATUS = "api.v1.jobs.actions.postJobStatus";
        public static final String JOB_INSTANCE_ACTION_SCALE_STAGE = "api.v1.jobs.instance.actions.scaleStage";
        public static final String JOB_INSTANCE_ACTION_RESUBMIT_WORKER = "api.v1.jobs.instance.actions.resubmitWorker";
        public static final String MASTER_INFO = "api.v1.masterInfo";
        public static final String MASTER_CONFIGS = "api.v1.masterConfigs";
        public static final String AGENT_CLUSTERS = "api.v1.agentClusters";
        public static final String AGENT_CLUSTERS_JOBS = "api.v1.agentClusters.jobs";
        public static final String AGENT_CLUSTERS_AUTO_SCALE_POLICY = "api.v1.agentClusters.autoScalePolicy";
        public static final String JOB_STATUS_STREAM = "api.v1.jobStatusStream.instance";
        public static final String JOB_DISCOVERY_STREAM = "api.v1.jobDiscoveryStream.instance";
        public static final String LAST_SUBMITTED_JOB_ID_STREAM = "api.v1.lastSubmittedJobIdStream.instance";

        public static final String RESOURCE_CLUSTERS = "api.v1.resourceClusters";

        private static String[] endpoints = new String[]{
                JOB_ARTIFACTS,
                JOB_ARTIFACTS_NAMES,
                JOB_CLUSTERS,
                JOB_CLUSTER_INSTANCE,
                JOB_CLUSTER_INSTANCE_LATEST_JOB_DISCOVERY_INFO,
                JOB_CLUSTER_INSTANCE_ACTION_UPDATE_ARTIFACT,
                JOB_CLUSTER_INSTANCE_ACTION_UPDATE_SLA,
                JOB_CLUSTER_INSTANCE_ACTION_UPDATE_MIGRATION_STRATEGY,
                JOB_CLUSTER_INSTANCE_ACTION_UPDATE_LABEL,
                JOB_CLUSTER_INSTANCE_ACTION_ENABLE_CLUSTER,
                JOB_CLUSTER_INSTANCE_ACTION_DISABLE_CLUSTER,
                JOBS,
                JOB_CLUSTER_INSTANCE_JOBS,
                JOB_INSTANCE,
                JOB_INSTANCE_ARCHIVED_WORKERS,
                JOB_CLUSTER_INSTANCE_JOB_INSTANCE,
                JOB_CLUSTER_INSTANCE_JOB_INSTANCE_ARCHIVED,
                JOBS_ACTION_QUICKSUBMIT,
                JOBS_ACTION_POST_JOB_STATUS,
                JOB_INSTANCE_ACTION_SCALE_STAGE,
                JOB_INSTANCE_ACTION_RESUBMIT_WORKER,
                MASTER_INFO,
                MASTER_CONFIGS,
                AGENT_CLUSTERS,
                AGENT_CLUSTERS_JOBS,
                AGENT_CLUSTERS_AUTO_SCALE_POLICY,
                JOB_STATUS_STREAM,
                JOB_DISCOVERY_STREAM,
                LAST_SUBMITTED_JOB_ID_STREAM,
                RESOURCE_CLUSTERS
        };

        private static Set<String> endpointSet = Sets.newHashSet(endpoints);
    }

    private final Registry registry;
    private static String METRIC_GROUP_ID = "apiv1";
    private static HttpRequestMetrics instance;

    private HttpRequestMetrics() {
        this.registry = SpectatorRegistryFactory.getRegistry();
    }

    public static HttpRequestMetrics getInstance() {
        if (instance == null) {
            instance = new HttpRequestMetrics();
        }
        return instance;
    }


    public void incrementEndpointMetrics(
            String endpoint,
            final Tag... tags) {
        Preconditions.checkArgument(
                Endpoints.endpointSet.contains(endpoint),
                String.format("endpoint %s is not valid", endpoint));
        MetricId id = new MetricId(METRIC_GROUP_ID, endpoint, tags);
        registry.counter(id.getSpectatorId(registry)).increment();
    }
}
