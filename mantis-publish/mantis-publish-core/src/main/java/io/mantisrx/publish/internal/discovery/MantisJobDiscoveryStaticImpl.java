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
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.discovery.proto.StageWorkers;
import io.mantisrx.discovery.proto.StreamJobClusterMap;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use for local testing. Returns a static JobDiscoveryInfo configuration.
 */
public class MantisJobDiscoveryStaticImpl implements MantisJobDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobDiscoveryCachingImpl.class);

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String DEFAULT_JOB_CLUSTER = "SharedMrePublishEventSource";
    private static final String JOB_CLUSTER_LOOKUP_FAILED = "JobClusterLookupFailed";

    String mreAppJobClusterMapStr="{\"version\": \"1\", "
            + "\"timestamp\": 12345, "
            + "\"mappings\": "
            + "{\"__default__\": {\"requestEventStream\": \"SharedPushRequestEventSource\","
                                + "\"__default__\": \"" + DEFAULT_JOB_CLUSTER + "\"}}}";
    private AppJobClustersMap appJobClustersMap;
    private String workerHost;
    private int workerPort;

    /**
     * For connecting to locally running source jobs
     * use localhost:9090
     * @param host
     * @param port
     */
    public MantisJobDiscoveryStaticImpl(String host, int port) {
        try {
            this.workerHost = host;
            this.workerPort = port;
            appJobClustersMap = mapper.readValue(mreAppJobClusterMapStr, AppJobClustersMap.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<AppJobClustersMap> getJobClusterMappings(String app) {
        return Optional.of(appJobClustersMap);
    }

    @Override
    public Optional<JobDiscoveryInfo> getCurrentJobWorkers(String jobCluster) {
        Map<Integer, StageWorkers> stageWorkersMap = new HashMap<>();
        List<MantisWorker> workerList = new ArrayList<>();
        MantisWorker mantisWorker = new MantisWorker(workerHost, workerPort);
        workerList.add(mantisWorker);
        stageWorkersMap.put(1, new StageWorkers(jobCluster, jobCluster + "-1",1, workerList));
        JobDiscoveryInfo jobDiscoveryInfo = new JobDiscoveryInfo(jobCluster, jobCluster + "-1",stageWorkersMap);
        return Optional.of(jobDiscoveryInfo);
    }

    @Override
    public Map<String, String> getStreamNameToJobClusterMapping(String app) {
        String appName = DEFAULT_JOB_CLUSTER;
        Optional<AppJobClustersMap> jobClusterMappingsO = getJobClusterMappings(appName);

        if (jobClusterMappingsO.isPresent()) {
            AppJobClustersMap appJobClustersMap = jobClusterMappingsO.get();
            StreamJobClusterMap streamJobClusterMap = appJobClustersMap.getStreamJobClusterMap(appName);

            return streamJobClusterMap.getStreamJobClusterMap();
        } else {
            logger.info("Failed to lookup stream to job cluster mapping for app {}", appName);
            return Collections.emptyMap();
        }
    }

    @Override
    public String getJobCluster(String app, String stream) {
        String appName = DEFAULT_JOB_CLUSTER;
        Optional<AppJobClustersMap> jobClusterMappingsO = getJobClusterMappings(appName);

        if (jobClusterMappingsO.isPresent()) {
            AppJobClustersMap appJobClustersMap = jobClusterMappingsO.get();
            StreamJobClusterMap streamJobClusterMap = appJobClustersMap.getStreamJobClusterMap(appName);

            return streamJobClusterMap.getJobCluster(stream);
        } else {
            logger.info("Failed to lookup job cluster for app {} stream {}", appName, stream);
            return JOB_CLUSTER_LOOKUP_FAILED;
        }
    }
}
