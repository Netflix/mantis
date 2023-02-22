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

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.*;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.publish.DefaultObjectMapper;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.mantisapi.DefaultMantisApiClient;
import io.mantisrx.publish.internal.discovery.mantisapi.MantisApiClient;
import io.mantisrx.publish.internal.discovery.proto.JobSchedulingInfo;
import io.mantisrx.publish.internal.discovery.proto.MantisJobState;
import io.mantisrx.publish.internal.discovery.proto.WorkerAssignments;
import io.mantisrx.publish.internal.discovery.proto.WorkerHost;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MantisJobDiscoveryTest {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobDiscoveryTest.class);
    private final Map<String, String> streamJobClusterMap = new HashMap<>();
    @Rule
    public WireMockRule mantisApi = new WireMockRule(options().dynamicPort());
    private MrePublishConfiguration config;
    private MantisJobDiscovery jobDiscovery;

    public MantisJobDiscoveryTest() {
        streamJobClusterMap.put(StreamType.DEFAULT_EVENT_STREAM, "RequestEventSubTrackerTestJobCluster");
        streamJobClusterMap.put(StreamType.LOG_EVENT_STREAM, "LogEventSubTrackerTestJobCluster");
    }

    private MrePublishConfiguration testConfig() {
        DefaultSettableConfig settableConfig = new DefaultSettableConfig();
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.DEFAULT_EVENT_STREAM, streamJobClusterMap.get(StreamType.DEFAULT_EVENT_STREAM));
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.LOG_EVENT_STREAM, streamJobClusterMap.get(StreamType.LOG_EVENT_STREAM));
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.SUBS_REFRESH_INTERVAL_SEC_PROP, 30);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.JOB_DISCOVERY_REFRESH_INTERVAL_SEC_PROP, 1);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.DISCOVERY_API_HOSTNAME_PROP, "127.0.0.1");
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.DISCOVERY_API_PORT_PROP, mantisApi.port());

        PropertyRepository propertyRepository = DefaultPropertyFactory.from(settableConfig);

        return new SampleArchaiusMrePublishConfiguration(propertyRepository);
    }

    @BeforeEach
    public void setup() {
        mantisApi.start();
        config = testConfig();
        Registry registry = new DefaultRegistry();
        MantisApiClient mantisApiClient = new DefaultMantisApiClient(config, HttpClient.create(registry));
        this.jobDiscovery = new MantisJobDiscoveryCachingImpl(config, registry, mantisApiClient);
    }

    @AfterEach
    public void teardown() {
        mantisApi.shutdown();
    }

    @Test
    public void testJobDiscoveryFetch() throws IOException {
        String jobCluster = "MantisJobDiscoveryTestJobCluster";
        String jobId = jobCluster + "-1";

        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(jobId, Collections.singletonMap(1, new WorkerAssignments(1, 1, Collections.singletonMap(1,
                new WorkerHost("127.0.0.1", 0, Collections.emptyList(), MantisJobState.Started, 1, 7777, 7151)
        ))));
        //        JobDiscoveryInfo jobDiscoveryInfo = new JobDiscoveryInfo(jobCluster, jobId,
        //            Collections.singletonMap(1, new StageWorkers(jobCluster, jobId, 1, Arrays.asList(
        //                new MantisWorker("127.0.0.1", 7151)
        //            ))));

        // worker 1 subs list
        mantisApi.stubFor(get("/jobClusters/discoveryInfo/" + jobCluster)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(jobSchedulingInfo)))
        );

        Optional<JobDiscoveryInfo> currentJobWorkers = jobDiscovery.getCurrentJobWorkers(jobCluster);
        assertTrue(currentJobWorkers.isPresent());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().size(), currentJobWorkers.get().getIngestStageWorkers().getWorkers().size());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getCustomPort(), currentJobWorkers.get().getIngestStageWorkers().getWorkers().get(0).getPort());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getHost(), currentJobWorkers.get().getIngestStageWorkers().getWorkers().get(0).getHost());
    }

    @Test
    @Ignore("another flaky test")
    public void testJobDiscoveryFetchFailureHandlingAfterSuccess() throws IOException, InterruptedException {
        String jobCluster = "MantisJobDiscoveryTestJobCluster";
        String jobId = jobCluster + "-1";

        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(jobId, Collections.singletonMap(1, new WorkerAssignments(1, 1, Collections.singletonMap(1,
                new WorkerHost("127.0.0.1", 0, Collections.emptyList(), MantisJobState.Started, 1, 7777, 7151)
        ))));

        //        JobDiscoveryInfo jobDiscoveryInfo = new JobDiscoveryInfo(jobCluster, jobId,
        //            Collections.singletonMap(1, new StageWorkers(jobCluster, jobId, 1, Arrays.asList(
        //                new MantisWorker("127.0.0.1", 7151)
        //            ))));

        // worker 1 subs list
        mantisApi.stubFor(get("/jobClusters/discoveryInfo/" + jobCluster)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(jobSchedulingInfo)))
        );

        Optional<JobDiscoveryInfo> currentJobWorkers = jobDiscovery.getCurrentJobWorkers(jobCluster);
        assertTrue(currentJobWorkers.isPresent());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().size(), currentJobWorkers.get().getIngestStageWorkers().getWorkers().size());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getCustomPort(), currentJobWorkers.get().getIngestStageWorkers().getWorkers().get(0).getPort());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getHost(), currentJobWorkers.get().getIngestStageWorkers().getWorkers().get(0).getHost());
        logger.info("sleep to force a refresh after configured interval");
        Thread.sleep((config.jobDiscoveryRefreshIntervalSec() + 1) * 1000);
        mantisApi.stubFor(get("/jobClusters/discoveryInfo/" + jobCluster)
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(jobSchedulingInfo)))
        );

        Optional<JobDiscoveryInfo> currentJobWorkers2 = jobDiscovery.getCurrentJobWorkers(jobCluster);
        assertTrue(currentJobWorkers2.isPresent());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().size(), currentJobWorkers2.get().getIngestStageWorkers().getWorkers().size());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getCustomPort(), currentJobWorkers2.get().getIngestStageWorkers().getWorkers().get(0).getPort());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getHost(), currentJobWorkers2.get().getIngestStageWorkers().getWorkers().get(0).getHost());
        logger.info("sleep to let async refresh complete");
        Thread.sleep(1000);
        mantisApi.verify(2, getRequestedFor(urlMatching("/jobClusters/discoveryInfo/" + jobCluster)));

        // should continue serving old Job Discovery data after a 503
        Optional<JobDiscoveryInfo> currentJobWorkers3 = jobDiscovery.getCurrentJobWorkers(jobCluster);
        assertTrue(currentJobWorkers3.isPresent());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().size(), currentJobWorkers3.get().getIngestStageWorkers().getWorkers().size());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getCustomPort(), currentJobWorkers3.get().getIngestStageWorkers().getWorkers().get(0).getPort());
        assertEquals(jobSchedulingInfo.getWorkerAssignments().get(1).getHosts().get(1).getHost(), currentJobWorkers3.get().getIngestStageWorkers().getWorkers().get(0).getHost());
    }

    @Test
    public void testJobDiscoveryFetch4XXRespHandling() throws InterruptedException {
        String jobCluster = "MantisJobDiscoveryTestJobCluster";

        // worker 1 subs list
        mantisApi.stubFor(get("/jobClusters/discoveryInfo/" + jobCluster)
                .willReturn(aResponse()
                        .withStatus(404)
                        .withBody("")
                ));

        int iterations = 3;
        for (int i = 0; i < iterations; i++) {
            Optional<JobDiscoveryInfo> currentJobWorkers = jobDiscovery.getCurrentJobWorkers(jobCluster);
            assertFalse(currentJobWorkers.isPresent());
            Thread.sleep((config.jobDiscoveryRefreshIntervalSec() + 1) * 1000);
        }
        mantisApi.verify(iterations, getRequestedFor(urlMatching("/jobClusters/discoveryInfo/" + jobCluster)));
    }
}
