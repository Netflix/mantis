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

package io.mantisrx.publish.internal.discovery.mantisapi;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.Lists;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.discovery.proto.AppJobClustersMap;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.discovery.proto.StageWorkers;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultMantisApiClientTest {
    @Rule
    public WireMockRule mantisAPI = new WireMockRule(options().dynamicPort());

    @BeforeEach
    public void setup() {
        mantisAPI.start();
    }

    @AfterEach
    public void teardown() {
        mantisAPI.shutdown();
    }

    @Test
    public void simpleTest() throws Exception {
        mantisAPI.stubFor(get(urlMatching("/jobClusters/discoveryInfo/TestJob"))
            .willReturn(aResponse()
            .withStatus(200)
            .withBody(readStub("mantisapi/jobclusters_discoveryinfo_stub.json"))));

        mantisAPI.stubFor(get(urlMatching("/api/v1/mantis/publish/streamJobClusterMap\\?app=testApp"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(readStub("mantisapi/jobclustermap_stub.json"))));

        final DefaultRegistry registry = new DefaultRegistry();
        final Properties props = new Properties();
        final DefaultSettableConfig config = new DefaultSettableConfig();

        props.put("mantis.publish.discovery.api.hostname", "127.0.0.1");
        props.put("mantis.publish.discovery.api.port", mantisAPI.port());
        config.setProperties(props);

        final PropertyRepository propsRepo = DefaultPropertyFactory.from(config);
        final DefaultMantisApiClient defaultMantisApiClient = new DefaultMantisApiClient(new SampleArchaiusMrePublishConfiguration(propsRepo), HttpClient.create(registry));
        final CompletableFuture<JobDiscoveryInfo> jobDiscoveryInfoCompletableFuture = defaultMantisApiClient.jobDiscoveryInfo("TestJob");
        final JobDiscoveryInfo jobDiscoveryInfo = jobDiscoveryInfoCompletableFuture.get(5, TimeUnit.SECONDS);
        final CompletableFuture<AppJobClustersMap> jobClusterMapping = defaultMantisApiClient.getJobClusterMapping(Optional.of("testApp"));
        final AppJobClustersMap appJobClustersMap = jobClusterMapping.get(5, TimeUnit.SECONDS);

        final Map<Integer, StageWorkers> stageWorkersMap = new HashMap<>();
        stageWorkersMap.put(1, new StageWorkers("TestJob",
            "TestJob",
            1,
            Lists.newArrayList(new MantisWorker("111.11.111.41", 7158), new MantisWorker("111.11.11.42", 7158))));
        final JobDiscoveryInfo expectedInfo = new JobDiscoveryInfo("TestJob", "TestJob", stageWorkersMap);

        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, String> vals = new HashMap<>();
        vals.put("__default__", "SharedMrePublishEventSource");
        mappings.put("testApp", vals);
        final AppJobClustersMap expectedMap = new AppJobClustersMap("1", 3, mappings);

        Assertions.assertEquals(expectedInfo, jobDiscoveryInfo);
        Assertions.assertEquals(expectedMap, appJobClustersMap);
    }

    private static final byte[] readStub(String resourceFile) throws IOException {
        InputStream inputStream = DefaultMantisApiClientTest.class.getClassLoader().getResourceAsStream(resourceFile);
        return IOUtils.toByteArray(inputStream);
    }
}
