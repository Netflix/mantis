/*
 * Copyright 2023 Netflix, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFrameworkFactory;
import io.mantisrx.shaded.org.apache.curator.retry.RetryOneTime;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.HttpUrl.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

@Slf4j
public class TestContainerHelloWorld {
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int CONTROL_PLANE_API_PORT = 8100;
    private static final int CONTROL_PLANE_API_V2_PORT = 8075;

    private static final String ZOOKEEPER_ALIAS = "zookeeper";

    private static final String CONTROL_PLANE_ALIAS = "mantiscontrolplane";

    private static final String CLUSTER_ID = "testcluster1";


    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void helloWorld() throws Exception {

        try (
            Network network = Network.newNetwork();
            GenericContainer<?> zookeeper =
                new GenericContainer<>("zookeeper:3.8.0")
                    .withNetwork(network)
                    .withNetworkAliases(ZOOKEEPER_ALIAS)
                    .withExposedPorts(ZOOKEEPER_PORT);

            GenericContainer<?> master =
            new GenericContainer<>("netflixoss/mantiscontrolplaneserver:latest")
                .withNetwork(network)
                .withNetworkAliases(CONTROL_PLANE_ALIAS)
                .withExposedPorts(CONTROL_PLANE_API_PORT);
                // .withExposedPorts(CONTROL_PLANE_API_V2_PORT); // failed to open?

        ) {
            zookeeper.start();
            zkCheck(zookeeper);

            // master.addEnv("MANTIS_ZOOKEEPER_CONNECTSTRING", "zk:2181");
            master.start();
            log.info("Finsih start");

            String url = String.format(
                "http://%s:%d/api/", master.getHost(), master.getMappedPort(CONTROL_PLANE_API_PORT));
            log.info("Using control plane url: " + url);

            OkHttpClient client = new OkHttpClient();

            Request request = new Request.Builder()
                .url(url + "v1/resourceClusters/list")
                .build();
            Response response = client.newCall(request).execute();
            log.info(response.body().string());

            // Create agent(s)
            final String agentId0 = "agent0";
            GenericContainer<?> agent0 = createAgent(agentId0, CLUSTER_ID, network);

            if (!ensureAgentStarted(master.getHost(),
                master.getMappedPort(CONTROL_PLANE_API_PORT),
                CLUSTER_ID,
                agentId0,
                agent0,
                5,
                Duration.ofSeconds(3).toMillis())) {
                    fail("Failed to register agent: " + agent0.getContainerId());
            }


            Thread.sleep(Duration.ofSeconds(360).toMillis());
        }

    }

    private void zkCheck(GenericContainer<?> zookeeper) throws Exception {
        final String path = "/messages/zk-tc";
        final String content = "Running Zookeeper with Testcontainers";
        String connectionString = zookeeper.getHost() + ":" + zookeeper.getMappedPort(ZOOKEEPER_PORT);
        log.info(connectionString);

        CuratorFramework curatorFramework = CuratorFrameworkFactory
            .builder()
            .connectString(connectionString)
            .retryPolicy(new RetryOneTime(100))
            .build();
        curatorFramework.start();
        curatorFramework.create().creatingParentsIfNeeded().forPath(path, content.getBytes());

        byte[] bytes = curatorFramework.getData().forPath(path);
        curatorFramework.close();

        assertEquals(new String(bytes, StandardCharsets.UTF_8), content);
        System.out.println("ZK check pass!");
    }

    private GenericContainer<?> createAgent(String agentId, String resourceClusterId, Network network) {
        return
            new GenericContainer<>("netflixoss/mantisserveragent:latest")
                .withEnv("resource_cluster_id".toUpperCase(), resourceClusterId)
                .withEnv("mantis_agent_id".toUpperCase(), agentId)
                .withNetwork(network);
    }

    private boolean ensureAgentStarted(
        String controlPlaneHost,
        int controlPlanePort,
        String resourceClusterId,
        String agentId,
        GenericContainer<?> agent,
        int retries,
        long sleepMillis)
        throws InterruptedException, IOException {
        agent.start();
        log.info("{} agent started.", agent.getContainerId());
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/resourceClusters")
            .addPathSegment(resourceClusterId)
            .addPathSegment("getRegisteredTaskExecutors")
            .build();
        log.info("Req: {}", reqUrl);

        for(int i = 0; i < retries; i++) {
            Request request = new Request.Builder()
                .url(reqUrl)
                .build();

            try {
                Response response = HTTP_CLIENT.newCall(request).execute();
                String responseBody = response.body().string();
                log.info("Registered agents: {}.", responseBody);

                List<TaskExecutorID> listResponses =
                    mapper.readValue(responseBody, new TypeReference<List<TaskExecutorID>>() {});

                for (TaskExecutorID taskExecutorID : listResponses) {
                    if (taskExecutorID.getResourceId().equals(agentId)) {
                        log.info("Agent {} has registered to {}.", agentId, resourceClusterId);
                        return true;
                    }
                }

                Thread.sleep(sleepMillis);
            } catch (Exception e) {
                log.warn("Get registred agent call error", e);
            }
        }
        return false;
    }
}
