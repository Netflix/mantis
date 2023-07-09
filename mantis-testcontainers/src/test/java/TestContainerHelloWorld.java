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

import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFrameworkFactory;
import io.mantisrx.shaded.org.apache.curator.retry.RetryOneTime;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class TestContainerHelloWorld {
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int CONTROL_PLANE_API_PORT = 8100;
    private static final int CONTROL_PLANE_API_V2_PORT = 8075;

    private static final String ZOOKEEPER_ALIAS = "zookeeper";

    private static final String CONTROL_PLANE_ALIAS = "mantiscontrolplane";


    @Test
    public void helloWorld() throws Exception {
        String path = "/messages/zk-tc";
        String content = "Running Zookeeper with Testcontainers";
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


            String connectionString = zookeeper.getHost() + ":" + zookeeper.getMappedPort(ZOOKEEPER_PORT);
            System.out.println(connectionString);

            // master.addEnv("MANTIS_ZOOKEEPER_CONNECTSTRING", "zk:2181");
            master.start();
            System.out.println("Finsih start");

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

            String url = String.format(
                "http://%s:%d/api/", master.getHost(), master.getMappedPort(CONTROL_PLANE_API_PORT));
            System.out.println("Using url: " + url);

            OkHttpClient client = new OkHttpClient();

            Request request = new Request.Builder()
                .url(url + "v1/resourceClusters/list")
                .build();
            Response response = client.newCall(request).execute();
            System.out.println(response.body().string());

            // Create agent(s)
            GenericContainer<?> agent0 =
                new GenericContainer<>("netflixoss/mantisserveragent:latest")
                    .withNetwork(network)
                    .withEnv("resource_cluster_id".toUpperCase(), "testcluster1");
            agent0.start();
            System.out.println("agent started.");

            Request request2 = new Request.Builder()
                .url(url + "v1/resourceClusters/testcluster1/getRegisteredTaskExecutors")
                .build();
            Response response2 = client.newCall(request).execute();
            System.out.println(response2.body().string());


            Thread.sleep(Duration.ofSeconds(360).toMillis());
        }

    }
}
