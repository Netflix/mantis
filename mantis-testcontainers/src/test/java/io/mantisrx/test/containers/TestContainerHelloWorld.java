package io.mantisrx.test.containers;/*
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.mantisrx.common.metrics.LoggingMetricsPublisher;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFrameworkFactory;
import io.mantisrx.shaded.org.apache.curator.retry.RetryOneTime;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.HttpUrl.Builder;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.MountableFile;

@Slf4j
public class TestContainerHelloWorld {

    private static final int ZOOKEEPER_PORT = 2181;
    private static final int CONTROL_PLANE_API_PORT = 8100;
    private static final int DEBUGGER_PORT = 5005;

    private static final String ZOOKEEPER_ALIAS = "zookeeper";

    // set to false to run remote/pre-bulit images.
    private static final boolean USE_LOCAL_BUILT_IMAGE = true;

    private static final String CONTROL_PLANE_ALIAS = "mantiscontrolplane";

    private static final String CLUSTER_ID = "testcluster1";

    private static final String JOB_CLUSTER_NAME = "hello-sine-testcontainers";

    private static final String CONTAINER_ARTIFACT_PATH = "/apps/mantis/mantis-server-agent/mantis-artifacts/storage/";

    private static final String LOGGING_ENABLED_METRICS_GROUP =
            "MasterApiMetrics;DeadLetterActor;JobDiscoveryRoute";
    private static final String JOB_CLUSTER_CREATE =
        Utils.getStringFromResource("Job_cluster_hello_sine_create.json");

    private static final String JOB_CLUSTER_SCALING_RULE_1_CREATE =
        Utils.getStringFromResource("Job_cluster_hello_sine_scaling_rule_create_1.json");

    private static final String QUICK_SUBMIT =
        "{\"name\":\"hello-sine-testcontainers\",\"user\":\"mantisoss\",\"jobSla\":{\"durationType\":\"Perpetual\","
            + "\"runtimeLimitSecs\":\"0\",\"minRuntimeSecs\":\"0\",\"userProvidedType\":\"\"}}";

    private static final String REGULAR_SUBMIT = "{\"name\":\"hello-sine-testcontainers\",\"user\":\"mantisoss\","
        + "\"jobJarFileLocation\":\"file:///mantis-examples-sine-function-%s.zip\",\"version\":\"0.2.9 2018-05-29 "
        + "16:12:56\","
        + "\"subscriptionTimeoutSecs\":\"0\",\"jobSla\":{\"durationType\":\"Transient\","
        + "\"runtimeLimitSecs\":\"300\",\"minRuntimeSecs\":\"0\"},"
        + "\"schedulingInfo\":{\"stages\":{\"1\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1,"
        + "\"memoryMB\":1024,\"diskMB\":1024,\"networkMbps\":128,\"numPorts\":\"1\"},\"scalable\":true,"
        + "\"softConstraints\":[],\"hardConstraints\":[]}}},\"parameters\":[{\"name\":\"useRandom\",\"value\":\"false\"}],"
        + "\"isReadyForJobMaster\":false}";

    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final Path path = Paths.get("../mantis-control-plane/mantis-control-plane-server/build/docker/Dockerfile");
    public static final String JAVA_OPTS = "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.net"
        + ".util=ALL-UNNAMED -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5005";
    private static ImageFromDockerfile controlPlaneDockerFile;
    private static ImageFromDockerfile agentDockerFile;
    private static Network network = Network.newNetwork();
    private static GenericContainer<?> zookeeper;
    private static GenericContainer<?> controlPlaneLeader;

    private static String controlPlaneLeaderUrl;
    private static String controlPlaneHost;
    private static int controlPlanePort;

    private static OkHttpClient client = new OkHttpClient();

    @BeforeClass
    public static void prepareControlPlane() throws Exception {
        log.info("Building control plane image from: {}", path);
        controlPlaneDockerFile =
            new ImageFromDockerfile("localhost/testcontainers/mantis_control_plane_server_" + Base58.randomString(4).toLowerCase())
                .withDockerfile(path);

        zookeeper =
            new GenericContainer<>("zookeeper:3.8.4")
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_ALIAS)
                .withExposedPorts(ZOOKEEPER_PORT);

        controlPlaneLeader = USE_LOCAL_BUILT_IMAGE ?
            new GenericContainer<>(controlPlaneDockerFile)
                .withEnv(LoggingMetricsPublisher.LOGGING_ENABLED_METRICS_GROUP_ID_LIST_KEY,
                    LOGGING_ENABLED_METRICS_GROUP)
                .withEnv("JAVA_OPTS", JAVA_OPTS)
                .withNetwork(network)
                .withNetworkAliases(CONTROL_PLANE_ALIAS)
                .withExposedPorts(CONTROL_PLANE_API_PORT, DEBUGGER_PORT)
                .withLogConsumer(out -> log.info("[Control Plane] {}", out.getUtf8String()))
            :
                new GenericContainer<>("netflixoss/mantiscontrolplaneserver:latest")
                    .withEnv(LoggingMetricsPublisher.LOGGING_ENABLED_METRICS_GROUP_ID_LIST_KEY,
                        LOGGING_ENABLED_METRICS_GROUP)
                    .withEnv("JAVA_OPTS", JAVA_OPTS)
                    .withNetwork(network)
                    .withNetworkAliases(CONTROL_PLANE_ALIAS)
                    .withExposedPorts(CONTROL_PLANE_API_PORT);
        zookeeper.start();
        zkCheck(zookeeper);

        // master.addEnv("MANTIS_ZOOKEEPER_CONNECTSTRING", "zk:2181");
        controlPlaneLeader.start();
        log.info("Finish start controlPlaneLeader");

        controlPlaneLeaderUrl = String.format(
            "http://%s:%d/api/", controlPlaneLeader.getHost(), controlPlaneLeader.getMappedPort(CONTROL_PLANE_API_PORT));
        log.info("Using control plane url: {}", controlPlaneLeaderUrl);
        controlPlaneHost = controlPlaneLeader.getHost();
        controlPlanePort = controlPlaneLeader.getMappedPort(CONTROL_PLANE_API_PORT);

        Path agentDockerFilePath = Paths.get("../mantis-server/mantis-server-agent/build/docker/Dockerfile");
        log.info("Building agent image from: {}", agentDockerFilePath);
        agentDockerFile =
            new ImageFromDockerfile("localhost/testcontainers/mantis_agent_" + Base58.randomString(4).toLowerCase())
                .withDockerfile(agentDockerFilePath);

        assertTrue(
            "Failed to create job cluster", createJobCluster(controlPlaneHost, controlPlanePort).isPresent());
        assertTrue(
            "Failed to get job cluster", getJobCluster(controlPlaneHost, controlPlanePort).isPresent());
    }

    @Test
    public void testQuickSubmitJob() throws IOException, InterruptedException {
        Request request = new Request.Builder()
            .url(controlPlaneLeaderUrl + "v1/resourceClusters/list")
            .build();
        Optional<String> listResourceClustersO = invokeRequest(request, "listResourceClusters");
        assertTrue(listResourceClustersO.isPresent());
        log.info(listResourceClustersO.get());

        // Create agent(s)
        Map<String, GenericContainer<?>> agents = new HashMap<>();
        for (int i = 0; i < 2; i ++) {
            final String agentId = "agentquicksubmit" + i;
            final String agentHostname = String.format("%s%shostname", agentId, CLUSTER_ID);
            GenericContainer<?> agent = createAgent(agentId, CLUSTER_ID, agentHostname, agentDockerFile, network);
            agent.withLogConsumer(out -> log.info("[Agent {} log] {}", agentId, out.getUtf8String()));

            if (!ensureAgentStarted(
                controlPlaneHost,
                controlPlanePort,
                CLUSTER_ID,
                agentId,
                agent,
                10,
                Duration.ofSeconds(2).toMillis())) {
                fail("Failed to register agent: " + agent.getContainerId());
            }
            agents.put(agentId, agent);
        }

        quickSubmitJobCluster(controlPlaneHost, controlPlanePort);

        if (!ensureJobWorkerStarted(
            controlPlaneHost,
            controlPlanePort,
            10,
            Duration.ofSeconds(3).toMillis())) {
            fail("Failed to start job worker.");
        }



        testInvalidTEStates(controlPlaneHost, controlPlanePort);
        for (String agentId : agents.keySet()) {
            Optional<String> workerIdO = testTEStates(controlPlaneHost, controlPlanePort, agentId);
            if (workerIdO.isPresent() && workerIdO.get().equals("hello-sine-testcontainers-1-0-2")) {
                log.info("Worker {} on {} is running. Test SSE.", workerIdO.get(), agentId);
                // test sse
                Thread.sleep(Duration.ofSeconds(5).toMillis());
                String cmd = "curl -N -H \"Accept: text/event-stream\"  \"localhost:5055\" & sleep 3; kill $!";
                Container.ExecResult lsResult = agents.get(agentId).execInContainer("bash", "-c", cmd);
                String stdout = lsResult.getStdout();

                log.info("SSE stdout: {}", stdout);
                assertTrue(stdout.contains("data: {\"x\":"));
            }
        }

        // add scaling rule.
        Optional<String> scalingRuleResO = createScalingRule(controlPlaneHost, controlPlanePort);
        assertTrue(scalingRuleResO.isPresent());

        Thread.sleep(Duration.ofSeconds(15).toMillis());
        // delete scaling rule.
        deleteScalingRule(controlPlaneHost, controlPlanePort);

        /*
        Uncomment following lines to keep the containers running.
         */
         log.warn("Waiting for exit test.");
         Thread.sleep(Duration.ofSeconds(3600).toMillis());
    }

    @Test
    public void testRegularSubmitJob() throws IOException, InterruptedException {
        // Create agent(s)
        final String agentId0 = "agentregularsubmit";
        final String agent0Hostname = String.format("%s%shostname", agentId0, CLUSTER_ID);
        GenericContainer<?> agent0 = createAgent(agentId0, CLUSTER_ID, agent0Hostname, agentDockerFile, network);
        agent0.withLogConsumer(out -> log.debug("[Agent log] {}", out.getUtf8String()));

        if (!ensureAgentStarted(
            controlPlaneHost,
            controlPlanePort,
            CLUSTER_ID,
            agentId0,
            agent0,
            5,
            Duration.ofSeconds(10).toMillis())) {
            fail("Failed to register agent: " + agent0.getContainerId());
        }

        submitJobCluster(controlPlaneHost, controlPlanePort);

        if (!ensureJobWorkerStarted(
            controlPlaneHost,
            controlPlanePort,
            5,
            Duration.ofSeconds(10).toMillis())) {
            fail("Failed to start job worker.");
        }

        // test sse
        String cmd = "curl -N -H \"Accept: text/event-stream\"  \"localhost:5055\" & sleep 3; kill $!";
        Container.ExecResult lsResult = agent0.execInContainer("bash", "-c", cmd);
        String stdout = lsResult.getStdout();

        log.info("stdout: {}", stdout);
        assertTrue(stdout.contains("data: {\"x\":"));
        testTEStates(controlPlaneHost, controlPlanePort, agentId0);

        /*
        Uncomment following lines to keep the containers running.
         */
         // log.warn("Waiting for exit test.");
         // Thread.sleep(Duration.ofSeconds(3600).toMillis());
    }

    private static void zkCheck(GenericContainer<?> zookeeper) throws Exception {
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

        assertEquals(content, new String(bytes, StandardCharsets.UTF_8));
        System.out.println("ZK check pass!");
    }

    private GenericContainer<?> createAgent(String agentId, String resourceClusterId, String hostname,
            ImageFromDockerfile dockerFile, Network network) {
        // setup sample job artifact
        MountableFile sampleArtifact = MountableFile.forHostPath(
            Paths.get(String.format(
                "../mantis-examples/mantis-examples-sine-function/build/distributions/mantis-examples-sine-function-%s.zip",
                Utils.getBuildVersion())));

        return USE_LOCAL_BUILT_IMAGE ?
            new GenericContainer<>(dockerFile)
                .withEnv("mantis_taskexecutor_cluster_id".toUpperCase(), resourceClusterId)
                .withEnv("mantis_taskexecutor_id".toUpperCase(), agentId)
                .withEnv("MANTIS_TASKEXECUTOR_RPC_EXTERNAL_ADDRESS", hostname)
                .withEnv("JAVA_OPTS", JAVA_OPTS)
                .withCopyFileToContainer(sampleArtifact, CONTAINER_ARTIFACT_PATH)
                .withNetwork(network)
                .withCreateContainerCmdModifier(it -> it.withName(hostname))
            :
            new GenericContainer<>("netflixoss/mantisserveragent:latest")
                .withEnv("mantis_taskexecutor_cluster_id".toUpperCase(), resourceClusterId)
                .withEnv("mantis_taskexecutor_id".toUpperCase(), agentId)
                .withEnv("MANTIS_TASKEXECUTOR_RPC_EXTERNAL_ADDRESS", hostname)
                .withEnv("JAVA_OPTS", JAVA_OPTS)
                .withCopyFileToContainer(sampleArtifact, CONTAINER_ARTIFACT_PATH)
                .withNetwork(network)
                .withCreateContainerCmdModifier(it -> it.withName(hostname));
    }

    private boolean ensureAgentStarted(
        String controlPlaneHost,
        int controlPlanePort,
        String resourceClusterId,
        String agentId,
        GenericContainer<?> agent,
        int retries,
        long sleepMillis) {
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

        for(int i = 0; i < retries; i++) {
            Request request = new Request.Builder()
                .url(reqUrl)
                .build();

            try {
                Optional<String> resO = invokeRequest(request, "ensureAgentStarted");
                log.info("Registered agents: {}.", resO);
                if (resO.isPresent()) {
                    List<TaskExecutorID> listResponses =
                        mapper.readValue(resO.get(), new TypeReference<List<TaskExecutorID>>() {});

                    for (TaskExecutorID taskExecutorID : listResponses) {
                        if (taskExecutorID.getResourceId().equals(agentId)) {
                            log.info("Agent {} has registered to {}.", agentId, resourceClusterId);
                            return true;
                        }
                    }
                }
                Thread.sleep(sleepMillis);
            } catch (Exception e) {
                log.warn("Get registered agent call error", e);
            }
        }
        return false;
    }

    private boolean ensureJobWorkerStarted(
        String controlPlaneHost,
        int controlPlanePort,
        int retries,
        long sleepMillis) throws InterruptedException {
        log.info("waiting for job worker to start.");
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/jobClusters")
            .addPathSegment(JOB_CLUSTER_NAME)
            .addPathSegment("latestJobDiscoveryInfo")
            .build();

        for(int i = 0; i < retries; i++) {
            Thread.sleep(sleepMillis);
            Request request = new Request.Builder()
                .url(reqUrl)
                .build();

            Optional<String> resO = invokeRequest(request, "ensureJobWorkerStarted");
            if (resO.isPresent() && resO.get().contains("\"state\":\"Started\"")) {
                log.info("Job worker started.");
                return true;
            }
        }
        return false;
    }

    private static Optional<String> createJobCluster(
        String controlPlaneHost,
        int controlPlanePort) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/jobClusters")
            .build();
        String payload = String.format(JOB_CLUSTER_CREATE, Utils.getBuildVersion());
        log.info("using payload: {}", payload);
        RequestBody body = RequestBody.create(
            payload, MediaType.parse("application/json; charset=utf-8"));

        Request request = new Request.Builder()
            .url(reqUrl)
            .post(body)
            .build();

        return invokeRequest(request, "createJobCluster");
    }

    private void testInvalidTEStates(
        String controlPlaneHost,
        int controlPlanePort) {
        try
        {
            Optional<Response> responseO = checkTeState(controlPlaneHost, controlPlanePort, "invalid");
            assertTrue(responseO.isPresent());
            assertEquals(404, responseO.get().code());
        }
        catch (Exception ioe) {
            log.error("failed to check TE state", ioe);
            fail("failed to check TE state " + ioe);
        }
    }

    private Optional<String> testTEStates(
        String controlPlaneHost,
        int controlPlanePort,
        String agentId) {
        try
        {
            Optional<Response> responseO = checkTeState(controlPlaneHost, controlPlanePort, agentId);
            assertTrue(responseO.isPresent());
            assertTrue(responseO.get().isSuccessful());
            String resBody = responseO.get().body().string();
            log.info("agent {}: {}", agentId, resBody);
            assertTrue(resBody.contains("\"registered\":true,\"runningTask\":true"));
            return Utils.getWorkerId(resBody);
        }
        catch (IOException ioe) {
            log.error("failed to check TE state", ioe);
            fail("failed to check TE state " + ioe);
            return Optional.empty();
        }
    }

    private Optional<Response> checkTeState(
        String controlPlaneHost,
        int controlPlanePort,
        String teId) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/resourceClusters")
            .addPathSegment(CLUSTER_ID)
            .addPathSegment("taskExecutors")
            .addPathSegment(teId)
            .addPathSegment("getTaskExecutorState")
            .build();
        log.info("Req: {}", reqUrl);
        Request request = new Request.Builder()
            .url(reqUrl)
            .get()
            .build();

        try {
            Response response = HTTP_CLIENT.newCall(request).execute();
            return Optional.of(response);
        } catch (Exception e) {
            log.warn("Get TE state call error", e);
            return Optional.empty();
        }
    }

    private Optional<String> quickSubmitJobCluster(
        String controlPlaneHost,
        int controlPlanePort) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/namedjob/quicksubmit")
            .build();
        log.info("Req: {}", reqUrl);

        RequestBody body = RequestBody.create(
            QUICK_SUBMIT, MediaType.parse("application/json; charset=utf-8"));

        Request request = new Request.Builder()
            .url(reqUrl)
            .post(body)
            .build();

        return invokeRequest(request, "quickSubmitJobCluster");
    }

    private Optional<String> createScalingRule(
        String controlPlaneHost,
        int controlPlanePort) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/jobClusters")
            .addPathSegment(JOB_CLUSTER_NAME)
            .addPathSegment("scalerRules")
            .build();

        RequestBody body = RequestBody.create(
            JOB_CLUSTER_SCALING_RULE_1_CREATE, MediaType.parse("application/json; charset=utf-8"));

        Request request = new Request.Builder()
            .url(reqUrl)
            .post(body)
            .build();

        return invokeRequest(request, "createScalingRule");
    }

    private Optional<String> deleteScalingRule(
        String controlPlaneHost,
        int controlPlanePort) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/jobClusters")
            .addPathSegment(JOB_CLUSTER_NAME)
            .addPathSegment("scalerRules")
            .addPathSegment("1") // rule id 1
            .build();

        Request request = new Request.Builder()
            .url(reqUrl)
            .delete()
            .build();

        return invokeRequest(request, "deleteScalingRule");
    }

    private Optional<String> submitJobCluster(
        String controlPlaneHost,
        int controlPlanePort) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/submit")
            .build();
        log.info("Req: {}", reqUrl);

        String buildVersion = Utils.getBuildVersion();
        log.info("using artifact version: {}", buildVersion);
        String payload = String.format(REGULAR_SUBMIT, Utils.getBuildVersion());
        log.info("Submit payload: {}", payload);
        RequestBody body = RequestBody.create(
            payload, MediaType.parse("application/json; charset=utf-8"));

        Request request = new Request.Builder()
            .url(reqUrl)
            .post(body)
            .build();
        return invokeRequest(request, "submitJobCluster");
    }

    private static Optional<String> getJobCluster(
        String controlPlaneHost,
        int controlPlanePort) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(controlPlaneHost)
            .port(controlPlanePort)
            .addPathSegments("api/v1/jobClusters")
            .addPathSegment(JOB_CLUSTER_NAME)
            .build();

        Request request = new Request.Builder()
            .url(reqUrl)
            .build();

        return invokeRequest(request, "getJobCluster");
    }

    private Optional<String> getJobSink(
        GenericContainer<?> agent) {
        HttpUrl reqUrl = new Builder()
            .scheme("http")
            .host(agent.getHost())
            .port(agent.getMappedPort(5055))
            .build();
        log.info("Req: {}", reqUrl);

        Request request = new Request.Builder()
            .url(reqUrl)
            .build();

        return invokeRequest(request, "getJobSink");
    }

    private static Optional<String> invokeRequest(Request request, String opsName) {
        log.info("{} request: {}.", opsName, request);
        String responseBody = null;
        try (Response response = HTTP_CLIENT.newCall(request).execute()) {
            responseBody = response.body().string();
            log.info("{} response: {}.", opsName, responseBody);
        } catch (Exception e) {
            log.warn("{}} call error", opsName, e);
        }
        return Optional.ofNullable(responseBody);
    }
}
