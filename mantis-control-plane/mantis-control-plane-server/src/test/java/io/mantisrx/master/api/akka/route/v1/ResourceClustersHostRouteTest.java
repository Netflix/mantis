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

package io.mantisrx.master.api.akka.route.v1;

import static io.mantisrx.master.api.akka.payloads.ResourceClustersPayloads.CLUSTER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import akka.actor.ActorRef;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.StatusCodes;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.control.plane.resource.cluster.ResourceClustersHostManagerActor;
import io.mantisrx.control.plane.resource.cluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterProvisionSubmissionResponse;
import io.mantisrx.control.plane.resource.cluster.proto.ScaleResourceRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ScaleResourceResponse;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.InMemoryOnlyResourceClusterStorageProvider;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.NoopResourceClusterResponseHandler;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.ResourceClusterProvider;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.ResourceClusterResponseHandler;
import io.mantisrx.master.api.akka.payloads.ResourceClustersPayloads;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.JsonNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ResourceClustersHostRouteTest extends RouteTestBase {
    private static Thread t;
    private static final int SERVER_PORT = 8200;
    private static CompletionStage<ServerBinding> binding;

    private static final UnitTestResourceProviderAdapter resourceProviderAdapter =
            new UnitTestResourceProviderAdapter();

    ResourceClustersHostRouteTest() {
        super("ResourceClustersRouteTest", SERVER_PORT);
    }

    @BeforeClass
    public void setup() throws Exception {
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                ActorRef resourceClustersHostManagerActor = system.actorOf(
                        ResourceClustersHostManagerActor.props(
                                resourceProviderAdapter, new InMemoryOnlyResourceClusterStorageProvider()),
                        "jobClustersManager");

                final ResourceClusterRouteHandler resourceClusterRouteHandler = new ResourceClusterRouteHandlerAkkaImpl(
                        resourceClustersHostManagerActor);

                final ResourceClustersHostRoute app = new ResourceClustersHostRoute(resourceClusterRouteHandler, system);
                log.info("starting test server on port {}", SERVER_PORT);
                latch.countDown();
                binding = http
                        .newServerAt("localhost", SERVER_PORT)
                        .bind(app.createRoute(Function.identity()));
            } catch (Exception e) {
                log.error("caught exception", e);
                latch.countDown();
            }
        });
        t.setDaemon(true);
        t.start();
        latch.await();
    }

    @AfterClass
    public void teardown() {
        log.info("ResourceClusterRouteTest teardown");
        binding.thenCompose(ServerBinding::unbind) // trigger unbinding from the port
                .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }

    @Test
    public void getClustersList() throws InterruptedException {
        testGet(
                getResourceClusterEndpoint(),
                StatusCodes.OK,
                resp -> compareClusterListPayload(resp, node -> assertEquals(0, node.size())));
    }

    @Test(dependsOnMethods = {"getClustersList"})
    public void testClusterCreate() throws InterruptedException {
        testPost(
                getResourceClusterEndpoint(),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_CREATE),
                StatusCodes.ACCEPTED,
                resp -> compareClusterSpecPayload(resp, node -> {
                    assertEquals(CLUSTER_ID, node.get("name").asText());
                    assertEquals(1, node.get("skuSpecs").size());
                    assertEquals("dev/mantistaskexecutor:main.test",
                            node.get("skuSpecs").get(0).get("imageId").asText());
                }));
    }

    @Test(dependsOnMethods = {"testClusterCreate"})
    public void getClustersList2() throws InterruptedException {
        testGet(
                getResourceClusterEndpoint(),
                StatusCodes.OK,
                resp -> compareClusterListPayload(resp, node -> {
                    assertEquals(1, node.size());
                    List<String> ids = node.findValuesAsText("id").stream().collect(Collectors.toList());
                    assertEquals(1, ids.size());
                    assertEquals("mantisResourceClusterUT1", ids.get(0));
                }));
    }

    @Test(dependsOnMethods = {"testClusterCreate"})
    public void getClusterSpec() throws InterruptedException {
        testGet(
                getResourceClusterEndpoint(CLUSTER_ID),
                StatusCodes.OK,
                resp -> compareClusterSpecPayload(resp, node -> {
                    assertEquals(CLUSTER_ID, node.get("clusterSpec").get("name").asText());
                    assertEquals(1, node.get("clusterSpec").get("skuSpecs").size());
                    assertEquals("dev/mantistaskexecutor:main.test",
                            node.get("clusterSpec").get("skuSpecs").get(0).get("imageId").asText());
                }));
    }

    @Test(dependsOnMethods = {"getClusterSpec"})
    public void scaleClusterSpec() throws InterruptedException {
        testPost(
                getResourceClusterEndpoint(CLUSTER_ID) + "/actions/scaleSku",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_SKU_SCALE),
                StatusCodes.ACCEPTED,
                resp -> compareClusterSpecPayload(resp, node -> {
                    assertEquals(CLUSTER_ID, node.get("clusterId").asText());
                    assertEquals("small", node.get("skuId").asText());
                    assertEquals("Prod", node.get("envType").asText());
                    assertEquals("11", node.get("desireSize").asText());
                }));
    }

    @Test(dependsOnMethods = {"scaleClusterSpec"})
    public void deregisterClusterSpec() throws InterruptedException {
        testDelete(
            getResourceClusterEndpoint(CLUSTER_ID),
            StatusCodes.OK,
            resp -> System.out.println("delete res: " + resp));
    }

    @Test(dependsOnMethods = {"deregisterClusterSpec"})
    public void getClustersList3() throws InterruptedException {
        testGet(
            getResourceClusterEndpoint(),
            StatusCodes.OK,
            resp -> compareClusterListPayload(resp, node -> {
                assertEquals(0, node.size());
            }));
    }

    @Test(dependsOnMethods = {"deregisterClusterSpec"})
    public void getClusterSpecErr() throws InterruptedException {
        testGet(
            getResourceClusterEndpoint("err_id"),
            StatusCodes.NOT_FOUND,
            resp -> System.out.println(resp));
    }

    final String getResourceClusterEndpoint() {
        return String.format(
                "http://127.0.0.1:%d/api/v1/resourceClusters",
                SERVER_PORT);
    }

    final String getResourceClusterEndpoint(String clusterId) {
        return String.format(
                "http://127.0.0.1:%d/api/v1/resourceClusters/%s",
                SERVER_PORT, clusterId);
    }

    private void compareClusterSpecPayload(String clusterSpecResponse, Consumer<JsonNode> valFunc) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode responseObj = mapper.readTree(clusterSpecResponse);
            valFunc.accept(responseObj);

        } catch (IOException ex) {
            fail(ex.getMessage());
        }
    }

    private void compareClusterListPayload(String clusterListResponse, Consumer<JsonNode> valFunc) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode responseObj = mapper.readTree(clusterListResponse);
            final String clusterListKey = "registeredResourceClusters";

            assertNotNull(responseObj.get(clusterListKey));
            valFunc.accept(responseObj.get(clusterListKey));

        } catch (IOException ex) {
            fail(ex.getMessage());
        }
    }

    private static class UnitTestResourceProviderAdapter implements ResourceClusterProvider {

        private ResourceClusterProvider injectedProvider;

        public void setInjectedProvider(ResourceClusterProvider injectedProvider) {
            this.injectedProvider = injectedProvider;
        }

        public void resetInjectedProvider() {
            this.injectedProvider = null;
        }

        @Override
        public CompletionStage<ResourceClusterProvisionSubmissionResponse> provisionClusterIfNotPresent(
                ProvisionResourceClusterRequest clusterSpec) {
            if (this.injectedProvider != null) return this.injectedProvider.provisionClusterIfNotPresent(clusterSpec);
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(500);
                    return ResourceClusterProvisionSubmissionResponse.builder().response("mock resp").build();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public CompletionStage<ScaleResourceResponse> scaleResource(ScaleResourceRequest scaleRequest) {
            if (this.injectedProvider != null) return this.injectedProvider.scaleResource(scaleRequest);
            return CompletableFuture.completedFuture(
                    ScaleResourceResponse.builder()
                            .message("test scale resp")
                            .region(scaleRequest.getRegion())
                            .skuId(scaleRequest.getSkuId())
                            .clusterId(scaleRequest.getClusterId())
                            .envType(scaleRequest.getEnvType())
                            .desireSize(scaleRequest.getDesireSize())
                            .responseCode(ResponseCode.SUCCESS)
                            .build());
        }

        @Override
        public ResourceClusterResponseHandler getResponseHandler() {
            if (this.injectedProvider != null) return this.injectedProvider.getResponseHandler();
            return new NoopResourceClusterResponseHandler();
        }
    }
}
