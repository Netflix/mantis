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
import static io.mantisrx.master.api.akka.payloads.ResourceClustersPayloads.RESOURCE_CLUSTER_DISABLE_TASK_EXECUTORS_ATTRS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.api.akka.payloads.ResourceClustersPayloads;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import io.mantisrx.master.resourcecluster.ResourceClustersHostManagerActor;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.DeleteResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse.RegisteredResourceCluster;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterProvisionSubmissionResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateAllResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesResponse;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceResponse;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersRequest;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import io.mantisrx.master.resourcecluster.resourceprovider.NoopResourceClusterResponseHandler;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProvider;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProviderAdapter;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProviderUpgradeRequest;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterResponseHandler;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.InMemoryPersistenceProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.PagedActiveJobOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class ResourceClusterNonLeaderRedirectRouteTest extends JUnitRouteTest {
    private static final UnitTestResourceProviderAdapter resourceProviderAdapter =
        new UnitTestResourceProviderAdapter();

    private final ResourceClusters resourceClusters = mock(ResourceClusters.class);
    private final ActorSystem system =
        ActorSystem.create(ResourceClusterNonLeaderRedirectRouteTest.class.getSimpleName());

    private final IMantisPersistenceProvider storageProvider = new InMemoryPersistenceProvider();

    private final ActorRef resourceClustersHostManagerActorWithNoopAdapter = system.actorOf(
        ResourceClustersHostManagerActor.props(
            new ResourceClusterProviderAdapter(ConfigurationProvider.getConfig().getResourceClusterProvider(), system),
            storageProvider),
        "jobClustersManagerNoop");

    private final ActorRef resourceClustersHostManagerActorWithTestAdapter = system.actorOf(
        ResourceClustersHostManagerActor.props(resourceProviderAdapter, storageProvider),
        "jobClustersManagerTest");

    private final ResourceClusterRouteHandler resourceClusterRouteHandlerWithNoopAdapter =
        new ResourceClusterRouteHandlerAkkaImpl(resourceClustersHostManagerActorWithNoopAdapter);

    private final ResourceClusterRouteHandler resourceClusterRouteHandlerWithTestAdapter =
        new ResourceClusterRouteHandlerAkkaImpl(resourceClustersHostManagerActorWithTestAdapter);

    private final TestRoute testRouteWithNoopAdapter =
        testRoute(new ResourceClustersNonLeaderRedirectRoute(resourceClusters, resourceClusterRouteHandlerWithNoopAdapter, system)
            .createRoute(route -> route));

    private final TestRoute testRoute =
        testRoute(new ResourceClustersNonLeaderRedirectRoute(resourceClusters, resourceClusterRouteHandlerWithTestAdapter, system)
            .createRoute(route -> route));

    @BeforeClass
    public static void init() {
        TestHelpers.setupMasterConfig();
    }

    @Test
    public void testGetTaskExecutorState() {
        TaskExecutorRegistration registration =
            TaskExecutorRegistration.builder()
                .taskExecutorID(TaskExecutorID.of("myExecutor"))
                .clusterID(ClusterID.of("myCluster"))
                .taskExecutorAddress("taskExecutorAddress")
                .hostname("hostName")
                .workerPorts(new WorkerPorts(1, 2, 3, 4, 5))
                .machineDefinition(new MachineDefinition(1, 1, 1, 1, 1))
                .taskExecutorAttributes(ImmutableMap.of())
                .build();

        TaskExecutorStatus status =
            new TaskExecutorStatus(registration, true, true, true, false, null, Instant.now().toEpochMilli(), null);
        ResourceCluster resourceCluster = mock(ResourceCluster.class);
        when(resourceCluster.getTaskExecutorState(TaskExecutorID.of("myExecutor")))
            .thenReturn(CompletableFuture.completedFuture(status));
        when(resourceClusters.getClusterFor(ClusterID.of("myCluster"))).thenReturn(resourceCluster);

        testRouteWithNoopAdapter.run(HttpRequest.GET("/api/v1/resourceClusters/myCluster/taskExecutors/myExecutor/getTaskExecutorState"))
            .assertStatusCode(200)
            .assertEntityAs(Jackson.unmarshaller(TaskExecutorStatus.class), status);
    }

    @Test
    public void testGetActiveJobOverview() {
        PagedActiveJobOverview overview1 = new PagedActiveJobOverview(ImmutableList.of(), 0);
        PagedActiveJobOverview overview2 = new PagedActiveJobOverview(ImmutableList.of("test"), 1);
        PagedActiveJobOverview overview3 = new PagedActiveJobOverview(ImmutableList.of("test"), 99);

        ResourceCluster resourceCluster = mock(ResourceCluster.class);
        when(resourceCluster.getActiveJobOverview(Optional.empty(), Optional.empty()))
            .thenReturn(CompletableFuture.completedFuture(overview1));
        when(resourceCluster.getActiveJobOverview(Optional.of(0), Optional.of(9)))
            .thenReturn(CompletableFuture.completedFuture(overview2));
        when(resourceCluster.getActiveJobOverview(Optional.empty(), Optional.of(99)))
            .thenReturn(CompletableFuture.completedFuture(overview3));
        when(resourceClusters.getClusterFor(ClusterID.of("myCluster"))).thenReturn(resourceCluster);

        testRouteWithNoopAdapter.run(HttpRequest.GET(
                "/api/v1/resourceClusters/myCluster/activeJobOverview"))
            .assertStatusCode(200)
            .assertEntityAs(Jackson.unmarshaller(PagedActiveJobOverview.class), overview1);

        testRouteWithNoopAdapter.run(HttpRequest.GET(
            "/api/v1/resourceClusters/myCluster/activeJobOverview?startingIndex=0&pageSize=9"))
            .assertStatusCode(200)
            .assertEntityAs(Jackson.unmarshaller(PagedActiveJobOverview.class), overview2);

        testRouteWithNoopAdapter.run(HttpRequest.GET(
                "/api/v1/resourceClusters/myCluster/activeJobOverview?pageSize=99"))
            .assertStatusCode(200)
            .assertEntityAs(Jackson.unmarshaller(PagedActiveJobOverview.class), overview3);
    }

    @Test
    public void testDisableTaskExecutorsRoute() {
        // set up the mocks
        ResourceCluster resourceCluster = mock(ResourceCluster.class);
        when(resourceCluster.disableTaskExecutorsFor(
            ArgumentMatchers.eq(RESOURCE_CLUSTER_DISABLE_TASK_EXECUTORS_ATTRS),
            ArgumentMatchers.argThat(expiry ->
                expiry.isAfter(Instant.now().plus(Duration.ofHours(17))) &&
                    expiry.isBefore(Instant.now().plus(Duration.ofHours(20)))),
            ArgumentMatchers.eq(Optional.empty())))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(resourceClusters.getClusterFor(ClusterID.of("myCluster"))).thenReturn(resourceCluster);

        // set up the HTTP request that needs to be issued
        HttpRequest request =
            HttpRequest
                .POST("/api/v1/resourceClusters/myCluster/disableTaskExecutors")
                .withEntity(
                    HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_DISABLE_TASK_EXECUTORS_PAYLOAD));

        // make the request and verify the response
        testRouteWithNoopAdapter
            .run(request)
            .assertStatusCode(200)
            .assertEntityAs(Jackson.unmarshaller(Ack.class), Ack.getInstance());
    }

    @Test
    public void testResourceClusterHostRoutesNoopAdapter() throws IOException {
        // test get empty clusters (nothing has been registered).
        testRouteWithNoopAdapter.run(HttpRequest.GET(getResourceClusterEndpoint()))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(ListResourceClustersResponse.class),
                ListResourceClustersResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .registeredResourceClusters(Collections.emptyList())
                    .build());

        // should return error due to NoopResourceClusterProvider.
        testRouteWithNoopAdapter.run(
                HttpRequest.POST(getResourceClusterEndpoint(CLUSTER_ID) + "/scaleSku")
                    .withEntity(HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_SKU_SCALE)))
            .assertStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testResourceClusterHostRelatedRoutes() throws IOException {
        // test get empty clusters (nothing has been registered).
        testRoute.run(HttpRequest.GET(getResourceClusterEndpoint()))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(ListResourceClustersResponse.class),
                ListResourceClustersResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .registeredResourceClusters(Collections.emptyList())
                    .build());

        // test register new cluster.
        ProvisionResourceClusterRequest provisionReq1 = Jackson.fromJSON(
            ResourceClustersPayloads.RESOURCE_CLUSTER_CREATE,
            ProvisionResourceClusterRequest.class);

        testRoute.run(
            HttpRequest.POST(getResourceClusterEndpoint())
                .withEntity(HttpEntities.create(
                    ContentTypes.APPLICATION_JSON,
                    ResourceClustersPayloads.RESOURCE_CLUSTER_CREATE)))
            .assertStatusCode(StatusCodes.ACCEPTED)
            .assertEntityAs(
                Jackson.unmarshaller(MantisResourceClusterSpec.class),
                provisionReq1.getClusterSpec());

        // test get one registered cluster.
        testRoute.run(HttpRequest.GET(getResourceClusterEndpoint()))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(ListResourceClustersResponse.class),
                ListResourceClustersResponse.builder()
                    // TODO this responseCode field is currently not covered due to lombok issue at {@link ListResourceClustersResponse}.
                    .responseCode(ResponseCode.SUCCESS)
                    .registeredResourceClusters(
                        Arrays.asList(RegisteredResourceCluster.builder()
                            .id(ClusterID.of(CLUSTER_ID)).version("").build()))
                    .build());

        // test get registered cluster spec
        testRoute.run(HttpRequest.GET(getResourceClusterEndpoint(CLUSTER_ID)))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(GetResourceClusterResponse.class),
                GetResourceClusterResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterSpec(provisionReq1.getClusterSpec())
                    .build());

        // test scale cluster sku
        testRoute.run(
                HttpRequest.POST(getResourceClusterEndpoint(CLUSTER_ID) + "/scaleSku")
                    .withEntity(HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_SKU_SCALE)))
            .assertStatusCode(StatusCodes.ACCEPTED)
            .assertEntityAs(
                Jackson.unmarshaller(ScaleResourceResponse.class),
                Jackson.fromJSON(
                    ResourceClustersPayloads.RESOURCE_CLUSTER_SCALE_RESULT,
                    ScaleResourceResponse.class));

        // test de-register cluster.
        testRoute.run(
                HttpRequest.DELETE(getResourceClusterEndpoint(CLUSTER_ID)))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(
                Jackson.unmarshaller(DeleteResourceClusterResponse.class),
                DeleteResourceClusterResponse.builder().responseCode(ResponseCode.SUCCESS).build());

        // test get registered cluster list
        testRoute.run(HttpRequest.GET(getResourceClusterEndpoint()))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(ListResourceClustersResponse.class),
                ListResourceClustersResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .registeredResourceClusters(Collections.emptyList())
                    .build());

        // test get de-registered cluster (404)
        testRoute.run(HttpRequest.GET(getResourceClusterEndpoint(CLUSTER_ID)))
            .assertStatusCode(StatusCodes.NOT_FOUND);
    }

    @Test
    public void testResourceClusterScaleRulesRoutes() throws IOException {
        ResourceCluster resourceCluster = mock(ResourceCluster.class);
        when(resourceCluster.refreshClusterScalerRuleSet())
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(resourceClusters.getClusterFor(ClusterID.of(CLUSTER_ID))).thenReturn(resourceCluster);

        // test get empty cluster rule.
        testRoute.run(HttpRequest.GET(getResourceClusterScaleRulesEndpoint(CLUSTER_ID)))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(GetResourceClusterScaleRulesResponse.class),
                GetResourceClusterScaleRulesResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterId(ClusterID.of(CLUSTER_ID))
                    .rules(Collections.emptyList())
                    .build());

        // test register new cluster rule.
        CreateAllResourceClusterScaleRulesRequest createRuleReq1 = Jackson.fromJSON(
            ResourceClustersPayloads.RESOURCE_CLUSTER_SCALE_RULES_CREATE,
            CreateAllResourceClusterScaleRulesRequest.class);

        testRoute.run(
                HttpRequest.POST(getResourceClusterScaleRulesEndpoint(CLUSTER_ID))
                    .withEntity(HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_SCALE_RULES_CREATE)))
            .assertStatusCode(StatusCodes.ACCEPTED)
            .assertEntityAs(Jackson.unmarshaller(GetResourceClusterScaleRulesResponse.class),
                GetResourceClusterScaleRulesResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterId(ClusterID.of(CLUSTER_ID))
                    .rules(createRuleReq1.getRules())
                    .build());

        // test get two cluster rules.
        testRoute.run(HttpRequest.GET(getResourceClusterScaleRulesEndpoint(CLUSTER_ID)))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(GetResourceClusterScaleRulesResponse.class),
                GetResourceClusterScaleRulesResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterId(ClusterID.of(CLUSTER_ID))
                    .rules(createRuleReq1.getRules())
                    .build());

        testRoute.run(
                HttpRequest.POST(getResourceClusterScaleRuleEndpoint(CLUSTER_ID))
                    .withEntity(HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        ResourceClustersPayloads.RESOURCE_CLUSTER_SINGLE_SCALE_RULE_CREATE)))
            .assertStatusCode(StatusCodes.ACCEPTED)
            .assertEntityAs(Jackson.unmarshaller(GetResourceClusterScaleRulesResponse.class),
                Jackson.fromJSON(
                    ResourceClustersPayloads.RESOURCE_CLUSTER_SCALE_RULES_RESULT,
                    GetResourceClusterScaleRulesResponse.class));

        testRoute.run(HttpRequest.GET(getResourceClusterScaleRulesEndpoint(CLUSTER_ID)))
            .assertStatusCode(StatusCodes.OK)
            .assertEntityAs(Jackson.unmarshaller(GetResourceClusterScaleRulesResponse.class),
                Jackson.fromJSON(
                    ResourceClustersPayloads.RESOURCE_CLUSTER_SCALE_RULES_RESULT,
                    GetResourceClusterScaleRulesResponse.class));

        verify(resourceCluster).refreshClusterScalerRuleSet();
    }

    @Test
    public void testResourceClusterUpgradeRoutes() throws IOException {
        UpgradeClusterContainersRequest createRuleReq1 = UpgradeClusterContainersRequest.builder()
            .clusterId(ClusterID.of(CLUSTER_ID))
            .region("us-east-1")
            .optionalBatchMaxSize(50)
            .optionalSkuId("large")
            .optionalEnvType(MantisResourceClusterEnvType.Prod)
            .build();

        testRoute.run(
                HttpRequest.POST(getResourceClusterUpgradeEndpoint(CLUSTER_ID))
                    .withEntity(HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        Jackson.toJson(createRuleReq1))))
            .assertStatusCode(StatusCodes.ACCEPTED)
            .assertEntityAs(Jackson.unmarshaller(UpgradeClusterContainersResponse.class),
                UpgradeClusterContainersResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterId(createRuleReq1.getClusterId())
                    .region(createRuleReq1.getRegion())
                    .optionalSkuId(createRuleReq1.getOptionalSkuId())
                    .optionalEnvType(createRuleReq1.getOptionalEnvType())
                    .build());
    }

    final String getResourceClusterEndpoint() {
        return "/api/v1/resourceClusters";
    }

    final String getResourceClusterEndpoint(String clusterId) {
        return String.format(
            "/api/v1/resourceClusters/%s",
            clusterId);
    }

    final String getResourceClusterScaleRulesEndpoint(String clusterId) {
        return String.format(
            "/api/v1/resourceClusters/%s/scaleRules",
            clusterId);
    }

    final String getResourceClusterScaleRuleEndpoint(String clusterId) {
        return String.format(
            "/api/v1/resourceClusters/%s/scaleRule",
            clusterId);
    }

    final String getResourceClusterUpgradeEndpoint(String clusterId) {
        return String.format(
            "/api/v1/resourceClusters/%s/upgrade",
            clusterId);
    }

    public static class UnitTestResourceProviderAdapter implements ResourceClusterProvider {

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
                    .envType(scaleRequest.getEnvType().get())
                    .desireSize(scaleRequest.getDesireSize())
                    .responseCode(ResponseCode.SUCCESS)
                    .build());
        }

        @Override
        public CompletionStage<UpgradeClusterContainersResponse> upgradeContainerResource(
            ResourceClusterProviderUpgradeRequest request) {
            return CompletableFuture.completedFuture(
                UpgradeClusterContainersResponse.builder()
                    .message("test scale resp")
                    .region(request.getRegion())
                    .optionalSkuId(request.getOptionalSkuId())
                    .clusterId(request.getClusterId())
                    .optionalEnvType(request.getOptionalEnvType())
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
