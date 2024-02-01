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

package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import io.mantisrx.master.resourcecluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.master.resourcecluster.proto.ListResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterProvisionSubmissionResponse;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersRequest;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProvider;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterResponseHandler;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.InMemoryPersistenceProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceClustersHostManagerActorTests {
    static ActorSystem system;

    private final IMantisPersistenceProvider storageProvider = mock(IMantisPersistenceProvider.class);


    @BeforeClass
    public static void setup() {
        Config config = ConfigFactory.parseString("akka {\n" +
                "  loggers = [\"akka.testkit.TestEventListener\"]\n" +
                "  loglevel = \"INFO\"\n" +
                "  stdout-loglevel = \"INFO\"\n" +
                "  test.single-expect-default = 300000 millis\n" +
                "}\n");
        system = ActorSystem.create("ResourceClusterManagerUnitTest", config.withFallback(ConfigFactory.load()));
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testProvisionAndGetResourceCluster() {
        TestKit probe = new TestKit(system);
        ResourceClusterProvider resProvider = mock(ResourceClusterProvider.class);
        ResourceClusterResponseHandler responseHandler = mock(ResourceClusterResponseHandler.class);
        ResourceClusterProvisionSubmissionResponse provisionResponse =
                ResourceClusterProvisionSubmissionResponse.builder().response("123").build();
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(CompletableFuture.completedFuture(
                provisionResponse
        ));
        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(ResourceClustersHostManagerActor.props(resProvider, new InMemoryPersistenceProvider()));

        ProvisionResourceClusterRequest request = buildProvisionRequest();

        resourceClusterActor.tell(request, probe.getRef());
        GetResourceClusterResponse createResp = probe.expectMsgClass(GetResourceClusterResponse.class);

        assertEquals(request.getClusterSpec(), createResp.getClusterSpec());

        ListResourceClusterRequest listReq = ListResourceClusterRequest.builder().build();
        resourceClusterActor.tell(listReq, probe.getRef());
        ListResourceClustersResponse listResp = probe.expectMsgClass(ListResourceClustersResponse.class);
        assertEquals(1, listResp.getRegisteredResourceClusters().size());
        assertEquals(request.getClusterId(), listResp.getRegisteredResourceClusters().get(0).getId());

        GetResourceClusterSpecRequest getReq =
                GetResourceClusterSpecRequest.builder().id(request.getClusterId()).build();
        resourceClusterActor.tell(getReq, probe.getRef());
        GetResourceClusterResponse getResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(request.getClusterSpec(), getResp.getClusterSpec());

        // verify access API
        verify(resProvider).provisionClusterIfNotPresent(request);
        verify(responseHandler).handleProvisionResponse(provisionResponse);

        // add second cluster
        ProvisionResourceClusterRequest request2 = buildProvisionRequest("clsuter2", "dev2@mantisrx.io");

        resourceClusterActor.tell(request2, probe.getRef());
        GetResourceClusterResponse createResp2 = probe.expectMsgClass(GetResourceClusterResponse.class);

        assertEquals(request2.getClusterSpec(), createResp2.getClusterSpec());

        ListResourceClusterRequest listReq2 = ListResourceClusterRequest.builder().build();
        resourceClusterActor.tell(listReq2, probe.getRef());
        ListResourceClustersResponse listResp2 = probe.expectMsgClass(ListResourceClustersResponse.class);
        assertEquals(2, listResp2.getRegisteredResourceClusters().size());
        assertEquals(1,
                listResp2.getRegisteredResourceClusters().stream().filter(e -> e.getId().equals(request2.getClusterId())).count());

        GetResourceClusterSpecRequest getReq2 =
                GetResourceClusterSpecRequest.builder().id(request2.getClusterId()).build();
        resourceClusterActor.tell(getReq2, probe.getRef());
        GetResourceClusterResponse getResp2 = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(request2.getClusterSpec(), getResp2.getClusterSpec());

        // verify access API
        verify(resProvider, times(1)).provisionClusterIfNotPresent(request2);
        verify(responseHandler, times(2)).handleProvisionResponse(provisionResponse);

        probe.getSystem().stop(resourceClusterActor);
    }

    @Test
    public void testProvisionSpecError() {
        TestKit probe = new TestKit(system);
        ResourceClusterProvider resProvider = mock(ResourceClusterProvider.class);
        ResourceClusterResponseHandler responseHandler = mock(ResourceClusterResponseHandler.class);

        ResourceClusterProvisionSubmissionResponse provisionResponse =
            ResourceClusterProvisionSubmissionResponse.builder().response("123").build();
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(CompletableFuture.completedFuture(
            provisionResponse
        ));

        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterHostActor = system.actorOf(
            ResourceClustersHostManagerActor.props(resProvider, storageProvider));

        ProvisionResourceClusterRequest request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .build();

        resourceClusterHostActor.tell(request, probe.getRef());
        GetResourceClusterResponse createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .clusterSpec(MantisResourceClusterSpec.builder()
                    .id(ClusterID.of("id2"))
                        .build())
                .build();

        resourceClusterHostActor.tell(request, probe.getRef());
        createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .clusterSpec(MantisResourceClusterSpec.builder()
                    .id(ClusterID.of("id1"))
                    .name("id1name")
                    .envType(MantisResourceClusterEnvType.Prod)
                    .ownerEmail("user")
                    .ownerName("user")
                    .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        //.skuId(ContainerSkuID.of("small"))
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                            .skuId(ContainerSkuID.of("small"))
                            .desireSize(2)
                            .maxSize(3)
                            .minSize(1)
                            .build())
                        .cpuCoreCount(2)
                        .memorySizeInMB(16384)
                        .diskSizeInMB(81920)
                        .networkMbps(700)
                        .imageId("dev/mantistaskexecutor:main-latest")
                        .skuMetadataField(
                            "skuKey",
                            "us-east-1")
                        .skuMetadataField(
                            "sgKey",
                            "sg-11, sg-22, sg-33, sg-44")
                        .build())
                    .build())
                .build();
        resourceClusterHostActor.tell(request, probe.getRef());
        createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .clusterSpec(MantisResourceClusterSpec.builder()
                    .id(ClusterID.of("id1"))
                    .name("id1name")
                    .envType(MantisResourceClusterEnvType.Prod)
                    .ownerEmail("user")
                    .ownerName("user")
                    .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId(ContainerSkuID.of("small"))
                        .cpuCoreCount(2)
                        .memorySizeInMB(16384)
                        .diskSizeInMB(81920)
                        .networkMbps(700)
                        .imageId("dev/mantistaskexecutor:main-latest")
                        .skuMetadataField(
                            "skuKey",
                            "us-east-1")
                        .skuMetadataField(
                            "sgKey",
                            "sg-11, sg-22, sg-33, sg-44")
                        .build())
                    .build())
                .build();
        resourceClusterHostActor.tell(request, probe.getRef());
        createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .clusterSpec(MantisResourceClusterSpec.builder()
                    .id(ClusterID.of("id1"))
                    .name("id1name")
                    .envType(MantisResourceClusterEnvType.Prod)
                    .ownerEmail("user")
                    .ownerName("user")
                    .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId(ContainerSkuID.of("small"))
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                            .skuId(ContainerSkuID.of("small"))
                            .desireSize(2)
                            .maxSize(3)
                            .minSize(1)
                            .build())
                        .memorySizeInMB(16384)
                        .diskSizeInMB(81920)
                        .networkMbps(700)
                        .imageId("dev/mantistaskexecutor:main-latest")
                        .skuMetadataField(
                            "skuKey",
                            "us-east-1")
                        .skuMetadataField(
                            "sgKey",
                            "sg-11, sg-22, sg-33, sg-44")
                        .build())
                    .build())
                .build();
        resourceClusterHostActor.tell(request, probe.getRef());
        createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .clusterSpec(MantisResourceClusterSpec.builder()
                    .id(ClusterID.of("id1"))
                    .name("id1name")
                    .envType(MantisResourceClusterEnvType.Prod)
                    .ownerEmail("user")
                    .ownerName("user")
                    .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId(ContainerSkuID.of("small"))
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                            .skuId(ContainerSkuID.of("small"))
                            .desireSize(2)
                            .maxSize(3)
                            .minSize(1)
                            .build())
                        .cpuCoreCount(2)
                        .diskSizeInMB(81920)
                        .networkMbps(700)
                        .imageId("dev/mantistaskexecutor:main-latest")
                        .skuMetadataField(
                            "skuKey",
                            "us-east-1")
                        .skuMetadataField(
                            "sgKey",
                            "sg-11, sg-22, sg-33, sg-44")
                        .build())
                    .build())
                .build();
        resourceClusterHostActor.tell(request, probe.getRef());
        createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        request =
            ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of("id1"))
                .clusterSpec(MantisResourceClusterSpec.builder()
                    .id(ClusterID.of("id1"))
                    .name("id1name")
                    .envType(MantisResourceClusterEnvType.Prod)
                    .ownerEmail("user")
                    .ownerName("user")
                    .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId(ContainerSkuID.of("small"))
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                            .skuId(ContainerSkuID.of("small"))
                            .desireSize(2)
                            .maxSize(3)
                            .minSize(1)
                            .build())
                        .cpuCoreCount(2)
                        .memorySizeInMB(16384)
                        .diskSizeInMB(0)
                        .networkMbps(700)
                        .imageId("dev/mantistaskexecutor:main-latest")
                        .skuMetadataField(
                            "skuKey",
                            "us-east-1")
                        .skuMetadataField(
                            "sgKey",
                            "sg-11, sg-22, sg-33, sg-44")
                        .build())
                    .build())
                .build();
        resourceClusterHostActor.tell(request, probe.getRef());
        createResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(ResponseCode.CLIENT_ERROR, createResp.responseCode);
        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());

        probe.getSystem().stop(resourceClusterHostActor);
    }

    @Test
    public void testProvisionPersisError() throws IOException {
        TestKit probe = new TestKit(system);
        ResourceClusterProvider resProvider = mock(ResourceClusterProvider.class);
        ResourceClusterResponseHandler responseHandler = mock(ResourceClusterResponseHandler.class);

        ResourceClusterProvisionSubmissionResponse provisionResponse =
                ResourceClusterProvisionSubmissionResponse.builder().response("123").build();
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(CompletableFuture.completedFuture(
                provisionResponse
        ));

        IOException err = new IOException("persist error");
        when(storageProvider.registerAndUpdateClusterSpec(any())).thenThrow(err);
        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(
                ResourceClustersHostManagerActor.props(resProvider, storageProvider));

        ProvisionResourceClusterRequest request = buildProvisionRequest();

        resourceClusterActor.tell(request, probe.getRef());
        GetResourceClusterResponse createResp = probe.expectMsgClass(GetResourceClusterResponse.class);

        assertEquals(ResponseCode.SERVER_ERROR, createResp.responseCode);

        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());
        verify(responseHandler, atMost(1)).handleProvisionResponse(
                argThat(ar -> ar.getResponse().equals(err.toString())));

        probe.getSystem().stop(resourceClusterActor);
    }

    @Test
    public void testProvisionSubmitError() throws InterruptedException {
        TestKit probe = new TestKit(system);
        CountDownLatch latch = new CountDownLatch(1);

        ResourceClusterProvider resProvider = mock(ResourceClusterProvider.class);
        ResourceClusterResponseHandler responseHandler = mock(ResourceClusterResponseHandler.class);
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(
                CompletableFuture.supplyAsync(() -> {
                    latch.countDown();
                    throw new RuntimeException("test err msg");
                }));

        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(ResourceClustersHostManagerActor.props(resProvider, new InMemoryPersistenceProvider()));

        ProvisionResourceClusterRequest request = buildProvisionRequest();

        resourceClusterActor.tell(request, probe.getRef());
        GetResourceClusterResponse createResp = probe.expectMsgClass(GetResourceClusterResponse.class);

        assertEquals(request.getClusterSpec(), createResp.getClusterSpec());

        ListResourceClusterRequest listReq = ListResourceClusterRequest.builder().build();
        resourceClusterActor.tell(listReq, probe.getRef());
        ListResourceClustersResponse listResp = probe.expectMsgClass(ListResourceClustersResponse.class);
        assertEquals(1, listResp.getRegisteredResourceClusters().size());
        assertEquals(request.getClusterId(), listResp.getRegisteredResourceClusters().get(0).getId());

        GetResourceClusterSpecRequest getReq =
                GetResourceClusterSpecRequest.builder().id(request.getClusterId()).build();
        resourceClusterActor.tell(getReq, probe.getRef());
        GetResourceClusterResponse getResp = probe.expectMsgClass(GetResourceClusterResponse.class);
        assertEquals(request.getClusterSpec(), getResp.getClusterSpec());
        assertEquals(ResponseCode.SUCCESS, getResp.responseCode);

        latch.await(3, TimeUnit.SECONDS);
        verify(resProvider).provisionClusterIfNotPresent(request);
        verify(responseHandler).handleProvisionResponse(argThat(r ->
                r.getError().getCause().getMessage()
                .equals("test err msg")));

        probe.getSystem().stop(resourceClusterActor);
    }

    @Test
    public void testUpgradeRequest() {
        TestKit probe = new TestKit(system);
        ResourceClusterProvider resProvider = mock(ResourceClusterProvider.class);
        ResourceClusterResponseHandler responseHandler = mock(ResourceClusterResponseHandler.class);

        UpgradeClusterContainersResponse upgradeRes =
            UpgradeClusterContainersResponse.builder().responseCode(ResponseCode.SUCCESS).build();
        when(resProvider.upgradeContainerResource(any())).thenReturn(CompletableFuture.completedFuture(
            upgradeRes
        ));

        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(
            ResourceClustersHostManagerActor.props(resProvider, new InMemoryPersistenceProvider()));

        UpgradeClusterContainersRequest request = UpgradeClusterContainersRequest.builder()
            .clusterId(ClusterID.of("mantisTestResCluster1"))
            .build();

        resourceClusterActor.tell(request, probe.getRef());
        UpgradeClusterContainersResponse createResp = probe.expectMsgClass(UpgradeClusterContainersResponse.class);

        assertEquals(ResponseCode.SUCCESS, createResp.responseCode);

        verify(resProvider, times(1)).upgradeContainerResource(any());
        probe.getSystem().stop(resourceClusterActor);
    }

    @Test
    public void testUpgradeRequestEnableSkuSpecUpgrade() throws IOException {
        TestKit probe = new TestKit(system);
        IMantisPersistenceProvider resStorageProvider = mock(IMantisPersistenceProvider.class);
        ResourceClusterProvider resProvider = mock(ResourceClusterProvider.class);
        ResourceClusterResponseHandler responseHandler = mock(ResourceClusterResponseHandler.class);

        UpgradeClusterContainersResponse upgradeRes =
            UpgradeClusterContainersResponse.builder().responseCode(ResponseCode.SUCCESS).build();
        when(resProvider.upgradeContainerResource(any())).thenReturn(CompletableFuture.completedFuture(
            upgradeRes
        ));

        ProvisionResourceClusterRequest provisionReq = buildProvisionRequest();
        when(resStorageProvider.getResourceClusterSpecWritable(any()))
            .thenReturn(
                ResourceClusterSpecWritable.builder()
                    .clusterSpec(provisionReq.getClusterSpec())
                    .id(provisionReq.getClusterId())
                    .build());

        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(
            ResourceClustersHostManagerActor.props(resProvider, resStorageProvider));

        UpgradeClusterContainersRequest request = UpgradeClusterContainersRequest.builder()
            .clusterId(provisionReq.getClusterId())
            .enableSkuSpecUpgrade(true)
            .build();

        resourceClusterActor.tell(request, probe.getRef());
        UpgradeClusterContainersResponse createResp = probe.expectMsgClass(UpgradeClusterContainersResponse.class);

        assertEquals(ResponseCode.SUCCESS, createResp.responseCode);

        verify(resProvider, times(1)).upgradeContainerResource(any());
        probe.getSystem().stop(resourceClusterActor);
    }

    private ProvisionResourceClusterRequest buildProvisionRequest() {
        return buildProvisionRequest("mantisTestResCluster1", "dev@mantisrx.io");
    }

    private ProvisionResourceClusterRequest buildProvisionRequest(String id, String user) {
        ProvisionResourceClusterRequest request = ProvisionResourceClusterRequest.builder()
                .clusterId(ClusterID.of(id))
                .clusterSpec(MantisResourceClusterSpec.builder()
                        .id(ClusterID.of(id))
                        .name(id)
                        .envType(MantisResourceClusterEnvType.Prod)
                        .ownerEmail(user)
                        .ownerName(user)
                        .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                                .skuId(ContainerSkuID.of("small"))
                                .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                                        .skuId(ContainerSkuID.of("small"))
                                        .desireSize(2)
                                        .maxSize(3)
                                        .minSize(1)
                                        .build())
                                .cpuCoreCount(2)
                                .memorySizeInMB(16384)
                                .diskSizeInMB(81920)
                                .networkMbps(700)
                                .imageId("dev/mantistaskexecutor:main-latest")
                                .skuMetadataField(
                                        "skuKey",
                                        "us-east-1")
                                .skuMetadataField(
                                        "sgKey",
                                        "sg-11, sg-22, sg-33, sg-44")
                                .build())
                        .build())
                .build();
        return request;
    }
}
