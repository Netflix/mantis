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

package io.mantisrx.control.plane.resource.cluster;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.control.plane.resource.cluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ListResourceClusterRequest;
import io.mantisrx.control.plane.resource.cluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.control.plane.resource.cluster.proto.MantisResourceClusterSpec;
import io.mantisrx.control.plane.resource.cluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterProvisionSubmissiomResponse;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.IResourceClusterProvider;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.IResourceClusterResponseHandler;
import io.mantisrx.control.plane.resource.cluster.resourceprovider.IResourceClusterStorageProvider;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import java.util.concurrent.CompletableFuture;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceClusterManagerActorTests {
    static ActorSystem system;

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
        IResourceClusterProvider resProvider = mock(IResourceClusterProvider.class);
        IResourceClusterResponseHandler responseHandler = mock(IResourceClusterResponseHandler.class);
        ResourceClusterProvisionSubmissiomResponse provisionResponse =
                ResourceClusterProvisionSubmissiomResponse.builder().response("123").build();
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(CompletableFuture.completedFuture(
                provisionResponse
        ));
        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(ResourceClustersManagerActor.props(resProvider));

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
    public void testProvisionPersisError() {
        TestKit probe = new TestKit(system);
        IResourceClusterStorageProvider resStorageProvider = mock(IResourceClusterStorageProvider.class);
        IResourceClusterProvider resProvider = mock(IResourceClusterProvider.class);
        IResourceClusterResponseHandler responseHandler = mock(IResourceClusterResponseHandler.class);

        ResourceClusterProvisionSubmissiomResponse provisionResponse =
                ResourceClusterProvisionSubmissiomResponse.builder().response("123").build();
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(CompletableFuture.completedFuture(
                provisionResponse
        ));

        val err = new RuntimeException("persist error");
        when(resStorageProvider.registerAndUpdateClusterSpec(any())).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw err;
        }));
        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(
                ResourceClustersManagerActor.props(resProvider, resStorageProvider));

        ProvisionResourceClusterRequest request = buildProvisionRequest();

        resourceClusterActor.tell(request, probe.getRef());
        GetResourceClusterResponse createResp = probe.expectMsgClass(GetResourceClusterResponse.class);

        assertEquals(ResponseCode.SERVER_ERROR, createResp.responseCode);

        verify(resProvider, times(0)).provisionClusterIfNotPresent(any());
        verify(responseHandler, times(1)).handleProvisionResponse(
                argThat(ar -> ar.getResponse().equals(err.toString())));

        probe.getSystem().stop(resourceClusterActor);
    }

    @Test
    public void testProvisionSubmitError() {
        TestKit probe = new TestKit(system);
        IResourceClusterProvider resProvider = mock(IResourceClusterProvider.class);
        IResourceClusterResponseHandler responseHandler = mock(IResourceClusterResponseHandler.class);
        when(resProvider.provisionClusterIfNotPresent(any())).thenReturn(
                CompletableFuture.supplyAsync(() -> { throw new RuntimeException("test err msg"); }
                ));

        when(resProvider.getResponseHandler()).thenReturn(responseHandler);

        ActorRef resourceClusterActor = system.actorOf(ResourceClustersManagerActor.props(resProvider));

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
        GetResourceClusterResponse getResp = probe.expectMsgClass(GetResourceClusterResponse .class);
        assertEquals(request.getClusterSpec(), getResp.getClusterSpec());

        verify(resProvider).provisionClusterIfNotPresent(request);
        verify(responseHandler).handleProvisionResponse(argThat(r ->
                r.getError().getCause().getMessage()
                .equals("test err msg")));

        probe.getSystem().stop(resourceClusterActor);
    }

    private ProvisionResourceClusterRequest buildProvisionRequest() {
        return buildProvisionRequest("mantisTestResCluster1", "dev@mantisrx.io");
    }

    private ProvisionResourceClusterRequest buildProvisionRequest(String id, String user) {
        ProvisionResourceClusterRequest request = ProvisionResourceClusterRequest.builder()
                .clusterId(id)
                .clusterSpec(MantisResourceClusterSpec.builder()
                        .id(id)
                        .name(id)
                        .envType(MantisResourceClusterEnvType.Prod)
                        .ownerEmail(user)
                        .ownerName(user)
                        .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                                .skuId("small")
                                .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                                        .skuId("small")
                                        .desireSize(2)
                                        .maxSize(3)
                                        .minSize(1)
                                        .build())
                                .cpuCoreCount(2)
                                .memorySizeInBytes(16384)
                                .diskSizeInBytes(81920)
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
