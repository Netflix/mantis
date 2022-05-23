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

import static io.mantisrx.master.resourcecluster.ResourceClusterActorTest.actorSystem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.ClusterAvailabilityRule;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.MachineDefinitionToSkuMapper;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.ScaleDecision;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByMachineDefinition;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class ResourceClusterScalerActorTests {
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final String skuSmall = "small";
    private static final String skuMedium = "medium";
    private static final String skuLarge = "large";
    private ActorRef scalerActor;
    private ResourceClusterStorageProvider storageProvider;
    private TestKit clusterActorProbe;
    private TestKit hostActorProbe;

    private static final MachineDefinition MACHINE_DEFINITION_S =
        new MachineDefinition(2, 2048, 700, 10240, 5);
    private static final MachineDefinition MACHINE_DEFINITION_L =
        new MachineDefinition(4, 16384, 1400, 81920, 5);

    private static final MachineDefinition MACHINE_DEFINITION_M =
        new MachineDefinition(3, 4096, 700, 10240, 5);

    @BeforeClass
    public static void setup() {
        actorSystem = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Before
    public void setupMocks() {
        clusterActorProbe = new TestKit(actorSystem);
        hostActorProbe = new TestKit(actorSystem);
        this.storageProvider = mock(ResourceClusterStorageProvider.class);

        when(this.storageProvider.getResourceClusterSpecWritable(ArgumentMatchers.anyString()))
            .thenReturn(CompletableFuture.completedFuture(
                ResourceClusterSpecWritable.builder().clusterSpec(buildClusterSpec()).build()));

        when(this.storageProvider.getResourceClusterScaleRules(ArgumentMatchers.anyString()))
            .thenReturn(CompletableFuture.completedFuture(
                ResourceClusterScaleRulesWritable.builder()
                    .scaleRule(skuSmall, ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID.getResourceID())
                        .skuId(skuSmall)
                        .coolDownSecs(10)
                        .maxIdleToKeep(10)
                        .minIdleToKeep(5)
                        .minSize(11)
                        .maxSize(15)
                        .build())
                    .scaleRule(skuSmall, ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID.getResourceID())
                        .skuId(skuLarge)
                        .coolDownSecs(10)
                        .maxIdleToKeep(15)
                        .minIdleToKeep(5)
                        .minSize(11)
                        .maxSize(15)
                        .build())
                    .build()
            ));
    }

    @Test
    public void testScaler() {

        final Props props =
            ResourceClusterScalerActor.props(
                CLUSTER_ID,
                Clock.systemDefaultZone(),
                Duration.ofSeconds(1),
                this.storageProvider,
                hostActorProbe.getRef(),
                clusterActorProbe.getRef());

        scalerActor = actorSystem.actorOf(props);
        GetClusterUsageRequest req = clusterActorProbe.expectMsgClass(GetClusterUsageRequest.class);
        assertEquals(CLUSTER_ID, req.getClusterID());

        scalerActor.tell(
            GetClusterUsageResponse.builder()
                .clusterID(CLUSTER_ID)
                .usage(
                    UsageByMachineDefinition.builder().def(MACHINE_DEFINITION_S).idleCount(4).totalCount(10).build())
                .usage(
                    UsageByMachineDefinition.builder().def(MACHINE_DEFINITION_L).idleCount(16).totalCount(16).build())
                .usage(
                    UsageByMachineDefinition.builder().def(MACHINE_DEFINITION_M).idleCount(8).totalCount(15).build())
                .build(),
            clusterActorProbe.getRef());

        assertNotNull(clusterActorProbe.expectMsgClass(Ack.class));

        ScaleResourceRequest decision1 = hostActorProbe.expectMsgClass(ScaleResourceRequest.class);
        int newSize = 11;
        assertEquals(
            ScaleResourceRequest.builder()
                .clusterId(CLUSTER_ID.getResourceID())
                .skuId(skuSmall)
                .desireSize(newSize)
                .build(),
            decision1);

        ScaleResourceRequest decision2 = hostActorProbe.expectMsgClass(ScaleResourceRequest.class);
        newSize = 15;
        assertEquals(
            ScaleResourceRequest.builder()
                .clusterId(CLUSTER_ID.getResourceID())
                .skuId(skuLarge)
                .desireSize(newSize)
                .build(),
            decision2);

        // Test trigger again
        GetClusterUsageRequest req2 = clusterActorProbe.expectMsgClass(GetClusterUsageRequest.class);
        assertEquals(CLUSTER_ID, req2.getClusterID());
    }

    @Test
    public void testRuleCoolDown() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID.getResourceID())
                .skuId(skuId)
                .coolDownSecs(10)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Clock.systemUTC().instant(), ZoneId.systemDefault()));

        MachineDefinition mDef = new MachineDefinition(2, 2048, 700, 10240, 5);

        // Test scale up
        UsageByMachineDefinition usage = UsageByMachineDefinition.builder().def(mDef).idleCount(4).totalCount(10).build();
        Optional<ScaleDecision> decision = rule.apply(usage);
        int newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID.getResourceID())
                    .skuId(skuId)
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .build()),
            decision);

        // test cool down
        usage = UsageByMachineDefinition.builder().def(mDef).idleCount(4).totalCount(10).build();
        assertEquals(Optional.empty(), rule.apply(usage));
    }

    @Test
    public void testRule() {
        // TestKit probe = new TestKit(actorSystem);
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID.getResourceID())
                .skuId(skuId)
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()));

        MachineDefinition mDef = new MachineDefinition(2, 2048, 700, 10240, 5);

        // Test scale up
        UsageByMachineDefinition usage = UsageByMachineDefinition.builder().def(mDef).idleCount(4).totalCount(10).build();
        Optional<ScaleDecision> decision =  rule.apply(usage);
        int newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID.getResourceID())
                    .skuId(skuId)
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .build()),
            decision);

        // Test scale up hits max
        usage = UsageByMachineDefinition.builder().def(mDef).idleCount(0).totalCount(11).build();
        decision =  rule.apply(usage);
        newSize = 15;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID.getResourceID())
                    .skuId(skuId)
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .build()),
            decision);

        // Test scale down
        usage = UsageByMachineDefinition.builder().def(mDef).idleCount(15).totalCount(20).build();
        decision =  rule.apply(usage);
        newSize = 15;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID.getResourceID())
                    .skuId(skuId)
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .build()),
            decision);

        // Test scale down hits min.
        usage = UsageByMachineDefinition.builder().def(mDef).idleCount(15).totalCount(15).build();
        decision =  rule.apply(usage);
        newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID.getResourceID())
                    .skuId(skuId)
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .build()),
            decision);
    }

    @Test
    public void testSkuMapper() {
        MachineDefinitionToSkuMapper mapper = new MachineDefinitionToSkuMapper(buildClusterSpec());
        assertEquals(Optional.of("small"),
            mapper.map(new MachineDefinition(2, 2048, 700, 10240, 5)));
        assertEquals(Optional.of("large"),
            mapper.map(new MachineDefinition(4, 16384, 1400, 81920, 5)));
        assertEquals(Optional.empty(),
            mapper.map(new MachineDefinition(3, 2048, 700, 10240, 5)));
    }

    private MantisResourceClusterSpec buildClusterSpec() {
        String id = CLUSTER_ID.getResourceID();
        String user = "mantisrx@mantis.io";

        return MantisResourceClusterSpec.builder()
            .id(id)
            .name(id)
            .envType(MantisResourceClusterEnvType.Prod)
            .ownerEmail(user)
            .ownerName(user)
            .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                .skuId(skuSmall)
                .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                    .skuId(skuSmall)
                    .desireSize(2)
                    .maxSize(3)
                    .minSize(1)
                    .build())
                .cpuCoreCount((int)Math.round(MACHINE_DEFINITION_S.getCpuCores()))
                .memorySizeInBytes((int)Math.round(MACHINE_DEFINITION_S.getMemoryMB()))
                .diskSizeInBytes((int)Math.round(MACHINE_DEFINITION_S.getDiskMB()))
                .networkMbps((int)Math.round(MACHINE_DEFINITION_S.getNetworkMbps()))
                .imageId("dev/mantistaskexecutor:main-latest")
                .build())
            .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                .skuId(skuLarge)
                .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                    .skuId(skuLarge)
                    .desireSize(9)
                    .maxSize(15)
                    .minSize(1)
                    .build())
                .cpuCoreCount((int)Math.round(MACHINE_DEFINITION_L.getCpuCores()))
                .memorySizeInBytes((int)Math.round(MACHINE_DEFINITION_L.getMemoryMB()))
                .diskSizeInBytes((int)Math.round(MACHINE_DEFINITION_L.getDiskMB()))
                .networkMbps((int)Math.round(MACHINE_DEFINITION_L.getNetworkMbps()))
                .imageId("dev/mantistaskexecutor:main-latest")
                .build())
            .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                .skuId(skuMedium)
                .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                    .skuId(skuMedium)
                    .desireSize(9)
                    .maxSize(15)
                    .minSize(1)
                    .build())
                .cpuCoreCount((int)Math.round(MACHINE_DEFINITION_M.getCpuCores()))
                .memorySizeInBytes((int)Math.round(MACHINE_DEFINITION_M.getMemoryMB()))
                .diskSizeInBytes((int)Math.round(MACHINE_DEFINITION_M.getDiskMB()))
                .networkMbps((int)Math.round(MACHINE_DEFINITION_M.getNetworkMbps()))
                .imageId("dev/mantistaskexecutor:main-latest")
                .build())
            .build();
    }
}
