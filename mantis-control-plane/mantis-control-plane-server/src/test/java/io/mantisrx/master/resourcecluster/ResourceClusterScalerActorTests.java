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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetReservationAwareClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.ClusterAvailabilityRule;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.GetRuleSetRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.GetRuleSetResponse;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.ScaleDecision;
import io.mantisrx.master.resourcecluster.ResourceClusterScalerActor.ScaleType;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceClusterScalerActorTests {
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final ContainerSkuID skuSmall = ContainerSkuID.of("small");
    private static final ContainerSkuID skuMedium = ContainerSkuID.of("medium");
    private static final ContainerSkuID skuLarge = ContainerSkuID.of("large");
    private ActorRef scalerActor;
    private IMantisPersistenceProvider storageProvider;
    private TestKit clusterActorProbe;
    private TestKit hostActorProbe;

    private static final MachineDefinition MACHINE_DEFINITION_S =
        new MachineDefinition(2, 2048, 700, 10240, 5);
    private static final MachineDefinition MACHINE_DEFINITION_L =
        new MachineDefinition(4, 16384, 1400, 81920, 5);

    private static final MachineDefinition MACHINE_DEFINITION_M =
        new MachineDefinition(3, 4096, 700, 10240, 5);

    private static ActorSystem actorSystem;

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
    public void setupMocks() throws IOException {
        clusterActorProbe = new TestKit(actorSystem);
        hostActorProbe = new TestKit(actorSystem);
        this.storageProvider = mock(IMantisPersistenceProvider.class);

        when(this.storageProvider.getResourceClusterScaleRules(CLUSTER_ID))
            .thenReturn(
                ResourceClusterScaleRulesWritable.builder()
                    .scaleRule(skuSmall.getResourceID(), ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID)
                        .skuId(skuSmall)
                        .coolDownSecs(10)
                        .maxIdleToKeep(10)
                        .minIdleToKeep(5)
                        .minSize(11)
                        .maxSize(15)
                        .build())
                    .scaleRule(skuLarge.getResourceID(), ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID)
                        .skuId(skuLarge)
                        .coolDownSecs(10)
                        .maxIdleToKeep(15)
                        .minIdleToKeep(5)
                        .minSize(11)
                        .maxSize(15)
                        .build())
                    .build());
    }

    @Test
    public void testScaler() {

        final Props props =
            ResourceClusterScalerActor.props(
                CLUSTER_ID,
                Clock.systemDefaultZone(),
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                this.storageProvider,
                hostActorProbe.getRef(),
                clusterActorProbe.getRef(),
                false);

        scalerActor = actorSystem.actorOf(props);
        GetClusterUsageRequest req = clusterActorProbe.expectMsgClass(GetClusterUsageRequest.class);
        assertEquals(CLUSTER_ID, req.getClusterID());

        scalerActor.tell(
            GetClusterUsageResponse.builder()
                .clusterID(CLUSTER_ID)
                .usage(
                    UsageByGroupKey.builder().usageGroupKey(skuSmall.getResourceID()).idleCount(4).totalCount(10).build())
                .usage(
                    UsageByGroupKey.builder().usageGroupKey(skuLarge.getResourceID()).idleCount(16).totalCount(16).build())
                .usage(
                    UsageByGroupKey.builder().usageGroupKey(skuMedium.getResourceID()).idleCount(8).totalCount(15).build())
                .build(),
            clusterActorProbe.getRef());

        assertEquals(
            GetClusterIdleInstancesRequest.builder()
                .skuId(skuLarge)
                .clusterID(CLUSTER_ID)
                .desireSize(15)
                .maxInstanceCount(1)
                .build(),
            clusterActorProbe.expectMsgClass(GetClusterIdleInstancesRequest.class));

        assertNotNull(clusterActorProbe.expectMsgClass(Ack.class));

        Set<ScaleResourceRequest> decisions = new HashSet<>();
        decisions.add(hostActorProbe.expectMsgClass(ScaleResourceRequest.class));
        //decisions.add(hostActorProbe.expectMsgClass(ScaleResourceRequest.class));

        int newSize = 11;
        assertTrue(decisions.contains(
            ScaleResourceRequest.builder()
                .clusterId(CLUSTER_ID)
                .skuId(skuSmall)
                .desireSize(newSize)
                .build()));

        // Test callback from fetch idle list.
        ImmutableList<TaskExecutorID> idleInstances = ImmutableList.of(
            TaskExecutorID.of("agent1"),
            TaskExecutorID.of("agent2"));
        scalerActor.tell(
            GetClusterIdleInstancesResponse.builder()
                .clusterId(CLUSTER_ID)
                .instanceIds(idleInstances)
                .skuId(skuLarge)
                .desireSize(15)
                .build(),
            clusterActorProbe.getRef());

        newSize = 15;
        assertEquals(
            ScaleResourceRequest.builder()
                .clusterId(CLUSTER_ID)
                .skuId(skuLarge)
                .desireSize(newSize)
                .idleInstances(idleInstances)
                .build(),
            hostActorProbe.expectMsgClass(ScaleResourceRequest.class));

        // validate the idle intances are disabled
        io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest disableTEReq =
            clusterActorProbe.expectMsgClass(io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest.class);
        io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest disableTEReq2 =
            clusterActorProbe.expectMsgClass(io.mantisrx.master.resourcecluster.DisableTaskExecutorsRequest.class);
        assertTrue(disableTEReq.getTaskExecutorID().isPresent());
        assertTrue(disableTEReq2.getTaskExecutorID().isPresent());
        assertEquals(
            ImmutableSet.of(disableTEReq2.getTaskExecutorID().get(), disableTEReq.getTaskExecutorID().get()),
            ImmutableSet.copyOf(idleInstances));

        // Test trigger again
        GetClusterUsageRequest req2 = clusterActorProbe.expectMsgClass(GetClusterUsageRequest.class);
        assertEquals(CLUSTER_ID, req2.getClusterID());
    }

    @Test
    public void testScalerRuleSetRefresh() throws InterruptedException, IOException {
        final Props props =
            ResourceClusterScalerActor.props(
                CLUSTER_ID,
                Clock.systemDefaultZone(),
                Duration.ofSeconds(100),
                Duration.ofSeconds(1),
                this.storageProvider,
                hostActorProbe.getRef(),
                clusterActorProbe.getRef(),
                false);

        scalerActor = actorSystem.actorOf(props);
        scalerActor.tell(GetRuleSetRequest.builder().build(), clusterActorProbe.getRef());
        GetRuleSetResponse rules = clusterActorProbe.expectMsgClass(GetRuleSetResponse.class);
        assertEquals(2, rules.getRules().size());

        when(this.storageProvider.getResourceClusterScaleRules(CLUSTER_ID))
            .thenReturn(
                ResourceClusterScaleRulesWritable.builder()
                    .scaleRule(skuMedium.getResourceID(), ResourceClusterScaleSpec.builder()
                        .clusterId(CLUSTER_ID)
                        .skuId(skuMedium)
                        .coolDownSecs(10)
                        .maxIdleToKeep(20)
                        .minIdleToKeep(5)
                        .minSize(11)
                        .maxSize(15)
                        .build())
                    .build());

        Thread.sleep(1500);

        scalerActor.tell(GetRuleSetRequest.builder().build(), clusterActorProbe.getRef());
        rules = clusterActorProbe.expectMsgClass(GetRuleSetResponse.class);
        assertEquals(1, rules.getRules().size());
        assertTrue(rules.getRules().containsKey(skuMedium));

    }

    @Test
    public void testRuleCoolDown() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(10)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Clock.systemUTC().instant(), ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Test scale up
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId).idleCount(4).totalCount(10).build();
        Optional<ScaleDecision> decision = rule.apply(usage);
        int newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);

        // test cool down
        usage = UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(4).totalCount(10).build();
        assertEquals(Optional.empty(), rule.apply(usage));
    }

    @Test
    public void testScaleResourceRequestToRequestName() {

        ScaleResourceRequest r1 =
            ScaleResourceRequest.builder()
                .idleInstance(TaskExecutorID.of("t1"))
                .clusterId(CLUSTER_ID)
                .skuId(skuLarge)
                .build();

        assertEquals("clusterId---large-0", r1.getScaleRequestId());
    }

    @Test
    public void testRuleFinishCoolDown() throws InterruptedException {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(2)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.systemUTC(),
            Instant.MIN,
            true);

        // Test scale up
        UsageByGroupKey usage =
            UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(4).totalCount(10).build();
        Optional<ScaleDecision> decision = rule.apply(usage);
        int newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);

        // test cool down
        usage = UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(4).totalCount(10).build();
        assertEquals(Optional.empty(), rule.apply(usage));

        Thread.sleep(Duration.ofSeconds(3).toMillis());
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            rule.apply(usage));
    }

    @Test
    public void testRule() {
        // TestKit probe = new TestKit(actorSystem);
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Test scale up
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId).idleCount(4).totalCount(10).build();
        Optional<ScaleDecision> decision =  rule.apply(usage);
        int newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);

        // Test empty
        usage = UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(9).totalCount(11).build();
        decision =  rule.apply(usage);
        assertEquals(
            Optional.empty(),
            decision);

        // Test scale up hits max
        usage = UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(0).totalCount(11).build();
        decision =  rule.apply(usage);
        newSize = 15;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);

        // Test scale down
        usage = UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(15).totalCount(20).build();
        decision =  rule.apply(usage);
        newSize = 15;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleDown)
                    .build()),
            decision);

        // Test scale down hits min.
        usage = UsageByGroupKey.builder().usageGroupKey(skuId).idleCount(15).totalCount(15).build();
        decision =  rule.apply(usage);
        newSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(newSize)
                    .minSize(newSize)
                    .maxSize(newSize)
                    .type(ScaleType.ScaleDown)
                    .build()),
            decision);
    }

    @Test
    public void testReservationAwareScalerSendsReservationAwareRequest() {
        final Props props =
            ResourceClusterScalerActor.props(
                CLUSTER_ID,
                Clock.systemDefaultZone(),
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                this.storageProvider,
                hostActorProbe.getRef(),
                clusterActorProbe.getRef(),
                true); // reservationSchedulingEnabled = true

        scalerActor = actorSystem.actorOf(props);

        // Should receive GetReservationAwareClusterUsageRequest instead of GetClusterUsageRequest
        GetReservationAwareClusterUsageRequest req =
            clusterActorProbe.expectMsgClass(GetReservationAwareClusterUsageRequest.class);
        assertEquals(CLUSTER_ID, req.getClusterID());
        assertNotNull(req.getGroupKeyFunc());
    }

    @Test
    public void testRuleScaleUpWithPendingReservations() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Case 1: idleCount (7) > minIdleToKeep (5), but effectiveIdleCount (7-3=4) < minIdleToKeep
        // Should scale up because effective idle is below threshold
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(7)
            .totalCount(10)
            .pendingReservationCount(3)
            .build();
        Optional<ScaleDecision> decision = rule.apply(usage);

        // effectiveIdleCount = 7 - 3 = 4 < minIdleToKeep (5)
        // step = (3 + 5 - 7) = 1
        // newSize = min(10 + 1, 15) = 11
        int expectedSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(expectedSize)
                    .minSize(expectedSize)
                    .maxSize(expectedSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);
    }

    @Test
    public void testRuleScaleUpWithLargePendingReservations() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Case: idleCount (5) = minIdleToKeep (5), but pendingReservations (8) > 0
        // effectiveIdleCount = max(0, 5 - 8) = 0 < minIdleToKeep (5)
        // Should scale up significantly to cover pending reservations
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(5)
            .totalCount(10)
            .pendingReservationCount(8)
            .build();
        Optional<ScaleDecision> decision = rule.apply(usage);

        // effectiveIdleCount = max(0, 5 - 8) = 0 < minIdleToKeep (5)
        // step = (8 + 5 - 5) = 8
        // newSize = min(10 + 8, 15) = 15 (hits max)
        int expectedSize = 15;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(expectedSize)
                    .minSize(expectedSize)
                    .maxSize(expectedSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);
    }

    @Test
    public void testRuleNoScaleDownWithPendingReservations() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Case: idleCount (15) > maxIdleToKeep (10), but effectiveIdleCount (15-6=9) < maxIdleToKeep
        // Should NOT scale down because effective idle is below max threshold
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(15)
            .totalCount(20)
            .pendingReservationCount(6)
            .build();
        Optional<ScaleDecision> decision = rule.apply(usage);

        // effectiveIdleCount = 15 - 6 = 9 < maxIdleToKeep (10)
        // Should not scale down
        assertEquals(Optional.empty(), decision);
    }

    @Test
    public void testRuleScaleDownWithPendingReservations() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Case: idleCount (18), pendingReservations (2)
        // effectiveIdleCount = 18 - 2 = 16 > maxIdleToKeep (10)
        // Should scale down because effective idle exceeds max threshold
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(18)
            .totalCount(20)
            .pendingReservationCount(2)
            .build();
        Optional<ScaleDecision> decision = rule.apply(usage);

        // effectiveIdleCount = 18 - 2 = 16 > maxIdleToKeep (10)
        // step = 16 - 10 = 6
        // newSize = max(20 - 6, 11) = 14
        int expectedSize = 14;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(expectedSize)
                    .minSize(expectedSize)
                    .maxSize(expectedSize)
                    .type(ScaleType.ScaleDown)
                    .build()),
            decision);
    }

    @Test
    public void testRuleEffectiveIdleCountCalculation() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Test edge case: pendingReservationCount > idleCount
        // effectiveIdleCount should be max(0, idleCount - pendingReservationCount) = 0
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(3)
            .totalCount(10)
            .pendingReservationCount(5) // More pending than idle
            .build();

        // Verify effectiveIdleCount calculation
        assertEquals(0, usage.getEffectiveIdleCount());

        // Should scale up because effectiveIdleCount (0) < minIdleToKeep (5)
        Optional<ScaleDecision> decision = rule.apply(usage);

        // step = (5 + 5 - 3) = 7
        // newSize = min(10 + 7, 15) = 15
        int expectedSize = 15;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(expectedSize)
                    .minSize(expectedSize)
                    .maxSize(expectedSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);
    }

    @Test
    public void testRuleBackwardCompatibilityNoPendingReservations() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Test backward compatibility: pendingReservationCount = 0 (default)
        // Should behave exactly like legacy code
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(4)
            .totalCount(10)
            // pendingReservationCount defaults to 0
            .build();

        assertEquals(4, usage.getEffectiveIdleCount()); // Should equal idleCount when no pending

        Optional<ScaleDecision> decision = rule.apply(usage);

        // Should scale up: idleCount (4) < minIdleToKeep (5)
        int expectedSize = 11;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(expectedSize)
                    .minSize(expectedSize)
                    .maxSize(expectedSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);
    }

    @Test
    public void testRuleScaleUpCalculationAccountsForPendingReservations() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(20) // Increased max to test calculation
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Test the scale up calculation formula:
        // step = (pendingReservations + minIdleToKeep - actualIdleCount)
        // newSize = min(totalCount + step, maxSize)
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(2) // actualIdleCount
            .totalCount(10)
            .pendingReservationCount(5) // pendingReservations
            .build();

        Optional<ScaleDecision> decision = rule.apply(usage);

        // effectiveIdleCount = max(0, 2 - 5) = 0 < minIdleToKeep (5)
        // step = (5 + 5 - 2) = 8
        // newSize = min(10 + 8, 20) = 18
        int expectedSize = 18;
        assertEquals(
            Optional.of(
                ScaleDecision.builder()
                    .clusterId(CLUSTER_ID)
                    .skuId(ContainerSkuID.of(skuId))
                    .desireSize(expectedSize)
                    .minSize(expectedSize)
                    .maxSize(expectedSize)
                    .type(ScaleType.ScaleUp)
                    .build()),
            decision);
    }

    @Test
    public void testRuleNoOpWhenEffectiveIdleInRange() {
        String skuId = "small";
        ClusterAvailabilityRule rule = new ClusterAvailabilityRule(
            ResourceClusterScaleSpec.builder()
                .clusterId(CLUSTER_ID)
                .skuId(ContainerSkuID.of(skuId))
                .coolDownSecs(0)
                .maxIdleToKeep(10)
                .minIdleToKeep(5)
                .minSize(11)
                .maxSize(15)
                .build(),
            Clock.fixed(Instant.MIN, ZoneId.systemDefault()),
            Instant.MIN,
            true);

        // Case: effectiveIdleCount is within range [minIdleToKeep, maxIdleToKeep]
        // idleCount (8), pendingReservations (2)
        // effectiveIdleCount = 8 - 2 = 6, which is between 5 and 10
        UsageByGroupKey usage = UsageByGroupKey.builder()
            .usageGroupKey(skuId)
            .idleCount(8)
            .totalCount(12)
            .pendingReservationCount(2)
            .build();

        Optional<ScaleDecision> decision = rule.apply(usage);

        // Should not scale (no op)
        assertEquals(Optional.empty(), decision);
    }
}
