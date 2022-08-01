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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByMachineDefinition;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusterTaskExecutorMapper;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;

public class ResourceClusterActorTest {
    private static final TaskExecutorID TASK_EXECUTOR_ID = TaskExecutorID.of("taskExecutorId");
    private static final TaskExecutorID TASK_EXECUTOR_ID_2 = TaskExecutorID.of("taskExecutorId2");
    private static final TaskExecutorID TASK_EXECUTOR_ID_3 = TaskExecutorID.of("taskExecutorId3");
    private static final String TASK_EXECUTOR_ADDRESS = "address";
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final Duration heartbeatTimeout = Duration.ofSeconds(10);
    private static final Duration assignmentTimeout = Duration.ofSeconds(1);
    private static final String HOST_NAME = "hostname";
    private static final String CONTAINER_DEF_ID_1 = "SKU1";
    private static final String CONTAINER_DEF_ID_2 = "SKU2";
    private static final String CONTAINER_DEF_ID_3 = "SKU3";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(2f, 2014, 128.0, 1024, 1);

    private static final MachineDefinition MACHINE_DEFINITION_2 =
        new MachineDefinition(4f, 4028, 128.0, 1024, 1);
    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION)
            .taskExecutorAttributes(ImmutableMap.of(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_1))
            .build();

    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION_2 =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_2)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION_2)
            .taskExecutorAttributes(ImmutableMap.of(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_2))
            .build();

    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION_3 =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_3)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION_2)
            .taskExecutorAttributes(ImmutableMap.of(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_2))
            .build();

    private static final WorkerId WORKER_ID =
        WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1");

    static ActorSystem actorSystem;

    private final TestingRpcService rpcService = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);

    private MantisJobStore mantisJobStore;
    private ResourceClusterTaskExecutorMapper mapper;
    private ActorRef resourceClusterActor;
    private ResourceCluster resourceCluster;

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
    public void setupRpcService() {
        rpcService.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
        mantisJobStore = mock(MantisJobStore.class);
        mapper = ResourceClusterTaskExecutorMapper.inMemory();
    }

    @Before
    public void setupActor() {
        final Props props =
            ResourceClusterActor.props(
                CLUSTER_ID,
                heartbeatTimeout,
                assignmentTimeout,
                Clock.systemDefaultZone(),
                rpcService,
                mantisJobStore);

        resourceClusterActor = actorSystem.actorOf(props);
        resourceCluster =
            new ResourceClusterAkkaImpl(
                resourceClusterActor,
                Duration.ofSeconds(1),
                CLUSTER_ID,
                mapper);
    }

    @Test
    public void testRegistration() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
    }

    @Test
    public void testInitializationAfterRestart() throws Exception {
        when(mantisJobStore.getTaskExecutor(Matchers.eq(TASK_EXECUTOR_ID))).thenReturn(TASK_EXECUTOR_REGISTRATION);
        assertEquals(
            Ack.getInstance(),
            resourceCluster.initializeTaskExecutor(TASK_EXECUTOR_ID, WORKER_ID).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getBusyTaskExecutors().get());
    }

    @Test
    public void testGetFreeTaskExecutors() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION, WORKER_ID).get());
        assertEquals(ImmutableList.of(), resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
    }

    @Test
    public void testGetTaskExecutorsUsage() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());

        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION_2).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID_2,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());

        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION_3).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID_3,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());

        // Test get cluster usage
        TestKit probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(
            CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());
        GetClusterUsageResponse usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        assertEquals(2, usageRes.getUsages().size());
        assertEquals(1, usageRes.getUsages().stream()
            .filter(usage -> usage.getContainerDefinitionId() == CONTAINER_DEF_ID_1).count());
        UsageByMachineDefinition usage1 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getContainerDefinitionId() == CONTAINER_DEF_ID_1).findFirst().get();
        assertEquals(1, usage1.getIdleCount());
        assertEquals(1, usage1.getTotalCount());

        assertEquals(1, usageRes.getUsages().stream()
            .filter(usage -> usage.getContainerDefinitionId() == CONTAINER_DEF_ID_2).count());
        UsageByMachineDefinition usage2 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getContainerDefinitionId() == CONTAINER_DEF_ID_2).findFirst().get();
        assertEquals(2, usage2.getIdleCount());
        assertEquals(2, usage2.getTotalCount());

        // test get idle list
        resourceClusterActor.tell(
            GetClusterIdleInstancesRequest.builder()
                .clusterID(CLUSTER_ID)
                .maxInstanceCount(2)
                .skuId(CONTAINER_DEF_ID_2)
                .build(),
            probe.getRef());
        GetClusterIdleInstancesResponse idleInstancesResponse =
            probe.expectMsgClass(GetClusterIdleInstancesResponse.class);
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID_3, TASK_EXECUTOR_ID_2), idleInstancesResponse.getInstanceIds());
        assertEquals(CONTAINER_DEF_ID_2, idleInstancesResponse.getSkuId());

        assertEquals(
            TASK_EXECUTOR_ID_3,
            resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION_2, WORKER_ID).get());

        probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(
                CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());
        usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        usage1 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getContainerDefinitionId().equals(CONTAINER_DEF_ID_1)).findFirst().get();
        assertEquals(1, usage1.getIdleCount());
        assertEquals(1, usage1.getTotalCount());

        usage2 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getContainerDefinitionId().equals(CONTAINER_DEF_ID_2)).findFirst().get();
        assertEquals(1, usage2.getIdleCount());
        assertEquals(2, usage2.getTotalCount());

        // test get idle list
        resourceClusterActor.tell(
            GetClusterIdleInstancesRequest.builder()
                .clusterID(CLUSTER_ID)
                .maxInstanceCount(2)
                .skuId(CONTAINER_DEF_ID_1)
                .build(),
            probe.getRef());
        idleInstancesResponse =
            probe.expectMsgClass(GetClusterIdleInstancesResponse.class);
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), idleInstancesResponse.getInstanceIds());
        assertEquals(CONTAINER_DEF_ID_1, idleInstancesResponse.getSkuId());

        assertEquals(
            TASK_EXECUTOR_ID_2,
            resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION_2, WORKER_ID).get());
        probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(
                CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());

        usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        usage1 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getContainerDefinitionId().equals(CONTAINER_DEF_ID_1)).findFirst().get();
        assertEquals(1, usage1.getIdleCount());
        assertEquals(1, usage1.getTotalCount());

        // test get idle list
        resourceClusterActor.tell(
            GetClusterIdleInstancesRequest.builder()
                .clusterID(CLUSTER_ID)
                .maxInstanceCount(2)
                .skuId(CONTAINER_DEF_ID_1)
                .build(),
            probe.getRef());
        idleInstancesResponse =
            probe.expectMsgClass(GetClusterIdleInstancesResponse.class);
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), idleInstancesResponse.getInstanceIds());
        assertEquals(CONTAINER_DEF_ID_1, idleInstancesResponse.getSkuId());

        usage2 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getContainerDefinitionId().equalsIgnoreCase(CONTAINER_DEF_ID_2)).findFirst().get();
        assertEquals(0, usage2.getIdleCount());
        assertEquals(2, usage2.getTotalCount());
    }

    @Test
    public void testAssignmentTimeout() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION, WORKER_ID).get());
        assertEquals(ImmutableList.of(), resourceCluster.getAvailableTaskExecutors().get());
        Thread.sleep(2000);
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION, WORKER_ID).get());
    }
}
