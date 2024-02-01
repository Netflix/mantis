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
import static org.testng.Assert.assertThrows;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceClusterActorClusterUsageTest {
    private static final String TASK_EXECUTOR_ADDRESS = "address";
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final Duration heartbeatTimeout = Duration.ofSeconds(10);
    private static final Duration checkForDisabledExecutorsInterval = Duration.ofSeconds(10);
    private static final Duration assignmentTimeout = Duration.ofSeconds(1);
    private static final String HOST_NAME = "hostname";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);

    private static final ContainerSkuID CONTAINER_DEF_ID_1 = ContainerSkuID.of("SKU1");
    private static final ContainerSkuID CONTAINER_DEF_ID_2 = ContainerSkuID.of("SKU2");
    private static final ContainerSkuID CONTAINER_DEF_ID_3 = ContainerSkuID.of("SKU2-JDK17");
    private static final TaskExecutorID TASK_EXECUTOR_ID_1 = TaskExecutorID.of("taskExecutorId1");
    private static final TaskExecutorID TASK_EXECUTOR_ID_2 = TaskExecutorID.of("taskExecutorId2");
    private static final TaskExecutorID TASK_EXECUTOR_ID_3 = TaskExecutorID.of("taskExecutorId3");
    private static final MachineDefinition MACHINE_DEFINITION_1 =
        new MachineDefinition(2f, 2014, 128.0, 1024, 1);
    private static final MachineDefinition MACHINE_DEFINITION_2 =
        new MachineDefinition(4f, 4028, 128.0, 1024, 1);

    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION_1 =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_1)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION_1)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_1.getResourceID()))
            .build();

    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION_2 =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_2)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION_2)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_2.getResourceID()))
            .build();

    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION_3 =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_3)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION_2)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_3.getResourceID(),
                    "jdk", "17"))
            .build();

    private static ActorSystem actorSystem;

    private final TestingRpcService rpcService = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);

    private MantisJobStore mantisJobStore;
    private ActorRef resourceClusterActor;
    private ResourceCluster resourceCluster;
    private JobMessageRouter jobMessageRouter;
    private final MantisPropertiesLoader propertiesLoader =
        new DefaultMantisPropertiesLoader(System.getProperties());

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
        jobMessageRouter = mock(JobMessageRouter.class);
    }

    @Before
    public void setupActor() {
        final Props props =
            ResourceClusterActor.props(
                CLUSTER_ID,
                heartbeatTimeout,
                assignmentTimeout,
                checkForDisabledExecutorsInterval,
                Clock.systemDefaultZone(),
                rpcService,
                mantisJobStore,
                jobMessageRouter,
                0,
                "",
                false,
                "jdk:8");

        resourceClusterActor = actorSystem.actorOf(props);
        resourceCluster =
            new ResourceClusterAkkaImpl(
                resourceClusterActor,
                Duration.ofSeconds(1),
                CLUSTER_ID,
                new LongDynamicProperty(propertiesLoader, "rate.limite.perSec", 10000L));
    }

    @Test
    public void testGetTaskExecutorsUsageWithAllocationAttributes() throws Exception {
        registerTes();

        // Test get cluster usage
        TestKit probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());

        GetClusterUsageResponse usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        // The 3 registrations belong to 3 different skus
        assertEquals(3, usageRes.getUsages().size());
        for (ContainerSkuID skuID : ImmutableList.of(CONTAINER_DEF_ID_1, CONTAINER_DEF_ID_2, CONTAINER_DEF_ID_3)) {
            // no scheduling happened so far, so each Te is reported as idle
            assertIdleAndTotalCount(usageRes, skuID.getResourceID(), 1, 1);
        }

        // reserve jdk 17 TE and check usage
        Set<TaskExecutorAllocationRequest> requests = Collections.singleton(TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1"), SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of("jdk", "17")), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID_3,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());

        probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());
        usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);

        for (ContainerSkuID skuID : ImmutableList.of(CONTAINER_DEF_ID_1, CONTAINER_DEF_ID_2)) {
            assertIdleAndTotalCount(usageRes, skuID.getResourceID(), 1, 1);
        }
        assertIdleAndTotalCount(usageRes, CONTAINER_DEF_ID_3.getResourceID(), 0, 1);
    }

    @Test
    public void testGetTaskExecutorsUsageWithAllocationAttributesWithPendingJobs() throws Exception {
        registerTes();

        // add pending workers
        Set<TaskExecutorAllocationRequest> requests = ImmutableSet.of(
            TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1"), SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of("jdk", "17")), null, 0),
            TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-2"), SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of("jdk", "17")), null, 0),
            TaskExecutorAllocationRequest.of(WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-3"), SchedulingConstraints.of(MACHINE_DEFINITION_2, ImmutableMap.of("jdk", "17")), null, 0));
        assertThrows(ExecutionException.class, () -> resourceCluster.getTaskExecutorsFor(requests).get());

        // Test get cluster usage
        TestKit probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());
        GetClusterUsageResponse usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        assertEquals(3, usageRes.getUsages().size());
        for (ContainerSkuID skuID : ImmutableList.of(CONTAINER_DEF_ID_1, CONTAINER_DEF_ID_2)) {
            assertIdleAndTotalCount(usageRes, skuID.getResourceID(), 1, 1);
        }

        assertIdleAndTotalCount(usageRes, CONTAINER_DEF_ID_3.getResourceID(), -2, 1);
    }

    private void registerTes() throws Exception {
        registerTe(TASK_EXECUTOR_REGISTRATION_1);
        registerTe(TASK_EXECUTOR_REGISTRATION_2);
        registerTe(TASK_EXECUTOR_REGISTRATION_3);
    }

    private void registerTe(TaskExecutorRegistration registration) throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(registration).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        registration.getTaskExecutorID(),
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
    }

    private void assertIdleAndTotalCount(GetClusterUsageResponse usageRes, String skuID, int idleCount, int totalCount) {
        assertEquals(1, usageRes.getUsages().stream()
            .filter(usage -> Objects.equals(usage.getUsageGroupKey(), skuID)).count());
        UsageByGroupKey usage =
            usageRes.getUsages().stream()
                .filter(u -> Objects.equals(u.getUsageGroupKey(), skuID))
                .findFirst().get();
        assertEquals(idleCount, usage.getIdleCount());
        assertEquals(totalCount, usage.getTotalCount());
    }
}
