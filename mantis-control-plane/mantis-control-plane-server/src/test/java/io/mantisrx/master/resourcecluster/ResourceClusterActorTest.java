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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.ExpireDisableTaskExecutorsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetActiveJobsRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetTaskExecutorStatusRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.PagedActiveJobOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.ResourceOverview;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorNotFoundException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.server.worker.TaskExecutorGateway.TaskNotFoundException;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Matchers;

@Slf4j
public class ResourceClusterActorTest {
    private static final TaskExecutorID TASK_EXECUTOR_ID = TaskExecutorID.of("taskExecutorId");
    private static final TaskExecutorID TASK_EXECUTOR_ID_2 = TaskExecutorID.of("taskExecutorId2");
    private static final TaskExecutorID TASK_EXECUTOR_ID_3 = TaskExecutorID.of("taskExecutorId3");
    private static final String TASK_EXECUTOR_ADDRESS = "address";
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final Duration heartbeatTimeout = Duration.ofSeconds(10);
    private static final Duration checkForDisabledExecutorsInterval = Duration.ofSeconds(10);
    private static final Duration schedulerLeaseExpirationDuration = Duration.ofMillis(100);
    private static final Duration assignmentTimeout = Duration.ofSeconds(1);
    private static final String HOST_NAME = "hostname";

    private static final ContainerSkuID CONTAINER_DEF_ID_1 = ContainerSkuID.of("SKU1");
    private static final ContainerSkuID CONTAINER_DEF_ID_2 = ContainerSkuID.of("SKU2");
    private static final ContainerSkuID CONTAINER_DEF_ID_3 = ContainerSkuID.of("SKU3");
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(2f, 2014, 128.0, 1024, 1);

    private static final MachineDefinition MACHINE_DEFINITION_2 =
        new MachineDefinition(4f, 4028, 128.0, 1024, 1);
    private static final Map<String, String> ATTRIBUTES =
        ImmutableMap.of("attr1", "attr1");
    private static final Map<String, String> ATTRIBUTES2 =
        ImmutableMap.of("attr2", "attr2");
    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_1.getResourceID(),
                    "attr1", "attr1"))
            .build();

    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION_2 =
        TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_2)
            .clusterID(CLUSTER_ID)
            .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
            .hostname(HOST_NAME)
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE_DEFINITION)
            .taskExecutorAttributes(
                ImmutableMap.of(
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_2.getResourceID(),
                    "attr2", "attr2"))
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
                    WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_2.getResourceID(),
                    "attr2", "attr2"))
            .build();

    private static final WorkerId WORKER_ID =
        WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1");

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
                schedulerLeaseExpirationDuration,
                Clock.systemDefaultZone(),
                rpcService,
                mantisJobStore,
                jobMessageRouter,
                0,
                "",
                false,
                ImmutableMap.of(),
                new CpuWeightedFitnessCalculator());

        resourceClusterActor = actorSystem.actorOf(props);
        resourceCluster =
            new ResourceClusterAkkaImpl(
                resourceClusterActor,
                Duration.ofSeconds(1),
                CLUSTER_ID,
                new LongDynamicProperty(propertiesLoader, "rate.limite.perSec", 10000L));
    }

    @Test
    public void testRegistration() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
    }

    @Test
    public void testMarkTaskCancelled() throws Exception {
        try {
            CompletableFuture<Ack> future = resourceCluster.markTaskExecutorWorkerCancelled(WORKER_ID);
            future.get();
        } catch (Exception e) {
            assertEquals(ExecutionException.class, e.getClass());
            assertEquals(
                String.format("io.mantisrx.server.worker.TaskExecutorGateway$TaskNotFoundException: Task %s not found",
                    WORKER_ID),
                e.getMessage());
        }

        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());

        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(ArgumentMatchers.eq(CLUSTER_ID)))
            .thenReturn(ImmutableList.of());
        when(mantisJobStore.getJobArtifactsToCache(ArgumentMatchers.eq(CLUSTER_ID))).thenReturn(ImmutableList.of());
        when(mantisJobStore.getTaskExecutor(ArgumentMatchers.eq(TASK_EXECUTOR_ID))).thenReturn(TASK_EXECUTOR_REGISTRATION);

        assertEquals(
            Ack.getInstance(),
            resourceCluster.initializeTaskExecutor(TASK_EXECUTOR_ID, WORKER_ID).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getBusyTaskExecutors().get());

        try {
            Ack ack = resourceCluster.markTaskExecutorWorkerCancelled(WORKER_ID).get();
            assertNotNull(ack);
        } catch (Exception e) {
            log.error("ex:", e);
            Assert.fail();
        }

        TaskExecutorStatus tEStatus = resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get();
        assertEquals(WORKER_ID, tEStatus.getCancelledWorkerId());

        // re-registration doesn't reset the cancel worker state.
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
        tEStatus = resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get();
        assertEquals(WORKER_ID, tEStatus.getCancelledWorkerId());

        // new heartbeat with available state reset the cancelled worker state.
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());

        tEStatus = resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get();
        assertEquals(null, tEStatus.getCancelledWorkerId());
    }

    @Test
    public void testInitializationAfterRestart() throws Exception {
        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(ArgumentMatchers.eq(CLUSTER_ID)))
            .thenReturn(ImmutableList.of());
        when(mantisJobStore.getJobArtifactsToCache(ArgumentMatchers.eq(CLUSTER_ID))).thenReturn(ImmutableList.of());
        when(mantisJobStore.getTaskExecutor(ArgumentMatchers.eq(TASK_EXECUTOR_ID))).thenReturn(TASK_EXECUTOR_REGISTRATION);
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
        final Set<TaskExecutorAllocationRequest> requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorAssignedFor(WORKER_ID).get());
        assertEquals(ImmutableList.of(), resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
    }

    @Test
    public void testGetTaskExecutorsUsageAndList() throws Exception {
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
            .filter(usage -> Objects.equals(usage.getUsageGroupKey(), CONTAINER_DEF_ID_1.getResourceID())).count());
        UsageByGroupKey usage1 =
            usageRes.getUsages().stream()
                .filter(usage -> Objects.equals(usage.getUsageGroupKey(), CONTAINER_DEF_ID_1.getResourceID()))
                .findFirst().get();
        assertEquals(1, usage1.getIdleCount());
        assertEquals(1, usage1.getTotalCount());

        // test get TE status
        resourceClusterActor.tell(new GetTaskExecutorStatusRequest(TASK_EXECUTOR_ID_2, CLUSTER_ID), probe.getRef());
        TaskExecutorStatus teStatusRes = probe.expectMsgClass(TaskExecutorStatus.class);
        assertEquals(TASK_EXECUTOR_REGISTRATION_2, teStatusRes.getRegistration());

        // test get invalid TE status
        resourceClusterActor.tell(new GetTaskExecutorStatusRequest(TaskExecutorID.of("invalid"), CLUSTER_ID),
            probe.getRef());
        Failure teNotFoundStatusRes = probe.expectMsgClass(Failure.class);
        assertTrue(teNotFoundStatusRes.cause() instanceof TaskExecutorNotFoundException);

        assertEquals(1, usageRes.getUsages().stream()
            .filter(usage -> Objects.equals(usage.getUsageGroupKey(), CONTAINER_DEF_ID_2.getResourceID())).count());
        UsageByGroupKey usage2 =
            usageRes.getUsages().stream()
                .filter(usage -> Objects.equals(usage.getUsageGroupKey(), CONTAINER_DEF_ID_2.getResourceID()))
                .findFirst().get();
        assertEquals(2, usage2.getIdleCount());
        assertEquals(2, usage2.getTotalCount());

        // test get empty job list
        resourceClusterActor.tell(new GetActiveJobsRequest(
                CLUSTER_ID),
            probe.getRef());
        PagedActiveJobOverview jobsList = probe.expectMsgClass(PagedActiveJobOverview.class);
        assertEquals(0, jobsList.getActiveJobs().size());
        assertEquals(0, jobsList.getEndPosition());

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

        Set<TaskExecutorAllocationRequest> requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION_2), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID_3,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());

        probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(
                CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());
        usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        usage1 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getUsageGroupKey().equals(CONTAINER_DEF_ID_1.getResourceID())).findFirst().get();
        assertEquals(1, usage1.getIdleCount());
        assertEquals(1, usage1.getTotalCount());

        usage2 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getUsageGroupKey().equals(CONTAINER_DEF_ID_2.getResourceID())).findFirst().get();
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

        requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID_2,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());
        probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetClusterUsageRequest(
                CLUSTER_ID, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            probe.getRef());

        usageRes = probe.expectMsgClass(GetClusterUsageResponse.class);
        usage1 =
            usageRes.getUsages().stream()
                .filter(usage -> usage.getUsageGroupKey().equals(CONTAINER_DEF_ID_1.getResourceID())).findFirst().get();
        assertEquals(1, usage1.getIdleCount());
        assertEquals(1, usage1.getTotalCount());

        // test get non-empty job list
        resourceClusterActor.tell(new GetActiveJobsRequest(
                CLUSTER_ID),
            probe.getRef());
        jobsList = probe.expectMsgClass(PagedActiveJobOverview.class);
        assertEquals(1, jobsList.getActiveJobs().size());
        assertTrue(jobsList.getActiveJobs().contains(WORKER_ID.getJobId()));
        assertEquals(1, jobsList.getEndPosition());

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
                .filter(usage -> usage.getUsageGroupKey().equalsIgnoreCase(CONTAINER_DEF_ID_2.getResourceID()))
                .findFirst().get();
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
        Set<TaskExecutorAllocationRequest> requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());
        assertEquals(ImmutableList.of(), resourceCluster.getAvailableTaskExecutors().get());
        Thread.sleep(2000);
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getAvailableTaskExecutors().get());
        requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());
    }

    @Test
    public void testGetMultipleActiveJobs() throws ExecutionException, InterruptedException {

        final Set<TaskExecutorAllocationRequest> requests = new HashSet<>();
        final Set<TaskExecutorID> expectedTaskExecutorIds = new HashSet<>();
        final int n = 10;
        List<String> expectedJobIdList = new ArrayList<>(n);
        for (int i = 0; i < n * 2; i ++) {
            int idx = (i % n);
            TaskExecutorID taskExecutorID = TaskExecutorID.of("taskExecutorId" + i);
            assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(
                TaskExecutorRegistration.builder()
                    .taskExecutorID(taskExecutorID)
                    .clusterID(CLUSTER_ID)
                    .taskExecutorAddress(TASK_EXECUTOR_ADDRESS)
                    .hostname(HOST_NAME + i)
                    .workerPorts(WORKER_PORTS)
                    .machineDefinition(MACHINE_DEFINITION)
                    .taskExecutorAttributes(
                        ImmutableMap.of(
                            WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_DEF_ID_1.getResourceID(),
                            "attr1", "attr1"))
                    .build()
            ).get());

            assertEquals(Ack.getInstance(),
                resourceCluster
                    .heartBeatFromTaskExecutor(
                        new TaskExecutorHeartbeat(
                            taskExecutorID,
                            CLUSTER_ID,
                            TaskExecutorReport.available())).get());

            WorkerId workerId =
                WorkerId.fromIdUnsafe(String.format("late-sine-function-tutorial-%d-worker-%d-1", idx, i));
            if (i < n) {
                expectedJobIdList.add(String.format("late-sine-function-tutorial-%d", idx));
            }

            expectedTaskExecutorIds.add(taskExecutorID);
            requests.add(TaskExecutorAllocationRequest.of(workerId, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        }
        assertEquals(
            expectedTaskExecutorIds,
            new HashSet<>(resourceCluster.getTaskExecutorsFor(requests).get().values()));

        TestKit probe = new TestKit(actorSystem);
        resourceClusterActor.tell(new GetActiveJobsRequest(
                CLUSTER_ID),
            probe.getRef());
        PagedActiveJobOverview jobsList = probe.expectMsgClass(PagedActiveJobOverview.class);
        assertEquals(n, jobsList.getActiveJobs().size());
        assertEquals(expectedJobIdList, jobsList.getActiveJobs());
        assertEquals(n, jobsList.getEndPosition());

        List<String> resJobsList = new ArrayList<>();
        int start = 0;

        do {
            resourceClusterActor.tell(
                GetActiveJobsRequest.builder()
                    .clusterID(CLUSTER_ID)
                    .startingIndex(Optional.of(start))
                    .pageSize(Optional.of(5))
                    .build(),
                probe.getRef());
            jobsList = probe.expectMsgClass(PagedActiveJobOverview.class);
            resJobsList.addAll(jobsList.getActiveJobs());
            assertTrue(jobsList.getActiveJobs().size() <= 5);
            start = jobsList.getEndPosition();
        } while (jobsList.getActiveJobs().size() > 0);
        assertEquals(expectedJobIdList, resJobsList);
    }

    @Test
    public void testIfDisableTaskExecutorRequestsMarkTaskExecutorsAsDisabled() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
        assertEquals(
            ImmutableList.of(TASK_EXECUTOR_ID),
            resourceCluster.getAvailableTaskExecutors().get());
        // mark task executor as disabled with an expiry set to 10 seconds
        resourceCluster.disableTaskExecutorsFor(ATTRIBUTES, Instant.now().plus(Duration.ofDays(1)), Optional.empty()).get();
        assertEquals(
            ImmutableList.of(),
            resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(
            new ResourceOverview(1, 0, 0, 0, 1),
            resourceCluster.resourceOverview().get());

        TestKit probe = new TestKit(actorSystem);
        resourceClusterActor.tell(
            new ExpireDisableTaskExecutorsRequest(
                new DisableTaskExecutorsRequest(
                    null,
                    CLUSTER_ID,
                    Instant.now().minus(Duration.ofSeconds(1)),
                    Optional.of(TASK_EXECUTOR_ID))),
            probe.getRef());
        assertEquals(
            ImmutableList.of(TASK_EXECUTOR_ID),
            resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(
            new ResourceOverview(1, 1, 0, 0, 0),
            resourceCluster.resourceOverview().get());
    }

    @Test
    public void testIfDisableTaskExecutorRequestsMarkLateTaskExecutorsAsDisabled() throws Exception {
        // mark task executor as disabled with an expiry set to 10 seconds
        resourceCluster.disableTaskExecutorsFor(ATTRIBUTES, Instant.now().plus(Duration.ofDays(1)), Optional.empty()).get();
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(
            ImmutableList.of(),
            resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(
            new ResourceOverview(1, 0, 0, 0, 1),
            resourceCluster.resourceOverview().get());
    }

    @Test
    public void testIfDisableTaskExecutorRequestsAreExpiredCorrectly() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        resourceCluster.disableTaskExecutorsFor(ATTRIBUTES, Instant.now().plus(Duration.ofSeconds(1)), Optional.empty()).get();
        assertEquals(
            new ResourceOverview(1, 0, 0, 0, 1),
            resourceCluster.resourceOverview().get());
        Thread.sleep(5000);
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION_2).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID_2,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
        assertEquals(
            new ResourceOverview(2, 1, 0, 0, 1),
            resourceCluster.resourceOverview().get());
    }

    @Test
    public void testIfDisabledTaskExecutorRequestsAreInitializedCorrectlyWhenTheControlPlaneStarts() throws Exception {
        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(Matchers.eq(CLUSTER_ID)))
            .thenReturn(ImmutableList.of(
                new DisableTaskExecutorsRequest(
                    ATTRIBUTES,
                    CLUSTER_ID,
                    Instant.now().plus(Duration.ofDays(1)),
                    Optional.empty())));

        actorSystem.stop(resourceClusterActor);
        setupActor();

        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(
            new ResourceOverview(1, 0, 0, 0, 1),
            resourceCluster.resourceOverview().get());
    }

    @Test
    public void testIfDisabledTaskExecutorsAreNotAvailableForScheduling() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION_2).get());
        assertEquals(
            Ack.getInstance(),
            resourceCluster.heartBeatFromTaskExecutor(
                new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).get());
        assertEquals(
            Ack.getInstance(),
            resourceCluster.heartBeatFromTaskExecutor(
                new TaskExecutorHeartbeat(TASK_EXECUTOR_ID_2, CLUSTER_ID, TaskExecutorReport.available())).get());
        resourceCluster.disableTaskExecutorsFor(ATTRIBUTES, Instant.now().plus(Duration.ofDays(1)), Optional.empty()).get();
        Set<TaskExecutorAllocationRequest> requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID_2,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());
    }

    @Test
    public void testIfTaskExecutorsThatWereRunningTasksPreviouslyAndRunningCorrectly() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(
            Ack.getInstance(),
            resourceCluster.heartBeatFromTaskExecutor(
                new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID))).get());
        resourceCluster.disableTaskExecutorsFor(ATTRIBUTES, Instant.now().plus(Duration.ofSeconds(1)), Optional.empty()).get();
        assertEquals(
            new ResourceOverview(1, 0, 1, 0, 1),
            resourceCluster.resourceOverview().get());

        ArgumentCaptor<WorkerEvent> workerEventCaptor = ArgumentCaptor.forClass(WorkerEvent.class);
        verify(jobMessageRouter).routeWorkerEvent(workerEventCaptor.capture());

        WorkerEvent actualWorkerEvent = workerEventCaptor.getValue();
        assertTrue(actualWorkerEvent instanceof WorkerOnDisabledVM);
        assertEquals(WORKER_ID, actualWorkerEvent.getWorkerId());
    }

    @Test(expected = TaskNotFoundException.class)
    public void testGetAssignedTaskExecutorAfterTaskCompletes() throws Throwable {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).join());
        assertEquals(
            Ack.getInstance(),
            resourceCluster.heartBeatFromTaskExecutor(
                new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).join());


        final Set<TaskExecutorAllocationRequest> requests = Collections.singleton(TaskExecutorAllocationRequest.of(WORKER_ID, SchedulingConstraints.of(MACHINE_DEFINITION), null, 0));
        assertEquals(
            TASK_EXECUTOR_ID,
            resourceCluster.getTaskExecutorsFor(requests).get().values().stream().findFirst().get());
        assertEquals(TASK_EXECUTOR_ID, resourceCluster.getTaskExecutorAssignedFor(WORKER_ID).join());
        assertEquals(Ack.getInstance(), resourceCluster.notifyTaskExecutorStatusChange(
            new TaskExecutorStatusChange(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.occupied(WORKER_ID))).join());

        assertEquals(Ack.getInstance(), resourceCluster.notifyTaskExecutorStatusChange(
            new TaskExecutorStatusChange(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).join());

        try {
            resourceCluster.getTaskExecutorAssignedFor(WORKER_ID).join();

        } catch (Exception e) {
            throw ExceptionUtils.stripCompletionException(e);
        }
    }

    @Test
    public void testTaskExecutorIsDisabledEvenAfterRestart() throws Exception {
        when(mantisJobStore.getTaskExecutor(ArgumentMatchers.eq(TASK_EXECUTOR_ID))).thenReturn(TASK_EXECUTOR_REGISTRATION);

        resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get();
        resourceCluster.disableTaskExecutorsFor(ATTRIBUTES, Instant.now().plus(Duration.ofDays(1)), Optional.empty()).get();
        assertTrue(resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get().isDisabled());

        resourceCluster.disconnectTaskExecutor(new TaskExecutorDisconnection(TASK_EXECUTOR_ID, CLUSTER_ID)).get();
        resourceCluster.heartBeatFromTaskExecutor(new TaskExecutorHeartbeat(TASK_EXECUTOR_ID, CLUSTER_ID, TaskExecutorReport.available())).get();

        assertTrue(resourceCluster.getTaskExecutorState(TASK_EXECUTOR_ID).get().isDisabled());
    }
}
