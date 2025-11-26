package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetPendingReservationsView;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.PendingReservationsView;
import static io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.*;
import io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.master.scheduler.WorkerLaunchFailed;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import java.net.URL;
import org.junit.After;
import org.mockito.ArgumentCaptor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReservationRegistryActorIntegrationTest {

    private static final MachineDefinition MACHINE =
        new MachineDefinition(2.0, 4096, 128.0, 10240, 1);
    private static final Duration PROCESS_INTERVAL = Duration.ofMillis(100);
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofMillis(200);
    private static final Duration DISABLED_CHECK_INTERVAL = Duration.ofSeconds(10);
    private static final Duration SCHEDULER_LEASE_EXPIRATION = Duration.ofMillis(100);
    private static final Instant BASE_INSTANT = Instant.parse("2024-01-01T00:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(BASE_INSTANT, ZoneOffset.UTC);
    private static final ClusterID TEST_CLUSTER_ID = ClusterID.of("test-cluster");
    private static final TaskExecutorID TASK_EXECUTOR_ID_1 = TaskExecutorID.of("te-1");
    private static final TaskExecutorID TASK_EXECUTOR_ID_2 = TaskExecutorID.of("te-2");
    private static final ContainerSkuID CONTAINER_SKU_ID = ContainerSkuID.of("SKU1");
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);

    private static ActorSystem system;

    private TestingRpcService rpcService;
    private TaskExecutorGateway gateway;
    private MantisJobStore mantisJobStore;
    private JobMessageRouter jobMessageRouter;
    private ActorRef resourceClusterActor;

    @BeforeClass
    public static void setUp() {
        system = ActorSystem.create("ReservationRegistryActorIntegrationTest");
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Before
    public void setupTest() throws Exception {
        rpcService = new TestingRpcService();
        gateway = mock(TaskExecutorGateway.class);
        rpcService.registerGateway("te-address-1", gateway);
        rpcService.registerGateway("te-address-2", gateway);

        mantisJobStore = mock(MantisJobStore.class);
        jobMessageRouter = mock(JobMessageRouter.class);

        when(mantisJobStore.loadAllDisableTaskExecutorsRequests(any())).thenReturn(ImmutableList.of());
        when(mantisJobStore.getJobArtifactsToCache(any())).thenReturn(ImmutableList.of());
        when(jobMessageRouter.routeWorkerEvent(any())).thenReturn(true);
        when(gateway.submitTask(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MasterConfiguration masterConfig = mock(MasterConfiguration.class);
        ExecuteStageRequestFactory executeStageRequestFactory = new ExecuteStageRequestFactory(masterConfig);
        resourceClusterActor = system.actorOf(
            ResourceClusterActor.props(
                TEST_CLUSTER_ID,
                HEARTBEAT_TIMEOUT,
                ASSIGNMENT_TIMEOUT,
                DISABLED_CHECK_INTERVAL,
                SCHEDULER_LEASE_EXPIRATION,
                FIXED_CLOCK,
                rpcService,
                mantisJobStore,
                jobMessageRouter,
                0,
                "",
                false,
                ImmutableMap.of(),
                new CpuWeightedFitnessCalculator(),
                executeStageRequestFactory,
                false));
    }

    @After
    public void cleanupTest() {
        if (resourceClusterActor != null) {
            TestKit watcher = new TestKit(system);
            watcher.watch(resourceClusterActor);
            system.stop(resourceClusterActor);
            watcher.expectTerminated(resourceClusterActor);
        }
    }

    @Test
    public void testFullReservationWorkflowWithTaskExecutorAllocation() throws Exception {
        TestKit probe = new TestKit(system);

        // Register 2 task executors
        TaskExecutorRegistration registration1 = TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_1)
            .clusterID(TEST_CLUSTER_ID)
            .taskExecutorAddress("te-address-1")
            .hostname("hostname-1")
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE)
            .taskExecutorAttributes(ImmutableMap.of(
                WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_SKU_ID.getResourceID()))
            .build();

        TaskExecutorRegistration registration2 = TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_2)
            .clusterID(TEST_CLUSTER_ID)
            .taskExecutorAddress("te-address-2")
            .hostname("hostname-2")
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE)
            .taskExecutorAttributes(ImmutableMap.of(
                WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_SKU_ID.getResourceID()))
            .build();

        resourceClusterActor.tell(registration1, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        resourceClusterActor.tell(registration2, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Send heartbeats to make them available
        resourceClusterActor.tell(
            new TaskExecutorHeartbeat(TASK_EXECUTOR_ID_1, TEST_CLUSTER_ID, TaskExecutorReport.available()),
            probe.getRef());
        probe.expectMsg(Ack.getInstance());

        resourceClusterActor.tell(
            new TaskExecutorHeartbeat(TASK_EXECUTOR_ID_2, TEST_CLUSTER_ID, TaskExecutorReport.available()),
            probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Create reservations with allocation requests
        ReservationKey key1 = ReservationKey.builder().jobId("job-1").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-2").stageNumber(1).build();

        SchedulingConstraints constraints = SchedulingConstraints.of(MACHINE, Optional.empty(), ImmutableMap.of());

        WorkerId worker1 = WorkerId.fromIdUnsafe("job-1-worker-0-1");
        WorkerId worker2 = WorkerId.fromIdUnsafe("job-2-worker-0-1");

        TaskExecutorAllocationRequest req1 = TaskExecutorAllocationRequest.of(worker1, constraints, createJobMetadata(), 1);
        TaskExecutorAllocationRequest req2 = TaskExecutorAllocationRequest.of(worker2, constraints, createJobMetadata(), 1);

        // Upsert reservations
        upsertReservation(resourceClusterActor, probe, key1, constraints, Set.of(req1), 1, BASE_INSTANT.toEpochMilli());
        upsertReservation(resourceClusterActor, probe, key2, constraints, Set.of(req2), 1, BASE_INSTANT.plusSeconds(1).toEpochMilli());

        // Mark reservation registry ready
        resourceClusterActor.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Check that reservations are pending
        resourceClusterActor.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(view.isReady());
        assertEquals(1, view.getGroups().size());
        assertEquals(
            2,
            view.getGroups().entrySet().stream().findFirst().get().getValue().getReservations().size());

        // Allow some time for processing
        Thread.sleep(300);

        // Check that reservations were processed
        resourceClusterActor.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        view = probe.expectMsgClass(PendingReservationsView.class);

        // Reservations should be cleared after successful allocation
        assertTrue(view.getGroups().isEmpty() || view.getGroups().values().stream()
            .allMatch(g -> g.getReservationCount() == 0));

        // Verify assignment was attempted on the gateway
        ArgumentCaptor<ExecuteStageRequest> executeStageRequestCaptor = ArgumentCaptor.forClass(ExecuteStageRequest.class);
        verify(gateway, timeout(5000).times(2)).submitTask(executeStageRequestCaptor.capture());

        List<ExecuteStageRequest> requests = executeStageRequestCaptor.getAllValues();
        Set<WorkerId> submittedWorkers = requests.stream()
            .map(r -> new WorkerId(r.getJobId(), r.getWorkerIndex(), r.getWorkerNumber()))
            .collect(Collectors.toSet());

        assertTrue(submittedWorkers.contains(worker1));
        assertTrue(submittedWorkers.contains(worker2));

        // Verify WorkerLaunched event was routed
        ArgumentCaptor<WorkerLaunched> workerLaunchedCaptor = ArgumentCaptor.forClass(WorkerLaunched.class);
        verify(jobMessageRouter, timeout(5000).times(2)).routeWorkerEvent(workerLaunchedCaptor.capture());

        List<WorkerLaunched> launchedEvents = workerLaunchedCaptor.getAllValues();
        Set<WorkerId> launchedWorkers = launchedEvents.stream()
            .map(WorkerLaunched::getWorkerId)
            .collect(Collectors.toSet());

        assertTrue(launchedWorkers.contains(worker1));
        assertTrue(launchedWorkers.contains(worker2));
    }

    @Test
    public void testReservationWithMultipleConstraintGroups() throws Exception {
        TestKit probe = new TestKit(system);

        // Register task executors
        TaskExecutorRegistration registration = TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_1)
            .clusterID(TEST_CLUSTER_ID)
            .taskExecutorAddress("te-address-1")
            .hostname("hostname-1")
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE)
            .taskExecutorAttributes(ImmutableMap.of(
                WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_SKU_ID.getResourceID()))
            .build();

        resourceClusterActor.tell(registration, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        resourceClusterActor.tell(
            new TaskExecutorHeartbeat(TASK_EXECUTOR_ID_1, TEST_CLUSTER_ID, TaskExecutorReport.available()),
            probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Create reservations with different constraints
        ReservationKey key1 = ReservationKey.builder().jobId("job-a").stageNumber(1).build();
        ReservationKey key2 = ReservationKey.builder().jobId("job-b").stageNumber(1).build();

        SchedulingConstraints constraintsA = SchedulingConstraints.of(
            MACHINE, Optional.of("sku-a"), ImmutableMap.of("zone", "us-west"));
        SchedulingConstraints constraintsB = SchedulingConstraints.of(
            MACHINE, Optional.of("sku-b"), ImmutableMap.of("zone", "us-east"));

        WorkerId workerA = WorkerId.fromIdUnsafe("job-a-worker-0-1");
        WorkerId workerB = WorkerId.fromIdUnsafe("job-b-worker-0-1");

        TaskExecutorAllocationRequest reqA = TaskExecutorAllocationRequest.of(workerA, constraintsA, createJobMetadata(), 1);
        TaskExecutorAllocationRequest reqB = TaskExecutorAllocationRequest.of(workerB, constraintsB, createJobMetadata(), 1);

        upsertReservation(resourceClusterActor, probe, key1, constraintsA, Set.of(reqA), 1, BASE_INSTANT.toEpochMilli());
        upsertReservation(resourceClusterActor, probe, key2, constraintsB, Set.of(reqB), 1, BASE_INSTANT.toEpochMilli());

        resourceClusterActor.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Check that both reservation groups exist
        resourceClusterActor.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);

        assertTrue(view.isReady());
        assertEquals(2, view.getGroups().size());
    }

    @Test
    public void testReservationCancellation() throws Exception {
        TestKit probe = new TestKit(system);

        ReservationKey key = ReservationKey.builder().jobId("job-cancel").stageNumber(1).build();
        SchedulingConstraints constraints = SchedulingConstraints.of(MACHINE, Optional.empty(), ImmutableMap.of());
        WorkerId worker = WorkerId.fromIdUnsafe("job-cancel-worker-0-1");
        TaskExecutorAllocationRequest req = TaskExecutorAllocationRequest.of(worker, constraints, createJobMetadata(), 1);
        resourceClusterActor.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        upsertReservation(resourceClusterActor, probe, key, constraints, Set.of(req), 1, BASE_INSTANT.toEpochMilli());

        // Verify reservation exists
        resourceClusterActor.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);
        assertFalse(view.getGroups().isEmpty());

        // Cancel reservation
        resourceClusterActor.tell(
            CancelReservation.builder().reservationKey(key).build(),
            probe.getRef());
        probe.expectMsgClass(Ack.class);

        // Verify reservation removed
        resourceClusterActor.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        view = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(view.getGroups().isEmpty());
    }

    @Test
    public void testAssignmentRetry() throws Exception {
        TestKit probe = new TestKit(system);

        // Reset gateway mock to have different behavior for this test
        // 1. Fail first
        // 2. Succeed second
        when(gateway.submitTask(any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        TaskExecutorRegistration registration = TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_1)
            .clusterID(TEST_CLUSTER_ID)
            .taskExecutorAddress("te-address-1")
            .hostname("hostname-1")
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE)
            .taskExecutorAttributes(ImmutableMap.of(
                WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_SKU_ID.getResourceID()))
            .build();

        resourceClusterActor.tell(registration, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        resourceClusterActor.tell(
            new TaskExecutorHeartbeat(TASK_EXECUTOR_ID_1, TEST_CLUSTER_ID, TaskExecutorReport.available()),
            probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Create reservation
        ReservationKey key = ReservationKey.builder().jobId("job-retry").stageNumber(1).build();
        SchedulingConstraints constraints = SchedulingConstraints.of(MACHINE, Optional.empty(), ImmutableMap.of());
        WorkerId worker = WorkerId.fromIdUnsafe("job-retry-worker-0-1");
        TaskExecutorAllocationRequest req = TaskExecutorAllocationRequest.of(worker, constraints, createJobMetadata(), 1);

        upsertReservation(resourceClusterActor, probe, key, constraints, Set.of(req), 1, BASE_INSTANT.toEpochMilli());

        resourceClusterActor.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Check that reservation is eventually processed
        Thread.sleep(500);

        resourceClusterActor.tell(GetPendingReservationsView.INSTANCE, probe.getRef());
        PendingReservationsView view = probe.expectMsgClass(PendingReservationsView.class);
        assertTrue(view.getGroups().isEmpty() || view.getGroups().values().stream()
            .allMatch(g -> g.getReservationCount() == 0));

        // Verify gateway was called twice
        verify(gateway, timeout(5000).times(2)).submitTask(any());

        // Verify WorkerLaunched event was routed (eventually success)
        verify(jobMessageRouter, timeout(5000).times(1)).routeWorkerEvent(any(WorkerLaunched.class));
    }

    @Test
    public void testAssignmentFailure() throws Exception {
        TestKit probe = new TestKit(system);

        // Fail always
        when(gateway.submitTask(any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Permanent failure")));

        TaskExecutorRegistration registration = TaskExecutorRegistration.builder()
            .taskExecutorID(TASK_EXECUTOR_ID_2)
            .clusterID(TEST_CLUSTER_ID)
            .taskExecutorAddress("te-address-2")
            .hostname("hostname-2")
            .workerPorts(WORKER_PORTS)
            .machineDefinition(MACHINE)
            .taskExecutorAttributes(ImmutableMap.of(
                WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, CONTAINER_SKU_ID.getResourceID()))
            .build();

        resourceClusterActor.tell(registration, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        resourceClusterActor.tell(
            new TaskExecutorHeartbeat(TASK_EXECUTOR_ID_2, TEST_CLUSTER_ID, TaskExecutorReport.available()),
            probe.getRef());
        probe.expectMsg(Ack.getInstance());

        ReservationKey key = ReservationKey.builder().jobId("job-fail").stageNumber(1).build();
        SchedulingConstraints constraints = SchedulingConstraints.of(MACHINE, Optional.empty(), ImmutableMap.of());
        WorkerId worker = WorkerId.fromIdUnsafe("job-fail-worker-0-1");
        TaskExecutorAllocationRequest req = TaskExecutorAllocationRequest.of(worker, constraints, createJobMetadata(), 1);

        upsertReservation(resourceClusterActor, probe, key, constraints, Set.of(req), 1, BASE_INSTANT.toEpochMilli());

        resourceClusterActor.tell(MarkReady.INSTANCE, probe.getRef());
        probe.expectMsg(Ack.getInstance());

        // Wait for retries to exhaust
        Thread.sleep(1000);

        // Verify gateway called multiple times (3 retries)
        verify(gateway, timeout(5000).atLeast(3)).submitTask(any());

        // Verify WorkerLaunchFailed event was routed
        verify(jobMessageRouter, timeout(5000).times(1)).routeWorkerEvent(any(WorkerLaunchFailed.class));
    }

    private void upsertReservation(
        ActorRef actor,
        TestKit probe,
        ReservationKey key,
        SchedulingConstraints constraints,
        Set<TaskExecutorAllocationRequest> allocationRequests,
        int stageTargetSize,
        long priorityTimestamp
    ) {
        ReservationPriority priority = ReservationPriority.builder()
            .type(ReservationPriority.PriorityType.NEW_JOB)
            .tier(0)
            .timestamp(priorityTimestamp)
            .build();

        UpsertReservation upsert = UpsertReservation.builder()
            .reservationKey(key)
            .schedulingConstraints(constraints)
            .allocationRequests(allocationRequests)
            .stageTargetSize(stageTargetSize)
            .priority(priority)
            .build();

        actor.tell(upsert, probe.getRef());
        probe.expectMsg(Ack.getInstance());
    }

    private JobMetadata createJobMetadata() {
        try {
            JobMetadata jm = mock(JobMetadata.class);
            when(jm.getJobJarUrl()).thenReturn(new URL("http://localhost"));
            when(jm.getParameters()).thenReturn(Collections.emptyList());
            when(jm.getSubscriptionTimeoutSecs()).thenReturn(10L);
            when(jm.getHeartbeatIntervalSecs()).thenReturn(10L);
            when(jm.getMinRuntimeSecs()).thenReturn(10L);
            return jm;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
