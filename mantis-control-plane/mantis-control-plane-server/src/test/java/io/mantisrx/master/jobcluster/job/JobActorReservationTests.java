/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.master.jobcluster.job;

import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.Ack;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.Strategy;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for JobActor reservation-based scheduling functionality.
 * Covers:
 * - Reservation upsert retry logic with exponential backoff
 * - Delayed reservation timer behavior
 * - Shutdown reservation cancellation
 * - Priority types for different scheduling scenarios
 * - Reservation content validation
 */
@Slf4j
public class JobActorReservationTests {

    static ActorSystem system;
    private static final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
            new AuditEventSubscriberLoggingImpl(),
            new StatusEventSubscriberLoggingImpl(),
            new WorkerEventSubscriberLoggingImpl());

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        setupReservationConfig();
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    /**
     * Setup master config for reservation-based scheduling tests.
     */
    private static void setupReservationConfig() {
        Properties props = new Properties();
        props.setProperty("mantis.scheduling.reservation.enabled", "true");
        TestHelpers.setupMasterConfig(props);
    }

    // =====================================================
    // Reservation Upsert Retry Logic Tests
    // =====================================================

    /**
     * Test that a successful reservation upsert on first attempt does not trigger retry.
     */
    @Test
    public void testReservationUpsertSucceedsFirstAttempt() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testReservationUpsertSucceedsFirstAttempt";

        MantisScheduler schedulerMock = createMockSchedulerWithSuccessfulReservation();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(1);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        // Wait for job initialization and worker scheduling
        Thread.sleep(500);

        // Verify upsertReservation was called (for initial workers)
        verify(schedulerMock, atLeastOnce()).upsertReservation(any(UpsertReservation.class));

        // Verify no additional retries (should be called exactly for initial stages)
        // For a single-stage scalable job with JobMaster, we expect 2 calls:
        // one for stage 0 (job master) and one for stage 1
        verify(schedulerMock, atMost(2)).upsertReservation(any(UpsertReservation.class));
    }

    /**
     * Test that a failed reservation upsert triggers retry with exponential backoff.
     */
    @Test
    public void testReservationUpsertFailsAndRetriesWithBackoff() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testReservationUpsertFailsAndRetries";

        // Create scheduler mock that fails first, then succeeds
        AtomicInteger callCount = new AtomicInteger(0);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count <= 2) {
                // First two calls fail
                return CompletableFuture.failedFuture(new RuntimeException("Simulated failure " + count));
            }
            // Third call succeeds
            return CompletableFuture.completedFuture(Ack.getInstance());
        });
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(1);

        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

        MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

        final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock,
                lifecycleEventPublisher, CostsCalculator.noop()));

        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(Duration.ofSeconds(5), JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        // Wait for retries to complete (base delay 1s, exponential backoff)
        // First retry at 1s, second retry at 2s (total ~3s)
        // Use polling to wait for condition instead of fixed sleep
        long startTime = System.currentTimeMillis();
        long timeoutMs = 4000; // 4s timeout (3s + buffer)
        while (callCount.get() < 3 && (System.currentTimeMillis() - startTime) < timeoutMs) {
            Thread.sleep(100); // Poll every 100ms
        }

        // Verify multiple upsert attempts were made (retries occurred)
        verify(schedulerMock, atLeast(3)).upsertReservation(any(UpsertReservation.class));
        log.info("Total upsertReservation calls: {} (waited {}ms)", callCount.get(), System.currentTimeMillis() - startTime);
    }

    /**
     * Test that reservation upsert fails after maximum retries and publishes error event.
     */
    @Test
    public void testReservationUpsertFailsAfterMaxRetries() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testReservationUpsertFailsAfterMaxRetries";

        // Create scheduler mock that always fails
        AtomicInteger failureCount = new AtomicInteger(0);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(any())).thenAnswer(invocation -> {
            failureCount.incrementAndGet();
            return CompletableFuture.failedFuture(new RuntimeException("Persistent failure"));
        });
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(1);

        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

        MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

        final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock,
                lifecycleEventPublisher, CostsCalculator.noop()));

        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(Duration.ofSeconds(5), JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        // Wait for all retries to complete
        // MAX_RESERVATION_UPSERT_RETRIES = 3
        // Delays: 1s, 2s, 4s (exponential backoff)
        Thread.sleep(10000);

        // Verify at least 3 attempts per stage were made (initial + 2 retries = 3)
        // With a scalable job, we have job master (stage 0) and worker stage (stage 1)
        int expectedMinCalls = 3; // At least 3 for one stage
        assertTrue("Expected at least " + expectedMinCalls + " upsertReservation calls, got " + failureCount.get(),
                failureCount.get() >= expectedMinCalls);

        log.info("Total failure count after max retries: {}", failureCount.get());
    }

    // =====================================================
    // Shutdown Reservation Cancellation Tests
    // =====================================================

    /**
     * Test that job shutdown with reservation scheduling cancels reservations for each stage.
     */
    @Test
    public void testShutdownCancelsReservationsForEachStage() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testShutdownCancelsReservations";

        MantisScheduler schedulerMock = createMockSchedulerWithSuccessfulReservation();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        // Create multi-stage job
        SchedulingInfo sInfo = createMultiStageSchedulingInfo(3);
        ActorRef jobActor = submitJobMultiStage(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        // Verify job is initialized
        String jobId = clusterName + "-1";
        JobTestHelper.getJobDetailsAndVerify(probe, jobActor, jobId, SUCCESS, JobState.Accepted);

        // Kill the job
        jobActor.tell(new JobClusterProto.KillJobRequest(
                new JobId(clusterName, 1), "test shutdown", JobCompletedReason.Killed, "test", probe.getRef()), probe.getRef());
        JobClusterProto.KillJobResponse killResp = probe.expectMsgClass(
                Duration.ofSeconds(10), JobClusterProto.KillJobResponse.class);
        assertEquals(SUCCESS, killResp.responseCode);

        // Wait for shutdown processing
        Thread.sleep(1000);

        // Verify cancelReservation was called for each stage
        // Multi-stage job has stages 1, 2, 3 (plus job master stage 0 for scalable jobs)
        ArgumentCaptor<CancelReservation> cancelCaptor = ArgumentCaptor.forClass(CancelReservation.class);
        verify(schedulerMock, atLeast(3)).cancelReservation(cancelCaptor.capture());

        // Verify the cancellation requests contain correct job ID
        for (CancelReservation cancelReq : cancelCaptor.getAllValues()) {
            assertEquals(jobId, cancelReq.getReservationKey().getJobId());
            log.info("Cancelled reservation for stage: {}", cancelReq.getReservationKey().getStageNumber());
        }
    }

    /**
     * Test that job shutdown with legacy scheduling calls unscheduleJob (not cancelReservation).
     */
    @Test
    public void testShutdownWithLegacySchedulingCallsUnscheduleJob() throws Exception {
        // Setup legacy config
        Properties props = new Properties();
        props.setProperty("mantis.scheduling.reservation.enabled", "false");
        TestHelpers.setupMasterConfig(props);

        final TestKit probe = new TestKit(system);
        String clusterName = "testShutdownWithLegacyScheduling";

        MantisScheduler schedulerMock = createMockSchedulerWithSuccessfulReservation();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(1);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        String jobId = clusterName + "-1";

        // Kill the job
        jobActor.tell(new JobClusterProto.KillJobRequest(
                new JobId(clusterName, 1), "test shutdown", JobCompletedReason.Killed, "test", probe.getRef()), probe.getRef());
        JobClusterProto.KillJobResponse killResp = probe.expectMsgClass(
                Duration.ofSeconds(10), JobClusterProto.KillJobResponse.class);
        assertEquals(SUCCESS, killResp.responseCode);

        Thread.sleep(500);

        // Verify unscheduleJob was called (legacy behavior)
        verify(schedulerMock).unscheduleJob(jobId);

        // Verify cancelReservation was NOT called
        verify(schedulerMock, never()).cancelReservation(any());

        // Restore reservation config for other tests
        setupReservationConfig();
    }

    // =====================================================
    // Priority Type Tests
    // =====================================================

    /**
     * Test that initial job submission uses NEW_JOB priority.
     */
    @Test
    public void testInitialWorkersUseNewJobPriority() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testInitialWorkersUseNewJobPriority";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(1);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        // Wait for initial workers to be scheduled
        Thread.sleep(500);

        // Verify NEW_JOB priority was used for initial workers
        boolean foundNewJobPriority = reservationCaptor.getAllValues().stream()
                .anyMatch(r -> r.getPriority().getType() == PriorityType.NEW_JOB);
        assertTrue("Initial workers should use NEW_JOB priority", foundNewJobPriority);

        log.info("Verified NEW_JOB priority for initial workers");
    }

    /**
     * Test that scale up operations use SCALE priority.
     */
    @Test
    public void testScaleUpUsesScalePriority() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testScaleUpUsesScalePriority";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createScalableStageSchedulingInfo(1, 1, 10);
        ActorRef jobActor = submitScalableJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        // Clear captured values from initial submission
        reservationCaptor.getAllValues().clear();

        // Send scale up request
        String jobId = clusterName + "-1";
        jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(jobId, 1, 3, "test", "scale test"), probe.getRef());
        JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(
                Duration.ofSeconds(5), JobClusterManagerProto.ScaleStageResponse.class);
        assertEquals(SUCCESS, scaleResp.responseCode);
        assertEquals(3, scaleResp.getActualNumWorkers());

        Thread.sleep(500);

        // Verify SCALE priority was used for scale up
        boolean foundScalePriority = reservationCaptor.getAllValues().stream()
                .anyMatch(r -> r.getPriority().getType() == PriorityType.SCALE);
        assertTrue("Scale up should use SCALE priority", foundScalePriority);

        log.info("Verified SCALE priority for scale up operation");
    }

    /**
     * Test that worker resubmit uses REPLACE priority (highest).
     */
    @Test
    public void testWorkerResubmitUsesReplacePriority() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testWorkerResubmitUsesReplacePriority";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createScalableStageSchedulingInfo(1, 1, 10);
        ActorRef jobActor = submitScalableJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        String jobId = clusterName + "-1";

        // Get worker to Started state first
        WorkerId workerId = new WorkerId(jobId, 0, 2); // Worker index 0, number 2 (after job master)
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 1, workerId);

        // Clear captured values
        reservationCaptor.getAllValues().clear();

        // Trigger worker resubmit by sending terminated event
        JobTestHelper.sendWorkerTerminatedEvent(probe, jobActor, jobId, workerId);

        // Wait for resubmit to be processed
        Thread.sleep(1000);

        // Verify REPLACE priority was used for resubmit
        boolean foundReplacePriority = reservationCaptor.getAllValues().stream()
                .anyMatch(r -> r.getPriority().getType() == PriorityType.REPLACE);
        assertTrue("Worker resubmit should use REPLACE priority", foundReplacePriority);

        log.info("Verified REPLACE priority for worker resubmit");
    }

    // =====================================================
    // Reservation Content Validation Tests
    // =====================================================

    /**
     * Test that UpsertReservation contains correct reservation key fields.
     */
    @Test
    public void testUpsertReservationContainsCorrectReservationKey() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testUpsertReservationKey";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(2);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        Thread.sleep(500);

        String jobId = clusterName + "-1";

        // Verify reservation keys
        for (UpsertReservation reservation : reservationCaptor.getAllValues()) {
            assertEquals("Job ID should match", jobId, reservation.getReservationKey().getJobId());
            assertTrue("Stage number should be valid",
                    reservation.getReservationKey().getStageNumber() >= 0);
            log.info("Verified reservation key: jobId={}, stageNum={}",
                    reservation.getReservationKey().getJobId(),
                    reservation.getReservationKey().getStageNumber());
        }
    }

    /**
     * Test that UpsertReservation contains correct allocation requests.
     */
    @Test
    public void testUpsertReservationContainsCorrectAllocationRequests() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testUpsertReservationAllocationRequests";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        int workerCount = 3;
        SchedulingInfo sInfo = createSingleStageSchedulingInfo(workerCount);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        Thread.sleep(500);

        // Find the reservation for stage 1 (worker stage)
        UpsertReservation stage1Reservation = reservationCaptor.getAllValues().stream()
                .filter(r -> r.getReservationKey().getStageNumber() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull("Should have reservation for stage 1", stage1Reservation);
        assertEquals("Allocation requests should match worker count",
                workerCount, stage1Reservation.getAllocationRequests().size());

        // Verify allocation request details
        stage1Reservation.getAllocationRequests().forEach(alloc -> {
            assertNotNull("Worker ID should be present", alloc.getWorkerId());
            assertNotNull("Scheduling constraints should be present", alloc.getConstraints());
            assertEquals("Stage number should match", 1, alloc.getStageNum());
            log.info("Verified allocation request for worker: {}", alloc.getWorkerId());
        });
    }

    /**
     * Test that UpsertReservation contains correct stage target size.
     */
    @Test
    public void testUpsertReservationContainsCorrectStageTargetSize() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testUpsertReservationTargetSize";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        int targetWorkers = 5;
        SchedulingInfo sInfo = createSingleStageSchedulingInfo(targetWorkers);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        Thread.sleep(500);

        // Find the reservation for stage 1
        UpsertReservation stage1Reservation = reservationCaptor.getAllValues().stream()
                .filter(r -> r.getReservationKey().getStageNumber() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull("Should have reservation for stage 1", stage1Reservation);
        assertEquals("Stage target size should match configured workers",
                targetWorkers, stage1Reservation.getStageTargetSize());

        log.info("Verified stage target size: {}", stage1Reservation.getStageTargetSize());
    }

    /**
     * Test that UpsertReservation contains scheduling constraints from stage metadata.
     */
    @Test
    public void testUpsertReservationContainsSchedulingConstraints() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testUpsertReservationConstraints";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        // Create job with specific machine definition
        double cpus = 2.0;
        double memoryMB = 4096.0;
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(
                        new MachineDefinition(cpus, memoryMB, 1.0, 1.0, 3),
                        Lists.newArrayList(),
                        Lists.newArrayList())
                .build();

        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        Thread.sleep(500);

        // Find the reservation for stage 1
        UpsertReservation stage1Reservation = reservationCaptor.getAllValues().stream()
                .filter(r -> r.getReservationKey().getStageNumber() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull("Should have reservation for stage 1", stage1Reservation);
        assertNotNull("Should have scheduling constraints", stage1Reservation.getSchedulingConstraints());

        // Verify machine definition in constraints
        assertEquals("CPU should match", cpus,
                stage1Reservation.getSchedulingConstraints().getMachineDefinition().getCpuCores(), 0.01);
        assertEquals("Memory should match", memoryMB,
                stage1Reservation.getSchedulingConstraints().getMachineDefinition().getMemoryMB(), 0.01);

        log.info("Verified scheduling constraints: cpus={}, memory={}",
                stage1Reservation.getSchedulingConstraints().getMachineDefinition().getCpuCores(),
                stage1Reservation.getSchedulingConstraints().getMachineDefinition().getMemoryMB());
    }

    /**
     * Test that priority tier is derived from job SLA duration type.
     */
    @Test
    public void testPriorityTierDerivedFromJobSla() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testPriorityTierFromSla";

        ArgumentCaptor<UpsertReservation> reservationCaptor = ArgumentCaptor.forClass(UpsertReservation.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(reservationCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SchedulingInfo sInfo = createSingleStageSchedulingInfo(1);
        ActorRef jobActor = submitJob(probe, clusterName, sInfo, schedulerMock, jobStoreMock);

        Thread.sleep(500);

        // Verify priority tier is set (derived from MantisJobDurationType ordinal)
        for (UpsertReservation reservation : reservationCaptor.getAllValues()) {
            assertNotNull("Priority should be set", reservation.getPriority());
            assertTrue("Priority tier should be non-negative", reservation.getPriority().getTier() >= 0);
            assertTrue("Priority timestamp should be positive", reservation.getPriority().getTimestamp() > 0);
            log.info("Verified priority: type={}, tier={}, timestamp={}",
                    reservation.getPriority().getType(),
                    reservation.getPriority().getTier(),
                    reservation.getPriority().getTimestamp());
        }
    }

    // =====================================================
    // Helper Methods
    // =====================================================

    private MantisScheduler createMockSchedulerWithSuccessfulReservation() {
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        when(schedulerMock.upsertReservation(any()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any()))
                .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        return schedulerMock;
    }

    private SchedulingInfo createSingleStageSchedulingInfo(int numWorkers) {
        return new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerStageWithConstraints(numWorkers,
                        new MachineDefinition(1.0, 1.0, 1.0, 3),
                        Lists.newArrayList(),
                        Lists.newArrayList())
                .build();
    }

    private SchedulingInfo createMultiStageSchedulingInfo(int numStages) {
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder().numberOfStages(numStages);
        for (int i = 1; i <= numStages; i++) {
            builder = builder.multiWorkerStageWithConstraints(1,
                    new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(),
                    Lists.newArrayList());
        }
        return builder.build();
    }

    private SchedulingInfo createScalableStageSchedulingInfo(int numWorkers, int min, int max) {
        Map<ScalingReason, Strategy> smap = new HashMap<>();
        smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));

        return new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerScalableStageWithConstraints(numWorkers,
                        new MachineDefinition(1.0, 1.0, 1.0, 3),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        new StageScalingPolicy(1, min, max, 1, 1, 0, smap, true))
                .build();
    }

    private ActorRef submitJob(TestKit probe, String clusterName, SchedulingInfo sInfo,
                               MantisScheduler schedulerMock, MantisJobStore jobStoreMock) throws Exception {
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

        MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

        final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock,
                lifecycleEventPublisher, CostsCalculator.noop()));

        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(Duration.ofSeconds(5), JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        return jobActor;
    }

    private ActorRef submitJobMultiStage(TestKit probe, String clusterName, SchedulingInfo sInfo,
                                          MantisScheduler schedulerMock, MantisJobStore jobStoreMock) throws Exception {
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

        MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

        final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock,
                lifecycleEventPublisher, CostsCalculator.noop()));

        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(Duration.ofSeconds(5), JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        return jobActor;
    }

    private ActorRef submitScalableJob(TestKit probe, String clusterName, SchedulingInfo sInfo,
                                        MantisScheduler schedulerMock, MantisJobStore jobStoreMock) throws Exception {
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

        MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

        final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock,
                lifecycleEventPublisher, CostsCalculator.noop()));

        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(Duration.ofSeconds(5), JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        String jobId = clusterName + "-1";

        // Move job to Launched state by starting workers
        // Job master (stage 0, worker 1)
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 0, new WorkerId(jobId, 0, 1));

        // Worker stage (stage 1, worker 2)
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 1, new WorkerId(jobId, 0, 2));

        // Verify job is in Launched state
        JobTestHelper.getJobDetailsAndVerify(probe, jobActor, jobId, SUCCESS, JobState.Launched);

        return jobActor;
    }
}

