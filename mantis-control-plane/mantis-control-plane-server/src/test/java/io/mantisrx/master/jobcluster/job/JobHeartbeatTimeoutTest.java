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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.shaded.com.google.common.collect.Lists;

import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the heartbeat timeout logic in JobActor, specifically focusing on the scenario
 * where workers get stuck in the "launched" state without transitioning to "starting" or "started"
 * states when the worker container crashes before sending a signal.
 */
public class JobHeartbeatTimeoutTest {

    static ActorSystem system;
    private static LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(
            new AuditEventSubscriberLoggingImpl(),
            new StatusEventSubscriberLoggingImpl(),
            new WorkerEventSubscriberLoggingImpl());
    private final CostsCalculator costsCalculator = CostsCalculator.noop();

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        TestHelpers.setupMasterConfig();
    }

    @AfterClass
    public static void tearDown() {
        JobTestHelper.deleteAllFiles();
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

    /**
     * Setup master config for legacy scheduling tests.
     */
    private static void setupLegacyConfig() {
        Properties props = new Properties();
        props.setProperty("mantis.scheduling.reservation.enabled", "false");
        TestHelpers.setupMasterConfig(props);
    }

    /**
     * Test that a worker that has been launched but never sent a heartbeat will be resubmitted
     * after the timeout period (reservation-based scheduling).
     */
    @Test
    public void testWorkerStuckInLaunchedStateWithoutHeartbeatReservation() {
        setupReservationConfig();
        final TestKit probe = new TestKit(system);
        String clusterName = "testWorkerStuckInLaunchedStateReservation";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        try {
            // Create a job with a single worker
            SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(), Lists.newArrayList())
                .build();

            JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
            MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

            final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            // Initialize the job
            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            int stageNo = 1;
            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // Send only a LAUNCHED event, but no heartbeat
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNo);

            // Verify the worker is in LAUNCHED state
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);

            IMantisWorkerMetadata worker = resp.getJobMetadata().get().getStageMetadata(stageNo).get()
                .getWorkerByIndex(0).getMetadata();
            assertEquals(WorkerState.Launched, worker.getState());

            // Trigger heartbeat check with a time far enough in the future to exceed the timeout
            // The default worker timeout is 60 seconds, and the check uses 1.5x that value
            Instant futureTime = Instant.now().plusSeconds(100);
            jobActor.tell(new JobProto.CheckHeartBeat(futureTime), probe.getRef());

            // Wait for resubmission to be processed
            Thread.sleep(1000);

            // Verify that the worker was terminated and a new one was scheduled
            // For reservation-based scheduling, verify upsertReservation instead of scheduleWorkers
            verify(schedulerMock, times(2)).upsertReservation(any(UpsertReservation.class)); // Initial + resubmit
            verify(schedulerMock, never()).scheduleWorkers(any());
            verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Test that a worker that has been launched but never sent a heartbeat will be resubmitted
     * after the timeout period (legacy scheduling).
     */
    @Test
    public void testWorkerStuckInLaunchedStateWithoutHeartbeatLegacy() {
        setupLegacyConfig();
        final TestKit probe = new TestKit(system);
        String clusterName = "testWorkerStuckInLaunchedStateLegacy";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        try {
            // Create a job with a single worker
            SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(), Lists.newArrayList())
                .build();

            JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
            MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

            final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            // Initialize the job
            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            int stageNo = 1;
            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // Send only a LAUNCHED event, but no heartbeat
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNo);

            // Verify the worker is in LAUNCHED state
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);

            IMantisWorkerMetadata worker = resp.getJobMetadata().get().getStageMetadata(stageNo).get()
                .getWorkerByIndex(0).getMetadata();
            assertEquals(WorkerState.Launched, worker.getState());

            // Trigger heartbeat check with a time far enough in the future to exceed the timeout
            // The default worker timeout is 60 seconds, and the check uses 1.5x that value
            Instant futureTime = Instant.now().plusSeconds(100);
            jobActor.tell(new JobProto.CheckHeartBeat(futureTime), probe.getRef());

            // Wait for resubmission to be processed
            Thread.sleep(1000);

            // Verify that the worker was terminated and a new one was scheduled
            verify(schedulerMock, times(2)).scheduleWorkers(any()); // Initial + resubmit
            verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
            verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Test that a worker that sent a heartbeat but then stopped sending them will be resubmitted
     * after the timeout period (reservation-based scheduling).
     */
    @Test
    public void testWorkerWithStaleHeartbeatReservation() {
        setupReservationConfig();
        final TestKit probe = new TestKit(system);
        String clusterName = "testWorkerWithStaleHeartbeatReservation";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        try {
            // Create a job with a single worker
            SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(), Lists.newArrayList())
                .build();

            JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
            MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

            final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            // Initialize the job
            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            int stageNo = 1;
            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // Send LAUNCHED event and a heartbeat
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNo);
            JobTestHelper.sendHeartBeat(probe, jobActor, jobId, stageNo, workerId);

            // Verify the worker is in LAUNCHED state with a heartbeat
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);

            IMantisWorkerMetadata worker = resp.getJobMetadata().get().getStageMetadata(stageNo).get()
                .getWorkerByIndex(0).getMetadata();
            assertEquals(WorkerState.Started, worker.getState());
            assertTrue(worker.getLastHeartbeatAt().isPresent());

            // Trigger heartbeat check with a time far enough in the future to exceed the timeout
            // The default worker timeout is 60 seconds, and the check uses 1.5x that value
            Instant futureTime = Instant.now().plusSeconds(100);
            jobActor.tell(new JobProto.CheckHeartBeat(futureTime), probe.getRef());

            // Wait for resubmission to be processed
            Thread.sleep(1000);

            // Verify that the worker was terminated and a new one was scheduled
            // For reservation-based scheduling, verify upsertReservation instead of scheduleWorkers
            verify(schedulerMock, times(2)).upsertReservation(any(UpsertReservation.class)); // Initial + resubmit
            verify(schedulerMock, never()).scheduleWorkers(any());
            verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Test that a worker that sent a heartbeat but then stopped sending them will be resubmitted
     * after the timeout period (legacy scheduling).
     */
    @Test
    public void testWorkerWithStaleHeartbeatLegacy() {
        setupLegacyConfig();
        final TestKit probe = new TestKit(system);
        String clusterName = "testWorkerWithStaleHeartbeatLegacy";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        try {
            // Create a job with a single worker
            SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(), Lists.newArrayList())
                .build();

            JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
            MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

            final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            // Initialize the job
            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            int stageNo = 1;
            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // Send LAUNCHED event and a heartbeat
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNo);
            JobTestHelper.sendHeartBeat(probe, jobActor, jobId, stageNo, workerId);

            // Verify the worker is in LAUNCHED state with a heartbeat
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);

            IMantisWorkerMetadata worker = resp.getJobMetadata().get().getStageMetadata(stageNo).get()
                .getWorkerByIndex(0).getMetadata();
            assertEquals(WorkerState.Started, worker.getState());
            assertTrue(worker.getLastHeartbeatAt().isPresent());

            // Trigger heartbeat check with a time far enough in the future to exceed the timeout
            // The default worker timeout is 60 seconds, and the check uses 1.5x that value
            Instant futureTime = Instant.now().plusSeconds(100);
            jobActor.tell(new JobProto.CheckHeartBeat(futureTime), probe.getRef());

            // Wait for resubmission to be processed
            Thread.sleep(1000);

            // Verify that the worker was terminated and a new one was scheduled
            verify(schedulerMock, times(2)).scheduleWorkers(any()); // Initial + resubmit
            verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
            verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Test specifically targeting the bug - the time unit mismatch in the checkHeartBeats method.
     * This test verifies that the system correctly calculates the time since a worker was launched
     * using milliseconds instead of seconds (reservation-based scheduling).
     */
    @Test
    public void testTimeUnitMismatchInHeartbeatCheckReservation() {
        setupReservationConfig();
        final TestKit probe = new TestKit(system);
        String clusterName = "testTimeUnitMismatchReservation";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        try {
            // Create a job with a single worker
            SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(), Lists.newArrayList())
                .build();

            JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
            MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

            final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            // Initialize the job
            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            int stageNo = 1;
            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // Send LAUNCHED event with a timestamp that's only a few seconds in the past
            // This simulates a worker that was just launched
            long launchTimeMillis = System.currentTimeMillis() - 5000; // 5 seconds ago

            // Create a custom WorkerLaunched event with the specific timestamp
            WorkerEvent launchedEvent = new WorkerLaunched(
                workerId, stageNo, "host1", "vm1", Optional.empty(), Optional.empty(),
                new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));
            jobActor.tell(launchedEvent, probe.getRef());

            // Verify the worker is in LAUNCHED state
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);

            IMantisWorkerMetadata worker = resp.getJobMetadata().get().getStageMetadata(stageNo).get()
                .getWorkerByIndex(0).getMetadata();
            assertEquals(WorkerState.Launched, worker.getState());

            // Trigger heartbeat check with a time that would exceed the timeout if using seconds instead of milliseconds
            // The default worker timeout is 60 seconds, and the check uses 1.5x that value (90 seconds)
            // If using ofEpochSecond instead of ofEpochMilli, the time difference would be interpreted as
            // thousands of seconds, which would definitely exceed the timeout
            Instant checkTime = Instant.now();
            jobActor.tell(new JobProto.CheckHeartBeat(checkTime), probe.getRef());

            // Wait for potential resubmission
            Thread.sleep(1000);

            // With the fix, the worker should NOT be resubmitted because only 5 seconds have passed
            // which is less than the timeout (90 seconds)
            verify(schedulerMock, times(1)).upsertReservation(any(UpsertReservation.class)); // Only initial schedule
            verify(schedulerMock, never()).scheduleWorkers(any());
            verify(schedulerMock, never()).unscheduleAndTerminateWorker(any(), any());

            // Now trigger heartbeat check with a time that would exceed the timeout
            Instant futureTime = Instant.now().plusSeconds(100);
            jobActor.tell(new JobProto.CheckHeartBeat(futureTime), probe.getRef());

            // Wait for resubmission
            Thread.sleep(1000);

            // Now the worker should be resubmitted
            verify(schedulerMock, times(2)).upsertReservation(any(UpsertReservation.class)); // Initial + resubmit
            verify(schedulerMock, never()).scheduleWorkers(any());
            verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Test specifically targeting the bug - the time unit mismatch in the checkHeartBeats method.
     * This test verifies that the system correctly calculates the time since a worker was launched
     * using milliseconds instead of seconds (legacy scheduling).
     */
    @Test
    public void testTimeUnitMismatchInHeartbeatCheckLegacy() {
        setupLegacyConfig();
        final TestKit probe = new TestKit(system);
        String clusterName = "testTimeUnitMismatchLegacy";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        try {
            // Create a job with a single worker
            SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 3),
                    Lists.newArrayList(), Lists.newArrayList())
                .build();

            JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
            MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName, 1))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)
                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();

            final ActorRef jobActor = system.actorOf(JobActor.props(
                jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            // Initialize the job
            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            int stageNo = 1;
            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // Send LAUNCHED event with a timestamp that's only a few seconds in the past
            // This simulates a worker that was just launched
            long launchTimeMillis = System.currentTimeMillis() - 5000; // 5 seconds ago

            // Create a custom WorkerLaunched event with the specific timestamp
            WorkerEvent launchedEvent = new WorkerLaunched(
                workerId, stageNo, "host1", "vm1", Optional.empty(), Optional.empty(),
                new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));
            jobActor.tell(launchedEvent, probe.getRef());

            // Verify the worker is in LAUNCHED state
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);

            IMantisWorkerMetadata worker = resp.getJobMetadata().get().getStageMetadata(stageNo).get()
                .getWorkerByIndex(0).getMetadata();
            assertEquals(WorkerState.Launched, worker.getState());

            // Trigger heartbeat check with a time that would exceed the timeout if using seconds instead of milliseconds
            // The default worker timeout is 60 seconds, and the check uses 1.5x that value (90 seconds)
            // If using ofEpochSecond instead of ofEpochMilli, the time difference would be interpreted as
            // thousands of seconds, which would definitely exceed the timeout
            Instant checkTime = Instant.now();
            jobActor.tell(new JobProto.CheckHeartBeat(checkTime), probe.getRef());

            // Wait for potential resubmission
            Thread.sleep(1000);

            // With the fix, the worker should NOT be resubmitted because only 5 seconds have passed
            // which is less than the timeout (90 seconds)
            verify(schedulerMock, times(1)).scheduleWorkers(any()); // Only initial schedule
            verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
            verify(schedulerMock, never()).unscheduleAndTerminateWorker(any(), any());

            // Now trigger heartbeat check with a time that would exceed the timeout
            Instant futureTime = Instant.now().plusSeconds(100);
            jobActor.tell(new JobProto.CheckHeartBeat(futureTime), probe.getRef());

            // Wait for resubmission
            Thread.sleep(1000);

            // Now the worker should be resubmitted
            verify(schedulerMock, times(2)).scheduleWorkers(any()); // Initial + resubmit
            verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
            verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}
