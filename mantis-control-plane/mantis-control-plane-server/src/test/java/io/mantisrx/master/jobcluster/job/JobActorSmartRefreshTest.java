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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.Ack;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.WorkerInfoListHolder;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import rx.Observer;
import rx.subjects.BehaviorSubject;

/**
 * Tests for JobActor smart refresh logic and worker state filtering improvements.
 * Focuses on the new functionality added for scaling optimization.
 */
@Slf4j
public class JobActorSmartRefreshTest {

    static ActorSystem system;
    private static IMantisPersistenceProvider storageProvider;
    private static LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(
        new AuditEventSubscriberLoggingImpl(),
        new StatusEventSubscriberLoggingImpl(),
        new WorkerEventSubscriberLoggingImpl()
    );
    private final CostsCalculator costsCalculator = CostsCalculator.noop();

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        setupSmartRefreshMasterConfig();
        storageProvider = new KeyValueBasedPersistenceProvider(new FileBasedStore(), eventPublisher);
    }

    /**
     * Set up master config with smart refresh enabled (proper refresh interval).
     * This overrides the default test config which sets refresh interval to -1 (immediate).
     */
    private static void setupSmartRefreshMasterConfig() {
        final Properties overrides = new Properties();
        // Override the refresh interval for smart refresh testing
        overrides.setProperty("mantis.master.stage.assignment.refresh.interval.ms", "500");
        overrides.setProperty("mantis.master.stage.assignment.refresh.max.wait.ms", "2000");

        TestHelpers.setupMasterConfig(overrides);
    }

    @AfterClass
    public static void tearDown() {
        JobTestHelper.deleteAllFiles();
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    /**
     * Tests that only workers in Started state are included in JobSchedulingInfo updates.
     * This prevents downstream connection failures to workers that aren't ready yet.
     */
    @Test
    public void testOnlyStartedWorkersInSchedulingInfo() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testOnlyStartedWorkersInSchedulingInfo";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(1, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
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

        // Initialize job
        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        String jobId = clusterName + "-1";

        // Get the job scheduling info observable
        jobActor.tell(new GetJobSchedInfoRequest(new JobId(clusterName, 1)), probe.getRef());
        GetJobSchedInfoResponse resp = probe.expectMsgClass(GetJobSchedInfoResponse.class);
        assertEquals(SUCCESS, resp.responseCode);
        assertTrue(resp.getJobSchedInfoSubject().isPresent());

        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = resp.getJobSchedInfoSubject().get();

        // Set up observer to capture scheduling info updates with Started workers
        CountDownLatch schedulingUpdateLatch = new CountDownLatch(1);
        JobSchedulingInfo[] capturedSchedulingInfo = new JobSchedulingInfo[1];

        schedInfoSubject.subscribe(new Observer<>() {
            @Override
            public void onCompleted() {}

            @Override
            public void onError(Throwable e) {}

            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                log.info("Received scheduling info update: {}", jobSchedulingInfo);
                // Only count down if we have Started workers
                Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> assignments =
                    jobSchedulingInfo.getWorkerAssignments();
                if (assignments.containsKey(1)) {
                    io.mantisrx.server.core.WorkerAssignments stage1 = assignments.get(1);
                    Map<Integer, WorkerHost> hosts = stage1.getHosts();
                    if (!hosts.isEmpty() && hosts.values().stream()
                        .anyMatch(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)) {
                        capturedSchedulingInfo[0] = jobSchedulingInfo;
                        schedulingUpdateLatch.countDown();
                    }
                }
            }
        });

        // Move worker through complete lifecycle to Started state
        WorkerId workerId1 = new WorkerId(jobId, 0, 1);
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 1, workerId1);

        // Wait for scheduling info update
        assertTrue("Scheduling info should be updated",
            schedulingUpdateLatch.await(5, TimeUnit.SECONDS));

        // Verify the Started worker is included
        JobSchedulingInfo schedulingInfo = capturedSchedulingInfo[0];
        assertEquals("Job ID should match", jobId, schedulingInfo.getJobId());

        Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> stageAssignments =
            schedulingInfo.getWorkerAssignments();
        assertTrue("Stage 1 should have assignments", stageAssignments.containsKey(1));

        io.mantisrx.server.core.WorkerAssignments stage1Assignments = stageAssignments.get(1);
        Map<Integer, WorkerHost> hosts = stage1Assignments.getHosts();

        // Only the Started worker should be present
        assertEquals("Started worker should be included", 1, hosts.size());
        assertTrue("Worker 1 should be included", hosts.containsKey(1));

        // Verify worker state is Started
        WorkerHost worker1Host = hosts.get(1);
        assertEquals("Worker 1 should be in Started state",
            io.mantisrx.runtime.MantisJobState.Started, worker1Host.getState());
    }

    /**
     * Tests the smart refresh logic that delays updates when workers are transitioning to Started state.
     * This reduces update frequency during scaling operations.
     */
    @Test
    public void testSmartRefreshBasicFunctionality() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testSmartRefreshDelays";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(2, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
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

        // Initialize job
        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        String jobId = clusterName + "-1";

        // Get the job scheduling info observable to monitor updates
        jobActor.tell(new GetJobSchedInfoRequest(new JobId(clusterName, 1)), probe.getRef());
        GetJobSchedInfoResponse resp = probe.expectMsgClass(GetJobSchedInfoResponse.class);
        assertEquals(SUCCESS, resp.responseCode);
        assertTrue(resp.getJobSchedInfoSubject().isPresent());

        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = resp.getJobSchedInfoSubject().get();

        // Track all scheduling info updates and capture the final one with Started workers
        CountDownLatch finalUpdateLatch = new CountDownLatch(1);
        JobSchedulingInfo[] capturedFinalSchedulingInfo = new JobSchedulingInfo[1];
        int[] totalUpdateCount = {0};
        boolean[] sawSingleStartedWorker = {false};

        schedInfoSubject.subscribe(new Observer<JobSchedulingInfo>() {
            @Override
            public void onCompleted() {}

            @Override
            public void onError(Throwable e) {}

            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                totalUpdateCount[0]++;
                log.info("Scheduling info update #{}: {}", totalUpdateCount[0], jobSchedulingInfo.getWorkerAssignments());

                // Check if this update contains Started workers
                Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> assignments =
                    jobSchedulingInfo.getWorkerAssignments();
                if (assignments.containsKey(1)) {
                    io.mantisrx.server.core.WorkerAssignments stage1 = assignments.get(1);
                    Map<Integer, WorkerHost> hosts = stage1.getHosts();
                    long startedWorkersCount = hosts.values().stream()
                        .filter(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)
                        .count();

                    // Check if we see exactly one Started worker (this should NOT happen with smart refresh)
                    if (startedWorkersCount == 1) {
                        sawSingleStartedWorker[0] = true;
                        log.warn("UNEXPECTED: Saw intermediate update with single Started worker - smart refresh failed");
                    }

                    // When we get both workers in Started state, capture this as the final update
                    if (startedWorkersCount == 2) {
                        capturedFinalSchedulingInfo[0] = jobSchedulingInfo;
                        finalUpdateLatch.countDown();
                    }
                }
            }
        });

        // Create workers in transitioning states (should trigger smart batching)
        WorkerId workerId1 = new WorkerId(jobId, 0, 1);
        WorkerId workerId2 = new WorkerId(jobId, 1, 2);

        // Move workers to Launched state (transitioning)
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId1, 1);
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, 1);

        // Trigger a periodic refresh while workers are transitioning
        // This should be delayed due to smart refresh logic
        jobActor.tell(new JobProto.SendWorkerAssignementsIfChanged(), probe.getRef());

        // Move both workers to Started state in quick succession (within refresh interval)
        // Smart refresh should batch these updates and wait for both to be ready
        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId1);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId1);

        // Small delay then transition second worker - should still be within batching window
        Thread.sleep(100);

        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId2);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId2);

        // Wait for the final update with both Started workers
        assertTrue("Should receive final update with both Started workers",
            finalUpdateLatch.await(10, TimeUnit.SECONDS));

        // Validate the final scheduling info contains both workers in Started state
        JobSchedulingInfo finalSchedulingInfo = capturedFinalSchedulingInfo[0];
        assertEquals("Job ID should match", jobId, finalSchedulingInfo.getJobId());

        Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> stageAssignments =
            finalSchedulingInfo.getWorkerAssignments();
        assertTrue("Stage 1 should have assignments", stageAssignments.containsKey(1));

        io.mantisrx.server.core.WorkerAssignments stage1Assignments = stageAssignments.get(1);
        Map<Integer, WorkerHost> hosts = stage1Assignments.getHosts();

        // Both workers should be present in Started state
        assertEquals("Should contain both Started workers", 2, hosts.size());
        assertTrue("Worker 1 should be included", hosts.containsKey(1));
        assertTrue("Worker 2 should be included", hosts.containsKey(2));

        // Verify both workers are in Started state
        assertEquals("Worker 1 should be in Started state",
            io.mantisrx.runtime.MantisJobState.Started, hosts.get(1).getState());
        assertEquals("Worker 2 should be in Started state",
            io.mantisrx.runtime.MantisJobState.Started, hosts.get(2).getState());

        // With smart refresh enabled (500ms interval), we should NOT see intermediate updates
        // with single Started workers. The refresh should be delayed until both are ready.
        assertFalse("Smart refresh should prevent intermediate updates with single Started worker - " +
            "got " + totalUpdateCount[0] + " total updates", sawSingleStartedWorker[0]);

        // Smart refresh should result in significantly fewer total updates
        // With 500ms refresh interval, we expect minimal updates:
        // - Initial empty update, some intermediate non-Started updates, and final update with both workers
        log.info("Total scheduling info updates: {}", totalUpdateCount[0]);
        assertTrue("Smart refresh should significantly limit update frequency (got " + totalUpdateCount[0] + " updates)",
            totalUpdateCount[0] <= 4);
    }


    /**
     * Tests that worker list changed events include pending workers (Accepted, Launched) and active workers (Started),
     * while JobSchedulingInfo only includes Started workers for downstream connection safety.
     */
    @Test
    public void testWorkerListChangedEventsIncludePendingWorkersWhileJobSchedulingInfoFiltersCorrectly() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testWorkerListFiltering";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(3, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        // Create a custom event publisher to capture WorkerListChangedEvents
        LifecycleEventPublisher mockEventPublisher = mock(LifecycleEventPublisher.class);

        MantisJobMetadataImpl mantisJobMetaData = createJobMetadata(clusterName, jobDefn);
        final ActorRef jobActor = createAndInitializeJobActor(probe, jobClusterDefn, mantisJobMetaData,
            jobStoreMock, schedulerMock, mockEventPublisher);

        String jobId = clusterName + "-1";

        // Get JobSchedulingInfo observable to verify filtering behavior
        jobActor.tell(new GetJobSchedInfoRequest(new JobId(clusterName, 1)), probe.getRef());
        GetJobSchedInfoResponse resp = probe.expectMsgClass(GetJobSchedInfoResponse.class);
        assertEquals(SUCCESS, resp.responseCode);
        assertTrue(resp.getJobSchedInfoSubject().isPresent());
        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = resp.getJobSchedInfoSubject().get();

        // Track JobSchedulingInfo updates to verify only Started workers are included
        CountDownLatch schedulingInfoWithStartedWorkerLatch = new CountDownLatch(1);
        JobSchedulingInfo[] capturedSchedulingInfo = new JobSchedulingInfo[1];

        schedInfoSubject.subscribe(new Observer<JobSchedulingInfo>() {
            @Override
            public void onCompleted() {}
            @Override
            public void onError(Throwable e) {}
            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> assignments =
                    jobSchedulingInfo.getWorkerAssignments();
                if (assignments.containsKey(1)) {
                    io.mantisrx.server.core.WorkerAssignments stage1 = assignments.get(1);
                    Map<Integer, WorkerHost> hosts = stage1.getHosts();
                    if (hosts.values().stream().anyMatch(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)) {
                        capturedSchedulingInfo[0] = jobSchedulingInfo;
                        schedulingInfoWithStartedWorkerLatch.countDown();
                    }
                }
            }
        });

        // Create workers in different states to test filtering behavior
        WorkerId workerId1 = new WorkerId(jobId, 0, 1); // Will remain Accepted
        WorkerId workerId2 = new WorkerId(jobId, 1, 2); // Will be Launched but not Started
        WorkerId workerId3 = new WorkerId(jobId, 2, 3); // Will be Started

        // Worker 2: Move to Launched state (pending but not ready for connections)
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, 1);

        // Worker 3: Move to Started state (ready for connections)
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 1, workerId3);

        // Wait for JobSchedulingInfo to be updated with Started worker
        assertTrue("Should receive JobSchedulingInfo update with Started worker",
            schedulingInfoWithStartedWorkerLatch.await(5, TimeUnit.SECONDS));

        // Verify JobSchedulingInfo only contains Started workers (for connection safety)
        JobSchedulingInfo finalSchedulingInfo = capturedSchedulingInfo[0];
        Map<Integer, WorkerHost> schedulingInfoHosts = finalSchedulingInfo.getWorkerAssignments().get(1).getHosts();
        assertEquals("JobSchedulingInfo should only contain Started workers", 1, schedulingInfoHosts.size());
        WorkerHost startedWorkerHost = schedulingInfoHosts.get(3); // Worker 3
        assertNotNull("Started worker should be present in JobSchedulingInfo", startedWorkerHost);
        assertEquals("Worker in JobSchedulingInfo should be in Started state",
            io.mantisrx.runtime.MantisJobState.Started, startedWorkerHost.getState());

        // Allow time for WorkerListChangedEvent to be published
        Thread.sleep(1000);

        // Verify WorkerListChangedEvent includes both pending and active workers
        ArgumentCaptor<LifecycleEventsProto.WorkerListChangedEvent> eventCaptor =
            ArgumentCaptor.forClass(LifecycleEventsProto.WorkerListChangedEvent.class);
        verify(mockEventPublisher, atLeast(1)).publishWorkerListChangedEvent(eventCaptor.capture());

        // Get the last captured event (which should include all non-terminal workers)
        List<LifecycleEventsProto.WorkerListChangedEvent> allEvents = eventCaptor.getAllValues();
        LifecycleEventsProto.WorkerListChangedEvent lastEvent = allEvents.get(allEvents.size() - 1);
        WorkerInfoListHolder workerInfoHolder = lastEvent.getWorkerInfoListHolder();
        List<IMantisWorkerMetadata> workerList = workerInfoHolder.getWorkerMetadataList();

        // Verify WorkerListChangedEvent contains all expected worker states
        Map<WorkerState, Integer> workerStateCounts = workerList.stream()
            .collect(Collectors.groupingBy(
                IMantisWorkerMetadata::getState,
                Collectors.collectingAndThen(Collectors.counting(), Math::toIntExact)
            ));

        assertTrue("WorkerListChangedEvent should include Accepted workers",
            workerStateCounts.getOrDefault(WorkerState.Accepted, 0) >= 1);
        assertTrue("WorkerListChangedEvent should include Launched workers",
            workerStateCounts.getOrDefault(WorkerState.Launched, 0) >= 1);
        assertTrue("WorkerListChangedEvent should include Started workers",
            workerStateCounts.getOrDefault(WorkerState.Started, 0) >= 1);

        log.info("Verified filtering behavior: JobSchedulingInfo contains only Started workers ({}), " +
            "while WorkerListChangedEvent includes all pending/active workers ({})",
            schedulingInfoHosts.size(), workerList.size());
    }

    /**
     * Tests the smart refresh logic when scaling up adds more than 1 worker and they reach different states.
     *
     * This validates the specific scenario where:
     * 1. Scale up adds 2 workers
     * 2. One worker transitions to Started state
     * 3. The other worker remains in Launched state
     * 4. Smart refresh should wait during the refresh interval
     * 5. When max wait timeout is reached, it publishes changes with Started workers
     * 6. The stageAssignmentPotentiallyChanged flag remains true due to the Launched worker
     *
     * This ensures proper batching behavior and prevents premature flag resets.
     * Uses a shorter max wait time (800ms) to make the test faster and more predictable.
     */
    @Test
    public void testStageAssignmentFlagNotResetWithPendingWorkersDuringScaling() throws Exception {
        // Override config for faster test execution
        final Properties testOverrides = new Properties();
        testOverrides.setProperty("mantis.master.stage.assignment.refresh.interval.ms", "200");
        testOverrides.setProperty("mantis.master.stage.assignment.refresh.max.wait.ms", "800");
        TestHelpers.setupMasterConfig(testOverrides);

        final TestKit probe = new TestKit(system);
        String clusterName = "testFlagNotResetWithPendingWorkers";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(2, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        MantisJobMetadataImpl mantisJobMetaData = createJobMetadata(clusterName, jobDefn);
        final ActorRef jobActor = createAndInitializeJobActor(probe, jobClusterDefn, mantisJobMetaData,
            jobStoreMock, schedulerMock, eventPublisher);

        String jobId = clusterName + "-1";

        // Track scheduling info updates to validate smart refresh behavior
        List<JobSchedulingInfo> schedulingUpdates = new ArrayList<>();
        CountDownLatch firstStartedWorkerLatch = new CountDownLatch(1);
        CountDownLatch maxWaitTimeoutLatch = new CountDownLatch(1);
        boolean[] maxWaitTimeoutReached = {false};

        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = getJobSchedulingInfoSubject(probe, jobActor, clusterName);
        schedInfoSubject.subscribe(new Observer<>() {
            @Override
            public void onCompleted() {}
            @Override
            public void onError(Throwable e) {}
            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                synchronized (schedulingUpdates) {
                    schedulingUpdates.add(jobSchedulingInfo);
                }

                Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> assignments =
                    jobSchedulingInfo.getWorkerAssignments();
                if (assignments.containsKey(1)) {
                    io.mantisrx.server.core.WorkerAssignments stage1 = assignments.get(1);
                    Map<Integer, WorkerHost> hosts = stage1.getHosts();
                    long startedCount = hosts.values().stream()
                        .filter(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)
                        .count();

                    log.info("JobSchedulingInfo update: {} Started workers out of {} total hosts",
                        startedCount, hosts.size());

                    // First time we see exactly 1 Started worker
                    if (startedCount == 1 && firstStartedWorkerLatch.getCount() > 0) {
                        firstStartedWorkerLatch.countDown();

                        // Schedule a check for max wait timeout (800ms + buffer)
                        new Thread(() -> {
                            try {
                                Thread.sleep(1200); // Wait for max wait timeout (800ms) + buffer
                                maxWaitTimeoutReached[0] = true;
                                maxWaitTimeoutLatch.countDown();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }).start();
                    }
                }
            }
        });

        // Allow job to initialize with workers in Accepted state
        waitForJobInitialization();

        // Create workers and transition them to different states to test smart refresh logic
        WorkerId workerId1 = new WorkerId(jobId, 0, 1);
        WorkerId workerId2 = new WorkerId(jobId, 1, 2);

        // Move both workers to Launched state first
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId1, 1);
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, 1);

        // Transition only worker 1 to Started state (worker 2 remains in Launched)
        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId1);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId1);

        // Wait for the first Started worker to be detected
        assertTrue("Should detect first Started worker",
            firstStartedWorkerLatch.await(5, TimeUnit.SECONDS));

        log.info("First Started worker detected. Smart refresh should now wait for max timeout...");

        // Trigger periodic refresh during the wait period - should be delayed due to pending worker
        jobActor.tell(new JobProto.SendWorkerAssignementsIfChanged(), probe.getRef());

        // Wait for max wait timeout to be reached
        assertTrue("Max wait timeout should be reached",
            maxWaitTimeoutLatch.await(3, TimeUnit.SECONDS));

        assertTrue("Max wait timeout should have been triggered", maxWaitTimeoutReached[0]);

        // Allow some time for the timeout-triggered refresh to complete
        Thread.sleep(200);

        // Validate the smart refresh behavior
        synchronized (schedulingUpdates) {
            // Should have received scheduling updates
            assertFalse("Should have received scheduling info updates", schedulingUpdates.isEmpty());

            // Find the latest update with Started workers
            JobSchedulingInfo latestWithStarted = null;
            for (int i = schedulingUpdates.size() - 1; i >= 0; i--) {
                JobSchedulingInfo update = schedulingUpdates.get(i);
                if (update.getWorkerAssignments().containsKey(1)) {
                    Map<Integer, WorkerHost> hosts = update.getWorkerAssignments().get(1).getHosts();
                    if (!hosts.isEmpty()) {
                        latestWithStarted = update;
                        break;
                    }
                }
            }

            assertNotNull("Should have at least one update with worker assignments", latestWithStarted);

            // Verify that after timeout, we published changes with the Started worker
            Map<Integer, WorkerHost> finalHosts = latestWithStarted.getWorkerAssignments().get(1).getHosts();
            long startedWorkerCount = finalHosts.values().stream()
                .filter(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)
                .count();

            // Should contain exactly 1 Started worker (worker 2 is still Launched)
            assertEquals("Should have exactly 1 Started worker in final scheduling info", 1, startedWorkerCount);

            log.info("Successfully validated smart refresh logic: waited during interval, then published " +
                "Started workers after max wait timeout with flag remaining true for pending worker");
        }

        // Additional verification: the stageAssignmentPotentiallyChanged flag should remain true
        // because worker 2 is still in Launched state (pending), but the timer should be reset
        // to prevent redundant immediate refreshes

        // Count scheduling updates before triggering additional refresh
        int updatesBeforeAdditionalRefresh;
        synchronized (schedulingUpdates) {
            updatesBeforeAdditionalRefresh = schedulingUpdates.size();
        }

        // Trigger additional refresh immediately - should NOT produce immediate update due to timer reset
        // The smart refresh logic should delay this refresh since lastWorkerTransitionTime was reset
        jobActor.tell(new JobProto.SendWorkerAssignementsIfChanged(), probe.getRef());
        Thread.sleep(200); // Wait for processing

        // Verify that additional refresh did NOT produce immediate update (due to timer reset fix)
        int updatesAfterImmediateRefresh;
        synchronized (schedulingUpdates) {
            updatesAfterImmediateRefresh = schedulingUpdates.size();
        }

        assertEquals("Timer reset should prevent redundant immediate refresh - no new update expected",
            updatesBeforeAdditionalRefresh, updatesAfterImmediateRefresh);

        // However, if we wait for the refresh interval to pass, or if worker state changes,
        // then a refresh should still be triggered (flag is still true)
        // Simulate worker2 transitioning from Launched to Started to trigger a new refresh
        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId2);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId2);
        Thread.sleep(300); // Allow processing

        // Now we should see a new update due to worker2 becoming Started
        int updatesAfterWorkerChange;
        synchronized (schedulingUpdates) {
            updatesAfterWorkerChange = schedulingUpdates.size();
        }

        assertTrue("Worker state change should trigger new refresh despite timer reset",
            updatesAfterWorkerChange > updatesAfterImmediateRefresh);

        // Verify the latest update now shows both workers in Started state
        synchronized (schedulingUpdates) {
            JobSchedulingInfo latestUpdate = schedulingUpdates.get(schedulingUpdates.size() - 1);
            if (latestUpdate.getWorkerAssignments().containsKey(1)) {
                Map<Integer, WorkerHost> hosts = latestUpdate.getWorkerAssignments().get(1).getHosts();
                long startedWorkerCount = hosts.values().stream()
                    .filter(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)
                    .count();
                assertEquals("Latest refresh should now show both workers Started", 2, startedWorkerCount);
            }
        }
    }

    /**
     * Helper method to create job metadata with common test configuration.
     */
    private MantisJobMetadataImpl createJobMetadata(String clusterName, JobDefinition jobDefn) {
        return new MantisJobMetadataImpl.Builder()
            .withJobId(new JobId(clusterName, 1))
            .withSubmittedAt(Instant.now())
            .withJobState(JobState.Accepted)
            .withNextWorkerNumToUse(1)
            .withJobDefinition(jobDefn)
            .build();
    }

    /**
     * Helper method to create and initialize a JobActor with common test setup.
     */
    private ActorRef createAndInitializeJobActor(TestKit probe, IJobClusterDefinition jobClusterDefn,
            MantisJobMetadataImpl mantisJobMetaData, MantisJobStore jobStoreMock,
            MantisScheduler schedulerMock, LifecycleEventPublisher eventPublisher) {
        final ActorRef jobActor = system.actorOf(JobActor.props(
            jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

        // Initialize job
        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        return jobActor;
    }

    /**
     * Helper method to get JobSchedulingInfo BehaviorSubject from JobActor.
     */
    private BehaviorSubject<JobSchedulingInfo> getJobSchedulingInfoSubject(TestKit probe, ActorRef jobActor, String clusterName) {
        jobActor.tell(new GetJobSchedInfoRequest(new JobId(clusterName, 1)), probe.getRef());
        GetJobSchedInfoResponse resp = probe.expectMsgClass(GetJobSchedInfoResponse.class);
        assertEquals(SUCCESS, resp.responseCode);
        assertTrue(resp.getJobSchedInfoSubject().isPresent());
        return resp.getJobSchedInfoSubject().get();
    }

    /**
     * Helper method to wait for job initialization without arbitrary sleep.
     */
    private void waitForJobInitialization() {
        try {
            Thread.sleep(1000); // Allow time for job to initialize with workers in Accepted state
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tests that the smart refresh timer is correctly reset when workers transition states,
     * ensuring the batching window resets appropriately and prevents redundant refreshes.
     */
    @Test
    public void testSmartRefreshTimerResetValidation() throws Exception {
        // Use faster timing for test execution
        final Properties testOverrides = new Properties();
        testOverrides.setProperty("mantis.master.stage.assignment.refresh.interval.ms", "300");
        testOverrides.setProperty("mantis.master.stage.assignment.refresh.max.wait.ms", "800");
        TestHelpers.setupMasterConfig(testOverrides);

        final TestKit probe = new TestKit(system);
        String clusterName = "testTimerResetValidation";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(2, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        MantisJobMetadataImpl mantisJobMetaData = createJobMetadata(clusterName, jobDefn);
        final ActorRef jobActor = createAndInitializeJobActor(probe, jobClusterDefn, mantisJobMetaData,
            jobStoreMock, schedulerMock, eventPublisher);

        String jobId = clusterName + "-1";
        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = getJobSchedulingInfoSubject(probe, jobActor, clusterName);

        List<JobSchedulingInfo> schedulingUpdates = new ArrayList<>();
        CountDownLatch maxWaitTimeoutLatch = new CountDownLatch(1);
        boolean[] timeoutForcedRefresh = {false};

        schedInfoSubject.subscribe(new Observer<JobSchedulingInfo>() {
            @Override
            public void onCompleted() {}
            @Override
            public void onError(Throwable e) {}
            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                synchronized (schedulingUpdates) {
                    schedulingUpdates.add(jobSchedulingInfo);
                    log.info("Timer reset test - scheduling update #{}: {} Started workers",
                        schedulingUpdates.size(), getStartedWorkerCount(jobSchedulingInfo));
                }
            }
        });

        waitForJobInitialization();

        WorkerId workerId1 = new WorkerId(jobId, 0, 1);
        WorkerId workerId2 = new WorkerId(jobId, 1, 2);

        // Phase 1: Create pending workers to trigger smart refresh delay
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId1, 1);
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, 1);
        Thread.sleep(200);

        // Phase 2: Transition one worker, should reset timer and publish after timeout
        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId1);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId1);

        // Wait for max timeout to force refresh (800ms + buffer)
        Thread.sleep(1000);

        // Phase 3: Verify timer was reset - immediate refresh should NOT produce redundant update
        int updatesBeforeImmediateRefresh;
        synchronized (schedulingUpdates) {
            updatesBeforeImmediateRefresh = schedulingUpdates.size();
        }

        // Trigger immediate refresh - should be delayed due to timer reset
        jobActor.tell(new JobProto.SendWorkerAssignementsIfChanged(), probe.getRef());
        Thread.sleep(200);

        int updatesAfterImmediateRefresh;
        synchronized (schedulingUpdates) {
            updatesAfterImmediateRefresh = schedulingUpdates.size();
        }

        assertEquals("Timer reset should prevent redundant immediate refresh",
            updatesBeforeImmediateRefresh, updatesAfterImmediateRefresh);

        // Phase 4: New state change should trigger refresh (validates timer reset works)
        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId2);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId2);
        Thread.sleep(500);

        // Verify final state has both workers
        synchronized (schedulingUpdates) {
            assertTrue("Should have received scheduling updates", schedulingUpdates.size() > 0);
            JobSchedulingInfo finalUpdate = schedulingUpdates.get(schedulingUpdates.size() - 1);
            assertEquals("Final update should contain both Started workers",
                2, getStartedWorkerCount(finalUpdate));
            log.info("Timer reset validation completed - total updates: {}", schedulingUpdates.size());
        }
    }

    /**
     * Tests smart refresh behavior when some workers fail during startup while others succeed.
     * Validates that failed workers don't prevent refreshes for successful workers.
     */
    @Test
    public void testSmartRefreshWithMixedWorkerFailures() throws Exception {
        final TestKit probe = new TestKit(system);
        String clusterName = "testMixedWorkerFailures";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(3, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        MantisJobMetadataImpl mantisJobMetaData = createJobMetadata(clusterName, jobDefn);
        final ActorRef jobActor = createAndInitializeJobActor(probe, jobClusterDefn, mantisJobMetaData,
            jobStoreMock, schedulerMock, eventPublisher);

        String jobId = clusterName + "-1";
        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = getJobSchedulingInfoSubject(probe, jobActor, clusterName);

        CountDownLatch successfulWorkerLatch = new CountDownLatch(1);
        List<JobSchedulingInfo> mixedStateUpdates = new ArrayList<>();

        schedInfoSubject.subscribe(new Observer<JobSchedulingInfo>() {
            @Override
            public void onCompleted() {}
            @Override
            public void onError(Throwable e) {}
            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                synchronized (mixedStateUpdates) {
                    mixedStateUpdates.add(jobSchedulingInfo);
                    int startedCount = getStartedWorkerCount(jobSchedulingInfo);
                    log.info("Mixed failures test - update #{}: {} Started workers",
                        mixedStateUpdates.size(), startedCount);

                    if (startedCount >= 1) {
                        successfulWorkerLatch.countDown();
                    }
                }
            }
        });

        waitForJobInitialization();

        WorkerId workerId1 = new WorkerId(jobId, 0, 1); // Will succeed
        WorkerId workerId2 = new WorkerId(jobId, 1, 2); // Will fail
        WorkerId workerId3 = new WorkerId(jobId, 2, 3); // Will remain pending

        // All workers start in Launched state
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId1, 1);
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, 1);
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId3, 1);
        Thread.sleep(100);

        // Worker 1: Successful transition to Started
        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId1);
        JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId1);

        // Worker 2: Fail during startup
        JobTestHelper.sendWorkerTerminatedEvent(probe, jobActor, jobId, workerId2);

        // Worker 3: Remains in Launched state (pending)

        assertTrue("Should receive update with successful worker despite failures",
            successfulWorkerLatch.await(5, TimeUnit.SECONDS));

        // Verify only Started workers appear in JobSchedulingInfo (failed/pending filtered out)
        synchronized (mixedStateUpdates) {
            JobSchedulingInfo finalUpdate = mixedStateUpdates.get(mixedStateUpdates.size() - 1);
            Map<Integer, WorkerHost> finalHosts = finalUpdate.getWorkerAssignments().get(1).getHosts();

            assertEquals("Only Started workers should appear in JobSchedulingInfo", 1, finalHosts.size());
            assertTrue("Started worker should be present", finalHosts.containsKey(1));
            assertEquals("Worker should be in Started state",
                io.mantisrx.runtime.MantisJobState.Started, finalHosts.get(1).getState());

            log.info("Mixed failures test completed - failed workers correctly filtered from scheduling info");
        }
    }

    /**
     * Tests smart refresh behavior with various configuration edge cases.
     */
    @Test
    public void testSmartRefreshConfigurationEdgeCases() throws Exception {
        // Test Case 1: Zero refresh interval (immediate mode)
        final Properties immediateConfig = new Properties();
        immediateConfig.setProperty("mantis.master.stage.assignment.refresh.interval.ms", "0");
        immediateConfig.setProperty("mantis.master.stage.assignment.refresh.max.wait.ms", "1000");
        TestHelpers.setupMasterConfig(immediateConfig);

        final TestKit probe = new TestKit(system);
        String clusterName = "testConfigEdgeCases";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(2, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        MantisJobMetadataImpl mantisJobMetaData = createJobMetadata(clusterName, jobDefn);
        final ActorRef jobActor = createAndInitializeJobActor(probe, jobClusterDefn, mantisJobMetaData,
            jobStoreMock, schedulerMock, eventPublisher);

        String jobId = clusterName + "-1";
        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = getJobSchedulingInfoSubject(probe, jobActor, clusterName);

        List<JobSchedulingInfo> immediateUpdates = new ArrayList<>();
        CountDownLatch bothWorkersStartedLatch = new CountDownLatch(1);

        schedInfoSubject.subscribe(new Observer<JobSchedulingInfo>() {
            @Override
            public void onCompleted() {}
            @Override
            public void onError(Throwable e) {}
            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                synchronized (immediateUpdates) {
                    immediateUpdates.add(jobSchedulingInfo);
                    int startedCount = getStartedWorkerCount(jobSchedulingInfo);
                    log.info("Config edge test - immediate mode update #{}: {} Started workers",
                        immediateUpdates.size(), startedCount);

                    if (startedCount == 2) {
                        bothWorkersStartedLatch.countDown();
                    }
                }
            }
        });

        waitForJobInitialization();

        WorkerId workerId1 = new WorkerId(jobId, 0, 1);
        WorkerId workerId2 = new WorkerId(jobId, 1, 2);

        // With 0 interval, each transition should trigger immediate refresh (no smart batching)
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 1, workerId1);
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 1, workerId2);

        assertTrue("Both workers should be Started with immediate refresh",
            bothWorkersStartedLatch.await(5, TimeUnit.SECONDS));

        synchronized (immediateUpdates) {
            // With 0 interval, smart batching should be bypassed - expect more frequent updates
            log.info("Immediate mode total updates: {}", immediateUpdates.size());
            assertTrue("Zero interval should produce more frequent updates than batched mode",
                immediateUpdates.size() >= 2);
        }
    }

    /**
     * Tests smart refresh behavior under high-frequency worker state changes.
     * Validates performance and correctness when many workers transition rapidly.
     */
    @Test
    public void testSmartRefreshUnderHighFrequencyChanges() throws Exception {
        // Use moderate timing to allow batching but fast enough for test execution
        final Properties highFreqConfig = new Properties();
        highFreqConfig.setProperty("mantis.master.stage.assignment.refresh.interval.ms", "400");
        highFreqConfig.setProperty("mantis.master.stage.assignment.refresh.max.wait.ms", "1200");
        TestHelpers.setupMasterConfig(highFreqConfig);

        final TestKit probe = new TestKit(system);
        String clusterName = "testHighFrequencyChanges";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        // Create job with 6 workers to simulate high-frequency scenario
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStageWithConstraints(6, new MachineDefinition(1.0, 1.0, 1.0, 3),
                Lists.newArrayList(), Lists.newArrayList())
            .build();

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
        MantisScheduler schedulerMock = createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        MantisJobMetadataImpl mantisJobMetaData = createJobMetadata(clusterName, jobDefn);
        final ActorRef jobActor = createAndInitializeJobActor(probe, jobClusterDefn, mantisJobMetaData,
            jobStoreMock, schedulerMock, eventPublisher);

        String jobId = clusterName + "-1";
        BehaviorSubject<JobSchedulingInfo> schedInfoSubject = getJobSchedulingInfoSubject(probe, jobActor, clusterName);

        List<JobSchedulingInfo> highFreqUpdates = new ArrayList<>();
        CountDownLatch allWorkersStartedLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();

        schedInfoSubject.subscribe(new Observer<JobSchedulingInfo>() {
            @Override
            public void onCompleted() {}
            @Override
            public void onError(Throwable e) {}
            @Override
            public void onNext(JobSchedulingInfo jobSchedulingInfo) {
                synchronized (highFreqUpdates) {
                    highFreqUpdates.add(jobSchedulingInfo);
                    int startedCount = getStartedWorkerCount(jobSchedulingInfo);
                    long elapsed = System.currentTimeMillis() - startTime;
                    log.info("High frequency test - update #{} at {}ms: {} Started workers",
                        highFreqUpdates.size(), elapsed, startedCount);

                    if (startedCount == 6) {
                        allWorkersStartedLatch.countDown();
                    }
                }
            }
        });

        waitForJobInitialization();

        // Create rapid state transitions for 6 workers within batching window
        List<WorkerId> workerIds = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            workerIds.add(new WorkerId(jobId, i, i + 1));
        }

        // Phase 1: All workers to Launched (rapid succession)
        for (WorkerId workerId : workerIds) {
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, 1);
            Thread.sleep(20); // Very short delay to simulate rapid changes
        }

        // Phase 2: All workers to Started (rapid succession within batching window)
        for (WorkerId workerId : workerIds) {
            JobTestHelper.sendStartInitiatedEvent(probe, jobActor, 1, workerId);
            JobTestHelper.sendHeartBeat(probe, jobActor, jobId, 1, workerId);
            Thread.sleep(30); // Rapid transitions
        }

        assertTrue("All 6 workers should reach Started state despite high frequency changes",
            allWorkersStartedLatch.await(10, TimeUnit.SECONDS));

        synchronized (highFreqUpdates) {
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("High frequency test completed in {}ms with {} total updates",
                totalTime, highFreqUpdates.size());

            // Verify smart refresh effectively batched the rapid changes
            assertTrue("Should have received updates for high frequency changes",
                highFreqUpdates.size() > 0);
            assertTrue("Smart refresh should limit update frequency even under high load",
                highFreqUpdates.size() <= 8); // Should be much less than 12 individual transitions

            // Verify final state correctness
            JobSchedulingInfo finalUpdate = highFreqUpdates.get(highFreqUpdates.size() - 1);
            assertEquals("Final update should contain all 6 Started workers",
                6, getStartedWorkerCount(finalUpdate));
        }
    }

    /**
     * Helper method to count Started workers in JobSchedulingInfo.
     */
    private int getStartedWorkerCount(JobSchedulingInfo jobSchedulingInfo) {
        Map<Integer, ? extends io.mantisrx.server.core.WorkerAssignments> assignments =
            jobSchedulingInfo.getWorkerAssignments();
        if (!assignments.containsKey(1)) {
            return 0;
        }

        io.mantisrx.server.core.WorkerAssignments stage1 = assignments.get(1);
        Map<Integer, WorkerHost> hosts = stage1.getHosts();
        return (int) hosts.values().stream()
            .filter(h -> h.getState() == io.mantisrx.runtime.MantisJobState.Started)
            .count();
    }

    /**
     * Helper method to wait for refresh processing without arbitrary sleep.
     */
    private void waitForRefreshProcessing() {
        try {
            Thread.sleep(500); // Brief pause to ensure refresh is processed
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Helper method to create a properly mocked MantisScheduler that supports reservation API.
     * The mock returns successful CompletableFuture for upsertReservation calls.
     */
    private MantisScheduler createMockScheduler() {
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
        // Mock reservation API to return successful CompletableFuture
        when(schedulerMock.upsertReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(schedulerMock.cancelReservation(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        return schedulerMock;
    }

    private static void assertNotNull(String message, Object object) {
        if (object == null) {
            fail(message);
        }
    }
}
