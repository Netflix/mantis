/*
 * Copyright 2021 Netflix, Inc.
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
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.JobClusterActor;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.Strategy;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

public class JobScaleUpDownTests {

	static {
		// Wire a real Spectator registry before any JobActor is constructed so Counter.value()
		// reflects increments instead of returning 0 from the default no-op composite registry.
		io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory.setRegistry(
				new com.netflix.spectator.api.DefaultRegistry());
	}

	static ActorSystem system;
	final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create();
		TestHelpers.setupMasterConfig();
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

	/**
	 * Setup master config for legacy scheduling tests.
	 */
	private static void setupLegacyConfig() {
		Properties props = new Properties();
		props.setProperty("mantis.scheduling.reservation.enabled", "false");
		TestHelpers.setupMasterConfig(props);
	}


	////////////////////////Scale up Tests ////////////////////////////////////
	@Test
	public void testJobScaleUpReservation() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleUpReservation";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// send scale up request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1", 1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleupResp " + scaleResp.message);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(3,scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		//scale up worker
		verify(jobStoreMock, times(2)).storeNewWorker(any());

		verify(jobStoreMock, times(6)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// initial worker + job master and scale up worker
        // should be twice because it is an initial request + scale up request
		// For reservation-based scheduling, verify upsertReservation instead of scheduleWorkers
		verify(schedulerMock, times(3)).upsertReservation(any(UpsertReservation.class));
		verify(schedulerMock, never()).scheduleWorkers(any());

	}

	@Test
	public void testJobScaleUpLegacy() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {
		setupLegacyConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleUpLegacy";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// send scale up request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1", 1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleupResp " + scaleResp.message);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(3,scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		//scale up worker
		verify(jobStoreMock, times(2)).storeNewWorker(any());

		verify(jobStoreMock, times(6)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// initial worker + job master and scale up worker
        //should be twice because it is an initial request + scale up request
		verify(schedulerMock, times(2)).scheduleWorkers(any());
		verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));

	}

	@Test
	public void testJobScaleDownReservation() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(2,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleDownReservation";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);
		// send scale down request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1",1, 1, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleDownResp " + scaleResp.message);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(1,scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		// 9 for worker events + 1 for scale down
		verify(jobStoreMock, times(10)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// 1 scale down
		verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(any(), any());

		// 1 job master + 2 workers
		// For reservation-based scheduling, verify upsertReservation instead of scheduleWorkers
		verify(schedulerMock, times(2)).upsertReservation(any(UpsertReservation.class));
		verify(schedulerMock, never()).scheduleWorkers(any());

	}

	@Test
	public void testJobScaleDownLegacy() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {
		setupLegacyConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(2,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleDownLegacy";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);



		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);
		// send scale down request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1",1, 1, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleDownResp " + scaleResp.message);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(1,scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		// 9 for worker events + 1 for scale down
		verify(jobStoreMock, times(10)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// 1 scale down
		verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(any(), any());

		// 1 job master + 2 workers
		verify(schedulerMock, times(1)).scheduleWorkers(any());
		verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));

	}

	@Test
	public void initFailureStopsActorAndPreventsDirtySnapshotReuse() throws Exception {
		setupLegacyConfig();
		final TestKit probe = new TestKit(system);
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerStageWithConstraints(
						1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList())
				.build();
		String clusterName = "initFailureStopsActor";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
		JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		doThrow(new IOException("induced init store failure"))
				.when(jobStoreMock).storeNewWorkers(any(), any());

		MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
				.withJobId(new JobId(clusterName, 1))
				.withSubmittedAt(Instant.now())
				.withJobState(JobState.Accepted)
				.withNextWorkerNumToUse(1)
				.withJobDefinition(jobDefn)
				.build();
		ActorRef jobActor = system.actorOf(JobActor.props(
				jobClusterDefn,
				mantisJobMetaData,
				jobStoreMock,
				schedulerMock,
				lifecycleEventPublisher,
				CostsCalculator.noop()));

		probe.watch(jobActor);
		jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
		JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
		assertEquals(SERVER_ERROR, initMsg.responseCode);
		assertTrue(initMsg.message.contains("Exception saving worker for Job"));
		probe.expectTerminated(jobActor);

		verify(jobStoreMock, times(1)).storeNewJob(any());
		verify(jobStoreMock, times(1)).storeNewWorkers(any(), any());
		verify(schedulerMock, never()).scheduleWorkers(any());
		verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
	}

	private void validateHost(Map<Integer, WorkerHost> hosts, int workerIdx, int workerNum, MantisJobState workerState) {
		assertTrue(hosts.containsKey(workerNum));
		assertEquals(hosts.get(workerNum).getHost(), "host1");
		assertEquals(hosts.get(workerNum).getState(), workerState);
		assertEquals(hosts.get(workerNum).getMetricsPort(), 8000);
		assertEquals(hosts.get(workerNum).getWorkerIndex(), workerIdx);
		assertEquals(hosts.get(workerNum).getWorkerNumber(), workerNum);
		assertEquals(hosts.get(workerNum).getPort(), Collections.singletonList(9020));
	}

	// TODO fix for timing issues
	//@Test
	public void testSchedulingInfo() throws Exception {
		CountDownLatch latch = new CountDownLatch(11);
	    List<JobSchedulingInfo> schedulingChangesList = new CopyOnWriteArrayList<>();
        final TestKit probe = new TestKit(system);
        Map<ScalingReason, Strategy> smap = new HashMap<>();
        smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerScalableStageWithConstraints(1,
                        new MachineDefinition(1.0,1.0,1.0,3),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
                .build();
        String clusterName = "testSchedulingInfo";
        MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        CountDownLatch worker1Started = new CountDownLatch(1);

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

        JobId jobId = new JobId(clusterName, 1);
        JobClusterManagerProto.GetJobSchedInfoRequest getJobSchedInfoRequest = new JobClusterManagerProto.GetJobSchedInfoRequest(jobId);

        jobActor.tell(getJobSchedInfoRequest, probe.getRef());
        JobClusterManagerProto.GetJobSchedInfoResponse resp = probe.expectMsgClass(JobClusterManagerProto.GetJobSchedInfoResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertTrue(resp.getJobSchedInfoSubject().isPresent());
        ObjectMapper mapper = new ObjectMapper();
        BehaviorSubject<JobSchedulingInfo> jobSchedulingInfoBehaviorSubject = resp.getJobSchedInfoSubject().get();
        jobSchedulingInfoBehaviorSubject.doOnNext((js) -> {
                                            System.out.println("Got --> " + js.toString());

                                        })
                                        .map((e) -> {
                                            try {
                                                return mapper.writeValueAsString(e);
                                            } catch (JsonProcessingException e1) {
                                                e1.printStackTrace();
                                                return "{\"error\":" + e1.getMessage() + "}";
                                            }
                                        })
                                        .map((js) -> {
                                            try {
                                                return mapper.readValue(js,JobSchedulingInfo.class);
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                                return null;
                                            }
                                        })
                                        .filter((j) -> j!=null)
                                        .doOnNext((js) -> {
//                                            Map<Integer, WorkerAssignments> workerAssignments = js.getWorkerAssignments();
//                                            WorkerAssignments workerAssignments1 = workerAssignments.get(1);
//                                            assertEquals(1, workerAssignments1.getNumWorkers());
//                                            Map<Integer, WorkerHost> hosts = workerAssignments1.getHosts();
//                                            // make sure worker number 1 exists
//                                            assertTrue(hosts.containsKey(1));
                                        })
                                        .doOnCompleted(() -> {
                                        	System.out.println("SchedulingInfo completed");
											System.out.println(schedulingChangesList.size() + " Sched changes received");

                                        })
                                        .observeOn(Schedulers.io())
                                        .subscribe((js) -> {
											latch.countDown();
                                            schedulingChangesList.add(js);
                                        });


        // send scale up request
        jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(jobId.getId(), 1, 2, "", ""), probe.getRef());
        JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
        System.out.println("ScaleupResp " + scaleResp.message);
        assertEquals(SUCCESS, scaleResp.responseCode);
        assertEquals(2,scaleResp.getActualNumWorkers());

        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobActor,jobId.getId(),1,new WorkerId(jobId.getId(),1,3));

        // worker gets lost
        JobTestHelper.sendWorkerTerminatedEvent(probe,jobActor,jobId.getId(),new WorkerId(jobId.getId(),1,3));

        // Send replacement worker messages
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobActor,jobId.getId(),1,new WorkerId(jobId.getId(),1,4));


        // scale down
        jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(jobId.getId(),1, 1, "", ""), probe.getRef());
        JobClusterManagerProto.ScaleStageResponse scaleDownResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
        System.out.println("ScaleDownResp " + scaleDownResp.message);
        assertEquals(SUCCESS, scaleDownResp.responseCode);
        assertEquals(1,scaleDownResp.getActualNumWorkers());

        // kill job
		jobActor.tell(new JobClusterProto.KillJobRequest(jobId,"killed", JobCompletedReason.Killed, "test", probe.getRef()),probe.getRef());
		probe.expectMsgClass(JobClusterProto.KillJobResponse.class);

		for (JobSchedulingInfo jobSchedulingInfo : schedulingChangesList) {
			System.out.println(jobSchedulingInfo);
		}
    /*
    SchedulingChange [jobId=testSchedulingInfo-1,
workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=1, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]],
													   3=WorkerHost [state=Launched, workerIndex=1, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]],
													   3=WorkerHost [state=StartInitiated, workerIndex=1, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]],
													   3=WorkerHost [state=Started, workerIndex=1, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]],
													   4=WorkerHost [state=Launched, workerIndex=1, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]],
														4=WorkerHost [state=StartInitiated, workerIndex=1, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=2, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]],
														4=WorkerHost [state=Started, workerIndex=1, host=host1, port=[9020]]}]}]
SchedulingChange [jobId=testSchedulingInfo-1, workerAssignments={
	0=WorkerAssignments [stage=0, numWorkers=1, hosts={1=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}],
	1=WorkerAssignments [stage=1, numWorkers=1, hosts={2=WorkerHost [state=Started, workerIndex=0, host=host1, port=[9020]]}
	]}]
     */

		latch.await(1000, TimeUnit.SECONDS);
		System.out.println("---->Verifying scheduling changes " + schedulingChangesList.size());
        assertEquals(11, schedulingChangesList.size());
        for(int i = 0;i < schedulingChangesList.size(); i++) {
            JobSchedulingInfo js = schedulingChangesList.get(i);
            // jobid is correct
            assertEquals(jobId.getId(),js.getJobId());

            Map<Integer, WorkerAssignments> workerAssignments = js.getWorkerAssignments();
            //has info about stage 1
			System.out.println("WorkerAssignments -> " + workerAssignments);
            //assertTrue(workerAssignments.containsKey(1));
            switch(i) {
                case 0:
                    WorkerAssignments wa0 = workerAssignments.get(1);
                    assertEquals(1, wa0.getNumWorkers());
                    Map<Integer, WorkerHost> hosts0 = wa0.getHosts();
                    // make sure worker number 2 exists
					validateHost(hosts0, 0, 2, MantisJobState.Started);
                    break;
                // scale up by 1
                case 1:
					WorkerAssignments wa1 = workerAssignments.get(1);
					assertEquals(2, wa1.getNumWorkers());
					Map<Integer, WorkerHost> hosts1 = wa1.getHosts();
					assertEquals(1, hosts1.size());
					// first update has only numWorkers updated but the new worker is still in Accepted state, so no host entry for it
					validateHost(hosts1, 0, 2, MantisJobState.Started);
					assertFalse(hosts1.containsKey(3));
					break;
                case 2:
					WorkerAssignments wa2 = workerAssignments.get(1);
					assertEquals(2, wa2.getNumWorkers());
					Map<Integer, WorkerHost> hosts2 = wa2.getHosts();
					assertEquals(2, hosts2.size());
					// next update should have both numWorkers and the new worker in Launched state
					validateHost(hosts2, 0, 2, MantisJobState.Started);
					validateHost(hosts2, 1, 3, MantisJobState.Launched);
					break;
                case 3:
					WorkerAssignments wa3 = workerAssignments.get(1);
					assertEquals(2, wa3.getNumWorkers());
					Map<Integer, WorkerHost> hosts3 = wa3.getHosts();
					assertEquals(2, hosts3.size());

					// this update is for new worker in StartInit state
					validateHost(hosts3, 0, 2, MantisJobState.Started);
					validateHost(hosts3, 1, 3, MantisJobState.StartInitiated);
                    break;
				case 4:
					WorkerAssignments wa4 = workerAssignments.get(1);
					assertEquals(2, wa4.getNumWorkers());
					Map<Integer, WorkerHost> hosts4 = wa4.getHosts();
					assertEquals(2, hosts4.size());
					// this update is for new worker in Started state
					validateHost(hosts4, 0, 2, MantisJobState.Started);
					validateHost(hosts4, 1, 3, MantisJobState.Started);
					break;
                case 5:
					// worker 3 is lost and should be resubmitted
					WorkerAssignments wa5 = workerAssignments.get(1);
					assertEquals(2, wa5.getNumWorkers());
					Map<Integer, WorkerHost> hosts5 = wa5.getHosts();
					assertEquals(1, hosts5.size());
					validateHost(hosts5, 0, 2, MantisJobState.Started);
					assertFalse(hosts5.containsKey(3));
					break;
                case 6:
                	// worker 3 is replaced by worker num 4
					WorkerAssignments wa6 = workerAssignments.get(1);
					assertEquals(2, wa6.getNumWorkers());
					Map<Integer, WorkerHost> hosts6 = wa6.getHosts();
					// this update should have both numWorkers and the new worker in Launched state
					assertEquals(2, hosts6.size());
					validateHost(hosts6, 0, 2, MantisJobState.Started);
					validateHost(hosts6, 1, 4, MantisJobState.Launched);
					break;
				case 7:
					WorkerAssignments wa7 = workerAssignments.get(1);
					assertEquals(2, wa7.getNumWorkers());
					Map<Integer, WorkerHost> hosts7 = wa7.getHosts();
					// update for new worker in StartInit state
					assertEquals(2, hosts7.size());
					validateHost(hosts7, 0, 2, MantisJobState.Started);
					validateHost(hosts7, 1, 4, MantisJobState.StartInitiated);
					break;
				case 8:
					WorkerAssignments wa8 = workerAssignments.get(1);
					assertEquals(2, wa8.getNumWorkers());
					Map<Integer, WorkerHost> hosts8 = wa8.getHosts();
					// update for new worker in Started state
					assertEquals(2, hosts8.size());
					validateHost(hosts8, 0, 2, MantisJobState.Started);
					validateHost(hosts8, 1, 4, MantisJobState.Started);
					break;
				case 9:
					// scale down, worker 4 should be gone now and numWorkers set to 1
					WorkerAssignments wa9 = workerAssignments.get(1);
					assertEquals(1, wa9.getNumWorkers());
					Map<Integer, WorkerHost> hosts9 = wa9.getHosts();
					assertTrue(hosts9.containsKey(2));
					assertEquals(1, hosts9.size());
					validateHost(hosts9, 0, 2, MantisJobState.Started);
                    break;
				case 10:

					// job has been killed
					assertTrue(workerAssignments.isEmpty());
					break;
                default:
                    fail();

            }





        }
//
//        verify(jobStoreMock, times(1)).storeNewJob(any());
//        // initial worker
//        verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());
//
//        //scale up worker
//        verify(jobStoreMock, times(1)).storeNewWorker(any());
//
//       // verify(jobStoreMock, times(17)).updateWorker(any());
//
//        verify(jobStoreMock, times(3)).updateJob(any());
//
//        // initial worker + job master and scale up worker + resubmit
//        verify(schedulerMock, times(4)).scheduleWorker(any());
//
//        verify(schedulerMock, times(4)).unscheduleAndTerminateWorker(any(), any());

    }

	@Test
	public void testJobScaleUpFailsIfNoScaleStrategyReservation() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);

		Map<ScalingReason, Strategy> smap = new HashMap<>();

		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleUpFailsIfNoScaleStrategyReservation";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);


		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// send scale up request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1",1, 2, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleupResp " + scaleResp.message);
		assertEquals(CLIENT_ERROR, scaleResp.responseCode);
		assertEquals(0, scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		//no scale up worker happened
		verify(jobStoreMock, times(0)).storeNewWorker(any());

		verify(jobStoreMock, times(3)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// initial worker only
		// For reservation-based scheduling, verify upsertReservation instead of scheduleWorkers
		verify(schedulerMock, times(1)).upsertReservation(any(UpsertReservation.class));
		verify(schedulerMock, never()).scheduleWorkers(any());
	}

	@Test
	public void testJobScaleUpFailsIfNoScaleStrategyLegacy() throws Exception {
		setupLegacyConfig();
		final TestKit probe = new TestKit(system);

		Map<ScalingReason, Strategy> smap = new HashMap<>();

		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleUpFailsIfNoScaleStrategyLegacy";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);


		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// send scale up request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1",1, 2, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleupResp " + scaleResp.message);
		assertEquals(CLIENT_ERROR, scaleResp.responseCode);
		assertEquals(0, scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		//no scale up worker happened
		verify(jobStoreMock, times(0)).storeNewWorker(any());

		verify(jobStoreMock, times(3)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// initial worker only
		verify(schedulerMock, times(1)).scheduleWorkers(any());
		verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
	}

	@Test
	public void testJobScaleUpFailsIfMinEqualsMaxReservation() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);

		Map<ScalingReason, Strategy> smap = new HashMap<>();

		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 1, 1, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleUpFailsIfMinEqualsMaxReservation";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);


		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// send scale up request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1",1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleupResp " + scaleResp.message);
		assertEquals(CLIENT_ERROR, scaleResp.responseCode);
		assertEquals(0, scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		//no scale up worker happened
		verify(jobStoreMock, times(0)).storeNewWorker(any());

		verify(jobStoreMock, times(3)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// initial worker only
		// For reservation-based scheduling, verify upsertReservation instead of scheduleWorkers
		verify(schedulerMock, times(1)).upsertReservation(any(UpsertReservation.class));
		verify(schedulerMock, never()).scheduleWorkers(any());
	}

	@Test
	public void testJobScaleUpFailsIfMinEqualsMaxLegacy() throws Exception {
		setupLegacyConfig();
		final TestKit probe = new TestKit(system);

		Map<ScalingReason, Strategy> smap = new HashMap<>();

		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0,1.0,1.0,3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 1, 1, 1, 1, 0, smap, true))
				.build();
		String clusterName = "testJobScaleUpFailsIfMinEqualsMaxLegacy";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);


		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// send scale up request
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1",1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		System.out.println("ScaleupResp " + scaleResp.message);
		assertEquals(CLIENT_ERROR, scaleResp.responseCode);
		assertEquals(0, scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(1)).storeNewJob(any());
		// initial worker
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

		//no scale up worker happened
		verify(jobStoreMock, times(0)).storeNewWorker(any());

		verify(jobStoreMock, times(3)).updateWorker(any());

		verify(jobStoreMock, times(3)).updateJob(any());

		// initial worker only
		verify(schedulerMock, times(1)).scheduleWorkers(any());
		verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));
	}

	////////////////////// Scale-up + storeNewWorker failure tests /////////////////////////
	// These verify that JobActor.scaleStage keeps worker indexes dense when storeNewWorker fails:
	// failed additions are rolled back from in-memory metadata, the stage target shrinks to the
	// realized worker count, and only durably-stored workers are queued to the scheduler.

	/**
	 * First storeNewWorker call (for worker index 1) throws; the next two attempts succeed.
	 * After the scale: the stage target shrinks to 3 workers, later successes reuse the failed
	 * index so the worker indexes remain dense, and the SCALE reservation contains only the
	 * 2 durably-stored new workers.
	 */
	@Test
	public void scaleUpStageWithStoreNewWorkerFailureOnNthIteration_shrinksToRealizedCount() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpStoreFailureRollback";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		// Fail the FIRST scale-up storeNewWorker call (worker index 1), allow the next two.
		// Initial-job path uses storeNewWorkers(...) (plural batch) — unaffected by this stub.
		doThrow(new IOException("induced storeNewWorker failure"))
				.doNothing()
				.doNothing()
				.when(jobStoreMock).storeNewWorker(any());

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(
				system, probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// Scale 1 -> 4 (delta=3): one will fail, two will succeed
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 4, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(3, scaleResp.getActualNumWorkers());
		assertTrue(scaleResp.message.contains("requested 4"));
		assertTrue(scaleResp.message.contains("1 add failures"));

		// All three storeNewWorker calls were attempted (the failure didn't abort the loop)
		verify(jobStoreMock, times(3)).storeNewWorker(any());

		// Inspect the live job metadata: stage 1 must contain exactly 3 workers with no missing index.
		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, detailsResp.responseCode);
		assertTrue(detailsResp.getJobMetadata().isPresent());
		IMantisJobMetadata md = detailsResp.getJobMetadata().get();
		IMantisStageMetadata stage1 = md.getStageMetadata().get(1);
		assertStageHasDenseWorkerIndexes(stage1, 3);

		// Capture the actual reservation upsert payloads. The scale event's upsert must carry
		// exactly 2 workers (only the successfully-stored ones). Initial submission and JobMaster
		// upserts are NEW_JOB-priority; the scale upsert is SCALE-priority — filter on that.
		ArgumentCaptor<UpsertReservation> upsertCap = ArgumentCaptor.forClass(UpsertReservation.class);
		verify(schedulerMock, atLeast(1)).upsertReservation(upsertCap.capture());
		UpsertReservation scaleUpsert = lastUpsertOfType(upsertCap.getAllValues(),
				io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType.SCALE);
		assertEquals("scale upsert must contain only the 2 successfully-stored workers",
				2, scaleUpsert.getAllocationRequests().size());

		// Follow-on scale-down must remove the highest realized index cleanly.
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 2, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleDownResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SUCCESS, scaleDownResp.responseCode);
		assertEquals(2, scaleDownResp.getActualNumWorkers());
		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse postScaleDownResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, postScaleDownResp.responseCode);
		assertStageHasDenseWorkerIndexes(postScaleDownResp.getJobMetadata().get().getStageMetadata().get(1), 2);
	}

	/**
	 * Every storeNewWorker call during the scale fails. After the scale: the stage target shrinks
	 * back to its pre-scale worker count (1), and the reservation upsert from the scale must
	 * contain zero workers (queueWorkers was called with an empty list).
	 */
	@Test
	public void scaleUpStageWithAllStoreNewWorkerFailures_keepsOriginalState() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpAllStoreFailures";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		// Make every scale-up storeNewWorker call throw.
		doThrow(new IOException("induced storeNewWorker failure"))
				.when(jobStoreMock).storeNewWorker(any());

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(
				system, probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// Scale 1 -> 4 (delta=3): all three will fail
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 4, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(1, scaleResp.getActualNumWorkers());
		assertTrue(scaleResp.message.contains("requested 4"));
		assertTrue(scaleResp.message.contains("3 add failures"));

		verify(jobStoreMock, times(3)).storeNewWorker(any());

		// Stage worker count must be back to its pre-scale value (1 initial worker).
		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, detailsResp.responseCode);
		IMantisJobMetadata md = detailsResp.getJobMetadata().get();
		IMantisStageMetadata stage1 = md.getStageMetadata().get(1);
		assertStageHasDenseWorkerIndexes(stage1, 1);

		// queueWorkersViaReservation short-circuits on an empty worker list (JobActor.java:1905)
		// so when every store call rolls back, NO SCALE-priority upsert should be sent at all —
		// the only upserts are the NEW_JOB ones from initial submission and JobMaster.
		ArgumentCaptor<UpsertReservation> upsertCap = ArgumentCaptor.forClass(UpsertReservation.class);
		verify(schedulerMock, atLeast(1)).upsertReservation(upsertCap.capture());
		long scaleUpserts = upsertCap.getAllValues().stream()
				.filter(u -> u.getPriority() != null
						&& u.getPriority().getType() == io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType.SCALE)
				.count();
		assertEquals("no SCALE upsert should be sent when every storeNewWorker rolls back",
				0L, scaleUpserts);
	}

	@Test
	public void scaleUpStageWithStoreNewWorkerFailureOnNthIteration_legacyShrinksToRealizedCount() throws Exception {
		setupLegacyConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpStoreFailureRollbackLegacy";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(false);
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		doThrow(new IOException("induced storeNewWorker failure"))
				.doNothing()
				.doNothing()
				.when(jobStoreMock).storeNewWorker(any());

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(
				system, probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 4, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(3, scaleResp.getActualNumWorkers());
		assertTrue(scaleResp.message.contains("requested 4"));
		assertTrue(scaleResp.message.contains("1 add failures"));

		verify(jobStoreMock, times(3)).storeNewWorker(any());
		verify(schedulerMock, times(2)).scheduleWorkers(any());
		verify(schedulerMock, never()).upsertReservation(any(UpsertReservation.class));

		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, detailsResp.responseCode);
		assertStageHasDenseWorkerIndexes(detailsResp.getJobMetadata().get().getStageMetadata().get(1), 3);
	}

	@Test
	public void scaleUpStageShrinkPersistenceFailurePreservesQueuedPartialResult() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpShrinkPersistenceFailure";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		doNothing()
				.doThrow(new IOException("induced storeNewWorker failure"))
				.when(jobStoreMock).storeNewWorker(any());
		doAnswer(invocation -> {
			IMantisStageMetadata stage = (IMantisStageMetadata) invocation.getArguments()[0];
			if (stage.getStageNum() == 1 && stage.getNumWorkers() == 3) {
				throw new IOException("induced shrink updateStage failure");
			}
			return null;
		}).when(jobStoreMock).updateStage(any());

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(
				system, probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		long scaleCounterBefore = readNumScaleStage(clusterName + "-1");
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 4, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SERVER_ERROR, scaleResp.responseCode);
		assertEquals(2, scaleResp.getActualNumWorkers());
		assertTrue(scaleResp.message.contains("requested 4"));
		assertTrue(scaleResp.message.contains("compensating shrink also failed"));
		assertTrue(scaleResp.message.contains("shrink did not stick"));
		assertTrue(scaleResp.message.contains("prior stage target 4 remains in force"));
		assertTrue("response message must include the original add-failure cause",
				scaleResp.message.contains("induced storeNewWorker failure"));
		assertTrue("response message must include the shrink-failure summary",
				scaleResp.message.contains("Failed to shrink stage"));

		verify(jobStoreMock, times(2)).storeNewWorker(any());

		ArgumentCaptor<UpsertReservation> upsertCap = ArgumentCaptor.forClass(UpsertReservation.class);
		verify(schedulerMock, atLeast(1)).upsertReservation(upsertCap.capture());
		UpsertReservation scaleUpsert = lastUpsertOfType(
				upsertCap.getAllValues(),
				io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType.SCALE);
		assertEquals("the already-persisted worker must still be queued",
				1, scaleUpsert.getAllocationRequests().size());

		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, detailsResp.responseCode);
		assertStageHasDenseWorkerIndexes(detailsResp.getJobMetadata().get().getStageMetadata().get(1), 4, 2);

		assertEquals("partial-success scale must increment numScaleStage",
				scaleCounterBefore + 1, readNumScaleStage(clusterName + "-1"));
	}

	@Test
	public void scaleUpStageRollbackCorruptionFailsJobWithoutQueueingScaleReservation() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpRollbackCorruption";
		MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
		when(schedulerMockFactory.forClusterID(any())).thenReturn(schedulerMock);
		List<UpsertReservation> upserts = new CopyOnWriteArrayList<>();
		doAnswer(invocation -> {
			UpsertReservation request = (UpsertReservation) invocation.getArguments()[0];
			upserts.add(request);
			return java.util.concurrent.CompletableFuture.completedFuture(io.mantisrx.common.Ack.getInstance());
		}).when(schedulerMock).upsertReservation(any());
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		final java.util.concurrent.atomic.AtomicReference<MantisStageMetadataImpl> stageRef =
				new java.util.concurrent.atomic.AtomicReference<>();
		final java.util.concurrent.atomic.AtomicInteger storeCallCount = new java.util.concurrent.atomic.AtomicInteger();
		doAnswer(invocation -> {
			int call = storeCallCount.incrementAndGet();
			if (call == 1) {
				return null;
			}
			IMantisWorkerMetadata stored = (IMantisWorkerMetadata) invocation.getArguments()[0];
			MantisStageMetadataImpl stage = stageRef.get();
			assertTrue("stage reference must be captured before rollback corruption fires", stage != null);
			corruptStageIndexEntry(stage, stored.getWorkerIndex(), duplicateWorker(stored));
			throw new IOException("induced storeNewWorker failure");
		}).when(jobStoreMock).storeNewWorker(any());

		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
		JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
		ActorRef jobClusterActor = system.actorOf(JobClusterActor.props(
				clusterName,
				jobStoreMock,
				schedulerMockFactory,
				lifecycleEventPublisher,
				CostsCalculator.noop(),
				0));
		jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(
				(io.mantisrx.server.master.domain.JobClusterDefinitionImpl) jobClusterDefn,
				"user",
				probe.getRef()), probe.getRef());
		JobClusterProto.InitializeJobClusterResponse initResp =
				probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
		assertEquals(SUCCESS, initResp.responseCode);

		String jobId = clusterName + "-1";
		JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);
		JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);
		JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(
				probe,
				jobClusterActor,
				jobId,
				0,
				new WorkerId(clusterName, jobId, 0, 1));
		JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(
				probe,
				jobClusterActor,
				jobId,
				1,
				new WorkerId(clusterName, jobId, 0, 2));
		JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

		ActorRef jobActor = resolveJobActor(probe, jobClusterActor, jobId);
		probe.watch(jobActor);

		jobClusterActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse pre = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		stageRef.set((MantisStageMetadataImpl) pre.getJobMetadata().get().getStageMetadata().get(1));

		long scaleCounterBefore = readNumScaleStage(jobId);
		long scaleUpsertsBefore = countUpsertsOfType(
				upserts,
				io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType.SCALE);

		jobClusterActor.tell(new JobClusterManagerProto.ScaleStageRequest(jobId, 1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SERVER_ERROR, scaleResp.responseCode);
		assertEquals(2, scaleResp.getActualNumWorkers());
		assertTrue(scaleResp.message.contains("rollback of failed addition also threw"));
		assertTrue("response message must include the original add-failure cause",
				scaleResp.message.contains("induced storeNewWorker failure"));
		assertTrue(scaleResp.message.contains("job will be failed"));

		assertEquals("fatal rollback corruption must not queue SCALE reservations",
				scaleUpsertsBefore,
				countUpsertsOfType(
						upserts,
						io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType.SCALE));
		assertEquals("fatal rollback corruption must NOT increment numScaleStage",
				scaleCounterBefore, readNumScaleStage(jobId));

		probe.expectTerminated(jobActor);
		verify(jobStoreMock, timeout(2000).times(1)).archiveJob(any());
	}

	/**
	 * Item 1 negative case: when scaleStage fails *before* anything is durably stored
	 * (the initial unsafeSetNumWorkers call throws), the scale produced no durable state
	 * change and numScaleStage must NOT increment.
	 */
	@Test
	public void scaleUpStageInitialTargetPersistFailureDoesNotIncrementScaleCounter() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpTargetPersistFailure";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(
				system, probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// Now arm updateStage to throw — the *initial* unsafeSetNumWorkers in scaleStage will fail
		// before any worker is durably stored.
		doThrow(new IOException("induced updateStage failure"))
				.when(jobStoreMock).updateStage(any());

		long scaleCounterBefore = readNumScaleStage(clusterName + "-1");

		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SERVER_ERROR, scaleResp.responseCode);

		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, detailsResp.responseCode);
		assertStageHasDenseWorkerIndexes(detailsResp.getJobMetadata().get().getStageMetadata().get(1), 1);

		assertEquals("scale that never durably stored anything must NOT increment numScaleStage",
				scaleCounterBefore, readNumScaleStage(clusterName + "-1"));
	}

	private long readNumScaleStage(String jobId) {
		// Look up the JobActor's metrics group from the global registry. We scan rather than
		// build the exact MetricGroupId so the lookup is robust against tag-ordering or
		// resourceCluster differences across test setups.
		for (Metrics m : MetricsRegistry.getInstance().getMetrics("JobActor")) {
			if (m.getMetricGroupId().id().contains("jobId=" + jobId)) {
				Counter c = m.getCounter("numScaleStage");
				return c == null ? 0L : c.value();
			}
		}
		return 0L;
	}

	private static UpsertReservation lastUpsertOfType(
			List<UpsertReservation> upserts,
			io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType priorityType) {
		UpsertReservation last = null;
		for (UpsertReservation u : upserts) {
			if (u.getPriority() != null && u.getPriority().getType() == priorityType) {
				last = u;
			}
		}
		if (last == null) {
			fail("no upsertReservation captured with priority type " + priorityType);
		}
		return last;
	}

	private static void assertStageHasDenseWorkerIndexes(IMantisStageMetadata stage, int expectedNumWorkers) throws InvalidJobException {
		assertStageHasDenseWorkerIndexes(stage, expectedNumWorkers, expectedNumWorkers);
	}

	private static void assertStageHasDenseWorkerIndexes(
			IMantisStageMetadata stage,
			int expectedTargetWorkers,
			int expectedRealizedWorkers) throws InvalidJobException {
		assertEquals("stage target size should match expected target", expectedTargetWorkers, stage.getNumWorkers());
		assertEquals("worker metadata count should match realized worker count", expectedRealizedWorkers, stage.getAllWorkers().size());
		for (int workerIndex = 0; workerIndex < expectedRealizedWorkers; workerIndex++) {
			assertEquals(workerIndex, stage.getWorkerByIndex(workerIndex).getMetadata().getWorkerIndex());
		}
		assertMissingWorkerIndex(stage, expectedRealizedWorkers);
	}

	private static ActorRef resolveJobActor(TestKit probe, ActorRef jobClusterActor, String jobId) {
		system.actorSelection(jobClusterActor.path().child("JobActor-" + jobId))
				.tell(new Identify(jobId), probe.getRef());
		ActorIdentity identity = probe.expectMsgClass(ActorIdentity.class);
		assertTrue("job actor must be resolvable for " + jobId, identity.getActorRef().isPresent());
		return identity.getActorRef().get();
	}

	private static long countUpsertsOfType(
			List<UpsertReservation> upserts,
			io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType priorityType) {
		long count = 0;
		for (UpsertReservation upsert : upserts) {
			if (upsert.getPriority() != null && upsert.getPriority().getType() == priorityType) {
				count++;
			}
		}
		return count;
	}

	private static void corruptStageIndexEntry(MantisStageMetadataImpl stageMeta, int workerIndex, JobWorker worker)
			throws Exception {
		java.lang.reflect.Field idxField = MantisStageMetadataImpl.class.getDeclaredField("workerByIndexMetadataSet");
		idxField.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<Integer, JobWorker> idxMap = (Map<Integer, JobWorker>) idxField.get(stageMeta);
		idxMap.put(workerIndex, worker);
	}

	private JobWorker duplicateWorker(IMantisWorkerMetadata stored) {
		return new JobWorker.Builder()
				.withJobId(stored.getJobId())
				.withWorkerIndex(stored.getWorkerIndex())
				.withWorkerNumber(stored.getWorkerNumber())
				.withNumberOfPorts(1 + io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl.MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
				.withWorkerPorts(new WorkerPorts(Lists.newArrayList(9091, 9092, 9093, 9094, 9095)))
				.withStageNum(stored.getStageNum())
				.withLifecycleEventsPublisher(lifecycleEventPublisher)
				.build();
	}

	private static void assertMissingWorkerIndex(IMantisStageMetadata stage, int workerIndex) {
		try {
			stage.getWorkerByIndex(workerIndex);
			fail("expected worker index " + workerIndex + " to be absent");
		} catch (InvalidJobException expected) {
			// Expected: dense index set stops before workerIndex.
		}
	}

	/**
	 * Regression guard: when every storeNewWorker call succeeds, the post-fix behavior must
	 * match the legacy expectation set by {@link #testJobScaleUpReservation()} — same
	 * storeNewWorker call count, same worker count after scale, same upsertReservation count.
	 */
	@Test
	public void scaleUpStageWithAllStoreNewWorkerSuccesses_unchangedBehavior() throws Exception {
		setupReservationConfig();
		final TestKit probe = new TestKit(system);
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		SchedulingInfo sInfo = new SchedulingInfo.Builder()
				.numberOfStages(1)
				.multiWorkerScalableStageWithConstraints(1,
						new MachineDefinition(1.0, 1.0, 1.0, 3),
						Lists.newArrayList(),
						Lists.newArrayList(),
						new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
				.build();
		String clusterName = "scaleUpAllStoreSuccesses";
		MantisScheduler schedulerMock = JobTestHelper.createMockScheduler();
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);

		ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(
				system, probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

		// Scale 1 -> 3 (delta=2): same as the existing reservation test
		jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName + "-1", 1, 3, "", ""), probe.getRef());
		JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
		assertEquals(SUCCESS, scaleResp.responseCode);
		assertEquals(3, scaleResp.getActualNumWorkers());

		verify(jobStoreMock, times(2)).storeNewWorker(any());

		jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(clusterName + "-1").get()), probe.getRef());
		JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
		assertEquals(SUCCESS, detailsResp.responseCode);
		IMantisJobMetadata md = detailsResp.getJobMetadata().get();
		IMantisStageMetadata stage1 = md.getStageMetadata().get(1);
		assertEquals("all-success path should yield 3 workers in stage", 3, stage1.getAllWorkers().size());
	}

	@Test
	public void stageScalingPolicyTest() {
		int stageNo = 1;
		int min = 0;
		int max = 10;
		int increment = 1;
		int decrement = 1;
		long cooldownsecs = 300;
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		StageScalingPolicy ssp = new StageScalingPolicy(stageNo, min, max, increment, decrement, cooldownsecs, smap, true);

		assertTrue(ssp.isEnabled());
	}

	@Test
	public void stageScalingPolicyNoStrategyTest() {
		int stageNo = 1;
		int min = 0;
		int max = 10;
		int increment = 1;
		int decrement = 1;
		long cooldownsecs = 300;
		Map<ScalingReason, Strategy> smap = new HashMap<>();

		StageScalingPolicy ssp = new StageScalingPolicy(stageNo, min, max, increment, decrement, cooldownsecs, smap, true);

		assertFalse(ssp.isEnabled());
	}

	@Test
	public void stageScalingPolicyMinEqMaxTest() {
		int stageNo = 1;
		int min = 10;
		int max = 10;
		int increment = 1;
		int decrement = 1;
		long cooldownsecs = 300;
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		StageScalingPolicy ssp = new StageScalingPolicy(stageNo, min, max, increment, decrement, cooldownsecs, smap, true);

		assertFalse(ssp.isEnabled());
	}

	@Test
	public void stageScalingPolicyMinGreaterThanMaxTest() {
		int stageNo = 1;
		int min = 10;
		int max = 1;
		int increment = 1;
		int decrement = 1;
		long cooldownsecs = 300;
		Map<ScalingReason, Strategy> smap = new HashMap<>();
		smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
		StageScalingPolicy ssp = new StageScalingPolicy(stageNo, min, max, increment, decrement, cooldownsecs, smap, true);

		assertTrue(ssp.isEnabled());
		// max will be set equal to min
		assertEquals(10, ssp.getMax());
	}


}
