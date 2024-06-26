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
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
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
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

public class JobScaleUpDownTests {

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


	////////////////////////Scale up Tests ////////////////////////////////////
	@Test
	public void testJobScaleUp() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {
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
		String clusterName = "testJobScaleUp";
		MantisScheduler schedulerMock = mock(MantisScheduler.class);
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
		verify(schedulerMock, times(3)).scheduleWorkers(any());

	}

	@Test
	public void testJobScaleDown() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {
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
		String clusterName = "testJobScaleUp";
		MantisScheduler schedulerMock = mock(MantisScheduler.class);
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
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
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
	public void testJobScaleUpFailsIfNoScaleStrategy() throws Exception {
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
		String clusterName = "testJobScaleUpFailsIfNoScaleStrategy";
		MantisScheduler schedulerMock = mock(MantisScheduler.class);
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
	}

	@Test
	public void testJobScaleUpFailsIfMinEqualsMax() throws Exception {
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
		String clusterName = "testJobScaleUpFailsIfNoScaleStrategy";
		MantisScheduler schedulerMock = mock(MantisScheduler.class);
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
