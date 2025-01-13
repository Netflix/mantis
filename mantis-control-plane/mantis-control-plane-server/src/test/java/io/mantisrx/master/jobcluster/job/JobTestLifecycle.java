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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.JobActor.WorkerNumberGenerator;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.BatchScheduleRequest;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class JobTestLifecycle {

	static ActorSystem system;
	private static MantisJobStore jobStore;
	private static IMantisPersistenceProvider storageProvider;
	private static LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());
    private final CostsCalculator costsCalculator = CostsCalculator.noop();

	private static final String user = "mantis";

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create();

		TestHelpers.setupMasterConfig();
		storageProvider = new KeyValueBasedPersistenceProvider(new FileBasedStore(), eventPublisher);
		jobStore = new MantisJobStore(storageProvider);
	}

	@AfterClass
	public static void tearDown() {
		JobTestHelper.deleteAllFiles();
		TestKit.shutdownActorSystem(system);
		system = null;
	}


	@Test
	public void testJobSubmitWithoutInit() {
		final TestKit probe = new TestKit(system);
		String clusterName = "testJobSubmitCluster";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);


		JobDefinition jobDefn;
		try {
			jobDefn = JobTestHelper.generateJobDefinition(clusterName);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName,1))
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefn)
                    .build();
			final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));
			String jobId = clusterName + "-1";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println(resp.message);
			assertEquals(CLIENT_ERROR, resp.responseCode);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testJobSubmit() {
		final TestKit probe = new TestKit(system);
		String clusterName = "testJobSubmitCluster";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);


		JobDefinition jobDefn;
		try {
			jobDefn  = JobTestHelper.generateJobDefinition(clusterName);
//			IMantisStorageProvider storageProvider = new SimpleCachedFileStorageProvider();
//			MantisJobStore	jobStore = new MantisJobStore(storageProvider);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName,1))
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefn)
                    .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));


			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SUCCESS, initMsg.responseCode);

			String jobId = clusterName + "-1";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(SUCCESS, resp.responseCode);
			assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());

			assertTrue(resp.getJobMetadata().get().getStageMetadata(1).isPresent());

			// send launched event

			WorkerId workerId = new WorkerId(jobId, 0, 1);
			int stageNum = 1;
			JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNum);

			JobTestHelper.sendStartInitiatedEvent(probe, jobActor, stageNum, workerId);

			// send heartbeat
			JobTestHelper.sendHeartBeat(probe, jobActor, jobId, stageNum, workerId);



			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp2 + " msg " + resp2.message);
			assertEquals(SUCCESS, resp2.responseCode);
			assertEquals(JobState.Launched,resp2.getJobMetadata().get().getState());

			verify(jobStoreMock, times(1)).storeNewJob(any());

			verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

			verify(jobStoreMock, times(3)).updateWorker(any());

			verify(jobStoreMock, times(3)).updateJob(any());

			//assertEquals(jobActor, probe.getLastSender());
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}

    @Test
    public void testJobSubmitPerpetual() {
        final TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitPerpetual";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);


        JobDefinition jobDefn;
        try {
            MachineDefinition machineDefinition = new MachineDefinition(1.0, 1.0, 1.0, 1.0, 3);
            SchedulingInfo schedInfo = new SchedulingInfo.Builder()
                    .numberOfStages(1)
                    .singleWorkerStageWithConstraints(machineDefinition,
                            Lists.newArrayList(),
                            Lists.newArrayList()).build();
            jobDefn  = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withParameters(Lists.newArrayList())
                    .withLabels(Lists.newArrayList())
                    .withSchedulingInfo(schedInfo)
                    .withJobJarUrl("http://myart")
                    .withArtifactName("myart")
                    .withSubscriptionTimeoutSecs(30)
                    .withUser("njoshi")
                    .withNumberOfStages(schedInfo.getStages().size())
                    .withJobSla(new JobSla(0, 0, null, MantisJobDurationType.Perpetual, null))
                    .build();
            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName,1))
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefn)
                    .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));


            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);

            String jobId = clusterName + "-1";
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            //jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            System.out.println("resp " + resp + " msg " + resp.message);
            assertEquals(SUCCESS, resp.responseCode);
            assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());

            assertTrue(resp.getJobMetadata().get().getStageMetadata(1).isPresent());
            assertEquals(resp.getJobMetadata().get().getJobJarUrl().toString(), "http://myart");
            assertEquals(resp.getJobMetadata().get().getArtifactName(), "myart");

            // send launched event

            WorkerId workerId = new WorkerId(jobId, 0, 1);
            int stageNum = 1;
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNum);

            JobTestHelper.sendStartInitiatedEvent(probe, jobActor, stageNum, workerId);

            // send heartbeat
            JobTestHelper.sendHeartBeat(probe, jobActor, jobId, stageNum, workerId);



            // check job status again
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            //jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
            System.out.println("resp " + resp2 + " msg " + resp2.message);
            assertEquals(SUCCESS, resp2.responseCode);
            assertEquals(JobState.Launched,resp2.getJobMetadata().get().getState());

            verify(jobStoreMock, times(1)).storeNewJob(any());

            verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

            verify(jobStoreMock, times(3)).updateWorker(any());

            verify(jobStoreMock, times(3)).updateJob(any());

			//verify(jobStoreMock, times(3))

            verify(schedulerMock,times(1)).scheduleWorkers(any());

            JobMetadata jobMetadata = new JobMetadata(jobId, new URL("http://myart" +
                    ""),"111", 1,"njoshi",schedInfo,Lists.newArrayList(),0,10, 0);
            ScheduleRequest scheduleRequest = new ScheduleRequest(
                workerId, 1, jobMetadata,MantisJobDurationType.Perpetual, SchedulingConstraints.of(machineDefinition),0);
            BatchScheduleRequest expectedRequest = new BatchScheduleRequest(Collections.singletonList(scheduleRequest));
            verify(schedulerMock).scheduleWorkers(expectedRequest);


            //assertEquals(jobActor, probe.getLastSender());
        } catch (InvalidJobException  e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

	@Test
	public void testJobSubmitInitalizationFails() {
		final TestKit probe = new TestKit(system);
		String clusterName = "testJobSubmitPersistenceFails";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);


		JobDefinition jobDefn;
		try {
			jobDefn = JobTestHelper.generateJobDefinition(clusterName);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName,1))
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefn)
                    .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SERVER_ERROR, initMsg.responseCode);
			System.out.println(initMsg.message);

			String jobId = clusterName + "-1";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(CLIENT_ERROR, resp.responseCode);
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}


	@Test
	public void testJobSubmitWithMultipleWorkers() {
		final TestKit probe = new TestKit(system);

		String clusterName = "testJobSubmitWithMultipleWorkersCluster";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);
		JobDefinition jobDefn;
		try {
			SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStageWithConstraints(2, new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();

			jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName,2))
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefn)
                    .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));


			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SUCCESS, initMsg.responseCode);

			String jobId = clusterName + "-2";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(SUCCESS, resp.responseCode);
			assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
			int stageNo = 1;
			// send launched event

			WorkerId workerId = new WorkerId(jobId, 0, 1);


			// send heartbeat

			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp2 + " msg " + resp2.message);
			assertEquals(SUCCESS, resp2.responseCode);

			// Only 1 worker has started.
			assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());

			// send launched event

			WorkerId workerId2 = new WorkerId(jobId, 1, 2);
			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId2);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp3 + " msg " + resp3.message);
			assertEquals(SUCCESS, resp3.responseCode);

			// 2 worker have started so job should be started.
			assertEquals(JobState.Launched,resp3.getJobMetadata().get().getState());

			verify(jobStoreMock, times(1)).storeNewJob(any());

			verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

			verify(jobStoreMock, times(6)).updateWorker(any());

			verify(jobStoreMock, times(3)).updateJob(any());

			//assertEquals(jobActor, probe.getLastSender());
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}


	@Test
	public void testJobSubmitWithMultipleStagesAndWorkers() {
		final TestKit probe = new TestKit(system);

		String clusterName = "testJobSubmitWithMultipleStagesAndWorkers";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);
		JobDefinition jobDefn;
		try {


            Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
            smap.put(StageScalingPolicy.ScalingReason.Memory, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, 0.1, 0.6, null));
            SchedulingInfo.Builder builder = new SchedulingInfo.Builder()
                    .numberOfStages(2)
                    .multiWorkerScalableStageWithConstraints(
                            2,
                            new MachineDefinition(1, 1.24, 0.0, 1, 1),
                            null, null,
                            new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap, true)
                    )
                    .multiWorkerScalableStageWithConstraints(
                            3,
                            new MachineDefinition(1, 1.24, 0.0, 1, 1),
                            null, null,
                            new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap, true)
                    );

            SchedulingInfo sInfo = builder.build();

            System.out.println("SchedulingInfo " + sInfo);

			jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
					.withJobId(new JobId(clusterName,1))
					.withSubmittedAt(Instant.now())
					.withJobState(JobState.Accepted)

					.withNextWorkerNumToUse(1)
					.withJobDefinition(jobDefn)
					.build();
			final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));


			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SUCCESS, initMsg.responseCode);

			String jobId = clusterName + "-1";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(SUCCESS, resp.responseCode);
			assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
			int stageNo = 0;
			// send launched event

			WorkerId workerId = new WorkerId(jobId, 0, 1);


			// send heartbeat

			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp2 + " msg " + resp2.message);
			assertEquals(SUCCESS, resp2.responseCode);

			// Only 1 worker has started.
			assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());


			// send launched events for the rest of the workers
            int nextWorkerNumber = 1;
            int stage = 0;
            Iterator<Map.Entry<Integer, StageSchedulingInfo>> it = sInfo.getStages().entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<Integer, StageSchedulingInfo> integerStageSchedulingInfoEntry = it.next();

                StageSchedulingInfo stageSchedulingInfo = integerStageSchedulingInfoEntry.getValue();
                System.out.println("Workers -> " + stageSchedulingInfo.getNumberOfInstances() + " in stage " + stage);
                for(int i=0; i<stageSchedulingInfo.getNumberOfInstances(); i++) {
                    WorkerId wId = new WorkerId(jobId, i, nextWorkerNumber++);
                    System.out.println("Sending events for worker --> " + wId + " Stage " + stage);
                    JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stage, wId);
                }
                stage++;


            }



			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp3 + " msg " + resp3.message);
			assertEquals(SUCCESS, resp3.responseCode);

			// 2 worker have started so job should be started.
			assertEquals(JobState.Launched,resp3.getJobMetadata().get().getState());

			verify(jobStoreMock, times(1)).storeNewJob(any());

			verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

			verify(jobStoreMock, times(19)).updateWorker(any());

			verify(jobStoreMock, times(3)).updateJob(any());

			//assertEquals(jobActor, probe.getLastSender());
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}


	@Test
	public void testListActiveWorkers() {
		final TestKit probe = new TestKit(system);

		String clusterName = "testListActiveWorkers";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);
		JobDefinition jobDefn;
		try {
			SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStageWithConstraints(2, new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();

			jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
					.withJobId(new JobId(clusterName,2))
					.withSubmittedAt(Instant.now())
					.withJobState(JobState.Accepted)

					.withNextWorkerNumToUse(1)
					.withJobDefinition(jobDefn)
					.build();
			final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));


			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SUCCESS, initMsg.responseCode);

			String jobId = clusterName + "-2";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(SUCCESS, resp.responseCode);
			assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
			int stageNo = 1;
			// send launched event

			WorkerId workerId = new WorkerId(jobId, 0, 1);


			// send heartbeat

			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp2 + " msg " + resp2.message);
			assertEquals(SUCCESS, resp2.responseCode);

			// Only 1 worker has started.
			assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());

			// send launched event

			WorkerId workerId2 = new WorkerId(jobId, 1, 2);
			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId2);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp3 + " msg " + resp3.message);
			assertEquals(SUCCESS, resp3.responseCode);

			// 2 worker have started so job should be started.
			assertEquals(JobState.Launched,resp3.getJobMetadata().get().getState());

			jobActor.tell(new JobClusterManagerProto.ListWorkersRequest(new JobId(clusterName, 1)),probe.getRef());

			JobClusterManagerProto.ListWorkersResponse listWorkersResponse = probe.expectMsgClass(JobClusterManagerProto.ListWorkersResponse.class);
			assertEquals(2, listWorkersResponse.getWorkerMetadata().size());

			int cnt = 0;
			for(IMantisWorkerMetadata workerMeta : listWorkersResponse.getWorkerMetadata()) {
			    if(workerMeta.getWorkerNumber() == 1 || workerMeta.getWorkerNumber() == 2) {
			        cnt ++;
                }
            }

            assertEquals(2, cnt);

			verify(jobStoreMock, times(1)).storeNewJob(any());

			verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

			verify(jobStoreMock, times(6)).updateWorker(any());

			verify(jobStoreMock, times(3)).updateJob(any());

			//assertEquals(jobActor, probe.getLastSender());
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}



	@Test
	public void testkill() throws Exception {
		final TestKit probe = new TestKit(system);

		String clusterName = "testKillCluster";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

		JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName);

		MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName,3))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)

                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();
        final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));


		jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
		probe.expectMsgClass(JobProto.JobInitialized.class);
		probe.watch(jobActor);
        JobId jId = new JobId(clusterName,3);
		jobActor.tell(new JobClusterProto.KillJobRequest(
			jId, "test reason", JobCompletedReason.Normal, "nj", probe.getRef()), probe.getRef());
		probe.expectMsgClass(JobClusterProto.KillJobResponse.class);
		JobTestHelper.sendWorkerTerminatedEvent(probe,jobActor,jId.getId(),new WorkerId(jId.getId(),0,1));
		Thread.sleep(1000);
		verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(any(), any());
		verify(schedulerMock, times(1)).scheduleWorkers(any());
		verify(jobStoreMock, times(1)).storeNewJob(any());
		verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());
		verify(jobStoreMock, times(2)).updateJob(any());
		//verify(jobStoreMock, times(1)).updateWorker(any());
		jobActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
		probe.expectTerminated(jobActor);
	}

    @Test
    public void testNoHeartBeatAfterLaunchResubmit() {
        final TestKit probe = new TestKit(system);
        String clusterName= "testHeartBeatMissingResubmit";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        JobDefinition jobDefn;
        try {
            SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStageWithConstraints(2, new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();

            jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);

            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName,2))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)

                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);
            String jobId = clusterName + "-2";
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);
            assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
            int stageNo = 1;

            WorkerId workerId = new WorkerId(jobId, 0, 1);
            // check job status again
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp2.responseCode);

            // No worker has started.
            assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());
            WorkerId workerId2 = new WorkerId(jobId, 1, 2);

            // check job status again
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp3.responseCode);

            // 2 worker have started so job should be started.
            assertEquals(JobState.Accepted, resp3.getJobMetadata().get().getState());

            Instant now = Instant.now();
            jobActor.tell(new JobProto.CheckHeartBeat(now.plusSeconds(240)), probe.getRef());
            Thread.sleep(1000);

            // 1 original submissions and 0 resubmits because of worker not in launched state with HB timeouts
            verify(schedulerMock, times(1)).scheduleWorkers(any());
            // 1 kills due to resubmits
            verify(schedulerMock, times(0)).unscheduleAndTerminateWorker(any(), any());

            // launch worker but no HB yet
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, stageNo);

            // check hb status in the future where we expect all last HBs to be stale.
            now = Instant.now();
            jobActor.tell(new JobProto.CheckHeartBeat(now.plusSeconds(240)), probe.getRef());
            Thread.sleep(1000);

            // job status remain as accepted
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp4 = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp4.responseCode);
            assertEquals(JobState.Accepted, resp4.getJobMetadata().get().getState());

            // 1 original submissions and 0 resubmits because of worker not in launched state with HB timeouts
            verify(schedulerMock, times(1)).scheduleWorkers(any());
            // 1 kills due to resubmits
            verify(schedulerMock, times(0)).unscheduleAndTerminateWorker(eq(workerId2), any());
        } catch (Exception e) {
            fail("unexpected exception " + e.getMessage());
        }
    }

    @Test
    public void testHeartBeatPendingSchedulingNoResubmit() {
        final TestKit probe = new TestKit(system);
        String clusterName= "testHeartBeatMissingResubmit";
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

        JobDefinition jobDefn;
        try {
            SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStageWithConstraints(2, new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();

            jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(new JobId(clusterName,2))
                .withSubmittedAt(Instant.now())
                .withJobState(JobState.Accepted)

                .withNextWorkerNumToUse(1)
                .withJobDefinition(jobDefn)
                .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);
            String jobId = clusterName + "-2";
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp.responseCode);
            assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
            int stageNo = 1;

            WorkerId workerId = new WorkerId(jobId, 0, 1);
            // check job status again
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp2.responseCode);

            // No worker has started.
            assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());
            WorkerId workerId2 = new WorkerId(jobId, 1, 2);

            // check job status again
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp3.responseCode);

            // 2 worker have started so job should be started.
            assertEquals(JobState.Accepted, resp3.getJobMetadata().get().getState());

            // trigger HB check far into the future where no retry on scheduling is expected because the worker has not
            // switched into launched state yet.
            Instant now = Instant.now();
            jobActor.tell(new JobProto.CheckHeartBeat(now.plusSeconds(99999)), probe.getRef());

            Thread.sleep(1000);

            // 1 original submissions and 0 resubmits because of worker not in launched state with HB timeouts
            verify(schedulerMock, times(1)).scheduleWorkers(any());
            // 0 kills due to resubmits
            verify(schedulerMock, times(0)).unscheduleAndTerminateWorker(any(), any());
        } catch (Exception e) {
            fail("unexpected exception " + e.getMessage());
        }
    }

	@Test
	public void testHeartBeatEnforcement() {
		final TestKit probe = new TestKit(system);
		String clusterName= "testHeartBeatEnforcementCluster";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);

		JobDefinition jobDefn;
		try {
			SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStageWithConstraints(2, new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();

			jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			MantisJobStore jobStoreMock = mock(MantisJobStore.class);
			MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
	                .withJobId(new JobId(clusterName,2))
	                .withSubmittedAt(Instant.now())
	                .withJobState(JobState.Accepted)

	                .withNextWorkerNumToUse(1)
	                .withJobDefinition(jobDefn)
	                .build();
	        final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, costsCalculator));

			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SUCCESS, initMsg.responseCode);
			String jobId = clusterName + "-2";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(SUCCESS, resp.responseCode);
			assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
			int stageNo = 1;

			WorkerId workerId = new WorkerId(jobId, 0, 1);

			// send Launched, Initiated and heartbeat
			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp2 + " msg " + resp2.message);
			assertEquals(SUCCESS, resp2.responseCode);

			// Only 1 worker has started.
			assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());

			// send launched event

			WorkerId workerId2 = new WorkerId(jobId, 1, 2);
			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId2);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp3 + " msg " + resp3.message);
			assertEquals(SUCCESS, resp3.responseCode);

			// 2 worker have started so job should be started.
			assertEquals(JobState.Launched,resp3.getJobMetadata().get().getState());

			JobTestHelper.sendHeartBeat(probe,jobActor,jobId,1,workerId2);

            JobTestHelper.sendHeartBeat(probe,jobActor,jobId,1,workerId);

			// check hb status in the future where we expect all last HBs to be stale.
			Instant now = Instant.now();
			jobActor.tell(new JobProto.CheckHeartBeat(now.plusSeconds(240)), probe.getRef());

			Thread.sleep(1000);

			// 2 original submissions and 2 resubmits because of HB timeouts
			verify(schedulerMock, times(3)).scheduleWorkers(any());
			// 2 kills due to resubmits
			verify(schedulerMock, times(2)).unscheduleAndTerminateWorker(any(), any());

			//assertEquals(jobActor, probe.getLastSender());
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}

//


	@Test
	public void testLostWorkerGetsReplaced() {
		final TestKit probe = new TestKit(system);
		String clusterName= "testLostWorkerGetsReplaced";
		IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);
		 ActorRef jobActor = null;
		JobDefinition jobDefn;
		try {
			SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStageWithConstraints(2, new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();

			jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);
			MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMock.schedulerHandlesAllocationRetries()).thenReturn(true);
			//MantisJobStore jobStoreMock = mock(MantisJobStore.class);

			MantisJobStore jobStoreSpied = Mockito.spy(jobStore);

            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
	                .withJobId(new JobId(clusterName,2))
	                .withSubmittedAt(Instant.now())
	                .withJobState(JobState.Accepted)

	                .withNextWorkerNumToUse(1)
	                .withJobDefinition(jobDefn)
	                .build();
	        jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreSpied, schedulerMock, eventPublisher, costsCalculator));



			jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
			assertEquals(SUCCESS, initMsg.responseCode);

			String jobId = clusterName + "-2";
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp + " msg " + resp.message);
			assertEquals(SUCCESS, resp.responseCode);
			assertEquals(JobState.Accepted,resp.getJobMetadata().get().getState());
			int stageNo = 1;
			// send launched event

			WorkerId workerId = new WorkerId(jobId, 0, 1);


			// send heartbeat

			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
			//jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
			GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp2 + " msg " + resp2.message);
			assertEquals(SUCCESS, resp2.responseCode);

			// Only 1 worker has started.
			assertEquals(JobState.Accepted,resp2.getJobMetadata().get().getState());

			// send launched event

			WorkerId workerId2 = new WorkerId(jobId, 1, 2);
			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId2);

			// check job status again
			jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());

			GetJobDetailsResponse resp3 = probe.expectMsgClass(GetJobDetailsResponse.class);
			System.out.println("resp " + resp3 + " msg " + resp3.message);
			assertEquals(SUCCESS, resp3.responseCode);

			// 2 worker have started so job should be started.
			assertEquals(JobState.Launched,resp3.getJobMetadata().get().getState());

			// worker 2 gets terminated abnormally
			JobTestHelper.sendWorkerTerminatedEvent(probe, jobActor, jobId, workerId2);

			// replaced worker comes up and sends events
			WorkerId workerId2_replaced = new WorkerId(jobId, 1, 3);
			JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId2_replaced);

            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());

            GetJobDetailsResponse resp4 = probe.expectMsgClass(GetJobDetailsResponse.class);

            IMantisJobMetadata jobMeta = resp4.getJobMetadata().get();
            Map<Integer, ? extends IMantisStageMetadata> stageMetadata = jobMeta.getStageMetadata();
            IMantisStageMetadata stage = stageMetadata.get(1);
            for (JobWorker worker : stage.getAllWorkers()) {
                System.out.println("worker -> " + worker.getMetadata());
            }

            // 2 initial schedules and 1 replacement
			verify(schedulerMock, timeout(1_000).times(2)).scheduleWorkers(any());

			// archive worker should get called once for the dead worker
		//	verify(jobStoreMock, timeout(1_000).times(1)).archiveWorker(any());
			Mockito.verify(jobStoreSpied).archiveWorker(any());


			//assertEquals(jobActor, probe.getLastSender());
		} catch (InvalidJobException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} finally {
		    system.stop(jobActor);
		}

	}


	@Test
	public void workerNumberGeneratorInvalidArgsTest() {
		try {
			WorkerNumberGenerator wng = new WorkerNumberGenerator(-1, 10);
			fail();
		} catch(Exception e) {

		}

		try {
			WorkerNumberGenerator wng = new WorkerNumberGenerator(0, 0);
			fail();
		} catch(Exception e) {

		}
	}

	@Test
	public void workerNumberGeneratorTest() {

		MantisJobMetadataImpl mantisJobMetaMock = mock(MantisJobMetadataImpl.class);
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		int incrementStep = 10;
		WorkerNumberGenerator wng = new WorkerNumberGenerator(0, incrementStep);
		for(int i=1; i<incrementStep; i++) {
			assertEquals(i, wng.getNextWorkerNumber(mantisJobMetaMock, jobStoreMock));
		}
		try {
			verify(mantisJobMetaMock,times(1)).setNextWorkerNumberToUse(incrementStep, jobStoreMock);
		//	verify(jobStoreMock, times(1)).updateJob(any());
		} catch ( Exception e) {

			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void workerNumberGeneratorWithNonZeroLastUsedTest() {

		MantisJobMetadataImpl mantisJobMetaMock = mock(MantisJobMetadataImpl.class);
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		int incrementStep = 10;
		int lastNumber = 7;
		WorkerNumberGenerator wng = new WorkerNumberGenerator(lastNumber, incrementStep);
		for(int i=lastNumber+1; i<incrementStep; i++) {
			assertEquals(i, wng.getNextWorkerNumber(mantisJobMetaMock, jobStoreMock));
		}
		try {
			verify(mantisJobMetaMock,times(1)).setNextWorkerNumberToUse(lastNumber + incrementStep, jobStoreMock);
			//verify(jobStoreMock,times(1)).updateJob(any());
		} catch (Exception e) {

			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void workerNumberGeneratorTest2() {

		MantisJobMetadataImpl mantisJobMetaMock = mock(MantisJobMetadataImpl.class);
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		WorkerNumberGenerator wng = new WorkerNumberGenerator();
		for(int i=1; i<20; i++) {
			assertEquals(i, wng.getNextWorkerNumber(mantisJobMetaMock, jobStoreMock));
		}


		try {
			InOrder inOrder = Mockito.inOrder(mantisJobMetaMock);
			inOrder.verify(mantisJobMetaMock).setNextWorkerNumberToUse(10, jobStoreMock);
			inOrder.verify(mantisJobMetaMock).setNextWorkerNumberToUse(20, jobStoreMock);
			//verify(jobStoreMock, times(2)).updateJob(any());
		} catch (Exception e) {

			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void workerNumberGeneratorUpdatesStoreTest2() {

		//MantisJobMetadataImpl mantisJobMetaMock = mock(MantisJobMetadataImpl.class);
		JobDefinition jobDefnMock = mock(JobDefinition.class);
		MantisJobMetadataImpl mantisJobMeta = new MantisJobMetadataImpl(JobId.fromId("job-1").get(),
				Instant.now().toEpochMilli(),Instant.now().toEpochMilli(), jobDefnMock, JobState.Accepted,
				0, 20, 20);

		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		WorkerNumberGenerator wng = new WorkerNumberGenerator();
		for(int i=1; i<20; i++) {
			assertEquals(i, wng.getNextWorkerNumber(mantisJobMeta, jobStoreMock));
		}


		try {
			//InOrder inOrder = Mockito.inOrder(mantisJobMetaMock);
			//inOrder.verify(mantisJobMetaMock).setNextWorkerNumberToUse(10, jobStoreMock);
			//inOrder.verify(mantisJobMetaMock).setNextWorkerNumberToUse(20, jobStoreMock);
			verify(jobStoreMock, times(2)).updateJob(any());
		} catch (Exception e) {

			e.printStackTrace();
			fail();
		}
	}


	@Test
	public void workerNumberGeneratorExceptionUpdatingJobTest() {
		MantisJobMetadataImpl mantisJobMetaMock = mock(MantisJobMetadataImpl.class);
		MantisJobStore jobStoreMock = mock(MantisJobStore.class);
		WorkerNumberGenerator wng = new WorkerNumberGenerator();
		try {
			Mockito.doThrow(IOException.class).when(jobStoreMock).updateJob(any());
			wng.getNextWorkerNumber(mantisJobMetaMock, jobStoreMock);
		} catch(Exception e) {
			e.printStackTrace();
		}



	}



}
