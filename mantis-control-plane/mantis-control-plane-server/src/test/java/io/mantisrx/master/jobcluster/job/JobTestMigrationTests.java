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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.WorkerMigrationConfig.MigrationStrategyEnum;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.BatchScheduleRequest;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;



public class JobTestMigrationTests {

    static ActorSystem system;


    private static final String user = "mantis";
    final LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());


    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();

        TestHelpers.setupMasterConfig();

    }

    @AfterClass
    public static void tearDown() {
        //((SimpleCachedFileStorageProvider)storageProvider).deleteAllFiles();
        TestKit.shutdownActorSystem(system);
        system = null;
    }


    @Test
    public void testWorkerMigration() {

        String clusterName= "testWorkerMigration";
        TestKit probe = new TestKit(system);
        SchedulingInfo sInfo = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(new MachineDefinition(1.0,1.0,1.0,3), Lists.newArrayList(), Lists.newArrayList()).build();
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo, new WorkerMigrationConfig(MigrationStrategyEnum.ONE_WORKER, "{}"));

        CountDownLatch scheduleCDL = new CountDownLatch(2);
        CountDownLatch unscheduleCDL = new CountDownLatch(1);
        JobDefinition jobDefn;
        try {
            jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

            MantisScheduler schedulerMock = new DummyScheduler(scheduleCDL, unscheduleCDL); //mock(MantisScheduler.class); //
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName,2))
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefn)
                    .build();
            final ActorRef jobActor = system.actorOf(JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock, eventPublisher, CostsCalculator.noop()));

            jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
            assertEquals(SUCCESS, initMsg.responseCode);
            String jobId = clusterName + "-2";
            int stageNo = 1;

            WorkerId workerId = new WorkerId(jobId, 0, 1);

            // send Launched, Initiated and heartbeat
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId);


            // check job status again
            jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId), probe.getRef());
            JobClusterManagerProto.GetJobDetailsResponse resp3 = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
            assertEquals(SUCCESS, resp3.responseCode);

            //  worker has started so job should be started.
            assertEquals(JobState.Launched,resp3.getJobMetadata().get().getState());

            // Send migrate worker message

            jobActor.tell(new WorkerOnDisabledVM(workerId), probe.getRef());


            // Trigger check hb status and that should start the migration. And migrate first worker
            Instant now = Instant.now();
            jobActor.tell(new JobProto.CheckHeartBeat(), probe.getRef());

            // send HB for the migrated worker
            WorkerId migratedWorkerId1 = new WorkerId(jobId, 0, 2);
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, migratedWorkerId1);

            // Trigger another check should be noop
          //  jobActor.tell(new JobProto.CheckHeartBeat(now.plusSeconds(120)), probe.getRef());
            scheduleCDL.await(1, TimeUnit.SECONDS);
            unscheduleCDL.await(1, TimeUnit.SECONDS);


//            // 1 original submissions and 1 resubmit because of migration


//            when(schedulerMock.scheduleWorker(any())).
//            verify(schedulerMock, times(2)).scheduleWorker(any());
////            // 1 kill due to resubmits
//           verify(schedulerMock, times(1)).unscheduleWorker(any(), any());
//
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

    class DummyScheduler implements MantisScheduler {

        CountDownLatch schedL;
        CountDownLatch unschedL;
        public DummyScheduler(CountDownLatch scheduleCDL, CountDownLatch unscheduleCDL) {
            schedL = scheduleCDL;
            unschedL = unscheduleCDL;
        }

        @Override
        public void scheduleWorkers(BatchScheduleRequest scheduleRequest) {
            // TODO:
        }

        @Override
        public void unscheduleJob(String jobId) {
            // TODO:
        }

        @Override
        public void unscheduleWorker(WorkerId workerId, Optional<String> hostname) {
            // TODO Auto-generated method stub
            unschedL.countDown();
        }

        @Override
        public void unscheduleAndTerminateWorker(WorkerId workerId, Optional<String> hostname) {
            // TODO Auto-generated method stub

        }

        @Override
        public void updateWorkerSchedulingReadyTime(WorkerId workerId, long when) {
            // TODO Auto-generated method stub

        }

        @Override
        public void initializeRunningWorker(ScheduleRequest scheduleRequest, String hostname, String hostID) {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean schedulerHandlesAllocationRetries(){
            return false;
        }
    }

    public static void main(String[] args) {

    }

}
