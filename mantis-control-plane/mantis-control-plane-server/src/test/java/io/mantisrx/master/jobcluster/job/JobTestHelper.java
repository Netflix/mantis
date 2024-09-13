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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Label;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.LabelManager.SystemLabels;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.master.jobcluster.job.worker.WorkerTerminate;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.Status.TYPE;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.Test;


public class JobTestHelper {

    private final static String SPOOL_DIR = "/tmp/MantisSpool";
    private final static String ARCHIVE_DIR = "/tmp/MantisArchive";

    public static void createDirsIfRequired() {
        File spoolDir = new File(SPOOL_DIR);
        File namedJobsDir = new File(SPOOL_DIR + "/" + "namedJobs");
        File archiveDir = new File(ARCHIVE_DIR);
        if (!spoolDir.exists()) {
            spoolDir.mkdir();
        }

        if (!archiveDir.exists()) {
            archiveDir.mkdir();
        }

        if (!namedJobsDir.exists()) {
            namedJobsDir.mkdir();
        }
    }

    public static void deleteAllFiles() {
        try {
            File spoolDir = new File(SPOOL_DIR);
            File archiveDir = new File(ARCHIVE_DIR);
            deleteDir(spoolDir);
            deleteDir(archiveDir);
        } catch (Exception e) {

        }
    }

    private static void deleteDir(File dir) {
        if (dir != null) {
            for (File file : dir.listFiles()) {
                if (file.isDirectory()) {
                    deleteDir(file);
                } else {

                    boolean delete = file.delete();

                }
            }
        }
    }

    public static IJobClusterDefinition generateJobClusterDefinition(String name, SchedulingInfo schedInfo) {
        return generateJobClusterDefinition(name, schedInfo, WorkerMigrationConfig.DEFAULT);
    }

    public static IJobClusterDefinition generateJobClusterDefinition(String name, SchedulingInfo schedInfo, WorkerMigrationConfig migrationConfig) {
        String artifactName = "myart";
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
            .withJobJarUrl("http://" + artifactName)
            .withArtifactName(artifactName)
            .withSchedulingInfo(schedInfo)
            .withVersion("0.0.1")
            .build();
        return new JobClusterDefinitionImpl.Builder()
            .withJobClusterConfig(clusterConfig)
            .withName(name)
            .withUser("user")
            .withParameters(Lists.newArrayList())
            .withIsReadyForJobMaster(true)
            .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
            .withMigrationConfig(migrationConfig)
            .withLabel(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"))
            .build();
    }

    public static IJobClusterDefinition generateJobClusterDefinition(String name) {
        return generateJobClusterDefinition(name, new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(new MachineDefinition(0, 0, 0, 0, 0), Lists.newArrayList(), Lists.newArrayList()).build());
    }

    public static JobDefinition generateJobDefinition(String clusterName, SchedulingInfo schedInfo) throws InvalidJobException {
        return new JobDefinition.Builder()
            .withName(clusterName)
            .withParameters(Lists.newArrayList())
            .withLabels(Lists.newArrayList())
            .withSchedulingInfo(schedInfo)
            .withJobJarUrl("http://myart")
            .withArtifactName("myart")
            .withSubscriptionTimeoutSecs(0)
            .withUser("njoshi")
            .withNumberOfStages(schedInfo.getStages().size())
            .withJobSla(new JobSla(0, 0, null, MantisJobDurationType.Perpetual, null))
            .build();
    }

    public static JobDefinition generateJobDefinition(String clusterName) throws InvalidJobException {
        return generateJobDefinition(clusterName, new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(new MachineDefinition(1.0, 1.0, 1.0, 1.0, 3), Lists.newArrayList(), Lists.newArrayList()).build());
    }

    public static void sendCheckHeartBeat(final TestKit probe, final ActorRef jobActor, Instant now) {
        jobActor.tell(new JobProto.CheckHeartBeat(now), probe.getRef());
    }

    public static void sendHeartBeat(final TestKit probe, final ActorRef jobActor, String jobId, int stageNo, WorkerId workerId2) {
        sendHeartBeat(probe, jobActor, jobId, stageNo, workerId2, System.currentTimeMillis());

    }

    public static void sendLaunchedInitiatedStartedEventsToWorker(final TestKit probe, final ActorRef jobActor, String jobId,
                                                                  int stageNo, WorkerId workerId2) {
        JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId2, stageNo);

        JobTestHelper.sendStartInitiatedEvent(probe, jobActor, stageNo, workerId2);

        // send started
        JobTestHelper.sendStartedEvent(probe, jobActor, stageNo, workerId2);

    }

    //    public static void sendLaunchedInitiatedStartedEventsToWorker(final TestKit probe, final ActorRef jobActor, String jobId,
    //            int stageNo, WorkerId workerId2) {
    //        sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, stageNo, workerId2, System.currentTimeMillis() + 1000);
    //    }


    public static void sendHeartBeat(final TestKit probe, final ActorRef jobActor, String jobId, int stageNo, WorkerId workerId2, long time) {
        WorkerEvent heartBeat2 = new WorkerHeartbeat(new Status(jobId, stageNo, workerId2.getWorkerIndex(), workerId2.getWorkerNum(), TYPE.HEARTBEAT, "", MantisJobState.Started, time));
        jobActor.tell(heartBeat2, probe.getRef());
    }

    public static void sendWorkerTerminatedEvent(final TestKit probe, final ActorRef jobActor, String jobId, WorkerId workerId2) {
        WorkerEvent workerTerminated = new WorkerTerminate(workerId2, WorkerState.Failed, JobCompletedReason.Lost);
        jobActor.tell(workerTerminated, probe.getRef());
    }

    public static void sendWorkerCompletedEvent(final TestKit probe, final ActorRef jobActor, String jobId, WorkerId workerId2) {
        WorkerEvent workerCompleted = new WorkerTerminate(workerId2, WorkerState.Completed, JobCompletedReason.Normal);
        jobActor.tell(workerCompleted, probe.getRef());
    }

    public static void sendStartInitiatedEvent(final TestKit probe, final ActorRef jobActor, final int stageNum, WorkerId workerId) {
        WorkerEvent startInitEvent = new WorkerStatus(new Status(
            workerId.getJobId(),
            stageNum,
            workerId.getWorkerIndex(),
            workerId.getWorkerNum(),
            TYPE.INFO,
            "test START_INITIATED event",
            MantisJobState.StartInitiated
        ));
        jobActor.tell(startInitEvent, probe.getRef());
    }

    public static void sendStartedEvent(final TestKit probe, final ActorRef jobActor, final int stageNum, WorkerId workerId) {
        WorkerEvent startedEvent = new WorkerStatus(new Status(
            workerId.getJobId(),
            stageNum,
            workerId.getWorkerIndex(),
            workerId.getWorkerNum(),
            TYPE.INFO,
            "test STARTED event",
            MantisJobState.Started
        ));
        jobActor.tell(startedEvent, probe.getRef());
    }

    public static void sendJobInitializeEvent(final TestKit probe, final ActorRef jobClusterActor) {
        JobProto.InitJob initJobEvent = new JobProto.InitJob(probe.getRef(), true);
        jobClusterActor.tell(initJobEvent, probe.getRef());
    }

    public static void sendWorkerLaunchedEvent(final TestKit probe, final ActorRef jobActor, WorkerId workerId2, int stageNo) {
        WorkerEvent launchedEvent2 = new WorkerLaunched(workerId2, stageNo, "host1", "vm1", Optional.empty(), Optional.empty(), new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));
        jobActor.tell(launchedEvent2, probe.getRef());
    }

    public static void killJobAndVerify(final TestKit probe, String clusterName, JobId jobId, ActorRef jobClusterActor) {
        jobClusterActor.tell(new JobClusterProto.KillJobRequest(jobId, "test reason", JobCompletedReason.Normal, "nj", probe.getRef()), probe.getRef());
        JobClusterManagerProto.KillJobResponse killJobResp = probe.expectMsgClass(JobClusterManagerProto.KillJobResponse.class);
        assertEquals(SUCCESS, killJobResp.responseCode);
    }

    public static void killJobSendWorkerTerminatedAndVerify(final TestKit probe, String clusterName, JobId jobId, ActorRef jobClusterActor, WorkerId workerId) {
        jobClusterActor.tell(new JobClusterProto.KillJobRequest(jobId, "test reason", JobCompletedReason.Normal, "nj", probe.getRef()), probe.getRef());
        JobClusterManagerProto.KillJobResponse killJobResp = probe.expectMsgClass(JobClusterManagerProto.KillJobResponse.class);

        sendWorkerTerminatedEvent(probe, jobClusterActor, jobId.getId(), workerId);
        assertEquals(SUCCESS, killJobResp.responseCode);
    }

    public static void getJobDetailsAndVerify(final TestKit probe, ActorRef jobClusterActor, String jobId, BaseResponse.ResponseCode expectedRespCode, JobState expectedState) {
        jobClusterActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
        JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(Duration.ofSeconds(60), JobClusterManagerProto.GetJobDetailsResponse.class);

        if (expectedRespCode == SUCCESS) {
            assertEquals(SUCCESS, detailsResp.responseCode);
            assertTrue(detailsResp.getJobMetadata().isPresent());
            assertEquals(jobId, detailsResp.getJobMetadata().get().getJobId().getId());
            assertEquals(expectedState, detailsResp.getJobMetadata().get().getState());
        } else {
            assertEquals(expectedRespCode, detailsResp.responseCode);
            assertFalse(detailsResp.getJobMetadata().isPresent());
        }
    }

    public static boolean verifyJobStatusWithPolling(final TestKit probe, final ActorRef actorRef, final String jobId1, final JobState expectedState) {
        boolean result = false;
        int cnt = 0;
        // try a few times for timing issue
        while (cnt < 100 || !result) {
            cnt++;
            actorRef.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(jobId1).get()), probe.getRef());
            JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(Duration.ofSeconds(2), JobClusterManagerProto.GetJobDetailsResponse.class);
            if (detailsResp.getJobMetadata().isPresent() && expectedState.equals(detailsResp.getJobMetadata().get().getState())) {
                result = true;
                break;
            }
        }
        return result;
    }

    public static void submitJobAndVerifySuccess(final TestKit probe, String clusterName, ActorRef jobClusterActor, final JobDefinition jobDefn,
                                                 String jobId) {
        submitJobAndVerifyStatus(probe, clusterName, jobClusterActor, jobDefn, jobId, SUCCESS);
    }

    public static void submitJobAndVerifyStatus(final TestKit probe, String clusterName, ActorRef jobClusterActor, @Nullable final JobDefinition jobDefn,
                                                String jobId, ResponseCode code) {
        final SubmitJobRequest request;
        if (jobDefn == null) {
            request = new SubmitJobRequest(clusterName, "user");
        } else {
            request = new JobClusterManagerProto.SubmitJobRequest(clusterName, "user", jobDefn);
        }
        jobClusterActor.tell(request, probe.getRef());
        JobClusterManagerProto.SubmitJobResponse submitResponse = probe.expectMsgClass(JobClusterManagerProto.SubmitJobResponse.class);
        assertEquals(code, submitResponse.responseCode);
        if (jobId == null) {
            assertTrue(!submitResponse.getJobId().isPresent());
        } else {
            assertEquals(jobId, submitResponse.getJobId().get().getId());
        }
    }

    public static void scaleStageAndVerify(final TestKit probe, ActorRef jobClusterActor,
                                           String jobId, int stageNum, int numberOfWorkers) {
        jobClusterActor.tell(new JobClusterManagerProto.ScaleStageRequest(jobId, stageNum, numberOfWorkers,"user", "testScale"), probe.getRef());
        JobClusterManagerProto.ScaleStageResponse scaleResponse = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
        assertEquals(SUCCESS, scaleResponse.responseCode);
        assertEquals(numberOfWorkers, scaleResponse.getActualNumWorkers());
    }

    public static ActorRef submitSingleStageScalableJob(ActorSystem system, TestKit probe, String clusterName, SchedulingInfo sInfo,
                                                        MantisScheduler schedulerMock, MantisJobStore jobStoreMock,
                                                        LifecycleEventPublisher lifecycleEventPublisher) throws io.mantisrx.runtime.command.InvalidJobException {

        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName, sInfo);
        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(clusterName, sInfo);

        MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
            .withJobId(new JobId(clusterName, 1))
            .withSubmittedAt(Instant.now())
            .withJobState(JobState.Accepted)

            .withNextWorkerNumToUse(1)
            .withJobDefinition(jobDefn)
            .build();
        final ActorRef jobActor = system.actorOf(
            JobActor.props(jobClusterDefn, mantisJobMetaData, jobStoreMock, schedulerMock,
                lifecycleEventPublisher, CostsCalculator.noop()));


        jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobProto.JobInitialized initMsg = probe.expectMsgClass(JobProto.JobInitialized.class);
        assertEquals(SUCCESS, initMsg.responseCode);

        String jobId = clusterName + "-1";
        jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
        //jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobClusterManagerProto.GetJobDetailsResponse resp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
        System.out.println("resp " + resp + " msg " + resp.message);
        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(JobState.Accepted, resp.getJobMetadata().get().getState());
        int stageNo = 1;
        // send launched event
        int lastWorkerNum = 0;
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobActor, jobId, 0, new WorkerId(jobId, 0, ++lastWorkerNum));

        //JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobActor,jobId,1,new WorkerId(jobId,0,2));

        for (int i = 0; i < sInfo.forStage(stageNo).getNumberOfInstances(); i++) {

            WorkerId workerId = new WorkerId(jobId, i, ++lastWorkerNum);
            JobTestHelper.sendWorkerLaunchedEvent(probe, jobActor, workerId, stageNo);

            // start initiated event
            JobTestHelper.sendStartInitiatedEvent(probe, jobActor, stageNo, workerId);

            // send heartbeat

            JobTestHelper.sendHeartBeat(probe, jobActor, jobId, stageNo, workerId, System.currentTimeMillis() + 1000);
        }


        // check job status again
        jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
        //jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
        JobClusterManagerProto.GetJobDetailsResponse resp2 = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
        System.out.println("resp " + resp2 + " msg " + resp2.message);
        assertEquals(SUCCESS, resp2.responseCode);

        //  1 worker has started. so job has started
        assertEquals(JobState.Launched, resp2.getJobMetadata().get().getState());

        return jobActor;
    }

    @Test
    public void testCalculateRuntimeLimitForAlreadyStartedJob() {
        Instant now = Instant.now();

        Instant startedAt = now.minusSeconds(5);

        assertEquals(5, JobHelper.calculateRuntimeDuration(10, startedAt));

    }

    @Test
    public void testCalculateRuntimeLimitForJustStartedJob() {
        Instant now = Instant.now();

        Instant startedAt = now;

        assertEquals(10, JobHelper.calculateRuntimeDuration(10, startedAt));

    }

    @Test
    public void testCalculateRuntimeLimitForAlreadyExpiredJob() {
        Instant now = Instant.now();

        Instant startedAt = now.minusSeconds(15);

        assertEquals(1, JobHelper.calculateRuntimeDuration(10, startedAt));

    }
}
