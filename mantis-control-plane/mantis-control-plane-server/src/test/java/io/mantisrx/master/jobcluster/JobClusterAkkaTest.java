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

package io.mantisrx.master.jobcluster;

import static io.mantisrx.master.jobcluster.JobClusterActor.JobInfo;
import static io.mantisrx.master.jobcluster.JobClusterActor.props;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobCriteria;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLAResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.common.Label;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerTerminate;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.WorkerMigrationConfig.MigrationStrategyEnum;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.DeploymentStrategy;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageDeploymentStrategy;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.Status.TYPE;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.server.master.store.NamedJob;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

public class JobClusterAkkaTest {
    public static final SLA NO_OP_SLA = new SLA(0, 0, null, null);

    public static final MachineDefinition DEFAULT_MACHINE_DEFINITION = new MachineDefinition(1, 10, 10, 10, 2);
    public static final SchedulingInfo SINGLE_WORKER_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(DEFAULT_MACHINE_DEFINITION, Lists.newArrayList(), Lists.newArrayList()).build();
    public static final SchedulingInfo TWO_WORKER_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStage(2, DEFAULT_MACHINE_DEFINITION).build();
    public static final JobOwner DEFAULT_JOB_OWNER = new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo");
    final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());
    static ActorSystem system;
    //private static TestKit probe;

    private MantisJobStore jobStore;
    private IMantisPersistenceProvider storageProvider;
    private static LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());
    private static final String user = "mantis";
    @Rule
    public TemporaryFolder rootDir = new TemporaryFolder();
    private CostsCalculator costsCalculator;

    @BeforeClass
    public static void setup() {
        Config config = ConfigFactory.parseString("akka {\n" +
            "  loggers = [\"akka.testkit.TestEventListener\"]\n" +
            "  loglevel = \"WARNING\"\n" +
            "  stdout-loglevel = \"WARNING\"\n" +
            "  test.single-expect-default = 1000 millis\n" +
            "}\n");
        system = ActorSystem.create("JobClusterTest", config.withFallback(ConfigFactory.load()));


        JobTestHelper.createDirsIfRequired();
        TestHelpers.setupMasterConfig();
    }

    @AfterClass
    public static void tearDown() {
        JobTestHelper.deleteAllFiles();
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Before
    public void setupStorageProvider() {
        storageProvider = new KeyValueBasedPersistenceProvider(
            new FileBasedStore(rootDir.getRoot()),
            eventPublisher);
        jobStore = new MantisJobStore(storageProvider);
        costsCalculator = CostsCalculator.noop();
    }


    private void deleteFiles(String dirName, final String jobId, final String filePrefix) {
        File spoolDir = new File(dirName);
        if (spoolDir != null) {
            for (File stageFile : spoolDir.listFiles((dir, name) -> {
                return name.startsWith(filePrefix + jobId + "-");
            })) {
                stageFile.delete();
            }
        }
    }

    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName) {
        return createFakeJobClusterDefn(clusterName, Lists.newArrayList());
    }
    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName, List<Label> labels)  {
        return createFakeJobClusterDefn(clusterName, labels, NO_OP_SLA);

    }


    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName, List<Label> labels, SLA sla)  {
        return createFakeJobClusterDefn(clusterName,labels, sla, SINGLE_WORKER_SCHED_INFO);
    }

    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName, List<Label> labels, SLA sla, SchedulingInfo schedulingInfo)  {
        String artifactName = "myart";
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withSchedulingInfo(schedulingInfo)
                .withVersion("0.0.1")
                .build();

        if (labels.stream().noneMatch(l -> l.getName().equals("_mantis.resourceCluster"))) {
            labels.add(new Label("_mantis.resourceCluster", "akkaTestCluster1"));
        }

        return new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(clusterName)
                .withParameters(Lists.newArrayList())
                .withLabels(labels)
                .withUser(user)
                .withIsReadyForJobMaster(true)
                .withOwner(DEFAULT_JOB_OWNER)
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .withSla(sla)
                .build();
    }

    private JobDefinition createJob(String name, List<Label> labelList) throws InvalidJobException {
        return createJob(name, 0, MantisJobDurationType.Perpetual,null, SINGLE_WORKER_SCHED_INFO, labelList, null);
    }

    private JobDefinition createJob(String name2, long subsTimeoutSecs, MantisJobDurationType durationType) throws InvalidJobException {
        return createJob(name2, subsTimeoutSecs, durationType, (String) null);
    }

    private JobDefinition createJob(String name2, MantisJobDurationType durationType, SchedulingInfo schedulingInfo)
            throws InvalidJobException {
        return createJob(name2, 1, durationType, null, schedulingInfo, Lists.newArrayList(), null);
    }

    private JobDefinition createJob(String name2, MantisJobDurationType durationType, SchedulingInfo schedulingInfo,
                                    DeploymentStrategy deploymentStrategy)
            throws InvalidJobException {
        return createJob(name2, 1, durationType, null, schedulingInfo, Lists.newArrayList(), deploymentStrategy);
    }

    private JobDefinition createJob(String name2, MantisJobDurationType durationType, SchedulingInfo schedulingInfo,
                                    String artifactName, String artifactVersion) throws InvalidJobException {
        return createJob(name2, 1, durationType, null, schedulingInfo,
                Lists.newArrayList(), artifactName, artifactVersion, null);
    }

    private JobDefinition createJob(String name2, MantisJobDurationType durationType, SchedulingInfo schedulingInfo,
                                    String artifactName, String artifactVersion, DeploymentStrategy deploymentStrategy) throws InvalidJobException {
        return createJob(name2, 1, durationType, null, schedulingInfo,
                Lists.newArrayList(), artifactName, artifactVersion, deploymentStrategy);
    }

    private JobDefinition createJob(String name2, long subsTimeoutSecs, MantisJobDurationType durationType, String userProvidedType) throws InvalidJobException {

        return createJob(name2, subsTimeoutSecs, durationType, userProvidedType, SINGLE_WORKER_SCHED_INFO, Lists.newArrayList(), null);
    }

    private JobDefinition createJob(String name2, long subsTimeoutSecs, MantisJobDurationType durationType,
                                    String userProvidedType, SchedulingInfo schedulingInfo, List<Label> labelList)
            throws InvalidJobException {
        return createJob(name2, subsTimeoutSecs, durationType, userProvidedType, schedulingInfo, labelList,
                "myart", null, null);
    }

    private JobDefinition createJob(String name2, long subsTimeoutSecs, MantisJobDurationType durationType,
                                    String userProvidedType, SchedulingInfo schedulingInfo, List<Label> labelList,
                                    DeploymentStrategy deploymentStrategy)
            throws InvalidJobException {
        return createJob(name2, subsTimeoutSecs, durationType, userProvidedType, schedulingInfo, labelList,
                "myart", null, deploymentStrategy);
    }

    private JobDefinition createJob(String name2, long subsTimeoutSecs, MantisJobDurationType durationType,
                                    String userProvidedType, SchedulingInfo schedulingInfo, List<Label> labelList,
                                    String artifactName, String artifactVersion, DeploymentStrategy deploymentStrategy)
            throws InvalidJobException {

        return new JobDefinition.Builder()
                .withName(name2)
                .withParameters(Lists.newArrayList())
                .withLabels(labelList)
                .withSchedulingInfo(schedulingInfo)
                .withDeploymentStrategy(deploymentStrategy)
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withVersion(artifactVersion)
                .withSubscriptionTimeoutSecs(subsTimeoutSecs)
                .withUser("njoshi")
                .withJobSla(new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, userProvidedType))

                .build();
    }

    private JobDefinition createJob(String name2) throws InvalidJobException {

        return createJob(name2, 0, MantisJobDurationType.Perpetual, null);
    }

    // CLUSTER CRUD TESTS ///////////////////////////////////////////////////////////////////////////
    @Test
    public void testJobClusterCreate() throws Exception  {

        String name = "testJobClusterCreate";
        TestKit probe = new TestKit(system);
        MantisSchedulerFactory schedulerMock = mock(MantisSchedulerFactory.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(name);
        ActorRef jobClusterActor = system.actorOf(props(name, jobStoreMock, schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        jobClusterActor.tell(new GetJobClusterRequest(name), probe.getRef());
        GetJobClusterResponse resp2 = probe.expectMsgClass(GetJobClusterResponse.class);
        System.out.println("resp2 " + resp2);
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(name, resp2.getJobCluster().get().getName());
        assertEquals("Nick", resp2.getJobCluster().get().getOwner().getName());
        assertEquals(1, resp2.getJobCluster().get().getLabels().size());
        assertEquals(1,resp2.getJobCluster().get().getJars().size());

        jobClusterActor.tell(new JobClusterProto.DeleteJobClusterRequest(user, name, probe.getRef()), probe.getRef());
        JobClusterProto.DeleteJobClusterResponse resp3 = probe.expectMsgClass(JobClusterProto.DeleteJobClusterResponse.class);
        assertEquals(SUCCESS, resp3.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        //verify(jobStoreMock, times(1)).storeNewJob(any());
        verify(jobStoreMock, times(1)).createJobCluster(any());

        verify(jobStoreMock, times(1)).deleteJobCluster(name);

        probe.getSystem().stop(jobClusterActor);
    }


    @Test
    public void testJobClusterEnable() {
        try {
            TestKit probe = new TestKit(system);
            String clusterName = "testJobClusterEnable";
            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            String jobId = clusterName + "-1";
            JobDefinition jobDefn = createJob(clusterName);
            IMantisJobMetadata job1 = new MantisJobMetadataImpl.Builder()
                    .withJobDefinition(jobDefn)
                    .withJobState(JobState.Completed)
                    .withJobId(new JobId(clusterName,1))
                    .withNextWorkerNumToUse(2)
                    .withSubmittedAt(1000)
                    .build();
            when(jobStoreMock.getArchivedJob(jobId)).thenReturn(of(job1));
            SLA sla = new SLA(1,1,null,null);
            final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla);
            ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
            jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
            JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
            assertEquals(SUCCESS, createResp.responseCode);

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            jobClusterActor.tell(new DisableJobClusterRequest(clusterName,user),probe.getRef());
            DisableJobClusterResponse resp = probe.expectMsgClass(DisableJobClusterResponse.class);

            assertTrue(BaseResponse.ResponseCode.SUCCESS.equals(resp.responseCode));

            jobClusterActor.tell(new EnableJobClusterRequest(clusterName,user),probe.getRef());
            EnableJobClusterResponse enableResp = probe.expectMsgClass(EnableJobClusterResponse.class);

            assertTrue(BaseResponse.ResponseCode.SUCCESS.equals(enableResp.responseCode));

            // first job was killed during disable
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Completed);

            // Sla will cause new job to get launched
            String jobId2 = clusterName + "-2";

            boolean accepted = false;
            int cnt = 0;
            // try a few times for timing issue
            while(cnt < 50) {
                jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId2).get()), probe.getRef());
                GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);
                if(detailsResp.responseCode.equals(BaseResponse.ResponseCode.SUCCESS)) {
                    accepted = true;
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue(accepted);


            // JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Completed);

//            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

//            verify(jobStoreMock, times(1)).createJobCluster(any());
//            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testJobClusterUpdateAndDelete() throws Exception  {

        TestKit probe = new TestKit(system);
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname","labelvalue");
        labels.add(l);
        String clusterName = "testJobClusterUpdateAndDelete";
        String artifactName = "myart";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withSchedulingInfo(SINGLE_WORKER_SCHED_INFO)
                .withVersion("0.0.2")
                .build();

        final JobClusterDefinitionImpl updatedJobCluster = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(clusterName)
                .withParameters(Lists.newArrayList())
                .withLabels(labels)
                .withUser(user)
                .withIsReadyForJobMaster(true)
                .withOwner(DEFAULT_JOB_OWNER)
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .withSla(NO_OP_SLA)
                .build();


        jobClusterActor.tell(new UpdateJobClusterRequest(updatedJobCluster, "user"), probe.getRef());
        UpdateJobClusterResponse resp = probe.expectMsgClass(UpdateJobClusterResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        assertEquals(2, resp3.getJobCluster().get().getLabels().size());
        assertEquals("labelname", resp3.getJobCluster().get().getLabels().get(0).getName());

        jobClusterActor.tell(new JobClusterProto.DeleteJobClusterRequest(user, clusterName, probe.getRef()), probe.getRef());
        JobClusterProto.DeleteJobClusterResponse resp4 = probe.expectMsgClass(JobClusterProto.DeleteJobClusterResponse.class);
        assertEquals(SUCCESS, resp4.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(1)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).deleteJobCluster(clusterName);


    }


    @Test
    public void testJobClusterUpdateFailsIfArtifactNotUnique() throws Exception  {

        TestKit probe = new TestKit(system);
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname","labelvalue");
        labels.add(l);
        String clusterName = "testJobClusterUpdateFailsIfArtifactNotUnique";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        jobClusterActor.tell(new UpdateJobClusterRequest(fakeJobCluster, "user"), probe.getRef());
        UpdateJobClusterResponse resp = probe.expectMsgClass(UpdateJobClusterResponse.class);

        assertEquals(CLIENT_ERROR, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(0)).updateJobCluster(any());

    }


    @Test
    public void testJobClusterDeleteFailsIfJobsActive() throws Exception  {

        TestKit probe = new TestKit(system);
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname","labelvalue");
        labels.add(l);
        String clusterName = "testJobClusterDeleteFailsIfJobsActive";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
        String jobId = clusterName + "-1";

        JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

        JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);


        jobClusterActor.tell(new JobClusterProto.DeleteJobClusterRequest(user, clusterName, probe.getRef()), probe.getRef());
        JobClusterProto.DeleteJobClusterResponse resp4 = probe.expectMsgClass(JobClusterProto.DeleteJobClusterResponse.class);
        assertEquals(CLIENT_ERROR, resp4.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(1)).updateJobCluster(any());
        verify(jobStoreMock, times(0)).deleteJobCluster(clusterName);


    }

    @Test
    @Ignore("todo: Purge logic changed")
    public void testJobClusterDeletePurgesCompletedJobs() throws Exception  {

        TestKit probe = new TestKit(system);
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname","labelvalue");
        labels.add(l);
        String clusterName = "testJobClusterDeletePurgesCompletedJobs";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
        String jobId = clusterName + "-1";

        JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

        JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

        jobClusterActor.tell(new DisableJobClusterRequest(clusterName, "user"), probe.getRef());
        DisableJobClusterResponse disableResp = probe.expectMsgClass(DisableJobClusterResponse.class);
        assertEquals(SUCCESS, disableResp.responseCode);

        Thread.sleep(1000);

        jobClusterActor.tell(new JobClusterProto.DeleteJobClusterRequest(user, clusterName, probe.getRef()), probe.getRef());
        JobClusterProto.DeleteJobClusterResponse resp4 = probe.expectMsgClass(JobClusterProto.DeleteJobClusterResponse.class);
        assertEquals(SUCCESS, resp4.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(2)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).deleteJobCluster(clusterName);
        verify(jobStoreMock, times(1)).storeCompletedJobForCluster(any(),any());
        verify(jobStoreMock, times(1)).deleteJob("testJobClusterDeletePurgesCompletedJobs-1");



    }


    @Test
    public void testJobClusterDisable() throws InterruptedException {

        TestKit probe = new TestKit(system);
        CountDownLatch storeCompletedCalled = new CountDownLatch(1);
        String clusterName = "testJobClusterDisable";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);



        try {

            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            IMantisJobMetadata completedJobMock = new MantisJobMetadataImpl.Builder()
                    .withJobId(new JobId(clusterName, 1))
                    .withJobDefinition(jobDefn)
                    .withJobState(JobState.Completed)
                    .build();
            when(jobStoreMock.loadCompletedJobsForCluster(any(), anyInt(), any()))
                // .thenReturn(ImmutableList.of());
                .thenReturn(ImmutableList.of(
                    new CompletedJob(
                        completedJobMock.getClusterName(),
                        completedJobMock.getJobId().getId(),
                        "v1",
                        JobState.Completed,
                        -1L,
                        -1L,
                        completedJobMock.getUser(),
                        completedJobMock.getLabels())));
            when(jobStoreMock.getArchivedJob(any())).thenReturn(of(completedJobMock));
            doAnswer((Answer) invocation -> {
                storeCompletedCalled.countDown();
                return null;
            }).when(jobStoreMock).storeCompletedJobForCluster(any(),any());

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            jobClusterActor.tell(new DisableJobClusterRequest(clusterName,"user"), probe.getRef());

            DisableJobClusterResponse disableResp = probe.expectMsgClass(DisableJobClusterResponse.class);

            assertEquals(SUCCESS, disableResp.responseCode);

            jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());

            GetJobClusterResponse getJobClusterResp = probe.expectMsgClass(GetJobClusterResponse.class);

            assertTrue(getJobClusterResp.getJobCluster().get().isDisabled());

            jobClusterActor.tell(new GetJobDetailsRequest(clusterName, JobId.fromId(jobId).get()),probe.getRef());

            GetJobDetailsResponse jobDetailsResp  = probe.expectMsgClass(GetJobDetailsResponse.class);

            assertEquals(SUCCESS, jobDetailsResp.responseCode);

            assertEquals(jobId, jobDetailsResp.getJobMetadata().get().getJobId().getId());

            assertEquals(JobState.Completed, jobDetailsResp.getJobMetadata().get().getState());

            verify(jobStoreMock, times(1)).createJobCluster(any());

            verify(jobStoreMock, times(2)).updateJobCluster(any());

            verify(jobStoreMock, times(1)).storeNewJob(any());

            verify(jobStoreMock, times(1)).updateStage(any());

            verify(jobStoreMock,times(2)).updateJob(any());

            verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

            storeCompletedCalled.await(1, TimeUnit.SECONDS);

        } catch (Exception  e) {

            e.printStackTrace();
            fail();
        }

    }


///////////////////////////////////////////// CLUSTER CRUD END ///////////////////////////////////////////////////////

    ////////////////////////////// CLUSTER UPDATE FLAVORS ///////////////////////////////////////////////////////////////

    @Test
    public void testJobClusterSLAUpdate() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterSLAUpdate";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);
        SLA newSLA = new SLA(0,10,null,null);
        UpdateJobClusterSLARequest updateSlaReq = new UpdateJobClusterSLARequest(clusterName, newSLA.getMin(), newSLA.getMax(), "user");
        jobClusterActor.tell(updateSlaReq, probe.getRef());
        UpdateJobClusterSLAResponse resp = probe.expectMsgClass(UpdateJobClusterSLAResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        assertEquals(newSLA, DataFormatAdapter.convertToSLA(resp3.getJobCluster().get().getSla()));

        verify(jobStoreMock, times(1)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).createJobCluster(any());
    }

    @Test
    public void testJobClusterMigrationConfigUpdate() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterMigrationConfigUpdate";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);
        WorkerMigrationConfig newConfig = new WorkerMigrationConfig(MigrationStrategyEnum.ONE_WORKER, "{'name':'value'}");
        UpdateJobClusterWorkerMigrationStrategyRequest updateMigrationConfigReq = new UpdateJobClusterWorkerMigrationStrategyRequest(clusterName, newConfig, "user");
        jobClusterActor.tell(updateMigrationConfigReq, probe.getRef());
        UpdateJobClusterWorkerMigrationStrategyResponse resp = probe.expectMsgClass(UpdateJobClusterWorkerMigrationStrategyResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        assertEquals(MigrationStrategyEnum.ONE_WORKER, resp3.getJobCluster().get().getMigrationConfig().getStrategy());

        verify(jobStoreMock, times(1)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).createJobCluster(any());
    }

    @Test
    public void testJobClusterArtifactUpdateBackCompat() throws Exception {
        String clusterName = "testJobClusterArtifactUpdateBackCompat";
        UpdateJobClusterArtifactRequest req = new UpdateJobClusterArtifactRequest(
            clusterName,
            "http://path1/artifact1-1.zip",
            null,
            "1",
            true,
            "user");
        assertEquals("http://path1/artifact1-1.zip", req.getArtifactName());
        assertEquals("http://path1/artifact1-1.zip", req.getjobJarUrl());

        UpdateJobClusterArtifactRequest req2 = new UpdateJobClusterArtifactRequest(
            clusterName,
            "artifact1-1.zip",
            null,
            "1",
            true,
            "user");
        assertEquals("artifact1-1.zip", req2.getArtifactName());
        assertEquals("http://artifact1-1.zip", req2.getjobJarUrl());

        UpdateJobClusterArtifactRequest req3 = new UpdateJobClusterArtifactRequest(
            clusterName,
            "https://path1/artifact1-1.zip",
            null,
            "1",
            true,
            "user");
        assertEquals("https://path1/artifact1-1.zip", req3.getArtifactName());
        assertEquals("https://path1/artifact1-1.zip", req3.getjobJarUrl());
    }

    @Test
    public void testJobClusterArtifactUpdate() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterArtifactUpdate";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        UpdateJobClusterArtifactRequest req = new UpdateJobClusterArtifactRequest(clusterName, "a1", "http://a1", "1.0.1", true, "user");

        jobClusterActor.tell(req, probe.getRef());
        UpdateJobClusterArtifactResponse resp = probe.expectMsgClass(UpdateJobClusterArtifactResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        assertEquals(2,resp3.getJobCluster().get().getJars().size());
        //assertEquals("a1", resp3.getJobCluster().getJobClusterDefinition().getJobClusterConfig().getArtifactName());
        assertEquals("1.0.1", resp3.getJobCluster().get().getLatestVersion());
        List<NamedJob.Jar> jars = resp3.getJobCluster().get().getJars();
        assertTrue(jars.get(jars.size()-1).getUploadedAt() != -1);


        verify(jobStoreMock, times(1)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).createJobCluster(any());
    }

    @Test
    public void testJobClusterArtifactUpdateNotUniqueFails() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterArtifactUpdateNotUniqueFails";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        UpdateJobClusterArtifactRequest req = new UpdateJobClusterArtifactRequest(clusterName, "a1", "http://a1", "0.0.1", true, "user");

        jobClusterActor.tell(req, probe.getRef());
        UpdateJobClusterArtifactResponse resp = probe.expectMsgClass(UpdateJobClusterArtifactResponse.class);

        assertEquals(CLIENT_ERROR, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("job cluster " + resp3.getJobCluster());
        assertEquals(1,resp3.getJobCluster().get().getJars().size());
        //assertEquals("a1", resp3.getJobCluster().getJobClusterDefinition().getJobClusterConfig().getArtifactName());
        assertEquals("0.0.1", resp3.getJobCluster().get().getLatestVersion());

        verify(jobStoreMock, times(0)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).createJobCluster(any());
    }


    @Test
    public void testJobClusterArtifactUpdateMultipleTimes() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterArtifactUpdateMultipleTimes";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        UpdateJobClusterArtifactRequest req = new UpdateJobClusterArtifactRequest(clusterName, "a1", "http://a1", "1.0.1", true, "user");

        jobClusterActor.tell(req, probe.getRef());
        UpdateJobClusterArtifactResponse resp = probe.expectMsgClass(UpdateJobClusterArtifactResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        //assertEquals("a1", resp3.getJobCluster().getJobClusterDefinition().getJobClusterConfig().getArtifactName());
        assertEquals("1.0.1", resp3.getJobCluster().get().getLatestVersion());
        List<NamedJob.Jar> jars = resp3.getJobCluster().get().getJars();
        System.out.println("jars --> " + jars);
        assertEquals(2, jars.size());

        // Update again

        req = new UpdateJobClusterArtifactRequest(clusterName, "a2", "http:/a2", "1.0.3", true, "user");

        jobClusterActor.tell(req, probe.getRef());
        resp = probe.expectMsgClass(UpdateJobClusterArtifactResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        //assertEquals("a1", resp3.getJobCluster().getJobClusterDefinition().getJobClusterConfig().getArtifactName());
        assertEquals("1.0.3", resp3.getJobCluster().get().getLatestVersion());
        jars = resp3.getJobCluster().get().getJars();
        System.out.println("jars --> " + jars);
        assertEquals(3, jars.size());



        verify(jobStoreMock, times(2)).updateJobCluster(any());
        verify(jobStoreMock, times(1)).createJobCluster(any());
    }


    @Test
    public void testJobClusterInvalidSLAUpdateIgnored() throws Exception  {


        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterInvalidSLAUpdateIgnored";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        UpdateJobClusterSLARequest updateSlaReq = new UpdateJobClusterSLARequest(clusterName, 2, 1, "user");
        jobClusterActor.tell(updateSlaReq, probe.getRef());
        UpdateJobClusterSLAResponse resp = probe.expectMsgClass(UpdateJobClusterSLAResponse.class);

        assertEquals(CLIENT_ERROR, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        // No changes to original SLA
        assertEquals(0, resp3.getJobCluster().get().getSla().getMin());
        assertEquals(0, resp3.getJobCluster().get().getSla().getMax());

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(0)).updateJobCluster(any());
    }

    @Test
    public void testJobClusterLabelsUpdate() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterLabelsUpdate";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);

        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);
        // assert initially no labels
        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        System.out.println("Updated job cluster " + resp3.getJobCluster());
        assertEquals(1, resp3.getJobCluster().get().getLabels().size());


        // new labels
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname","labelvalue");
        labels.add(l);
        labels.add(new Label("_mantis.resourceCluster","cl2"));

        UpdateJobClusterLabelsRequest updateLabelsReq = new UpdateJobClusterLabelsRequest(clusterName, labels, "user");
        jobClusterActor.tell(updateLabelsReq, probe.getRef());
        UpdateJobClusterLabelsResponse resp = probe.expectMsgClass(UpdateJobClusterLabelsResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        // get job cluster details
        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        //assert label list is of size 1
        assertEquals(2, resp3.getJobCluster().get().getLabels().size());
        assertEquals(l, resp3.getJobCluster().get().getLabels().get(0));

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(1)).updateJobCluster(any());
    }

    ////////////////////////////////////// CLUSTER UPDATE FLAVORS END ////////////////////////////////////////////////////

    ////////////////////////////////////// JOB SUBMIT OPERATIONS /////////////////////////////////////////////////////////////

    @Test
    public void testJobSubmit() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmit";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.killJobSendWorkerTerminatedAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor, new WorkerId(jobId, 0 ,1));

            Thread.sleep(500);
            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());

            verify(jobStoreMock, timeout(2000).times(1)).archiveJob(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testJobSubmitWithNoJarAndSchedInfo() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithNoJarAndSchedInfo";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withParameters(Lists.newArrayList())
                    .withUser("njoshi")
                    .withSubscriptionTimeoutSecs(300)
                    .withJobSla(new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, ""))

                    .build();;
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);


            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
            GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);

            // make sure it inherits from cluster
            assertEquals("myart", detailsResp.getJobMetadata().get().getArtifactName());
            // inherits cluster scheduling Info
            assertEquals(SINGLE_WORKER_SCHED_INFO,detailsResp.getJobMetadata().get().getSchedulingInfo());

            //JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }


    @Test
    public void testJobSubmitWithVersionAndNoSchedInfo() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithVersionAndNoSchedInfo";
        String artifactName = "myart2";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withSchedulingInfo(TWO_WORKER_SCHED_INFO)
                .withVersion("0.0.2")
                .build();

        List<Label> labels = Lists.newLinkedList();
        labels.add(new Label("_mantis.resourceCluster","cl2"));

        final JobClusterDefinitionImpl updatedFakeJobCluster = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(clusterName)
                .withParameters(Lists.newArrayList())
                .withLabels(labels)
                .withUser(user)
                .withIsReadyForJobMaster(true)
                .withOwner(DEFAULT_JOB_OWNER)
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .withSla(NO_OP_SLA)
                .build();

        jobClusterActor.tell(new UpdateJobClusterRequest(updatedFakeJobCluster, "user"), probe.getRef());
        UpdateJobClusterResponse resp = probe.expectMsgClass(UpdateJobClusterResponse.class);

        jobClusterActor.tell(new GetJobClusterRequest(clusterName),probe.getRef());
        GetJobClusterResponse getJobClusterResponse = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(2,getJobClusterResponse.getJobCluster().get().getJars().size());

        try {
            final JobDefinition jobDefn = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withParameters(Lists.newArrayList())
                    .withUser("njoshi")
                    .withVersion("0.0.2")
                    .withSubscriptionTimeoutSecs(300)
                    .withJobSla(new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, ""))

                    .build();;
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);


            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
            GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);

            // make sure it inherits from cluster
            assertEquals("myart2", detailsResp.getJobMetadata().get().getArtifactName());
            // inherits cluster scheduling Info corresponding to the given artifact
            assertEquals(TWO_WORKER_SCHED_INFO,detailsResp.getJobMetadata().get().getSchedulingInfo());


            // Now submit with a different artifact and no scheduling Info

            final JobDefinition jobDefn2 = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withParameters(Lists.newArrayList())
                    .withUser("njoshi")
                    .withVersion("0.0.1")
                    .withSubscriptionTimeoutSecs(300)
                    .withJobSla(new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, ""))

                    .build();;
            String jobId2 = clusterName + "-2";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2, jobId2);


            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId2).get()), probe.getRef());
            GetJobDetailsResponse detailsResp2 = probe.expectMsgClass(GetJobDetailsResponse.class);

            // make sure it inherits from cluster
            assertEquals("myart", detailsResp2.getJobMetadata().get().getArtifactName());
            // inherits cluster scheduling Info corresponding to the given artifact
            assertEquals(SINGLE_WORKER_SCHED_INFO,detailsResp2.getJobMetadata().get().getSchedulingInfo());


            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(3)).updateJobCluster(any());

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }


    @Ignore
    @Test
    public void testJobComplete() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobComplete";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            JobTestHelper.sendWorkerCompletedEvent(probe,jobClusterActor,jobId,new WorkerId(jobId, 0,1));

            JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor, jobId, JobState.Completed);

            verify(jobStoreMock, timeout(2000).times(1)).archiveJob(any());

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void testJobKillTriggersSLAToLaunchNew() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobKillTriggersSLAToLaunchNew";
        MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SLA sla = new SLA(1,1,null,null);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, schedulerMockFactory, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);
        String jobId = clusterName + "-1";
        WorkerId workerId1 = new WorkerId(clusterName, jobId, 0, 1);
        doAnswer(invocation -> {
            WorkerEvent terminate = new WorkerTerminate(workerId1, WorkerState.Completed, JobCompletedReason.Killed, System.currentTimeMillis());
            jobClusterActor.tell(terminate, probe.getRef());
            return null;
        }).when(schedulerMock).unscheduleWorker(any(),any());

        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            JobId jId = new JobId(clusterName,1);

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            JobTestHelper.killJobAndVerify(probe,clusterName,jId,jobClusterActor);
            Thread.sleep(500);
            // a new job should have been submitted
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, clusterName + "-2", SUCCESS, JobState.Accepted);


            //JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

//            verify(jobStoreMock, times(1)).createJobCluster(any());
//            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }


    // TODO
// TODO    @Test
    public void testJobSubmitTriggersSLAToKillOld() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitTriggersSLAToKillOld";
        MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SLA sla = new SLA(1,1,null,null);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, schedulerMockFactory, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);
        String jobId = clusterName + "-1";
        WorkerId workerId1 = new WorkerId(clusterName, jobId, 0, 1);
        doAnswer(invocation -> {
            WorkerEvent terminate = new WorkerTerminate(workerId1, WorkerState.Completed, JobCompletedReason.Killed, System.currentTimeMillis());
            jobClusterActor.tell(terminate, probe.getRef());
            return null;
        }).when(schedulerMock).unscheduleWorker(any(),any());

        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);


            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            // submit 2nd job
            String jobId2 = clusterName + "-2";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId2);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId2,1,new WorkerId(clusterName,jobId2,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Launched);

            boolean completed = false;

            assertTrue(JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor,jobId,JobState.Completed));

//            jobClusterActor.tell(new ListJobIdsRequest(), probe.getRef());
//            ListJobIdsResponse listJobIdsResponse = probe.expectMsgClass(ListJobIdsResponse.class);
//            assertEquals(SUCCESS, listJobIdsResponse.responseCode);
//            assertEquals(1,listJobIdsResponse.getJobIds().size());
//            assertEquals(jobId2, listJobIdsResponse.getJobIds().get(0).getId());


//            // try a few times for timing issue
//            for(int i=0; i<10; i++) {
//                jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
//                GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);
//                if(JobState.Completed.equals(detailsResp.getJobMetadata().get().getState())) {
//                    completed = true;
//                    break;
//                }
//            }
//            assertTrue(completed);


            // JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Completed);

            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

//            verify(jobStoreMock, times(1)).createJobCluster(any());
//            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

 //TODO   @Test
    public void testJobSubmitTriggersSLAToKillOldHandlesErrors() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitTriggersSLAToKillOldHandlesErrors";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SLA sla = new SLA(1,1,null,null);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {

            doThrow(new NullPointerException("NPE archiving worker")).when(jobStoreMock).archiveWorker(any());

            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            // submit 2nd job
            String jobId2 = clusterName + "-2";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId2);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId2,1,new WorkerId(clusterName,jobId2,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Launched);

            boolean completed = false;

            assertTrue(JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor,jobId,JobState.Completed));
            // try a few times for timing issue
//            for(int i=0; i<10; i++) {
//                jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
//                GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);
//                if(JobState.Completed.equals(detailsResp.getJobMetadata().get().getState())) {
//                    completed = true;
//                    break;
//                }
//            }
//            assertTrue(completed);

            jobClusterActor.tell(new ListJobIdsRequest(), probe.getRef());

            ListJobIdsResponse listResp = probe.expectMsgClass(ListJobIdsResponse.class);

            assertEquals(1,listResp.getJobIds().size());
            assertEquals(jobId2, listResp.getJobIds().get(0).getJobId());


            // JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Completed);

            //JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

//            verify(jobStoreMock, times(1)).createJobCluster(any());
//            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }


    /**
     * {@see <a href="https://github.com/Netflix/mantis/issues/195">Github issue</a> for more context}
     */
    @Ignore
    @Test
    public void testCronTriggersSLAToKillOld() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitTriggersSLAToKillOld";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SLA sla = new SLA(1,1,"0/1 * * * * ?",IJobClusterDefinition.CronPolicy.KEEP_NEW);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);


            // try a few times for timing issue
            String jobId2 = clusterName + "-2";
            assertTrue(JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor,jobId2,JobState.Accepted));


            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId2,1,new WorkerId(clusterName,jobId2,0,1));


            assertTrue(JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor,jobId,JobState.Completed));



//            verify(jobStoreMock, times(1)).createJobCluster(any());
//            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCronDefined() {
        TestKit probe = new TestKit(system);
        String clusterName = "testInvalidCronSubmit";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        SLA sla = new SLA(1,1,"a b * * * * * * *",IJobClusterDefinition.CronPolicy.KEEP_NEW);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla); //should throw IllegalArgumentException
    }

    @Test
    public void testInvalidCronSLAUpdate() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterInvalidSLAUpdateIgnored";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        UpdateJobClusterSLARequest updateSlaReq = new UpdateJobClusterSLARequest(clusterName, 2, 1,"a b * * * * * * *",IJobClusterDefinition.CronPolicy.KEEP_NEW,false,"user" );
        jobClusterActor.tell(updateSlaReq, probe.getRef());
        UpdateJobClusterSLAResponse resp = probe.expectMsgClass(UpdateJobClusterSLAResponse.class);

        assertEquals(CLIENT_ERROR, resp.responseCode);
        assertEquals(jobClusterActor, probe.getLastSender());

        jobClusterActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp3 = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, resp3.responseCode);
        assertTrue(resp3.getJobCluster() != null);
        System.out.println("Job cluster " + resp3.getJobCluster());
        assertEquals(clusterName, resp3.getJobCluster().get().getName());
        // No changes to original SLA
        assertEquals(0, resp3.getJobCluster().get().getSla().getMin());
        assertEquals(0, resp3.getJobCluster().get().getSla().getMax());

        verify(jobStoreMock, times(1)).createJobCluster(any());
        verify(jobStoreMock, times(0)).updateJobCluster(any());
    }

    @Test
    public void testJobSubmitWithUnique() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithUnique";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient, "mytype");
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            jobClusterActor.tell(new SubmitJobRequest(clusterName,"user", jobDefn), probe.getRef());
            SubmitJobResponse submitResponse = probe.expectMsgClass(SubmitJobResponse.class);

            // Get the same job id back
            assertTrue(submitResponse.getJobId().isPresent());
            assertEquals(jobId,submitResponse.getJobId().get().getId());


            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());
            verify(jobStoreMock, times(1)).storeNewJob(any());



        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJobSubmitWithoutInheritInstance() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithInheritInstance";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            // default job with 1 stage == (1 worker)
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            // submit another job this time with job with inheritInstance enabled
            final String jobId2 = clusterName + "-2";
            final SchedulingInfo schedulingInfo = new SchedulingInfo.Builder()
                    .numberOfStages(1)
                    .multiWorkerStage(3, DEFAULT_MACHINE_DEFINITION)
                    .build();
            final JobDefinition jobDefn2Workers = createJob(clusterName, MantisJobDurationType.Transient, schedulingInfo);
            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2Workers, jobId2);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Accepted);

            // verify instance count is from previous job 1.
            final JobId jobId2Id = JobId.fromId(jobId2).get();
            jobClusterActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId2Id), probe.getRef());
            JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(Duration.ofSeconds(60), JobClusterManagerProto.GetJobDetailsResponse.class);
            assertTrue(detailsResp.getJobMetadata().isPresent());
            assertEquals(jobId2, detailsResp.getJobMetadata().get().getJobId().getId());

            final SchedulingInfo actualSchedulingInfo = detailsResp.getJobMetadata().get().getSchedulingInfo();
            assertEquals(1, actualSchedulingInfo.getStages().size());
            assertEquals(3, actualSchedulingInfo.forStage(1).getNumberOfInstances());

            JobTestHelper.killJobAndVerify(probe, clusterName, jobId2Id, jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(2)).updateJobCluster(any());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJobSubmitWithInheritInstanceFlagsSingleStage() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithInheritInstance";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            // default job with 1 stage == (1 worker)
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            // submit another job this time with job with inheritInstance enabled
            final String jobId2 = clusterName + "-2";
            final SchedulingInfo schedulingInfo = new SchedulingInfo.Builder()
                    .numberOfStages(1)
                    .multiWorkerStage(3, DEFAULT_MACHINE_DEFINITION)
                    .build();
            final DeploymentStrategy deploymentStrategy = DeploymentStrategy.builder()
                    .stage(1, StageDeploymentStrategy.builder().inheritInstanceCount(true).build())
                    .build();
            final JobDefinition jobDefn2Workers = createJob(
                    clusterName, MantisJobDurationType.Transient, schedulingInfo, deploymentStrategy);
            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2Workers, jobId2);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Accepted);

            // verify instance count is from previous job 1.
            final JobId jobId2Id = JobId.fromId(jobId2).get();
            jobClusterActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId2Id), probe.getRef());
            JobClusterManagerProto.GetJobDetailsResponse detailsResp = probe.expectMsgClass(Duration.ofSeconds(60), JobClusterManagerProto.GetJobDetailsResponse.class);
            assertTrue(detailsResp.getJobMetadata().isPresent());
            assertEquals(jobId2, detailsResp.getJobMetadata().get().getJobId().getId());

            final SchedulingInfo actualSchedulingInfo = detailsResp.getJobMetadata().get().getSchedulingInfo();
            assertEquals(1, actualSchedulingInfo.getStages().size());
            assertEquals(1, actualSchedulingInfo.forStage(1).getNumberOfInstances());

            JobTestHelper.killJobAndVerify(probe, clusterName, jobId2Id, jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(2)).updateJobCluster(any());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJobSubmitWithInheritInstanceFlagsMultiStage() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithInheritInstance";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        // default job with 3 stage == (2 worker)
        final SchedulingInfo schedulingInfo1 = new SchedulingInfo.Builder()
                .numberOfStages(4)
                .multiWorkerStage(2, DEFAULT_MACHINE_DEFINITION)
                .multiWorkerStage(2, DEFAULT_MACHINE_DEFINITION)
                .multiWorkerStage(2, DEFAULT_MACHINE_DEFINITION)
                .multiWorkerStage(2, DEFAULT_MACHINE_DEFINITION)
                .build();
        final JobClusterDefinitionImpl fakeJobCluster =
                createFakeJobClusterDefn(clusterName, Lists.newArrayList(), NO_OP_SLA, schedulingInfo1);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(
                fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp =
                probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            final JobDefinition jobDefn = createJob(clusterName,MantisJobDurationType.Transient, schedulingInfo1);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            // submit another job this time with job, in which some stages with inheritInstance enabled
            final String jobId2 = clusterName + "-2";
            final SchedulingInfo schedulingInfo2 = new SchedulingInfo.Builder()
                    .numberOfStages(4)
                    .multiWorkerStage(3, DEFAULT_MACHINE_DEFINITION)
                    .multiWorkerStage(4, DEFAULT_MACHINE_DEFINITION)
                    .multiWorkerStage(5, DEFAULT_MACHINE_DEFINITION)
                    .multiWorkerStage(6, DEFAULT_MACHINE_DEFINITION)
                    .build();
            final DeploymentStrategy deploymentStrategy =  DeploymentStrategy.builder()
                    .stage(1, StageDeploymentStrategy.builder().inheritInstanceCount(true).build())
                    .stage(3, StageDeploymentStrategy.builder().inheritInstanceCount(true).build())
                    .stage(4, StageDeploymentStrategy.builder().inheritInstanceCount(false).build())
                    .build();
            final String artifactV2 = "artVer-2";
            final JobDefinition jobDefn2Workers = createJob(clusterName, MantisJobDurationType.Transient, schedulingInfo2,
                    jobDefn.getArtifactName(), artifactV2, deploymentStrategy);
            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2Workers, jobId2);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Accepted);

            // verify instance count is from previous job.
            final JobId jobId2Id = JobId.fromId(jobId2).get();
            jobClusterActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId2Id), probe.getRef());
            JobClusterManagerProto.GetJobDetailsResponse detailsResp =
                    probe.expectMsgClass(Duration.ofSeconds(60), JobClusterManagerProto.GetJobDetailsResponse.class);
            assertTrue(detailsResp.getJobMetadata().isPresent());
            assertEquals(jobId2, detailsResp.getJobMetadata().get().getJobId().getId());
            assertEquals(artifactV2, detailsResp.getJobMetadata().get().getJobDefinition().getVersion());

            final SchedulingInfo actualSchedulingInfo = detailsResp.getJobMetadata().get().getSchedulingInfo();
            assertEquals(4, actualSchedulingInfo.getStages().size());

            // stage 1/3 inherits from previous job while stage 2 should apply new instance count.
            assertEquals(2, actualSchedulingInfo.forStage(1).getNumberOfInstances());
            assertEquals(4, actualSchedulingInfo.forStage(2).getNumberOfInstances());
            assertEquals(2, actualSchedulingInfo.forStage(3).getNumberOfInstances());
            assertEquals(6, actualSchedulingInfo.forStage(4).getNumberOfInstances());
            JobTestHelper.killJobAndVerify(probe, clusterName, jobId2Id, jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(2)).updateJobCluster(any());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJobSubmitWithInheritInstanceFlagsScaled() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithInheritInstance";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        // default job with 3 stage == (2 worker)
        final SchedulingInfo schedulingInfo1 = new SchedulingInfo.Builder()
                .numberOfStages(2)
                .multiWorkerStage(1, DEFAULT_MACHINE_DEFINITION, true)
                .multiWorkerStage(1, DEFAULT_MACHINE_DEFINITION, true)
                .build();
        final JobClusterDefinitionImpl fakeJobCluster =
                createFakeJobClusterDefn(clusterName, Lists.newArrayList(), NO_OP_SLA, schedulingInfo1);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(
                fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp =
                probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            final JobDefinition jobDefn = createJob(clusterName,MantisJobDurationType.Transient, schedulingInfo1);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobClusterActor, jobId,1, new WorkerId(jobId,0,1));
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobClusterActor, jobId,2, new WorkerId(jobId,0,2));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            // try scale job
            JobTestHelper.scaleStageAndVerify(probe, jobClusterActor, jobId, 1, 2);

            // submit another job this time with job, in which some stages with inheritInstance enabled
            final String jobId2 = clusterName + "-2";
            final SchedulingInfo schedulingInfo2 = new SchedulingInfo.Builder()
                    .numberOfStages(2)
                    .multiWorkerStage(3, DEFAULT_MACHINE_DEFINITION)
                    .multiWorkerStage(4, DEFAULT_MACHINE_DEFINITION)
                    .build();
            final DeploymentStrategy deploymentStrategy = DeploymentStrategy.builder()
                    .stage(1, StageDeploymentStrategy.builder().inheritInstanceCount(true).build())
                    .stage(2, StageDeploymentStrategy.builder().inheritInstanceCount(true).build())
                    .build();
            final JobDefinition jobDefn2Workers = createJob(
                    clusterName, MantisJobDurationType.Transient, schedulingInfo2, deploymentStrategy);
            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2Workers, jobId2);
            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId2, SUCCESS, JobState.Accepted);

            // verify instance count is from previous job.
            final JobId jobId2Id = JobId.fromId(jobId2).get();
            jobClusterActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("nj", jobId2Id), probe.getRef());
            JobClusterManagerProto.GetJobDetailsResponse detailsResp =
                    probe.expectMsgClass(Duration.ofSeconds(60), JobClusterManagerProto.GetJobDetailsResponse.class);
            assertTrue(detailsResp.getJobMetadata().isPresent());
            assertEquals(jobId2, detailsResp.getJobMetadata().get().getJobId().getId());

            final SchedulingInfo actualSchedulingInfo = detailsResp.getJobMetadata().get().getSchedulingInfo();
            assertEquals(2, actualSchedulingInfo.getStages().size());

            // stage 1 inherits from previous job while stage 2 should apply new instance count.
            assertEquals(2, actualSchedulingInfo.forStage(1).getNumberOfInstances());
            assertEquals(1, actualSchedulingInfo.forStage(2).getNumberOfInstances());
            JobTestHelper.killJobAndVerify(probe, clusterName, jobId2Id, jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(2)).updateJobCluster(any());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }


    @Test
    public void testQuickJobSubmit() {
        TestKit probe = new TestKit(system);
        String clusterName = "testQuickJobSubmit";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            // submit another job this time with no job definition
            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, null, clusterName + "-2");

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, clusterName + "-2", SUCCESS, JobState.Accepted);


            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(2)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testQuickJobSubmitWithNoSchedInfoInPreviousJob() {
        TestKit probe = new TestKit(system);
        String clusterName = "testQuickJobSubmitWithNoSchedInfoInPreviousJob";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            // job defn with scheduling info
            final JobDefinition jobDefn = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withParameters(Lists.newArrayList())
                    .withLabels(Lists.newArrayList())
                    .withVersion("0.0.1")
                    .withSubscriptionTimeoutSecs(300)
                    .withUser("njoshi")
                    .withJobSla(new JobSla(0, 0, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, "abc"))
                    .build();

            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            // submit another job this time with no job definition
            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, null, clusterName + "-2");

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, clusterName + "-2", SUCCESS, JobState.Accepted);


            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(2)).updateJobCluster(any());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testJobSubmitWithNoSchedInfoUsesJobClusterValues() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitWithNoSchedInfoUsesJobClusterValues";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        List<Label> clusterLabels = new ArrayList<>();
        Label label = new Label("clabelName", "cLabelValue");
        clusterLabels.add(label);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, clusterLabels);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {

            final JobDefinition jobDefn = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withVersion("0.0.1")
                    .withSubscriptionTimeoutSecs(0)
                    .withUser("njoshi")
                    .build();

            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
            GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);

            assertEquals(SUCCESS, detailsResp.responseCode);
            assertEquals(JobState.Accepted, detailsResp.getJobMetadata().get().getState());
            //
            assertEquals(clusterLabels.size() + LabelManager.numberOfMandatoryLabels(),detailsResp.getJobMetadata().get().getLabels().size());
            // confirm that the clusters labels got inherited
            assertEquals(1, detailsResp.getJobMetadata().get()
                    .getLabels().stream().filter(l -> l.getName().equals("clabelName")).count());
            //assertEquals(label, detailsResp.getJobMetadata().get().getLabels().get(0));


            // Now submit another one with labels, it should not inherit cluster labels
            Label jobLabel = new Label("jobLabel", "jobValue");
            List<Label> jobLabelList = new ArrayList<>();
            jobLabelList.add(jobLabel);

            final JobDefinition jobDefn2 = new JobDefinition.Builder()
                    .withName(clusterName)
                    .withVersion("0.0.1")
                    .withLabels(jobLabelList)
                    .withSubscriptionTimeoutSecs(0)
                    .withUser("njoshi")
                    .build();

            String jobId2 = clusterName + "-2";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2, jobId2);

            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId2).get()), probe.getRef());
            GetJobDetailsResponse detailsResp2 = probe.expectMsgClass(GetJobDetailsResponse.class);

            assertEquals(SUCCESS, detailsResp2.responseCode);
            assertEquals(JobState.Accepted, detailsResp2.getJobMetadata().get().getState());
            assertEquals(clusterLabels.size() + 3, detailsResp2.getJobMetadata().get().getLabels().size());
            // confirm that the clusters labels got inherited
            //assertEquals(jobLabel, detailsResp2.getJobMetadata().get().getLabels().get(0));
            assertEquals(1, detailsResp2.getJobMetadata().get()
                    .getLabels().stream().filter(l -> l.getName().equals(jobLabel.getName())).count());


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testQuickJobSubmitWithNoPreviousHistoryFails() {
        TestKit probe = new TestKit(system);
        String clusterName = "testQuickJobSubmitWithNoPreviousHistoryFails";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            final JobDefinition jobDefn = null;
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifyStatus(probe, clusterName, jobClusterActor, jobDefn, jobId, SUCCESS);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testUpdateJobClusterArtifactWithAutoSubmit() {
        TestKit probe = new TestKit(system);
        try {
            String clusterName = "testUpdateJobClusterArtifactWithAutoSubmit";
            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            // when running concurrently with testGetJobDetailsForArchivedJob the following mock return is needed to avoid null pointer exception.
            when(jobStoreMock.getArchivedJob(anyString())).thenReturn(empty());

            SLA sla = new SLA(1,1,null,null);
            final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, Lists.newArrayList(),sla);

            ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
            jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
            JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
            assertEquals(SUCCESS, createResp.responseCode);

            // submit job with different scheduling info instance count compared to cluster default one.
            final int job1InstanceCnt = 3;
            final JobDefinition jobDefn = createJob(
                    clusterName,
                    MantisJobDurationType.Transient,
                    new SchedulingInfo.Builder().numberOfStages(1)
                            .addStage(fakeJobCluster.getJobClusterConfig().getSchedulingInfo().forStage(1).toBuilder()
                                    .numberOfInstances(job1InstanceCnt)
                                    .build())
                            .build());

            String jobId = clusterName + "-1";

            jobClusterActor.tell(new SubmitJobRequest(clusterName,"user", jobDefn), probe.getRef());
            SubmitJobResponse submitResponse = probe.expectMsgClass(SubmitJobResponse.class);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe,jobClusterActor,jobId, BaseResponse.ResponseCode.SUCCESS,JobState.Accepted);

            // Update artifact with skip submit = false
            String artifact = "newartifact.zip";
            String version = "0.0.2";
            jobClusterActor.tell(new UpdateJobClusterArtifactRequest(clusterName, artifact, "http://" + artifact, version,false, user), probe.getRef());
            UpdateJobClusterArtifactResponse resp = probe.expectMsgClass(UpdateJobClusterArtifactResponse.class);

            // ensure new job was launched
            String jobId2 = clusterName + "-2";
            assertTrue(JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor, jobId2, JobState.Accepted));

            // send it worker events to move it to started state
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId2,1,new WorkerId(clusterName,jobId2,0,1));

            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId2).get()), probe.getRef());
            GetJobDetailsResponse detailsResp = probe.expectMsgClass(Duration.ofSeconds(5), GetJobDetailsResponse.class);
            assertEquals(JobState.Accepted, detailsResp.getJobMetadata().get().getState());

            assertEquals(artifact, detailsResp.getJobMetadata().get().getArtifactName());

            // verify newly launched job inherited instance count from previous job instance.
            AtomicBoolean hasStage = new AtomicBoolean(false);
            detailsResp.getJobMetadata().get().getSchedulingInfo().getStages().forEach((stageId, stageInfo) -> {
                hasStage.set(true);
                assertEquals(
                        job1InstanceCnt,
                        detailsResp.getJobMetadata().get().getSchedulingInfo().forStage(stageId).getNumberOfInstances());
            });
            assertTrue(hasStage.get());

            assertTrue(JobTestHelper.verifyJobStatusWithPolling(probe, jobClusterActor, jobId2, JobState.Accepted));


        } catch (InvalidJobException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJobSubmitFails() {
        TestKit probe = new TestKit(system);
        try {
            String clusterName = "testJobSubmitFails";
            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
            Mockito.doThrow(Exception.class).when(jobStoreMock).storeNewJob(any());
            ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
            jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
            JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
            assertEquals(SUCCESS, createResp.responseCode);


            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            jobClusterActor.tell(new SubmitJobRequest(clusterName,"user", jobDefn), probe.getRef());
            SubmitJobResponse submitResponse = probe.expectMsgClass(SubmitJobResponse.class);

            assertEquals(SERVER_ERROR, submitResponse.responseCode);
            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());
            verify(jobStoreMock, times(0)).storeNewWorker(any());
            verify(jobStoreMock, times(0)).storeNewWorkers(any(),any());
        } catch (Exception e) {
            fail();
        }
    }



    ////////////////////////////////// JOB SUBMIT OPERATIONS END/////////////////////////////////////////////////////////////

    ////////////////////////////////// OTHER JOB OPERATIONS //////////////////////////////////////////////////////////////

    @Test
    public void testGetLastSubmittedJobSubject() {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetLastSubmittedJobSubject";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {

            jobClusterActor.tell(new GetLastSubmittedJobIdStreamRequest(clusterName), probe.getRef());
            GetLastSubmittedJobIdStreamResponse getLastSubmittedJobIdStreamResponse = probe.expectMsgClass(GetLastSubmittedJobIdStreamResponse.class);

            assertEquals(SUCCESS, getLastSubmittedJobIdStreamResponse.responseCode);

            CountDownLatch jobIdLatch = new CountDownLatch(1);
            assertTrue(getLastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject().isPresent());
            BehaviorSubject<JobId> jobIdBehaviorSubject =
                    getLastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject().get();

            jobIdBehaviorSubject.subscribeOn(Schedulers.io()).subscribe((jId) -> {
                System.out.println("Got Jid ------> " + jId);
                String jIdStr = jId.getId();
                assertEquals(clusterName + "-1",jIdStr);
                jobIdLatch.countDown();
            });



            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(
                probe, jobClusterActor, jobId,1, new WorkerId(clusterName,jobId,0,1));


            jobIdLatch.await(1,TimeUnit.SECONDS);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testGetLastSubmittedJobSubjectWithWrongClusterNameFails() {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetLastSubmittedJobSubjectWithWrongClusterNameFails";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {

            jobClusterActor.tell(new GetLastSubmittedJobIdStreamRequest("randomCluster"), probe.getRef());
            GetLastSubmittedJobIdStreamResponse getLastSubmittedJobIdStreamResponse = probe.expectMsgClass(GetLastSubmittedJobIdStreamResponse.class);

            assertEquals(CLIENT_ERROR, getLastSubmittedJobIdStreamResponse.responseCode);


            assertTrue(!getLastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject().isPresent());


            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);



        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
        //Mockito.doThrow(IOException.class).when(jobStoreMock).storeNewJob(any());

    }

    @Test
    public void testListArchivedWorkers() {
        TestKit probe = new TestKit(system);
        String clusterName = "testListArchivedWorkers";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisScheduler scheduler = mock(MantisScheduler.class);


        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStore, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        String jobId = clusterName + "-1";

        try {

            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);


            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            jobClusterActor.tell(new ResubmitWorkerRequest(jobId,1,user,of("justbecause")),probe.getRef());

            ResubmitWorkerResponse resp = probe.expectMsgClass(ResubmitWorkerResponse.class);
            assertTrue(BaseResponse.ResponseCode.SUCCESS.equals(resp.responseCode));


            jobClusterActor.tell(new ListArchivedWorkersRequest(new JobId(clusterName, 1)),probe.getRef());
            ListArchivedWorkersResponse archivedWorkersResponse = probe.expectMsgClass(ListArchivedWorkersResponse.class);

            assertEquals(SUCCESS, archivedWorkersResponse.responseCode);
            assertEquals(1,archivedWorkersResponse.getWorkerMetadata().size());
            IMantisWorkerMetadata archivedWorker = archivedWorkersResponse.getWorkerMetadata().get(0);
            assertEquals(1,archivedWorker.getWorkerNumber());
            assertEquals(0,archivedWorker.getWorkerIndex());
            assertEquals(0, archivedWorker.getResubmitOf());


            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);



        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
    }

    @Test
    @Ignore("todo: fix")
    public void testZombieWorkerKilledOnMessage() {
        String clusterName = "testZombieWorkerKilledOnMessage";
        TestKit probe = new TestKit(system);
        MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, schedulerMockFactory, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            String jobId = clusterName + "-1";
            when(jobStoreMock.getArchivedJob(jobId))
                .thenReturn(Optional.of(
                    new MantisJobMetadataImpl.Builder().withJobDefinition(mock(JobDefinition.class)).build()));
            WorkerId workerId = new WorkerId(clusterName, jobId,0,1);
            WorkerEvent heartBeat2 = new WorkerHeartbeat(new Status(jobId, 1, workerId.getWorkerIndex(), workerId.getWorkerNum(), TYPE.HEARTBEAT, "", MantisJobState.Started, System.currentTimeMillis()));
            jobClusterActor.tell(heartBeat2, probe.getRef());
            jobClusterActor.tell(new GetJobClusterRequest(clusterName),probe.getRef());
            GetJobClusterResponse resp = probe.expectMsgClass(GetJobClusterResponse.class);
            assertEquals(clusterName,resp.getJobCluster().get().getName());
            verify(schedulerMock,times(1)).unscheduleAndTerminateWorker(workerId,empty());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testZombieWorkerTerminateEventIgnored() {
        TestKit probe = new TestKit(system);
        String clusterName = "testZombieWorkerTerminateEventIgnored";
        MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, schedulerMockFactory, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);

        try {
            String jobId = clusterName + "-1";
            WorkerId workerId = new WorkerId(clusterName, jobId,0,1);
            JobTestHelper.sendWorkerTerminatedEvent(probe,jobClusterActor,jobId,workerId);

            verify(schedulerMock,times(0)).unscheduleAndTerminateWorker(workerId,empty());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void testResubmitWorker() {
        TestKit probe = new TestKit(system);
        String clusterName = "testResubmitWorker";
        MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, schedulerMockFactory, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(jobId,0,1));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Launched);

            jobClusterActor.tell(new ResubmitWorkerRequest(jobId,1,user,of("justbecause")),probe.getRef());

            ResubmitWorkerResponse resp = probe.expectMsgClass(ResubmitWorkerResponse.class);
            assertTrue(BaseResponse.ResponseCode.SUCCESS.equals(resp.responseCode));

            jobClusterActor.tell(new GetJobDetailsRequest("nj", JobId.fromId(jobId).get()), probe.getRef());
            GetJobDetailsResponse detailsResp = probe.expectMsgClass(GetJobDetailsResponse.class);


            IMantisWorkerMetadata workerMetadata = detailsResp.getJobMetadata().get().getWorkerByIndex(1,0).get().getMetadata();

            assertEquals(2,workerMetadata.getWorkerNumber());
            assertEquals(1,workerMetadata.getResubmitOf());
            assertEquals(1, workerMetadata.getTotalResubmitCount());

            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());
            verify(jobStoreMock,times(1)).replaceTerminatedWorker(any(),any());



        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testScaleStage() {
        TestKit probe = new TestKit(system);
        try {
            String clusterName = "testScaleStage";
            MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);

            final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);

            ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, schedulerMockFactory, eventPublisher, costsCalculator, 0));
            jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
            JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
            assertEquals(SUCCESS, createResp.responseCode);

            Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
            smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
            smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));

            SchedulingInfo SINGLE_WORKER_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1)
                    .multiWorkerScalableStageWithConstraints(1,DEFAULT_MACHINE_DEFINITION,Lists.newArrayList(),Lists.newArrayList(),new StageScalingPolicy(1,1,10,1,1,1, smap, true)).build();

            final JobDefinition jobDefn = createJob(clusterName, 1, MantisJobDurationType.Transient, "USER_TYPE", SINGLE_WORKER_SCHED_INFO, Lists.newArrayList());

            String jobId = clusterName + "-1";

            jobClusterActor.tell(new SubmitJobRequest(clusterName, "user", jobDefn), probe.getRef());
            SubmitJobResponse submitResponse = probe.expectMsgClass(SubmitJobResponse.class);

            System.out.println("SUBMIT RESP: " + submitResponse);

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobClusterActor, jobId, 0, new WorkerId(clusterName, jobId, 0, 1));
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobClusterActor, jobId, 1, new WorkerId(clusterName, jobId, 0, 2));

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, BaseResponse.ResponseCode.SUCCESS, JobState.Launched);

            jobClusterActor.tell(new ScaleStageRequest(jobId, 1, 2, user,"No reason"), probe.getRef());
            ScaleStageResponse scaleResp = probe.expectMsgClass(ScaleStageResponse.class);
            System.out.println("scale Resp: " + scaleResp.message);
            assertEquals(SUCCESS, scaleResp.responseCode);
            assertEquals(2,scaleResp.getActualNumWorkers());

            verify(jobStoreMock, times(1)).storeNewJob(any());
            // initial worker
            verify(jobStoreMock, times(1)).storeNewWorkers(any(),any());

            //scale up worker
            verify(jobStoreMock, times(1)).storeNewWorker(any());

            verify(jobStoreMock, times(6)).updateWorker(any());

            verify(jobStoreMock, times(3)).updateJob(any());

            // initial worker and scale up worker
            verify(schedulerMock, times(2)).scheduleWorkers(any());



        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    ////////////////////////////////// OTHER JOB OPERATIONS //////////////////////////////////////////////////////////////
    /////////////////////////// JOB LIST OPERATIONS /////////////////////////////////////////////////////////////////

    @Test
    public void testGetJobDetailsForArchivedJob() {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetJobDetailsForArchivedJob";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);
        String jobId = clusterName + "-1";

        try {

            when(jobStoreMock.getArchivedJob(jobId)).thenReturn(of(new MantisJobMetadataImpl.Builder()
                    .withJobState(JobState.Completed)
                    .withJobId(new JobId(clusterName, 1))
                    .withSubmittedAt(1000)
                    .withNextWorkerNumToUse(2)
                    .build()));
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);


            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

            JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

            JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);



            jobClusterActor.tell(new WorkerTerminate(new WorkerId(clusterName + "-1",0,1),WorkerState.Completed,JobCompletedReason.Killed,System.currentTimeMillis()),probe.getRef());

            Thread.sleep(1000);

            jobClusterActor.tell(new GetJobDetailsRequest(user,jobId),probe.getRef());
            GetJobDetailsResponse resp = probe.expectMsgClass(GetJobDetailsResponse.class);
//
            assertEquals(SUCCESS,resp.responseCode);
            assertEquals(JobState.Completed, resp.getJobMetadata().get().getState());

            verify(jobStoreMock, times(1)).createJobCluster(any());
            verify(jobStoreMock, times(1)).updateJobCluster(any());
     //       verify(jobStoreMock, times(1)).getArchivedJob(jobId);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testListJobIdsForCluster() throws InvalidJobException {
        TestKit probe = new TestKit(system);
        String clusterName = "testListJobsForCluster";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        final JobDefinition jobDefn1 = createJob(clusterName);
        String jobId = clusterName + "-1";

        JobTestHelper.submitJobAndVerifySuccess(probe,clusterName, jobClusterActor, jobDefn1, jobId);

        String jobId2 = clusterName + "-2";

        JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn1, jobId2);

        jobClusterActor.tell(new ListJobIdsRequest(), probe.getRef());

        ListJobIdsResponse listResp = probe.expectMsgClass(ListJobIdsResponse.class);

        assertEquals(SUCCESS, listResp.responseCode);

        assertEquals(2, listResp.getJobIds().size());

        boolean foundJob1 = false;
        boolean foundJob2 = false;
        for(JobClusterProtoAdapter.JobIdInfo jobIdInfo : listResp.getJobIds()) {
            if(jobIdInfo.getJobId().equals(jobId)) {
                foundJob1 = true;
            } else if(jobIdInfo.getJobId().equals(jobId2)) {
                foundJob2 = true;
            }
        }

        assertTrue(foundJob1);
        assertTrue(foundJob2);

        JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

        jobClusterActor.tell(
            new ListJobIdsRequest(empty(),
                empty(),
                of(true),
                empty(),
                empty(),
                empty(),
                empty()),
            probe.getRef());

        ListJobIdsResponse listResp2 = probe.expectMsgClass(ListJobIdsResponse.class);

        assertEquals(SUCCESS, listResp2.responseCode);

        assertEquals(1, listResp2.getJobIds().size());

//        assertFalse(listResp2.getJobIds().contains(JobId.fromId(jobId).get()));
//        assertTrue(listResp2.getJobIds().contains(JobId.fromId(jobId2).get()));

         foundJob1 = false;
         foundJob2 = false;
        for(JobClusterProtoAdapter.JobIdInfo jobIdInfo : listResp2.getJobIds()) {
            if(jobIdInfo.getJobId().equals(jobId)) {
                foundJob1 = true;
            } else if(jobIdInfo.getJobId().equals(jobId2)) {
                foundJob2 = true;
            }
        }

        assertFalse(foundJob1);
        assertTrue(foundJob2);



        JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

        jobClusterActor.tell(new ListJobIdsRequest(), probe.getRef());

        ListJobIdsResponse listResp3 = probe.expectMsgClass(ListJobIdsResponse.class);

        assertEquals(SUCCESS, listResp3.responseCode);

        assertEquals(0, listResp3.getJobIds().size());

//        assertFalse(listResp3.getJobIds().contains(JobId.fromId(jobId).get()));
//        assertFalse(listResp3.getJobIds().contains(JobId.fromId(jobId2).get()));

         foundJob1 = false;
         foundJob2 = false;
        for(JobClusterProtoAdapter.JobIdInfo jobIdInfo : listResp3.getJobIds()) {
            if(jobIdInfo.getJobId().equals(jobId)) {
                foundJob1 = true;
            } else if(jobIdInfo.getJobId().equals(jobId2)) {
                foundJob2 = true;
            }
        }

        assertFalse(foundJob1);
        assertFalse(foundJob2);


    }

    @Test
    public void testListJobsForCluster() throws InvalidJobException, InterruptedException {
        TestKit probe = new TestKit(system);
        String clusterName = "testListJobsForCluster";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        final JobDefinition jobDefn1 = createJob(clusterName);
        String jobId = clusterName + "-1";

        JobTestHelper.submitJobAndVerifySuccess(probe,clusterName, jobClusterActor, jobDefn1, jobId);

        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId,1,new WorkerId(clusterName,jobId,0,1));

        String jobId2 = clusterName + "-2";

        JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn1, jobId2);

        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobClusterActor,jobId2,1,new WorkerId(clusterName,jobId2,0,1));

        jobClusterActor.tell(new ListJobsRequest(), probe.getRef());

        //   Thread.sleep(1000);

        ListJobsResponse listResp = probe.expectMsgClass(ListJobsResponse.class);

        assertEquals(SUCCESS, listResp.responseCode);

        assertEquals(2, listResp.getJobList().size());

        //        assertTrue(listResp.getJobIds().contains(JobId.fromId(jobId).get()));
        //        assertTrue(listResp.getJobIds().contains(JobId.fromId(jobId2).get()));

        JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);


        jobClusterActor.tell(new ListJobsRequest(new ListJobCriteria(empty(),
                empty(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                of(true),
                empty(),
                empty(),
                empty(),
                empty())), probe.getRef());

        ListJobsResponse listResp2 = probe.expectMsgClass(ListJobsResponse.class);

        assertEquals(SUCCESS, listResp2.responseCode);

        assertEquals(1, listResp2.getJobList().size());

        //        assertFalse(listResp2.getJobIds().contains(JobId.fromId(jobId).get()));
        //        assertTrue(listResp2.getJobIds().contains(JobId.fromId(jobId2).get()));


        JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 2), jobClusterActor);

        jobClusterActor.tell(new ListJobsRequest(new ListJobCriteria(empty(),
            empty(),
            Lists.newArrayList(),
            Lists.newArrayList(),
            Lists.newArrayList(),
            Lists.newArrayList(),
            of(true),
            empty(),
            empty(),
            empty(),
            empty())), probe.getRef());

        ListJobsResponse listResp3 = probe.expectMsgClass(ListJobsResponse.class);

        assertEquals(SUCCESS, listResp3.responseCode);

        assertEquals(0, listResp3.getJobList().size());

        //        assertFalse(listResp3.getJobIds().contains(JobId.fromId(jobId).get()));
        //        assertFalse(listResp3.getJobIds().contains(JobId.fromId(jobId2).get()));

    }



    @Test
    public void testGetLastSubmittedJob() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetLastSubmittedJob";

        final JobDefinition jobDefn1 = createJob(clusterName);


        JobId jobId3 = new JobId(clusterName, 3);

        JobInfo jInfo3 = new JobInfo(jobId3,jobDefn1, 1000L, null, JobState.Launched, "user1");
        JobId jobId4 = new JobId(clusterName, 4);
        JobInfo jInfo4 = new JobInfo(jobId4,jobDefn1, 2000L, null, JobState.Launched, "user1");


        JobId jobId1 = new JobId(clusterName, 1);
        JobClusterDefinitionImpl.CompletedJob cJob1 = new JobClusterDefinitionImpl.CompletedJob(clusterName, jobId1.getId(), "0.0.1", JobState.Completed, 800L, 900L, "user1", new ArrayList<>());

        JobId jobId2 = new JobId(clusterName, 2);
        JobClusterDefinitionImpl.CompletedJob cJob2 = new JobClusterDefinitionImpl.CompletedJob(clusterName, jobId2.getId(), "0.0.1", JobState.Completed, 900L, 1000L, "user1", new ArrayList<>());
        List<JobClusterDefinitionImpl.CompletedJob> completedJobs = new ArrayList<>();
        completedJobs.add(cJob1);
        completedJobs.add(cJob2);

        List<JobInfo> activeList = new ArrayList<>();
        activeList.add(jInfo3);
        activeList.add(jInfo4);

        Optional<JobId> lastJobIdOp = JobListHelper.getLastSubmittedJobId(activeList,completedJobs);
        assertTrue(lastJobIdOp.isPresent());
        assertEquals(jobId4, lastJobIdOp.get());


    }

    /**
     * With only completed jobs getlastSubmitted should return the completed job with highest job number
     * @throws Exception
     */

    @Test
    public void testGetLastSubmittedJobWithCompletedOnly() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetLastSubmittedJobWithCompletedOnly";

        final JobDefinition jobDefn1 = createJob(clusterName);

        JobId jobId1 = new JobId(clusterName, 1);
        JobClusterDefinitionImpl.CompletedJob cJob1 = new JobClusterDefinitionImpl.CompletedJob(clusterName, jobId1.getId(), "0.0.1", JobState.Completed, 800L, 900L, "user1", new ArrayList<>());

        JobId jobId2 = new JobId(clusterName, 2);
        JobClusterDefinitionImpl.CompletedJob cJob2 = new JobClusterDefinitionImpl.CompletedJob(clusterName, jobId2.getId(), "0.0.1", JobState.Completed, 900L, 1000L, "user1", new ArrayList<>());
        List<JobClusterDefinitionImpl.CompletedJob> completedJobs = new ArrayList<>();
        completedJobs.add(cJob1);
        completedJobs.add(cJob2);

        List<JobInfo> activeList = new ArrayList<>();

        Optional<JobId> lastJobIdOp = JobListHelper.getLastSubmittedJobId(activeList,completedJobs);
        assertTrue(lastJobIdOp.isPresent());
        assertEquals(jobId2, lastJobIdOp.get());

    }

    /**
     * No Active or completed jobs should return an empty Optional
     * @throws Exception
     */
    @Test
    public void testGetLastSubmittedJobWithNoJobs() throws Exception  {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetLastSubmittedJobWithNoJobs";

        final JobDefinition jobDefn1 = createJob(clusterName);

        List<JobClusterDefinitionImpl.CompletedJob> completedJobs = new ArrayList<>();

        List<JobInfo> activeList = new ArrayList<>();

        Optional<JobId> lastJobIdOp = JobListHelper.getLastSubmittedJobId(activeList,completedJobs);
        assertFalse(lastJobIdOp.isPresent());
    }

    @Test
    public void testListJobWithLabelMatch() {
        TestKit probe = new TestKit(system);
        String clusterName = "testListJobWithLabelMatch";
        try {
            MantisScheduler schedulerMock = mock(MantisScheduler.class);
            MantisJobStore jobStoreMock = mock(MantisJobStore.class);
            final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
            ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreMock, jobDfn -> schedulerMock, eventPublisher, costsCalculator, 0));
            jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
            JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
            assertEquals(SUCCESS, createResp.responseCode);


            final JobDefinition jobDefn1;

            List<Label> labelList1 = new ArrayList<>();
            labelList1.add(new Label("l1","l1v1"));
            jobDefn1 = createJob(clusterName, labelList1);

            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe,clusterName, jobClusterActor, jobDefn1, jobId);

            List<Label> labelList2 = new ArrayList<>();
            labelList2.add(new Label("l2","l2v2"));

            String jobId2 = clusterName + "-2";


            JobDefinition jobDefn2 = createJob(clusterName, labelList2);

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn2, jobId2);

            // Query for Label1
            List<Integer> emptyIntList = Lists.newArrayList();
            List<WorkerState.MetaState> workerState = Lists.newArrayList();
            ListJobCriteria criteria1 = new ListJobCriteria(Optional.empty(), Optional.empty(),
                emptyIntList, emptyIntList, emptyIntList, workerState, Optional.empty(),
                Optional.empty(), of("l1=l1v1"), Optional.empty(),
                empty());

            jobClusterActor.tell(new ListJobsRequest(criteria1), probe.getRef());
            ListJobsResponse listResp = probe.expectMsgClass(ListJobsResponse.class);

            assertEquals(SUCCESS, listResp.responseCode);

            // Only job1 should be returned

            assertEquals(1, listResp.getJobList().size());

            assertEquals(jobId, listResp.getJobList().get(0).getJobMetadata().getJobId());

            assertTrue(listResp.getJobList().get(0).getStageMetadataList().size() == 1);

            System.out.println("Workers returned : " + listResp.getJobList().get(0).getWorkerMetadataList());
            assertTrue(listResp.getJobList().get(0).getWorkerMetadataList().size() == 1);
            // Query with an OR query for both labels
            ListJobCriteria criteria2 = new ListJobCriteria(Optional.empty(), Optional.empty(),
                emptyIntList, emptyIntList, emptyIntList, workerState, Optional.empty(),
                Optional.empty(), of("l1=l1v1,l2=l2v2"), Optional.empty(),
                empty());

            jobClusterActor.tell(new ListJobsRequest(criteria2), probe.getRef());
            ListJobsResponse listRes2 = probe.expectMsgClass(ListJobsResponse.class);

            assertEquals(SUCCESS, listRes2.responseCode);

            // Both jobs should be returned

            assertEquals(2, listRes2.getJobList().size());

            assertTrue(jobId.equals(listRes2.getJobList().get(0).getJobMetadata().getJobId()) || jobId.equals(listRes2.getJobList().get(1).getJobMetadata().getJobId()));
            assertTrue(jobId2.equals(listRes2.getJobList().get(0).getJobMetadata().getJobId()) || jobId2.equals(listRes2.getJobList().get(1).getJobMetadata().getJobId()));


            // Query with an AND query for both labels
            ListJobCriteria criteria3 = new ListJobCriteria(Optional.empty(), Optional.empty(),
                emptyIntList, emptyIntList, emptyIntList, workerState, Optional.empty(),
                Optional.empty(), of("l1=l1v1,l2=l2v2"), of("and"),
                empty());

            jobClusterActor.tell(new ListJobsRequest(criteria3), probe.getRef());
            ListJobsResponse listRes3 = probe.expectMsgClass(ListJobsResponse.class);

            assertEquals(SUCCESS, listRes3.responseCode);

            // No jobs should be returned

            assertEquals(0, listRes3.getJobList().size());


        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testLostWorkerGetsReplaced() {

        TestKit probe = new TestKit(system);
        String clusterName = "testLostWorkerGetsReplaced";
        MantisSchedulerFactory schedulerMockFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        //MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        MantisJobStore jobStoreSpied = Mockito.spy(jobStore);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName);
        ActorRef jobClusterActor = system.actorOf(props(clusterName, jobStoreSpied, schedulerMockFactory, eventPublisher, costsCalculator, 0));
        jobClusterActor.tell(new JobClusterProto.InitializeJobClusterRequest(fakeJobCluster, user, probe.getRef()), probe.getRef());
        JobClusterProto.InitializeJobClusterResponse createResp = probe.expectMsgClass(JobClusterProto.InitializeJobClusterResponse.class);
        assertEquals(SUCCESS, createResp.responseCode);


        try {
            final JobDefinition jobDefn = createJob(clusterName,1, MantisJobDurationType.Transient);
            String jobId = clusterName + "-1";

            JobTestHelper.submitJobAndVerifySuccess(probe, clusterName, jobClusterActor, jobDefn, jobId);

       //     JobTestHelper.getJobDetailsAndVerify(probe, jobClusterActor, jobId, SUCCESS, JobState.Accepted);

       //     JobTestHelper.killJobAndVerify(probe, clusterName, new JobId(clusterName, 1), jobClusterActor);

            verify(jobStoreSpied, times(1)).createJobCluster(any());
            verify(jobStoreSpied, times(1)).updateJobCluster(any());

            int stageNo = 1;
            // send launched event

            WorkerId workerId = new WorkerId(jobId, 0, 1);


            // send heartbeat

            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobClusterActor, jobId, stageNo, workerId);

            // check job status again
            jobClusterActor.tell(new GetJobDetailsRequest("nj", jobId), probe.getRef());
            //jobActor.tell(new JobProto.InitJob(probe.getRef()), probe.getRef());
            GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);
            System.out.println("resp " + resp2 + " msg " + resp2.message);
            assertEquals(SUCCESS, resp2.responseCode);

            // Job started
            assertEquals(JobState.Launched, resp2.getJobMetadata().get().getState());

            // send launched event



            // worker 2 gets terminated abnormally
            JobTestHelper.sendWorkerTerminatedEvent(probe, jobClusterActor, jobId, workerId);

            // replaced worker comes up and sends events
            WorkerId workerId2_replaced = new WorkerId(jobId, 0, 2);
            JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe, jobClusterActor, jobId, stageNo, workerId2_replaced);

            jobClusterActor.tell(new GetJobDetailsRequest("nj", jobId), probe.getRef());

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

            jobClusterActor.tell(new ListJobsRequest(), probe.getRef());

            ListJobsResponse listResp2 = probe.expectMsgClass(ListJobsResponse.class);

            assertEquals(SUCCESS, listResp2.responseCode);

            assertEquals(1, listResp2.getJobList().size());

            for(MantisJobMetadataView jb : listResp2.getJobList() ) {
                System.out.println("Jb -> " + jb);
            }

            //assertEquals(jobActor, probe.getLastSender());
        } catch (InvalidJobException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            system.stop(jobClusterActor);
        }

    }



        @Test
    public void testExpireOldJobs() {
        //TODO
    }



}
