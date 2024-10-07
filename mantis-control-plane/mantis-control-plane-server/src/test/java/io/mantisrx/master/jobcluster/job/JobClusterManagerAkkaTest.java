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

import static io.mantisrx.master.jobcluster.JobClusterAkkaTest.DEFAULT_JOB_OWNER;
import static io.mantisrx.master.jobcluster.JobClusterAkkaTest.NO_OP_SLA;
import static io.mantisrx.master.jobcluster.JobClusterAkkaTest.TWO_WORKER_SCHED_INFO;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR_CONFLICT;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR_NOT_FOUND;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS_CREATED;
import static java.util.Optional.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.MantisJobClusterMetadataView;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.JobClustersManagerInitializeResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.WorkerMigrationConfig.MigrationStrategyEnum;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.Status.TYPE;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

public class JobClusterManagerAkkaTest {

    static ActorSystem system;
    private MantisJobStore jobStoreMock;
    private ActorRef jobClusterManagerActor;
    private MantisSchedulerFactory schedulerMockFactory;
    private MantisScheduler schedulerMock;
    private static LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(
        new AuditEventSubscriberLoggingImpl(),
        new StatusEventSubscriberLoggingImpl(),
        new WorkerEventSubscriberLoggingImpl());
    private final CostsCalculator costsCalculator = CostsCalculator.noop();
    private static final String user = "nj";

    @Rule
    public TemporaryFolder rootDir = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = new Timeout(2000);

    @BeforeClass
    public static void setup() {
        Config config = ConfigFactory.parseString("akka {\n" +
            "  loggers = [\"akka.testkit.TestEventListener\"]\n" +
            "  loglevel = \"WARNING\"\n" +
            "  stdout-loglevel = \"WARNING\"\n" +
            "}\n");
        system = ActorSystem.create(
            "JobClusterManagerTest",
            config.withFallback(ConfigFactory.load()));

        TestHelpers.setupMasterConfig();
    }

    @Before
    public void setupState() {
        jobStoreMock = mock(MantisJobStore.class);
        schedulerMockFactory = mock(MantisSchedulerFactory.class);
        schedulerMock = mock(MantisScheduler.class);
        when(schedulerMockFactory.forJob(any())).thenReturn(schedulerMock);
        jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreMock,
            eventPublisher,
            costsCalculator,
            0));
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            true), ActorRef.noSender());
    }

    @AfterClass
    public static void tearDown() {
        JobTestHelper.deleteAllFiles();
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    private JobClusterDefinitionImpl createFakeJobClusterDefn(
        final String name,
        List<Label> labels) {
        return createFakeJobClusterDefn(name, labels, WorkerMigrationConfig.DEFAULT);
    }

    private JobClusterDefinitionImpl createFakeJobClusterDefn(
        final String name,
        List<Label> labels,
        WorkerMigrationConfig migrationConfig) {
        String artifactName = "myart";
        if (labels.stream().noneMatch(l -> l.getName().equals("_mantis.resourceCluster"))) {
            labels.add(new Label("_mantis.resourceCluster", "akkaTestCluster1"));
        }

        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
            .withJobJarUrl("http://" + artifactName)
            .withArtifactName(artifactName)
            .withSchedulingInfo(new SchedulingInfo.Builder().numberOfStages(1)
                .singleWorkerStageWithConstraints(
                    new MachineDefinition(
                        0,
                        0,
                        0,
                        0,
                        0),
                    Lists.newArrayList(),
                    Lists.newArrayList())
                .build())
            .withVersion("0.0.1")

            .build();
        return new JobClusterDefinitionImpl.Builder()
            .withName(name)
            .withUser(user)
            .withJobClusterConfig(clusterConfig)
            .withParameters(Lists.newArrayList())
            .withLabels(labels)
            .withSla(new SLA(0, 1, null, IJobClusterDefinition.CronPolicy.KEEP_EXISTING))
            .withIsReadyForJobMaster(true)
            .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
            .withMigrationConfig(migrationConfig)
            .build();


    }

    private JobDefinition createJob(String name2) throws InvalidJobException {
        return new JobDefinition.Builder()
            .withName(name2)
            .withParameters(Lists.newArrayList())
            .withLabels(Lists.newArrayList())
            .withSchedulingInfo(new SchedulingInfo.Builder().numberOfStages(1)
                .singleWorkerStageWithConstraints(
                    new MachineDefinition(
                        1,
                        10,
                        10,
                        10,
                        2),
                    Lists.newArrayList(),
                    Lists.newArrayList())
                .build())
            .withJobJarUrl("http://myart")
            .withArtifactName("myart")
            .withSubscriptionTimeoutSecs(0)
            .withUser("njoshi")
            .withJobSla(new JobSla(0, 0, null, MantisJobDurationType.Transient, null))
            .build();

    }

    private void createJobClusterAndAssert(ActorRef jobClusterManagerActor, String clusterName) {
        createJobClusterAndAssert(
            jobClusterManagerActor,
            clusterName,
            WorkerMigrationConfig.DEFAULT);
    }

    private void createJobClusterAndAssert(
        ActorRef jobClusterManagerActor,
        String clusterName,
        WorkerMigrationConfig migrationConfig) {
        TestKit probe = new TestKit(system);
        JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList(),
            migrationConfig);
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(resp.toString(), SUCCESS_CREATED, resp.responseCode);
    }

    private void submitJobAndAssert(ActorRef jobClusterManagerActor, String cluster) {
        TestKit probe = new TestKit(system);
        JobDefinition jobDefn;
        try {
            jobDefn = createJob(cluster);
            jobClusterManagerActor.tell(
                new JobClusterManagerProto.SubmitJobRequest(
                    cluster,
                    "me",
                    jobDefn),
                probe.getRef());
            JobClusterManagerProto.SubmitJobResponse submitResp = probe.expectMsgClass(
                JobClusterManagerProto.SubmitJobResponse.class);
            assertEquals(SUCCESS, submitResp.responseCode);

        } catch (InvalidJobException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testBootStrapJobClustersAndJobs1() {

        TestKit probe = new TestKit(system);
        JobTestHelper.deleteAllFiles();
        MantisJobStore jobStore = new MantisJobStore(new KeyValueBasedPersistenceProvider(
            new FileBasedStore(rootDir.getRoot()),
            eventPublisher));
        MantisJobStore jobStoreSpied = Mockito.spy(jobStore);
//        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreSpied,
            eventPublisher,
            costsCalculator,
            0));
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            true), probe.getRef());
        JobClustersManagerInitializeResponse iResponse = probe.expectMsgClass(Duration.of(
                10,
                ChronoUnit.MINUTES),
            JobClustersManagerInitializeResponse.class);
        //List<String> clusterNames = Lists.newArrayList("testBootStrapJobClustersAndJobs1");
        String clusterWithNoJob = "testBootStrapJobClusterWithNoJob";
        createJobClusterAndAssert(jobClusterManagerActor, clusterWithNoJob);

        // kill 1 of the jobs to test archive path
        // Stop job cluster Manager Actor
        system.stop(jobClusterManagerActor);

        // create new instance
        jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStore,
            eventPublisher,
            costsCalculator,
            0));
        // initialize it
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            true), probe.getRef());

        //JobClusterManagerProto.JobClustersManagerInitializeResponse initializeResponse = probe.expectMsgClass(JobClusterManagerProto.JobClustersManagerInitializeResponse.class);
        JobClustersManagerInitializeResponse initializeResponse = probe.expectMsgClass(Duration.of(
            10,
            ChronoUnit.MINUTES), JobClustersManagerInitializeResponse.class);

        assertEquals(SUCCESS, initializeResponse.responseCode);

        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterWithNoJob), probe.getRef());
        GetJobClusterResponse jobClusterResponse = probe.expectMsgClass(Duration.of(
                10,
                ChronoUnit.MINUTES),
            GetJobClusterResponse.class);

        assertEquals(SUCCESS, jobClusterResponse.responseCode);
        assertTrue(jobClusterResponse.getJobCluster().isPresent());
        assertEquals(clusterWithNoJob, jobClusterResponse.getJobCluster().get().getName());

//        // 1 running worker
//        verify(schedulerMock,timeout(100_1000).times(1)).initializeRunningWorker(any(),any());
//
//        // 2 worker schedule requests
//        verify(schedulerMock,timeout(100_000).times(4)).scheduleWorker(any());

        try {
            Mockito.verify(jobStoreSpied).loadAllJobClusters();
            Mockito.verify(jobStoreSpied).loadAllActiveJobs();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }


    }

    @Test
    public void testBootStrapJobClustersAndJobsNegativeTest() throws IOException {

        TestKit probe = new TestKit(system);
        JobTestHelper.deleteAllFiles();
        KeyValueBasedPersistenceProvider storageProviderAdapter = mock(
            KeyValueBasedPersistenceProvider.class);
        when(storageProviderAdapter.loadAllJobClusters()).thenThrow(new IOException(
            "StorageException"));
        MantisJobStore jobStore = new MantisJobStore(storageProviderAdapter);
        MantisJobStore jobStoreSpied = Mockito.spy(jobStore);
//        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreSpied,
            eventPublisher,
            costsCalculator,
            0));
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            true), probe.getRef());
        JobClustersManagerInitializeResponse iResponse = probe.expectMsgClass(Duration.of(
                10,
                ChronoUnit.MINUTES),
            JobClustersManagerInitializeResponse.class);

        assertEquals(BaseResponse.ResponseCode.SERVER_ERROR, iResponse.responseCode);

    }


    @Test
    public void testBootStrapJobClustersAndJobs() {

        TestKit probe = new TestKit(system);
        JobTestHelper.deleteAllFiles();
        MantisJobStore jobStore = new MantisJobStore(new KeyValueBasedPersistenceProvider(
            new FileBasedStore(rootDir.getRoot()),
            eventPublisher));
        MantisJobStore jobStoreSpied = Mockito.spy(jobStore);
        ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreSpied,
            eventPublisher,
            costsCalculator,
            0));
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            false), probe.getRef());
        JobClustersManagerInitializeResponse iResponse = probe.expectMsgClass(Duration.of(
                10,
                ChronoUnit.MINUTES),
            JobClustersManagerInitializeResponse.class);
        List<String> clusterNames = Lists.newArrayList("testBootStrapJobClustersAndJobs1",
            "testBootStrapJobClustersAndJobs2",
            "testBootStrapJobClustersAndJobs3");
        String clusterWithNoJob = "testBootStrapJobClusterWithNoJob";
        createJobClusterAndAssert(jobClusterManagerActor, clusterWithNoJob);

        WorkerMigrationConfig migrationConfig = new WorkerMigrationConfig(
            MigrationStrategyEnum.PERCENTAGE,
            "{\"percentToMove\":60, \"intervalMs\":30000}");
        // Create 3 clusters and submit 1 job each
        for (String cluster : clusterNames) {

            createJobClusterAndAssert(jobClusterManagerActor, cluster, migrationConfig);

            submitJobAndAssert(jobClusterManagerActor, cluster);

            if (cluster.equals("testBootStrapJobClustersAndJobs1")) {
                // send worker events for job 1 so it goes to started state
                String jobId = "testBootStrapJobClustersAndJobs1-1";
                WorkerId workerId = new WorkerId(jobId, 0, 1);
                WorkerEvent launchedEvent = new WorkerLaunched(
                    workerId,
                    0,
                    "host1",
                    "vm1",
                    empty(),
                    Optional.empty(),
                    new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));

                jobClusterManagerActor.tell(launchedEvent, probe.getRef());

                WorkerEvent startInitEvent = new WorkerStatus(new Status(
                    workerId.getJobId(),
                    1,
                    workerId.getWorkerIndex(),
                    workerId.getWorkerNum(),
                    TYPE.INFO,
                    "test START_INIT",
                    MantisJobState.StartInitiated));
                jobClusterManagerActor.tell(startInitEvent, probe.getRef());

                WorkerEvent heartBeat = new WorkerHeartbeat(new Status(
                    jobId,
                    1,
                    workerId.getWorkerIndex(),
                    workerId.getWorkerNum(),
                    TYPE.HEARTBEAT,
                    "",
                    MantisJobState.Started));
                jobClusterManagerActor.tell(heartBeat, probe.getRef());

                // get Job status
                jobClusterManagerActor.tell(
                    new GetJobDetailsRequest(
                        "user",
                        JobId.fromId(jobId).get()),
                    probe.getRef());
                GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);

                // Ensure its launched
                assertEquals(SUCCESS, resp2.responseCode);
                assertEquals(JobState.Launched, resp2.getJobMetadata().get().getState());
            }


        }
// kill 1 of the jobs to test archive path
        JobClusterManagerProto.KillJobRequest killRequest = new JobClusterManagerProto.KillJobRequest(
            "testBootStrapJobClustersAndJobs2-1",
            JobCompletedReason.Killed.toString(),
            "njoshi");
        jobClusterManagerActor.tell(killRequest, probe.getRef());

        JobClusterManagerProto.KillJobResponse killJobResponse = probe.expectMsgClass(
            JobClusterManagerProto.KillJobResponse.class);
        assertEquals(SUCCESS, killJobResponse.responseCode);

        JobTestHelper.sendWorkerTerminatedEvent(
            probe,
            jobClusterManagerActor,
            "testBootStrapJobClustersAndJobs2-1",
            new WorkerId("testBootStrapJobClustersAndJobs2-1",
                0,
                1));

        jobClusterManagerActor.tell(new GetJobDetailsRequest(
            "user",
            JobId.fromId("testBootStrapJobClustersAndJobs3-1")
                .get()), probe.getRef());
        GetJobDetailsResponse acceptedResponse = probe.expectMsgClass(
            Duration.of(10, ChronoUnit.MINUTES),
            GetJobDetailsResponse.class);

        // Ensure its Accepted
        assertEquals(SUCCESS, acceptedResponse.responseCode);
        assertEquals(JobState.Accepted, acceptedResponse.getJobMetadata().get().getState());

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Stop job cluster Manager Actor
        system.stop(jobClusterManagerActor);

        // create new instance
        jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreSpied,
            eventPublisher,
            costsCalculator,
            0));
        // initialize it
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            true), probe.getRef());

        JobClustersManagerInitializeResponse initializeResponse = probe.expectMsgClass(
            JobClustersManagerInitializeResponse.class);
        //probe.expectMsgClass(Duration.of(10, ChronoUnit.MINUTES),JobClusterManagerProto.JobClustersManagerInitializeResponse.class);
        //probe.expectMsgClass(JobClusterManagerProto.JobClustersManagerInitializeResponse.class);
        assertEquals(SUCCESS, initializeResponse.responseCode);

        // Get Cluster Config
        jobClusterManagerActor.tell(new GetJobClusterRequest("testBootStrapJobClustersAndJobs1"),
            probe.getRef());
        GetJobClusterResponse clusterResponse = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, clusterResponse.responseCode);
        assertTrue(clusterResponse.getJobCluster().isPresent());
        WorkerMigrationConfig mConfig = clusterResponse.getJobCluster().get().getMigrationConfig();
        assertEquals(migrationConfig.getStrategy(), mConfig.getStrategy());
        assertEquals(migrationConfig.getConfigString(), migrationConfig.getConfigString());

        // get Job status

        jobClusterManagerActor.tell(new GetJobDetailsRequest(
            "user",
            JobId.fromId("testBootStrapJobClustersAndJobs1-1")
                .get()), probe.getRef());
        GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);

        // Ensure its launched
        System.out.println("Resp2 -> " + resp2);
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(JobState.Launched, resp2.getJobMetadata().get().getState());

        // 1 jobs should be in completed state
        jobClusterManagerActor.tell(new GetJobDetailsRequest(
            "user",
            JobId.fromId("testBootStrapJobClustersAndJobs2-1")
                .get()), probe.getRef());
        resp2 = probe.expectMsgClass(Duration.of(10, ChronoUnit.MINUTES),
            GetJobDetailsResponse.class);

        //TODO(hmitnflx): Need to fix this test after support completed jobs async loading
        // Ensure its completed
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(JobState.Completed, resp2.getJobMetadata().get().getState());

        jobClusterManagerActor.tell(new GetJobDetailsRequest(
            "user",
            JobId.fromId("testBootStrapJobClustersAndJobs3-1")
                .get()), probe.getRef());
        resp2 = probe.expectMsgClass(Duration.of(10, ChronoUnit.MINUTES),
            GetJobDetailsResponse.class);

        // Ensure its Accepted
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(JobState.Accepted, resp2.getJobMetadata().get().getState());

        try {
            Optional<JobWorker> workerByIndex = resp2.getJobMetadata().get().getWorkerByIndex(1, 0);
            assertTrue(workerByIndex.isPresent());

            Optional<IMantisStageMetadata> stageMetadata = resp2.getJobMetadata()
                .get()
                .getStageMetadata(1);
            assertTrue(stageMetadata.isPresent());

            JobWorker workerByIndex1 = stageMetadata.get().getWorkerByIndex(0);
            System.out.println("Got worker by index : " + workerByIndex1);

            Optional<JobWorker> worker = resp2.getJobMetadata().get().getWorkerByNumber(1);

            assertTrue(worker.isPresent());

        } catch (io.mantisrx.server.master.persistence.exceptions.InvalidJobException e) {
            e.printStackTrace();
        }

        jobClusterManagerActor.tell(new GetLastSubmittedJobIdStreamRequest(
            "testBootStrapJobClustersAndJobs1"), probe.getRef());
        GetLastSubmittedJobIdStreamResponse lastSubmittedJobIdStreamResponse = probe.expectMsgClass(
            Duration.of(10, ChronoUnit.MINUTES),
            GetLastSubmittedJobIdStreamResponse.class);
        lastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject()
            .get()
            .take(1)
            .toBlocking()
            .subscribe((jId) -> {
                assertEquals(new JobId(
                    "testBootStrapJobClustersAndJobs1",
                    1), jId);
            });

        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterWithNoJob), probe.getRef());
        GetJobClusterResponse jobClusterResponse = probe.expectMsgClass(Duration.of(
                10,
                ChronoUnit.MINUTES),
            GetJobClusterResponse.class);

        assertEquals(SUCCESS, jobClusterResponse.responseCode);
        assertTrue(jobClusterResponse.getJobCluster().isPresent());
        assertEquals(clusterWithNoJob, jobClusterResponse.getJobCluster().get().getName());

        // 1 running worker
        verify(schedulerMock, timeout(100_1000).times(1)).initializeRunningWorker(any(), any(),
            any());

        // 2 worker schedule requests
        verify(schedulerMock, timeout(100_000).times(4)).scheduleWorkers(any());

        try {
            Mockito.verify(jobStoreSpied).loadAllArchivedJobsAsync();
            Mockito.verify(jobStoreSpied).loadAllActiveJobs();
            Mockito.verify(jobStoreSpied).archiveWorker(any());
            Mockito.verify(jobStoreSpied).archiveJob(any());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }


    }

    /**
     * Case for a master leader re-election when a new master re-hydrates corrupted job worker
     * metadata.
     */
    @Test
    public void testBootstrapJobClusterAndJobsWithCorruptedWorkerPorts()
        throws IOException, io.mantisrx.server.master.persistence.exceptions.InvalidJobException {

        TestKit probe = new TestKit(system);
        JobTestHelper.deleteAllFiles();
        MantisJobStore jobStore = new MantisJobStore(new KeyValueBasedPersistenceProvider(
            new FileBasedStore(rootDir.getRoot()),
            eventPublisher));
        MantisJobStore jobStoreSpied = Mockito.spy(jobStore);
//        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreSpied,
            eventPublisher,
            costsCalculator,
            0));
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            false), probe.getRef());
        probe.expectMsgClass(Duration.of(
                10,
                ChronoUnit.MINUTES),
            JobClustersManagerInitializeResponse.class);
        String jobClusterName = "testBootStrapJobClustersAndJobs1";
        WorkerMigrationConfig migrationConfig = new WorkerMigrationConfig(
            MigrationStrategyEnum.PERCENTAGE,
            "{\"percentToMove\":60, \"intervalMs\":30000}");

        createJobClusterAndAssert(jobClusterManagerActor, jobClusterName, migrationConfig);
        submitJobAndAssert(jobClusterManagerActor, jobClusterName);
        String jobId = "testBootStrapJobClustersAndJobs1-1";
        WorkerId workerId = new WorkerId(jobId, 0, 1);
        WorkerEvent launchedEvent = new WorkerLaunched(
            workerId,
            0,
            "host1",
            "vm1",
            empty(),
            Optional.empty(),
            new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));

        jobClusterManagerActor.tell(launchedEvent, probe.getRef());

        WorkerEvent startInitEvent = new WorkerStatus(new Status(
            workerId.getJobId(),
            1,
            workerId.getWorkerIndex(),
            workerId.getWorkerNum(),
            TYPE.INFO,
            "test START_INIT",
            MantisJobState.StartInitiated));
        jobClusterManagerActor.tell(startInitEvent, probe.getRef());

        WorkerEvent heartBeat = new WorkerHeartbeat(new Status(
            jobId,
            1,
            workerId.getWorkerIndex(),
            workerId.getWorkerNum(),
            TYPE.HEARTBEAT,
            "",
            MantisJobState.Started));
        jobClusterManagerActor.tell(heartBeat, probe.getRef());

        // get Job status
        jobClusterManagerActor.tell(
            new GetJobDetailsRequest(
                "user",
                JobId.fromId(jobId).get()),
            probe.getRef());
        GetJobDetailsResponse resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);

        // Ensure its launched
        assertEquals(SUCCESS, resp2.responseCode);

        JobWorker worker = new JobWorker.Builder()
            .withWorkerIndex(0)
            .withWorkerNumber(1)
            .withJobId(jobId)
            .withStageNum(1)
            .withNumberOfPorts(5)
            .withWorkerPorts(null)
            .withState(WorkerState.Started)
            .withLifecycleEventsPublisher(eventPublisher)
            .build();
        jobStoreSpied.updateWorker(worker.getMetadata());

        // Stop job cluster Manager Actor
        system.stop(jobClusterManagerActor);

        // create new instance
        jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(
            jobStoreSpied,
            eventPublisher,
            costsCalculator,
            0));
        // initialize it
        jobClusterManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
            schedulerMockFactory,
            true), probe.getRef());

        JobClustersManagerInitializeResponse initializeResponse = probe.expectMsgClass(
            JobClustersManagerInitializeResponse.class);
        assertEquals(SUCCESS, initializeResponse.responseCode);

        WorkerId newWorkerId = new WorkerId(jobId, 0, 11);
        launchedEvent = new WorkerLaunched(
            newWorkerId,
            0,
            "host1",
            "vm1",
            empty(),
            Optional.empty(),
            new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));
        jobClusterManagerActor.tell(launchedEvent, probe.getRef());
        // Get Cluster Config
        jobClusterManagerActor.tell(new GetJobClusterRequest("testBootStrapJobClustersAndJobs1"),
            probe.getRef());
        GetJobClusterResponse clusterResponse = probe.expectMsgClass(GetJobClusterResponse.class);

        assertEquals(SUCCESS, clusterResponse.responseCode);
        assertTrue(clusterResponse.getJobCluster().isPresent());
        WorkerMigrationConfig mConfig = clusterResponse.getJobCluster().get().getMigrationConfig();
        assertEquals(migrationConfig.getStrategy(), mConfig.getStrategy());
        assertEquals(migrationConfig.getConfigString(), migrationConfig.getConfigString());

        // get Job status
        jobClusterManagerActor.tell(new GetJobDetailsRequest(
            "user",
            JobId.fromId("testBootStrapJobClustersAndJobs1-1")
                .get()), probe.getRef());
        resp2 = probe.expectMsgClass(GetJobDetailsResponse.class);

        // Ensure its launched
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(JobState.Launched, resp2.getJobMetadata().get().getState());

        IMantisWorkerMetadata mantisWorkerMetadata = resp2.getJobMetadata().get()
            .getWorkerByIndex(1, 0).get()
            .getMetadata();
        assertNotNull(mantisWorkerMetadata.getWorkerPorts());
        assertEquals(11, mantisWorkerMetadata.getWorkerNumber());
        assertEquals(1, mantisWorkerMetadata.getTotalResubmitCount());

        jobClusterManagerActor.tell(new GetLastSubmittedJobIdStreamRequest(
            "testBootStrapJobClustersAndJobs1"), probe.getRef());
        GetLastSubmittedJobIdStreamResponse lastSubmittedJobIdStreamResponse = probe.expectMsgClass(
            Duration.of(10, ChronoUnit.MINUTES),
            GetLastSubmittedJobIdStreamResponse.class);
        lastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject()
            .get()
            .take(1)
            .toBlocking()
            .subscribe((jId) -> {
                assertEquals(new JobId(
                    "testBootStrapJobClustersAndJobs1",
                    1), jId);
            });

        // Two schedules: one for the initial success, one for a resubmit from corrupted worker ports.
        verify(schedulerMock, times(2)).scheduleWorkers(any());
        // One unschedule from corrupted worker ID 1 (before the resubmit).
        verify(schedulerMock, times(1)).unscheduleAndTerminateWorker(eq(workerId), any());

        try {
            Mockito.verify(jobStoreSpied).loadAllArchivedJobsAsync();
            Mockito.verify(jobStoreSpied).loadAllActiveJobs();
            Mockito.verify(jobStoreSpied).archiveWorker(any());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJobClusterCreate() throws MalformedURLException {

        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterCreateCluster";
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp2 = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(clusterName, resp2.getJobCluster().get().getName());

        //assertEquals(jobClusterManagerActor, probe.getLastSender().path());

    }

    @Test
    public void testJobClusterCreateDupFails() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterCreateDupFails";
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp2 = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, resp2.responseCode);
        assertEquals(clusterName, resp2.getJobCluster().get().getName());

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp3 = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        System.out.println("Got resp -> " + resp3);
        assertEquals(CLIENT_ERROR_CONFLICT, resp3.responseCode);

        // make sure first cluster is still there
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse resp4 = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, resp4.responseCode);
        assertEquals(clusterName, resp4.getJobCluster().get().getName());

        //assertEquals(jobClusterManagerActor, probe.getLastSender().path());

    }

    @Test
    public void testListJobClusters() {
        TestKit probe = new TestKit(system);
        String clusterName = "testListJobClusters";
        JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        String clusterName2 = "testListJobClusters2";
        fakeJobCluster = createFakeJobClusterDefn(clusterName2, Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        resp = probe.expectMsgClass(JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        jobClusterManagerActor.tell(
            new JobClusterManagerProto.ListJobClustersRequest(),
            probe.getRef());
        JobClusterManagerProto.ListJobClustersResponse resp2 = probe.expectMsgClass(
            JobClusterManagerProto.ListJobClustersResponse.class);

        assertTrue(2 <= resp2.getJobClusters().size());
        List<MantisJobClusterMetadataView> jClusters = resp2.getJobClusters();
        int cnt = 0;
        for (MantisJobClusterMetadataView jCluster : jClusters) {
            if (jCluster.getName().equals(clusterName) || jCluster.getName().equals(clusterName2)) {
                cnt++;
            }
        }
        assertEquals(2, cnt);

    }

    @Test
    public void testListJobs() throws InvalidJobException {
        TestKit probe = new TestKit(system);
        //create cluster 1
        String clusterName = "testListJobs";
        JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        // submit job to this cluster
        JobDefinition jobDefn = createJob(clusterName);
        jobClusterManagerActor.tell(
            new JobClusterManagerProto.SubmitJobRequest(
                clusterName,
                "me",
                jobDefn),
            probe.getRef());
        JobClusterManagerProto.SubmitJobResponse submitResp = probe.expectMsgClass(
            JobClusterManagerProto.SubmitJobResponse.class);
        assertEquals(SUCCESS, submitResp.responseCode);

        // create cluster 2
        String clusterName2 = "testListJobs2";
        fakeJobCluster = createFakeJobClusterDefn(clusterName2, Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        resp = probe.expectMsgClass(JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        // submit job to this cluster
        jobDefn = createJob(clusterName2);
        jobClusterManagerActor.tell(
            new JobClusterManagerProto.SubmitJobRequest(
                clusterName2,
                "me",
                jobDefn),
            probe.getRef());
        submitResp = probe.expectMsgClass(JobClusterManagerProto.SubmitJobResponse.class);
        assertEquals(SUCCESS, submitResp.responseCode);

        jobClusterManagerActor.tell(new JobClusterManagerProto.ListJobsRequest(), probe.getRef());
        JobClusterManagerProto.ListJobsResponse listResp = probe.expectMsgClass(
            JobClusterManagerProto.ListJobsResponse.class);

        System.out.println("Got " + listResp.getJobList().size());
        boolean foundJob1 = false;
        boolean foundJob2 = false;
        for (MantisJobMetadataView v : listResp.getJobList()) {
            System.out.println("Job -> " + v.getJobMetadata().getJobId());
            String jId = v.getJobMetadata().getJobId();
            if (jId.equals("testListJobs-1")) {
                foundJob1 = true;
            } else if (jId.equals("testListJobs2-1")) {
                foundJob2 = true;
            }
        }
        assertTrue(listResp.getJobList().size() >= 2);
        assertTrue(foundJob1 && foundJob2);


    }


    @Test
    public void testJobClusterUpdateAndDelete() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterUpdateAndDeleteCluster";
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname", "labelvalue");
        labels.add(l);
        labels.add(new Label("_mantis.resourceCluster", "cl2"));
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);

        String artifactName = "myart2";
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
            .withJobJarUrl("http://" + artifactName)
            .withArtifactName(artifactName)
            .withSchedulingInfo(TWO_WORKER_SCHED_INFO)
            .withVersion("0.0.2")
            .build();

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

        jobClusterManagerActor.tell(new JobClusterManagerProto.UpdateJobClusterRequest(
            updatedFakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.UpdateJobClusterResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.UpdateJobClusterResponse.class);

        if (SUCCESS != updateResp.responseCode) {
            System.out.println("Update cluster response: " + updateResp);
        }
        assertEquals(SUCCESS, updateResp.responseCode);
        // assertEquals(jobClusterManagerActor, probe.getLastSender());

        jobClusterManagerActor.tell(
            new JobClusterManagerProto.DeleteJobClusterRequest(user,
                clusterName),
            probe.getRef());
        JobClusterManagerProto.DeleteJobClusterResponse deleteResp = probe.expectMsgClass(
            JobClusterManagerProto.DeleteJobClusterResponse.class);
        assertEquals(SUCCESS, deleteResp.responseCode);
        // assertEquals(jobClusterManagerActor, probe.getLastSender());

    }

    @Test
    public void testJobClusterSLAUpdate() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterSLAUpdate";
        List<Label> labels = Lists.newLinkedList();
        Label l = new Label("labelname", "labelvalue");
        labels.add(l);
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);
        UpdateJobClusterSLARequest req = new JobClusterManagerProto.UpdateJobClusterSLARequest(
            clusterName,
            1,
            2,
            "user");
        jobClusterManagerActor.tell(req, probe.getRef());
        JobClusterManagerProto.UpdateJobClusterSLAResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.UpdateJobClusterSLAResponse.class);

        assertEquals(SUCCESS, updateResp.responseCode);
        // assertEquals(jobClusterManagerActor, probe.getLastSender());

        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        assertEquals(1, getResp.getJobCluster().get().getSla().getMin());
        assertEquals(2, getResp.getJobCluster().get().getSla().getMax());
        // assertEquals(jobClusterManagerActor, probe.getLastSender());

    }

    @Test
    public void testJobClusterLabelUpdate() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterLabelUpdate";
        List<Label> labels = Lists.newLinkedList();
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);

        List<Label> labels2 = Lists.newLinkedList();
        Label l = new Label("labelname", "labelvalue");
        labels2.add(l);
        labels2.add(new Label("_mantis.resourceCluster", "cl2"));

        UpdateJobClusterLabelsRequest req = new JobClusterManagerProto.UpdateJobClusterLabelsRequest(
            clusterName,
            labels2,
            "user");
        jobClusterManagerActor.tell(req, probe.getRef());
        JobClusterManagerProto.UpdateJobClusterLabelsResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.UpdateJobClusterLabelsResponse.class);

        assertEquals(SUCCESS, updateResp.responseCode);
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        assertEquals(2, getResp.getJobCluster().get().getLabels().size());
        assertEquals(l, getResp.getJobCluster().get().getLabels().get(0));
    }

    @Test
    public void testJobClusterArtifactUpdate() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterArtifactUpdate";
        List<Label> labels = Lists.newLinkedList();
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);

        UpdateJobClusterArtifactRequest req = new JobClusterManagerProto.UpdateJobClusterArtifactRequest(
            clusterName,
            "myjar",
            "http://myjar",
            "1.0.1",
            true,
            "user");
        jobClusterManagerActor.tell(req, probe.getRef());
        JobClusterManagerProto.UpdateJobClusterArtifactResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.UpdateJobClusterArtifactResponse.class);

        assertEquals(SUCCESS, updateResp.responseCode);
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        //assertEquals("myjar", getResp.getJobCluster().get().g.getArtifactName());
        assertEquals("1.0.1", getResp.getJobCluster().get().getLatestVersion());
    }

    @Test
    public void testJobClusterWorkerMigrationUpdate() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterWorkerMigrationUpdate";
        List<Label> labels = Lists.newLinkedList();
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);

        UpdateJobClusterWorkerMigrationStrategyRequest req = new JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest(
            clusterName,
            new WorkerMigrationConfig(MigrationStrategyEnum.ONE_WORKER, "{}"),
            clusterName);
        jobClusterManagerActor.tell(req, probe.getRef());
        JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse.class);

        assertEquals(SUCCESS, updateResp.responseCode);
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        assertEquals(
            MigrationStrategyEnum.ONE_WORKER,
            getResp.getJobCluster().get().getMigrationConfig().getStrategy());

    }

    @Test
    public void testJobClusterDisable() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterDisable";
        List<Label> labels = Lists.newLinkedList();
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);

        DisableJobClusterRequest req = new JobClusterManagerProto.DisableJobClusterRequest(
            clusterName,
            "user");
        jobClusterManagerActor.tell(req, probe.getRef());
        JobClusterManagerProto.DisableJobClusterResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.DisableJobClusterResponse.class);

        assertEquals(SUCCESS, updateResp.responseCode);
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        assertTrue(getResp.getJobCluster().get().isDisabled());

    }

    @Test
    public void testJobClusterEnable() throws MalformedURLException {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobClusterEnable";
        List<Label> labels = Lists.newLinkedList();
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            labels);

        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse createResp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        assertEquals(SUCCESS_CREATED, createResp.responseCode);

        DisableJobClusterRequest req = new JobClusterManagerProto.DisableJobClusterRequest(
            clusterName,
            "user");
        jobClusterManagerActor.tell(req, probe.getRef());
        JobClusterManagerProto.DisableJobClusterResponse updateResp = probe.expectMsgClass(
            JobClusterManagerProto.DisableJobClusterResponse.class);

        assertEquals(SUCCESS, updateResp.responseCode);
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        GetJobClusterResponse getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        assertTrue(getResp.getJobCluster().get().isDisabled());

        EnableJobClusterRequest req2 = new JobClusterManagerProto.EnableJobClusterRequest(
            clusterName,
            "user");
        jobClusterManagerActor.tell(req2, probe.getRef());
        JobClusterManagerProto.EnableJobClusterResponse updateResp2 = probe.expectMsgClass(
            JobClusterManagerProto.EnableJobClusterResponse.class);

        assertEquals(SUCCESS, updateResp2.responseCode);
        jobClusterManagerActor.tell(new GetJobClusterRequest(clusterName), probe.getRef());
        getResp = probe.expectMsgClass(GetJobClusterResponse.class);
        assertEquals(SUCCESS, getResp.responseCode);
        assertFalse(getResp.getJobCluster().get().isDisabled());

    }


    @Test
    public void testJobSubmit() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmit";
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        System.out.println("response----->" + resp);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        JobDefinition jobDefn;
        try {
            jobDefn = createJob(clusterName);
            jobClusterManagerActor.tell(
                new JobClusterManagerProto.SubmitJobRequest(
                    clusterName,
                    "me",
                    jobDefn),
                probe.getRef());
            JobClusterManagerProto.SubmitJobResponse submitResp = probe.expectMsgClass(
                JobClusterManagerProto.SubmitJobResponse.class);
            assertEquals(SUCCESS, submitResp.responseCode);

            jobClusterManagerActor.tell(
                new JobClusterManagerProto.KillJobRequest(
                    clusterName + "-1",
                    "",
                    clusterName),
                probe.getRef());
            JobClusterManagerProto.KillJobResponse kill = probe.expectMsgClass(
                JobClusterManagerProto.KillJobResponse.class);
        } catch (InvalidJobException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }

        //assertEquals(jobClusterManagerActor, probe.getLastSender().path());

    }

    @Test
    public void testWorkerList() {
        TestKit probe = new TestKit(system);
        String clusterName = "testWorkerList";
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        System.out.println("response----->" + resp);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        JobDefinition jobDefn;
        try {
            jobDefn = createJob(clusterName);
            jobClusterManagerActor.tell(
                new JobClusterManagerProto.SubmitJobRequest(
                    clusterName,
                    "me",
                    jobDefn),
                probe.getRef());
            JobClusterManagerProto.SubmitJobResponse submitResp = probe.expectMsgClass(
                JobClusterManagerProto.SubmitJobResponse.class);
            assertEquals(SUCCESS, submitResp.responseCode);

            jobClusterManagerActor.tell(new JobClusterManagerProto.ListWorkersRequest(new JobId(
                clusterName,
                1)), probe.getRef());
            JobClusterManagerProto.ListWorkersResponse listWorkersResponse = probe.expectMsgClass(
                JobClusterManagerProto.ListWorkersResponse.class);

            assertEquals(SUCCESS, listWorkersResponse.responseCode);
            assertEquals(1, listWorkersResponse.getWorkerMetadata().size());

            // send list workers request to non existent cluster

            jobClusterManagerActor.tell(new JobClusterManagerProto.ListWorkersRequest(new JobId(
                "randomCluster",
                1)), probe.getRef());
            JobClusterManagerProto.ListWorkersResponse listWorkersResponse2 = probe.expectMsgClass(
                JobClusterManagerProto.ListWorkersResponse.class);

            assertEquals(CLIENT_ERROR, listWorkersResponse2.responseCode);
            assertEquals(0, listWorkersResponse2.getWorkerMetadata().size());

            jobClusterManagerActor.tell(
                new JobClusterManagerProto.KillJobRequest(
                    clusterName + "-1",
                    "",
                    clusterName),
                probe.getRef());
            JobClusterManagerProto.KillJobResponse kill = probe.expectMsgClass(
                JobClusterManagerProto.KillJobResponse.class);
        } catch (InvalidJobException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }

        //assertEquals(jobClusterManagerActor, probe.getLastSender().path());

    }

    @Test
    public void testGetJobIdSubject() {
        TestKit probe = new TestKit(system);
        String clusterName = "testGetJobIdSubject";
        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        System.out.println("response----->" + resp);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        JobDefinition jobDefn;
        try {

            jobClusterManagerActor.tell(
                new GetLastSubmittedJobIdStreamRequest(clusterName),
                probe.getRef());
            JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse getLastSubmittedJobIdStreamResponse = probe
                .expectMsgClass(JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse.class);

            assertEquals(SUCCESS, getLastSubmittedJobIdStreamResponse.responseCode);

            CountDownLatch jobIdLatch = new CountDownLatch(2);
            assertTrue(getLastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject().isPresent());
            BehaviorSubject<JobId> jobIdBehaviorSubject =
                getLastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject().get();

            jobIdBehaviorSubject.subscribeOn(Schedulers.io()).subscribe((jId) -> {
                System.out.println("Got Jid -> " + jId);
                if (jId.getId().endsWith("1")) {
                    assertEquals(clusterName + "-1", jId.getId());
                    jobIdLatch.countDown();
                }
                else if (jId.getId().endsWith("2")) {
                    assertEquals(clusterName + "-2", jId.getId());
                    jobIdLatch.countDown();
                }
            });

            jobDefn = createJob(clusterName);
            jobClusterManagerActor.tell(
                new JobClusterManagerProto.SubmitJobRequest(
                    clusterName,
                    "me",
                    jobDefn),
                probe.getRef());
            JobClusterManagerProto.SubmitJobResponse submitResp = probe.expectMsgClass(
                JobClusterManagerProto.SubmitJobResponse.class);
            assertEquals(SUCCESS, submitResp.responseCode);
            assertTrue(submitResp.getJobId().isPresent());

            // mark job as launched
            WorkerId workerId = new WorkerId(submitResp.getJobId().get().getId(), 0, 1);
            WorkerEvent startEvent = new WorkerHeartbeat(new Status(
                submitResp.getJobId().get().getId(),
                1,
                workerId.getWorkerIndex(),
                workerId.getWorkerNum(),
                TYPE.HEARTBEAT,
                "",
                MantisJobState.Started));
            WorkerEvent launchEvent = new WorkerLaunched(
                workerId,
                1,
                "hostname",
                "vmid1",
                Optional.empty(),
                Optional.empty(),
                new WorkerPorts(1, 2, 3, 4, 5));

            jobClusterManagerActor.tell(launchEvent, probe.getRef());
            jobClusterManagerActor.tell(startEvent, probe.getRef());

            jobClusterManagerActor.tell(
                new JobClusterManagerProto.SubmitJobRequest(
                    clusterName,
                    "me",
                    jobDefn),
                probe.getRef());
            submitResp = probe.expectMsgClass(
                JobClusterManagerProto.SubmitJobResponse.class);
            assertEquals(SUCCESS, submitResp.responseCode);
            assertTrue(submitResp.getJobId().isPresent());
            workerId = new WorkerId(submitResp.getJobId().get().getId(), 0, 1);
            startEvent = new WorkerHeartbeat(new Status(
                submitResp.getJobId().get().getId(),
                1,
                workerId.getWorkerIndex(),
                workerId.getWorkerNum(),
                TYPE.HEARTBEAT,
                "",
                MantisJobState.Started));
            launchEvent = new WorkerLaunched(
                workerId,
                1,
                "hostname",
                "vmid1",
                Optional.empty(),
                Optional.empty(),
                new WorkerPorts(1, 2, 3, 4, 5));

            jobClusterManagerActor.tell(launchEvent, probe.getRef());
            jobClusterManagerActor.tell(startEvent, probe.getRef());

            assertTrue(jobIdLatch.await(10, TimeUnit.SECONDS));
            // try a non existent cluster
            jobClusterManagerActor.tell(
                new GetLastSubmittedJobIdStreamRequest("randomC"),
                probe.getRef());
            getLastSubmittedJobIdStreamResponse = probe.expectMsgClass(
                JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse.class);

            assertEquals(CLIENT_ERROR_NOT_FOUND, getLastSubmittedJobIdStreamResponse.responseCode);

            assertTrue(!getLastSubmittedJobIdStreamResponse.getjobIdBehaviorSubject().isPresent());

            jobClusterManagerActor.tell(
                new JobClusterManagerProto.KillJobRequest(
                    clusterName + "-1",
                    "",
                    clusterName),
                probe.getRef());
            JobClusterManagerProto.KillJobResponse kill = probe.expectMsgClass(
                JobClusterManagerProto.KillJobResponse.class);
        } catch (InvalidJobException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //assertEquals(jobClusterManagerActor, probe.getLastSender().path());

    }

    @Test
    public void testJobSubmitToNonExistentCluster() {
        TestKit probe = new TestKit(system);
        String clusterName = "testJobSubmitToNonExistentClusterCluster";

        JobDefinition jobDefn;
        try {
            jobDefn = createJob(clusterName);
            jobClusterManagerActor.tell(
                new JobClusterManagerProto.SubmitJobRequest(
                    clusterName,
                    "me",
                    jobDefn),
                probe.getRef());
            JobClusterManagerProto.SubmitJobResponse submitResp = probe.expectMsgClass(
                JobClusterManagerProto.SubmitJobResponse.class);
            assertEquals(CLIENT_ERROR_NOT_FOUND, submitResp.responseCode);

        } catch (InvalidJobException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail();
        }

        //assertEquals(jobClusterManagerActor, probe.getLastSender().path());

    }

    @Test
    public void testTerminalEventFromZombieWorkerIgnored() {
        TestKit probe = new TestKit(system);
        String clusterName = "testZombieWorkerHandling";

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        System.out.println("response----->" + resp);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        WorkerId zWorker1 = new WorkerId("randomCluster2", "randomCluster2-1", 0, 1);
        JobTestHelper.sendWorkerTerminatedEvent(probe,
            jobClusterManagerActor,
            "randomCluster2-1",
            zWorker1);

        verify(schedulerMock, timeout(1_000).times(0)).unscheduleAndTerminateWorker(zWorker1,
            empty());


    }

    @Test
    public void testNonTerminalEventFromZombieWorkerLeadsToTermination() throws IOException {
        TestKit probe = new TestKit(system);
        String clusterName = "testNonTerminalEventFromZombieWorkerLeadsToTermination";

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(
            clusterName,
            Lists.newArrayList());
        jobClusterManagerActor.tell(new JobClusterManagerProto.CreateJobClusterRequest(
            fakeJobCluster,
            "user"), probe.getRef());
        JobClusterManagerProto.CreateJobClusterResponse resp = probe.expectMsgClass(
            JobClusterManagerProto.CreateJobClusterResponse.class);
        System.out.println("response----->" + resp);
        assertEquals(SUCCESS_CREATED, resp.responseCode);

        WorkerId zWorker1 = new WorkerId("randomCluster", "randomCluster-1", 0, 1);
        when(jobStoreMock.loadCompletedJobsForCluster(any(), anyInt(), any()))
            // .thenReturn(ImmutableList.of());
            .thenReturn(ImmutableList.of(
                new CompletedJob(
                    clusterName,
                    clusterName + "-1",
                    "v1",
                    JobState.Completed,
                    -1L,
                    -1L,
                    "ut",
                    ImmutableList.of())));
        when(jobStoreMock.getArchivedJob(zWorker1.getJobId()))
            .thenReturn(Optional.of(
                new MantisJobMetadataImpl.Builder().withJobDefinition(mock(JobDefinition.class))
                    .build()));
        JobTestHelper.sendStartInitiatedEvent(probe, jobClusterManagerActor, 1, zWorker1);

        verify(schedulerMock, timeout(1_000).times(1)).unscheduleAndTerminateWorker(zWorker1,
            empty());


    }


}
