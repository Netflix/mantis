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

package io.mantisrx.master.api.akka.route.v1;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.stream.javadsl.Flow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.api.akka.payloads.JobPayloads;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.MantisMasterRoute;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import io.mantisrx.master.api.akka.route.v0.AgentClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobDiscoveryRoute;
import io.mantisrx.master.api.akka.route.v0.JobRoute;
import io.mantisrx.master.api.akka.route.v0.JobStatusRoute;
import io.mantisrx.master.api.akka.route.v0.MasterDescriptionRoute;
import io.mantisrx.master.events.*;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.payloads.JobClusterPayloads;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.vm.AgentClusterOperations;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.LeadershipManagerLocalImpl;
import io.mantisrx.server.master.persistence.MantisJobStore;

import io.mantisrx.server.master.scheduler.MantisScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class JobsRouteTest extends RouteTestBase {
    private final static Logger logger = LoggerFactory.getLogger(JobsRouteTest.class);
    private static Thread t;
    private static final int SERVER_PORT = 8204;
    private static CompletionStage<ServerBinding> binding;

    private static final String TEST_CLUSTER = "sine-function";
    private static final String TEST_JOB_ID = "sine-function-1";

    JobsRouteTest() {
        super("JobsRoute", SERVER_PORT);
    }

    @BeforeClass
    public void setup() throws Exception {
        JobTestHelper.deleteAllFiles();
        JobTestHelper.createDirsIfRequired();
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
                        new AuditEventSubscriberLoggingImpl(),
                        new StatusEventSubscriberLoggingImpl(),
                        new WorkerEventSubscriberLoggingImpl());

                ActorRef jobClustersManagerActor = system.actorOf(JobClustersManagerActor.props(
                        new MantisJobStore(new io.mantisrx.server.master.persistence.SimpleCachedFileStorageProvider(
                                true)), lifecycleEventPublisher), "jobClustersManager");

                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                jobClustersManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
                        fakeScheduler,
                        false), ActorRef.noSender());

                final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(
                        jobClustersManagerActor);
                final JobRouteHandler jobRouteHandler = new JobRouteHandlerAkkaImpl(
                        jobClustersManagerActor);

                MasterDescription masterDescription = new MasterDescription(
                        "127.0.0.1",
                        "127.0.0.1",
                        SERVER_PORT,
                        SERVER_PORT,
                        SERVER_PORT,
                        "api/postjobstatus",
                        SERVER_PORT,
                        System.currentTimeMillis());

                Duration idleTimeout = system.settings()
                                             .config()
                                             .getDuration("akka.http.server.idle-timeout");

                logger.info("idle timeout {} sec ", idleTimeout.getSeconds());
                final AgentClusterOperations mockAgentClusterOps = mock(AgentClusterOperations.class);
                final JobStatusRouteHandler jobStatusRouteHandler = mock(JobStatusRouteHandler.class);
                when(jobStatusRouteHandler.jobStatus(anyString())).thenReturn(Flow.create());

                final JobRoute v0JobRoute = new JobRoute(jobRouteHandler, system);
                JobDiscoveryRouteHandler jobDiscoveryRouteHandler = new JobDiscoveryRouteHandlerAkkaImpl(
                        jobClustersManagerActor,
                        idleTimeout);
                final JobDiscoveryRoute v0JobDiscoveryRoute = new JobDiscoveryRoute(
                        jobDiscoveryRouteHandler);
                final JobClusterRoute v0JobClusterRoute = new JobClusterRoute(
                        jobClusterRouteHandler,
                        jobRouteHandler,
                        system);
                final JobStatusRoute v0JobStatusRoute = new JobStatusRoute(jobStatusRouteHandler);
                final AgentClusterRoute v0AgentClusterRoute = new AgentClusterRoute(
                        mockAgentClusterOps,
                        system);
                final MasterDescriptionRoute v0MasterDescriptionRoute = new MasterDescriptionRoute(
                        masterDescription);

                final JobsRoute v1JobsRoute = new JobsRoute(
                        jobClusterRouteHandler,
                        jobRouteHandler,
                        system);
                final JobClustersRoute v1JobClusterRoute = new JobClustersRoute(
                        jobClusterRouteHandler, system);
                final AgentClustersRoute v1AgentClustersRoute = new AgentClustersRoute(
                        mockAgentClusterOps);
                final JobStatusStreamRoute v1JobStatusStreamRoute = new JobStatusStreamRoute(
                        jobStatusRouteHandler);
                final AdminMasterRoute v1AdminMasterRoute = new AdminMasterRoute(masterDescription);
                final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute = new JobDiscoveryStreamRoute(
                        jobDiscoveryRouteHandler);
                final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute = new LastSubmittedJobIdStreamRoute(
                        jobDiscoveryRouteHandler);

                LocalMasterMonitor localMasterMonitor = new LocalMasterMonitor(masterDescription);
                LeadershipManagerLocalImpl leadershipMgr = new LeadershipManagerLocalImpl(
                        masterDescription);
                leadershipMgr.setLeaderReady();
                LeaderRedirectionFilter leaderRedirectionFilter = new LeaderRedirectionFilter(
                        localMasterMonitor,
                        leadershipMgr);
                final MantisMasterRoute app = new MantisMasterRoute(
                        leaderRedirectionFilter,
                        v0MasterDescriptionRoute,
                        v0JobClusterRoute,
                        v0JobRoute,
                        v0JobDiscoveryRoute,
                        v0JobStatusRoute,
                        v0AgentClusterRoute,
                        v1JobClusterRoute,
                        v1JobsRoute,
                        v1AdminMasterRoute,
                        v1AgentClustersRoute,
                        v1JobDiscoveryStreamRoute,
                        v1LastSubmittedJobIdStreamRoute,
                        v1JobStatusStreamRoute);

                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute()
                                                                              .orElse(v1JobsRoute.createRoute(
                                                                                      Function.identity()))
                                                                              .flow(
                                                                                      system,
                                                                                      materializer);
                logger.info("starting test server on port {}", SERVER_PORT);
                latch.countDown();
                binding = http.bindAndHandle(
                        routeFlow,
                        ConnectHttp.toHost("localhost", SERVER_PORT),
                        materializer);
            } catch (Exception e) {
                logger.info("caught exception", e);
                latch.countDown();
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();
        latch.await();
    }

    @AfterClass
    public void teardown() {
        logger.info("V1JobsRouteTest teardown");
        binding.thenCompose(ServerBinding::unbind) // trigger unbinding from the port
               .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }


    @Test
    public void cleanupExistingJobs() throws InterruptedException {
        super.deleteClusterIfExist(TEST_CLUSTER);
        assert !this.isClusterExist(TEST_CLUSTER);
    }

    @Test(dependsOnMethods = {"cleanupExistingJobs"})
    public void setupJobCluster() throws InterruptedException {
        testPost(
                getJobClustersEndpoint(),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_CREATE),
                StatusCodes.CREATED,
                null);

        assert this.isClusterExist(TEST_CLUSTER);
    }

    @Test(dependsOnMethods = {"setupJobCluster"})
    public void testJobSubmit() throws InterruptedException {
        testPost(
                getClusterJobsEndpoint(TEST_CLUSTER),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_SUBMIT),
                StatusCodes.CREATED,
                this::validateJobResponse);
    }

    @Test()
    public void testPutOnJobsEp_NotAllowed() throws InterruptedException {
        testPut(
                getJobsEndpoint(),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);

        testPut(
                getClusterJobsEndpoint(TEST_CLUSTER),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    @Test()
    public void testDeleteOnJobsEp_NotAllowed() throws InterruptedException {
        testDelete(
                getJobsEndpoint(),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);

        testDelete(
                getClusterJobsEndpoint(TEST_CLUSTER),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testPostOnJobInstanceEp_NotAllowed() throws InterruptedException {
        testPost(
                getJobInstanceEndpoint(TEST_JOB_ID),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);

        testPost(
                getJobInstanceEndpoint(TEST_CLUSTER, TEST_JOB_ID),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testPutOnJobInstanceEp_NotAllowed() throws InterruptedException {
        testPut(
                getJobInstanceEndpoint(TEST_JOB_ID),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);

        testPut(
                getJobInstanceEndpoint(TEST_CLUSTER, TEST_JOB_ID),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetLatestJobDiscoveryInfo() throws InterruptedException {
        testGet(
            getJobClusterLatestJobDiscoveryInfoEp(TEST_CLUSTER),
            StatusCodes.OK,
            this::validateSchedulingInfo);
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetOnJobInstanceActionsEp_NotAllowed() throws InterruptedException {

        for (String action : new String[]{"resubmitWorker", "quickSubmit", "scaleStage"}) {
            testPut(
                    getJobInstanceEndpoint(TEST_JOB_ID) + "/actions/" + action,
                    StatusCodes.METHOD_NOT_ALLOWED,
                    null);

            testPut(
                    getJobInstanceEndpoint(TEST_CLUSTER, TEST_JOB_ID) + "/actions/" + action,
                    StatusCodes.METHOD_NOT_ALLOWED,
                    null);
        }
    }


    @Test(dependsOnMethods = {"setupJobCluster"})
    public void testValidJobSubmitToNonExistentCluster() throws InterruptedException {
        testPost(
                getClusterJobsEndpoint("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_SUBMIT_NonExistent),
                StatusCodes.NOT_FOUND,
                (m) -> {
                    assert m.contains("Job Cluster NonExistent doesn't exist");
                });
    }

    @Test(dependsOnMethods = {"setupJobCluster"})
    public void testInvalidJobSubmitToNonExistentCluster() throws InterruptedException {
        testPost(
                getClusterJobsEndpoint("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_SUBMIT),
                StatusCodes.BAD_REQUEST,
                (m) -> {
                    assert m.contains("Cluster name specified in request payload [sine-function]" +
                                      " does not match with what specified in resource endpoint [NonExistent]");
                });
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobsRouteViaClusterJobsEp() throws InterruptedException {
        testGet(
                getClusterJobsEndpoint(TEST_CLUSTER),
                StatusCodes.OK,
                resp -> validateJobsListResponse(resp, 1, false));
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobsRouteViaJobsEp() throws InterruptedException {
        testGet(
                getJobsEndpoint(),
                StatusCodes.OK,
                resp -> validateJobsListResponse(resp, 1, false));
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobsRouteViaJobsEpCompactResp() throws InterruptedException {
        testGet(
                getJobsEndpoint() + "?compact=true",
                StatusCodes.OK,
                resp -> validateJobsListResponse(resp, 1, true));
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobsRouteViaClusterJobEpCompactResp() throws InterruptedException {
        testGet(
                getClusterJobsEndpoint(TEST_CLUSTER) + "?compact=true",
                StatusCodes.OK,
                resp -> validateJobsListResponse(resp, 1, true));
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobInstanceWithClusterName() throws InterruptedException {
        testGet(
                getJobInstanceEndpoint(TEST_CLUSTER, TEST_JOB_ID),
                StatusCodes.OK,
                this::validateJobDetails);
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobInstanceWithoutClusterName() throws InterruptedException {
        testGet(
                getJobInstanceEndpoint(TEST_JOB_ID),
                StatusCodes.OK,
                this::validateJobDetails);
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetNonExistentJobInstanceWithoutClusterName() throws InterruptedException {
        testGet(
                getJobInstanceEndpoint("NonExistent-1"),
                StatusCodes.NOT_FOUND,
                (m) -> {
                    assert m.contains("Job NonExistent-1 doesn't exist");
                });
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetJobInstanceWithNonMatchingClusterName() throws InterruptedException {
        testGet(
                getJobInstanceEndpoint("NonExistent", TEST_JOB_ID),
                StatusCodes.NOT_FOUND,
                (m) -> {
                    assert m.contains("JobId [sine-function-1] exists but does not " +
                                      "belong to specified cluster [NonExistent]");
                });
    }


    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testGetNonExistentJobInstance() throws InterruptedException {
        testGet(
                getJobInstanceEndpoint(TEST_CLUSTER, "NonExistent-1"),
                StatusCodes.NOT_FOUND,
                (m) -> {
                    assert m.contains("Job NonExistent-1 doesn't exist");
                });
    }

    @Test(dependsOnMethods = {"testGetNonExistentJobInstance"})
    public void testJobQuickSubmit() throws InterruptedException {
        testPost(
                getJobsEndpoint() + "/actions/quickSubmit",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.QUICK_SUBMIT),
                StatusCodes.CREATED,
                this::validateJobResponse);
    }

    @Test(dependsOnMethods = {"testJobQuickSubmit"})
    public void testNonExistentJobQuickSubmit() throws InterruptedException {
        testPost(
                getJobsEndpoint() + "/actions/quickSubmit",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.QUICK_SUBMIT_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }


    @Test(dependsOnMethods = {"testNonExistentJobQuickSubmit"})
    public void testJobResubmitWorker() throws InterruptedException {
        testPost(
                getJobInstanceEndpoint(TEST_JOB_ID) + "/actions/resubmitWorker",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobPayloads.RESUBMIT_WORKER),
                StatusCodes.NO_CONTENT,
                null);
    }

    @Test(dependsOnMethods = {"testJobResubmitWorker"})
    public void testNonExistentJobResubmitWorker() throws InterruptedException {
        testPost(
                getJobInstanceEndpoint("NonExistent-1") + "/actions/resubmitWorker",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobPayloads.RESUBMIT_WORKER_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }



    @Test(dependsOnMethods = {"testNonExistentJobResubmitWorker"})
    public void testJobScaleStage() throws InterruptedException {
        testPost(
                getJobInstanceEndpoint(TEST_JOB_ID) + "/actions/scaleStage",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobPayloads.SCALE_STAGE),
                StatusCodes.NO_CONTENT,
                null);
    }

    @Test(dependsOnMethods = {"testJobScaleStage"})
    public void testNonExistentJobScaleStage() throws InterruptedException {
        testPost(
                getJobInstanceEndpoint("NonExistent-1") + "/actions/scaleStage",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobPayloads.SCALE_STAGE_NonExistent),
                StatusCodes.NOT_FOUND,
                null);
    }

    @Test(dependsOnMethods = {"testNonExistentJobScaleStage"})
    public void testInvalidJobScaleStage() throws InterruptedException {
        testPost(
                getJobInstanceEndpoint("NonExistent-1") + "/actions/scaleStage",
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobPayloads.SCALE_STAGE),
                StatusCodes.BAD_REQUEST,
                (m) -> {
                    assert m.contains("JobId specified in request payload [sine-function-1] " +
                                      "does not match with resource uri [NonExistent-1]");
                });
    }


    @Test(dependsOnMethods = {"testInvalidJobScaleStage"})
    public void testJobKill() throws InterruptedException {
        testDelete(
                getJobInstanceEndpoint(TEST_JOB_ID) + "?user=test&reason=unittest",
                StatusCodes.ACCEPTED,
                null);
    }

    @Test(dependsOnMethods = {"testJobResubmitWorker"})
    public void testNonExistentJobKill() throws InterruptedException {
        testDelete(
                getJobInstanceEndpoint("NonExistent-1") + "?user=test&reason=unittest",
                StatusCodes.NOT_FOUND,
                null);
    }

    private void validateJobResponse(String resp) {
        try {
            assert !Strings.isNullOrEmpty(resp);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode responseObj = mapper.readTree(resp);
            assert responseObj.get("jobMetadata").get("name").asText().equals(TEST_CLUSTER);
            assert responseObj.get("jobMetadata").get("jobId").asText().startsWith("sine-function-");
            assert responseObj.get("jobMetadata").get("sla") != null;
            assert responseObj.get("jobMetadata").get("labels") != null;

            assert responseObj.get("stageMetadataList") != null;
            assert responseObj.get("workerMetadataList") != null;

        } catch (IOException ex) {
            logger.error("Failed to validate job response: " + ex.getMessage());
            assert false;
        }

    }

    private void validateJobDetails(String resp) {
        try {
            assert !Strings.isNullOrEmpty(resp);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode responseObj = mapper.readTree(resp);
            validateJobsListItem(responseObj,false);
        } catch (IOException ex) {
            logger.error("Failed to validate job details response: " + ex.getMessage());
            assert false;
        }

    }

    private void validateSchedulingInfo(String s) {
        try {
            assert !Strings.isNullOrEmpty(s);
            JobSchedulingInfo jsi = Jackson.fromJSON(s, JobSchedulingInfo.class);
            assert jsi.getJobId().equals(TEST_JOB_ID);
            Map<Integer, WorkerAssignments> wa = jsi.getWorkerAssignments();
            assert wa.size() == 2;
            assert wa.containsKey(0);
            assert wa.get(0).getNumWorkers() == 1;

            assert wa.containsKey(1);
            assert wa.get(1).getNumWorkers() == 1;
        } catch (IOException e) {
            logger.error("caught unexpected exc {}", e.getMessage(), e);
            assert false;
        }
    }

    private void validateJobsListResponse(String resp, int expectedJobsCount, boolean isCompact) {
        try {
            assert !Strings.isNullOrEmpty(resp);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode responseObj = mapper.readTree(resp).get("list");
            assert responseObj.size() == expectedJobsCount;
            for (int i = 0; i < expectedJobsCount; i++) {
                validateJobsListItem(responseObj.get(i), isCompact);
            }

        } catch (IOException ex) {
            logger.error("Failed to validate job response: " + ex.getMessage());
            assert false;
        }

    }

    private void validateJobDefinition(JsonNode responseObj) {
        assert responseObj != null;
        assert responseObj.get("name").asText().equals(TEST_CLUSTER);
        assert responseObj.get("artifactName").asText().equals(
                "https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/" +
                "mantis-examples-sine-function-0.2.9.zip");

        assert responseObj.get("parameters").size() == 2;
        assert responseObj.get("jobSla").get("durationType").asText().equals("Perpetual");
        assert responseObj.get("numberOfStages").asInt() == 2;
        assert responseObj.get("schedulingInfo") != null;
        assert responseObj.get("labels").size() == 7;
    }

    private void validateJobsListItem(JsonNode responseObj, boolean isCompact) {
        assert responseObj != null;

        if (isCompact) {
            assert responseObj.get("jobMetadata") == null;
            assert responseObj.get("stageMetadataList") == null;
            assert responseObj.get("workerMetadataList") == null;

            assert responseObj.get("submittedAt") != null;
            assert responseObj.get("user") != null;
            assert responseObj.get("type").asText().equals("Perpetual");
            assert responseObj.get("numStages").asInt() == 2;
            assert responseObj.get("numWorkers").asInt() == 2;
            assert responseObj.get("totCPUs").asInt() == 2;
            assert responseObj.get("totMemory").asInt() == 400;
            assert responseObj.get("labels").size() == 7;
            assert responseObj.get("jobId").asText().startsWith("sine-function-");


        } else {
            assert responseObj.get("jobMetadata")
                              .get("jobId")
                              .asText()
                              .startsWith("sine-function-");
            assert responseObj.get("jobMetadata").get("name").asText().equals("sine-function");
            assert responseObj.get("jobMetadata").get("jarUrl").asText().equals(
                    "https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/" +
                    "mantis-examples-sine-function-0.2.9.zip");
            assert responseObj.get("jobMetadata").get("numStages").asInt() == 2;
            assert responseObj.get("jobMetadata").get("parameters").size() == 2;
            assert responseObj.get("jobMetadata").get("labels").size() == 7;

            assert responseObj.get("jobMetadata") != null;
            assert responseObj.get("stageMetadataList") != null;
            assert responseObj.get("workerMetadataList") != null;

            assert responseObj.get("stageMetadataList").size() == 2;
            assert responseObj.get("workerMetadataList").size() == 2;
        }

    }
}
