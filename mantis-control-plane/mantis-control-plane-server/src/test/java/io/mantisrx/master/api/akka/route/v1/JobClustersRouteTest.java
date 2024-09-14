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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.payloads.JobClusterPayloads;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.JsonNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobClustersRouteTest extends RouteTestBase {
    private final static Logger logger = LoggerFactory.getLogger(JobClustersRouteTest.class);

    private static Thread t;
    private static final int SERVER_PORT = 8200;
    private static CompletionStage<ServerBinding> binding;
    private static File stateDirectory;

    private static String TEST_CLUSTER_NAME = "sine-function";

    public JobClustersRouteTest() {
        super("JobClustersRouteTest", SERVER_PORT);
    }

    @BeforeClass
    public static void setup() throws Exception {
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);
        stateDirectory = Files.createTempDirectory("test").toFile();

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);
                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
                        new AuditEventSubscriberLoggingImpl(),
                        new StatusEventSubscriberLoggingImpl(),
                        new WorkerEventSubscriberLoggingImpl());

                ActorRef jobClustersManagerActor = system.actorOf(
                    JobClustersManagerActor.props(
                        new MantisJobStore(new FileBasedPersistenceProvider(stateDirectory, true)),
                        lifecycleEventPublisher,
                        CostsCalculator.noop(),
                        0),
                    "jobClustersManager");
                MantisSchedulerFactory mantisSchedulerFactory = mock(MantisSchedulerFactory.class);
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                when(mantisSchedulerFactory.forJob(any())).thenReturn(fakeScheduler);
                jobClustersManagerActor.tell(
                        new JobClusterManagerProto.JobClustersManagerInitialize(
                                mantisSchedulerFactory, false), ActorRef.noSender());


                final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(
                        jobClustersManagerActor);

                final JobClustersRoute app = new JobClustersRoute(jobClusterRouteHandler, system);
                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
                        app.createRoute(Function.identity())
                           .flow(system, materializer);
                logger.info("starting test server on port {}", SERVER_PORT);
                binding = http.bindAndHandle(
                        routeFlow,
                        ConnectHttp.toHost("localhost", SERVER_PORT),
                        materializer);
                latch.countDown();
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
    public static void tearDown() throws Exception {
        logger.info("V1JobClusterRouteTest teardown");
        binding.thenCompose(ServerBinding::unbind) // trigger unbinding from the port
               .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
        FileUtils.deleteDirectory(stateDirectory);
    }

    @Test
    public void testIt() throws Exception {
        cleanupExistingJobs();
        testJobClusterCreate();
        testDuplicateJobClusterCreate();
        testNonExistentJobClusterLatestJobDiscoveryInfo();
        testJobClusterLatestJobDiscoveryInfoNoRunningJobs();
        testJobClustersList();
        testJobClustersDelete();
        testJobClustersPut();
        testJobClusterInstanceGET();
        testNonExistentJobClusterInstanceGET();
        testJobClusterInstancePOSTNotAllowed();
        testJobClusterInstanceValidUpdate();
        testJobClusterInstanceInvalidUpdate();
        testJobClusterInstanceNonExistentUpdate();
        testJobClusterNonExistentDelete();
        testJobClusterActionUpdateArtifactPost();
        testJobClusterActionUpdateArtifactPostNonExistent();
        testJobClusterActionUpdateArtifactPostNonMatchedResource();
        testJobClusterActionUpdateArtifactGetNotAllowed();
        testJobClusterActionUpdateArtifactPUTNotAllowed();
        testJobClusterActionUpdateArtifactDELETENotAllowed();
        testJobClusterActionUpdateSlaPost();
        testJobClusterActionUpdateSlaPostNonExistent();
        testJobClusterActionUpdateSlaPostNonMatchedResource();
        testJobClusterActionUpdateSlaGetNotAllowed();
        testJobClusterActionUpdateSlaPUTNotAllowed();
        testJobClusterActionUpdateSlaDELETENotAllowed();
        testJobClusterActionUpdateMigrationPost();
        testJobClusterActionUpdateMigrationPostNonExistent();
        testJobClusterActionUpdateMigrationPostNonMatchedResource();
        testJobClusterActionUpdateMigrationGetNotAllowed();
        testJobClusterActionUpdateMigrationPUTNotAllowed();
        testJobClusterActionUpdateMigrationDELETENotAllowed();
        testJobClusterActionUpdateLabelPost();
        testJobClusterActionUpdateLabelPostNonExistent();
        testJobClusterActionUpdateLabelPostNonMatchedResource();
        testJobClusterActionUpdateLabelGetNotAllowed();
        testJobClusterActionUpdateLabelPUTNotAllowed();
        testJobClusterActionUpdateLabelDELETENotAllowed();
        testJobClusterActionEnablePost();
        testJobClusterActionEnablePostNonExistent();
        testJobClusterActionEnablePostNonMatchedResource();
        testJobClusterActionEnableGetNotAllowed();
        testJobClusterActionEnablePUTNotAllowed();
        testJobClusterActionEnableDELETENotAllowed();
        testJobClusterActionDisablePost();
        testJobClusterActionDisablePostNonExistent();
        testJobClusterActionDisablePostNonMatchedResource();
        testJobClusterActionDisableGetNotAllowed();
        testJobClusterActionDisablePUTNotAllowed();
        testJobClusterActionDisableDELETENotAllowed();
        testJobClusterDeleteWithoutRequiredParam();
        testJobClusterValidDelete();
    }


    private void cleanupExistingJobs() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.DELETE(getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME))
        );
        responseFuture.whenComplete((msg, t) -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    private void testJobClusterCreate() throws InterruptedException {
        testPost(
                getJobClustersEndpoint(),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_CREATE),
                StatusCodes.CREATED,
                this::compareClusterInstancePayload);

        assert this.isClusterExist(TEST_CLUSTER_NAME);
    }

    private void testDuplicateJobClusterCreate() throws InterruptedException {
        testPost(
                getJobClustersEndpoint(),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_CREATE),
                StatusCodes.CONFLICT,
                null);
    }

    private void testNonExistentJobClusterLatestJobDiscoveryInfo() throws InterruptedException {
        testGet(
            getJobClusterLatestJobDiscoveryInfoEp("NonExistentCluster"),
            StatusCodes.NOT_FOUND,
            null);
    }

    private void testJobClusterLatestJobDiscoveryInfoNoRunningJobs() throws InterruptedException {
        testGet(
            getJobClusterLatestJobDiscoveryInfoEp(TEST_CLUSTER_NAME),
            StatusCodes.NOT_FOUND,
            null);
    }

    private void testJobClustersList() throws InterruptedException {
        testGet(
                getJobClustersEndpoint(),
                StatusCodes.OK,
                this::compareClustersPayload
        );
    }

    private void testJobClustersDelete() throws InterruptedException {

        testDelete(
                getJobClustersEndpoint(),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClustersPut() throws InterruptedException {
        testPut(
                getJobClustersEndpoint(),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterInstanceGET() throws InterruptedException {
        testGet(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                StatusCodes.OK,
                this::compareClusterInstancePayload);
    }

    private void testNonExistentJobClusterInstanceGET() throws InterruptedException {
        testGet(
                getJobClusterInstanceEndpoint("doesNotExist"),
                StatusCodes.NOT_FOUND,
                null
        );
    }

    private void testJobClusterInstancePOSTNotAllowed() throws InterruptedException {
        testPost(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterInstanceValidUpdate() throws InterruptedException {

        testPut(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_VALID_UPDATE),
                StatusCodes.OK,
                this::compareClusterInstancePayload);
    }

    private void testJobClusterInstanceInvalidUpdate() throws InterruptedException {
        testPut(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_INVALID_UPDATE),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterInstanceNonExistentUpdate() throws InterruptedException {
        testPut(
                getJobClusterInstanceEndpoint("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_INVALID_UPDATE),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterNonExistentDelete() throws InterruptedException {
        testDelete(
                getJobClusterInstanceEndpoint("NonExistent") + "?user=test&reason=unittest",
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionUpdateArtifactPost() throws InterruptedException {
        testPost(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT),
                StatusCodes.NO_CONTENT,
                EMPTY_RESPONSE_VALIDATOR);
    }

    private void testJobClusterActionUpdateArtifactPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateArtifactEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT),
                StatusCodes.BAD_REQUEST,
                (m) -> {
                    assert m.contains(
                            "Cluster name specified in request payload sine-function does " +
                            "not match with what specified in resource path NonExistent");
                });
    }

    private void testJobClusterActionUpdateArtifactPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateArtifactEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT_NON_EXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionUpdateArtifactGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterActionUpdateArtifactPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterActionUpdateArtifactDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null
        );
    }

    /** test Update SLA actions **/
    private void testJobClusterActionUpdateSlaPost() throws InterruptedException {
        testPost(getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                 HttpEntities.create(
                         ContentTypes.APPLICATION_JSON,
                         JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA),
                 StatusCodes.NO_CONTENT, null);
    }

    private void testJobClusterActionUpdateSlaPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateSlaEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionUpdateSlaPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateSlaEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterActionUpdateSlaGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterActionUpdateSlaPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }


    private void testJobClusterActionUpdateSlaDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    /** Update migration strategy actions tests **/

    private void testJobClusterActionUpdateMigrationPost() throws InterruptedException {
        testPost(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE),
                StatusCodes.NO_CONTENT,
                null);
    }


    private void testJobClusterActionUpdateMigrationPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateMigrationStrategyEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionUpdateMigrationPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateMigrationStrategyEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterActionUpdateMigrationGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterActionUpdateMigrationPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterActionUpdateMigrationDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    /** Update label actions tests **/

    private void testJobClusterActionUpdateLabelPost() throws InterruptedException {
        testPost(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS),
                StatusCodes.NO_CONTENT,
                null);
    }

    private void testJobClusterActionUpdateLabelPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateLabelEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionUpdateLabelPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateLabelEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterActionUpdateLabelGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    private void testJobClusterActionUpdateLabelPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }


    private void testJobClusterActionUpdateLabelDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    /** enable cluster action test **/

    private void testJobClusterActionEnablePost() throws InterruptedException {
        testPost(
                getJobClusterEnableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE),
                StatusCodes.NO_CONTENT,
                null);
    }


    private void testJobClusterActionEnablePostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterEnableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionEnablePostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterEnableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterActionEnableGetNotAllowed() throws InterruptedException {
        testGet(getJobClusterEnableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

    private void testJobClusterActionEnablePUTNotAllowed() throws InterruptedException {
        testPut(getJobClusterEnableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE),
                StatusCodes.METHOD_NOT_ALLOWED, null);
    }


    private void testJobClusterActionEnableDELETENotAllowed() throws InterruptedException {
        testDelete(getJobClusterEnableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

    /** disable cluster action test **/


    private void testJobClusterActionDisablePost() throws InterruptedException {
        testPost(
                getJobClusterDisableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE),
                StatusCodes.NO_CONTENT,
                null
        );
    }


    private void testJobClusterActionDisablePostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterDisableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

    private void testJobClusterActionDisablePostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterDisableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterActionDisableGetNotAllowed() throws InterruptedException {
        testGet(getJobClusterDisableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

    private void testJobClusterActionDisablePUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterDisableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }


    private void testJobClusterActionDisableDELETENotAllowed() throws InterruptedException {
        testDelete(getJobClusterDisableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

    private void testJobClusterDeleteWithoutRequiredParam() throws InterruptedException {
        testDelete(
                getJobClusterInstanceEndpoint("sine-function"),
                StatusCodes.BAD_REQUEST,
                null);
    }

    private void testJobClusterValidDelete() throws InterruptedException {
        assert isClusterExist("sine-function");

        testDelete(getJobClusterInstanceEndpoint("sine-function") + "?user=test&reason=unittest",
                   StatusCodes.ACCEPTED, null);
        boolean clusterExist = isClusterExist("sine-function");
        int retry = 10;
        while (clusterExist && retry > 0) {
            Thread.sleep(1000);
            clusterExist = isClusterExist("sine-function");
            retry--;
        }
        assert !clusterExist;
    }

    private void compareClusterInstancePayload(String clusterGetResponse) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode requestObj = mapper.readTree(JobClusterPayloads.JOB_CLUSTER_CREATE);
            JsonNode responseObj = mapper.readTree(clusterGetResponse);

            assertEquals(
                    responseObj.get("name").toString(),
                    requestObj.get("jobDefinition").get("name").toString());

            assertEquals(
                    responseObj.get("jars").get(0).get("url").toString(),
                    requestObj.get("jobDefinition").get("jobJarFileLocation").toString());

            assertEquals(
                    responseObj.get("jars").get(0).get("version").toString(),
                    requestObj.get("jobDefinition").get("version").toString());


        } catch (IOException ex) {
            assert ex == null;
        }
    }

    private void compareClustersPayload(String clusterListResponse) {
        try {
            ObjectMapper mapper = new ObjectMapper();

            JsonNode responseObj = mapper.readTree(clusterListResponse);

            assert (responseObj.get("list") != null);
            assert (responseObj.get("prev") != null);
            assert (responseObj.get("next") != null);

            compareClusterInstancePayload(responseObj.get("list").get(0).toString());

        } catch (IOException ex) {
            assert ex == null;
        }
    }
}
