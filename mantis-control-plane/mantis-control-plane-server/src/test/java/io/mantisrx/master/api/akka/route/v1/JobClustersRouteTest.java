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
import io.mantisrx.master.events.*;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.SimpleCachedFileStorageProvider;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.JsonNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

    private static String TEST_CLUSTER_NAME = "sine-function";


    JobClustersRouteTest() {
        super("JobClustersRouteTest", SERVER_PORT);

    }

    @BeforeClass
    public void setup() throws Exception {
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);

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
                                new MantisJobStore(new SimpleCachedFileStorageProvider(true)),
                                lifecycleEventPublisher),
                        "jobClustersManager");
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                jobClustersManagerActor.tell(
                        new JobClusterManagerProto.JobClustersManagerInitialize(
                                fakeScheduler, false), ActorRef.noSender());


                final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(
                        jobClustersManagerActor);

                final JobClustersRoute app = new JobClustersRoute(jobClusterRouteHandler, system);
                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
                        app.createRoute(Function.identity())
                           .flow(system, materializer);
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
        logger.info("V1JobClusterRouteTest teardown");
        binding.thenCompose(ServerBinding::unbind) // trigger unbinding from the port
               .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }

    @Test
    public void cleanupExistingJobs() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.DELETE(getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME))
        );
        responseFuture.whenComplete((msg, t) -> latch.countDown());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

//    (dependsOnMethods = {"cleanupExistingJobs"})
    @Test
    public void testJobClusterCreate() throws InterruptedException {
        testPost(
                getJobClustersEndpoint(),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_CREATE),
                StatusCodes.CREATED,
                this::compareClusterInstancePayload);

        assert this.isClusterExist(TEST_CLUSTER_NAME);
    }

//    (dependsOnMethods = {"testJobClusterCreate"})
    @Test
    public void testDuplicateJobClusterCreate() throws InterruptedException {
        testPost(
                getJobClustersEndpoint(),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_CREATE),
                StatusCodes.CONFLICT,
                null);
    }

//    (dependsOnMethods = {"testDuplicateJobClusterCreate"})
    @Test
    public void testNonExistentJobClusterLatestJobDiscoveryInfo() throws InterruptedException {
        testGet(
            getJobClusterLatestJobDiscoveryInfoEp("NonExistentCluster"),
            StatusCodes.NOT_FOUND,
            null);
    }

//    (dependsOnMethods = {"testDuplicateJobClusterCreate"})
    @Test
    public void testJobClusterLatestJobDiscoveryInfoNoRunningJobs() throws InterruptedException {
        testGet(
            getJobClusterLatestJobDiscoveryInfoEp(TEST_CLUSTER_NAME),
            StatusCodes.NOT_FOUND,
            null);
    }

//    (dependsOnMethods = "testDuplicateJobClusterCreate")
    @Test
    public void testJobClustersList() throws InterruptedException {
        testGet(
                getJobClustersEndpoint(),
                StatusCodes.OK,
                this::compareClustersPayload
        );
    }

    @Test()
    public void testJobClustersDelete() throws InterruptedException {

        testDelete(
                getJobClustersEndpoint(),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    @Test()
    public void testJobClustersPut() throws InterruptedException {
        testPut(
                getJobClustersEndpoint(),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClustersList")
    @Test
    public void testJobClusterInstanceGET() throws InterruptedException {
        testGet(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                StatusCodes.OK,
                this::compareClusterInstancePayload);
    }

//    (dependsOnMethods = "testJobClusterInstanceGET")
    @Test
    public void testNonExistentJobClusterInstanceGET() throws InterruptedException {
        testGet(
                getJobClusterInstanceEndpoint("doesNotExist"),
                StatusCodes.NOT_FOUND,
                null
        );
    }

//    (dependsOnMethods = "testNonExistentJobClusterInstanceGET")
    @Test
    public void testJobClusterInstancePOSTNotAllowed() throws InterruptedException {
        testPost(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterInstancePOSTNotAllowed")
    @Test
    public void testJobClusterInstanceValidUpdate() throws InterruptedException {

        testPut(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_VALID_UPDATE),
                StatusCodes.OK,
                this::compareClusterInstancePayload);
    }

//    (dependsOnMethods = "testJobClusterInstanceValidUpdate")
    @Test
    public void testJobClusterInstanceInvalidUpdate() throws InterruptedException {
        testPut(
                getJobClusterInstanceEndpoint(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_INVALID_UPDATE),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterInstanceInvalidUpdate")
    @Test
    public void testJobClusterInstanceNonExistentUpdate() throws InterruptedException {
        testPut(
                getJobClusterInstanceEndpoint("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_INVALID_UPDATE),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterInstanceNonExistentUpdate")
    @Test
    public void testJobClusterNonExistentDelete() throws InterruptedException {
        testDelete(
                getJobClusterInstanceEndpoint("NonExistent") + "?user=test&reason=unittest",
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterNonExistentDelete")
    @Test
    public void testJobClusterActionUpdateArtifactPost() throws InterruptedException {
        testPost(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT),
                StatusCodes.NO_CONTENT,
                EMPTY_RESPONSE_VALIDATOR);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateArtifactPost")
    @Test
    public void testJobClusterActionUpdateArtifactPostNonExistent() throws InterruptedException {
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

//    (dependsOnMethods = "testJobClusterActionUpdateArtifactPostNonExistent")
    @Test
    public void testJobClusterActionUpdateArtifactPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateArtifactEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT_NON_EXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateArtifactPostNonMatchedResource")
    @Test
    public void testJobClusterActionUpdateArtifactGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateArtifactGetNotAllowed")
    @Test
    public void testJobClusterActionUpdateArtifactPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateArtifactPUTNotAllowed")
    @Test
    public void testJobClusterActionUpdateArtifactDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateArtifactEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null
        );
    }

    /** test Update SLA actions **/
//    (dependsOnMethods = "testJobClusterActionUpdateArtifactDELETENotAllowed")
    @Test
    public void testJobClusterActionUpdateSlaPost() throws InterruptedException {
        testPost(getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                 HttpEntities.create(
                         ContentTypes.APPLICATION_JSON,
                         JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA),
                 StatusCodes.NO_CONTENT, null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateSlaPost")
    @Test
    public void testJobClusterActionUpdateSlaPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateSlaEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateSlaPostNonExistent")
    @Test
    public void testJobClusterActionUpdateSlaPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateSlaEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateSlaPostNonMatchedResource")
    @Test
    public void testJobClusterActionUpdateSlaGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateSlaGetNotAllowed")
    @Test
    public void testJobClusterActionUpdateSlaPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }


//    (dependsOnMethods = "testJobClusterActionUpdateArtifactPUTNotAllowed")
    @Test
    public void testJobClusterActionUpdateSlaDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateSlaEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    /** Update migration strategy actions tests **/

//    (dependsOnMethods = "testJobClusterActionUpdateSlaDELETENotAllowed")
    @Test
    public void testJobClusterActionUpdateMigrationPost() throws InterruptedException {
        testPost(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE),
                StatusCodes.NO_CONTENT,
                null);
    }


//    (dependsOnMethods = "testJobClusterActionUpdateMigrationPost")
    @Test
    public void testJobClusterActionUpdateMigrationPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateMigrationStrategyEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateMigrationPostNonExistent")
    @Test
    public void testJobClusterActionUpdateMigrationPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateMigrationStrategyEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateMigrationPostNonMatchedResource")
    @Test
    public void testJobClusterActionUpdateMigrationGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateMigrationGetNotAllowed")
    @Test
    public void testJobClusterActionUpdateMigrationPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.MIGRATE_STRATEGY_UPDATE),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateMigrationPUTNotAllowed")
    @Test
    public void testJobClusterActionUpdateMigrationDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateMigrationStrategyEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    /** Update label actions tests **/

//    (dependsOnMethods = "testJobClusterActionUpdateMigrationDELETENotAllowed")
    @Test
    public void testJobClusterActionUpdateLabelPost() throws InterruptedException {
        testPost(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS),
                StatusCodes.NO_CONTENT,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateLabelPost")
    @Test
    public void testJobClusterActionUpdateLabelPostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterUpdateLabelEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateLabelPostNonExistent")
    @Test
    public void testJobClusterActionUpdateLabelPostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterUpdateLabelEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateLabelPostNonMatchedResource")
    @Test
    public void testJobClusterActionUpdateLabelGetNotAllowed() throws InterruptedException {
        testGet(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionUpdateLabelGetNotAllowed")
    @Test
    public void testJobClusterActionUpdateLabelPUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }


//    (dependsOnMethods = "testJobClusterActionUpdateLabelPUTNotAllowed")
    @Test
    public void testJobClusterActionUpdateLabelDELETENotAllowed() throws InterruptedException {
        testDelete(
                getJobClusterUpdateLabelEp(TEST_CLUSTER_NAME),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }

    /** enable cluster action test **/

//    (dependsOnMethods = "testJobClusterActionUpdateLabelDELETENotAllowed")
    @Test
    public void testJobClusterActionEnablePost() throws InterruptedException {
        testPost(
                getJobClusterEnableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE),
                StatusCodes.NO_CONTENT,
                null);
    }


//    (dependsOnMethods = "testJobClusterActionEnablePost")
    @Test
    public void testJobClusterActionEnablePostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterEnableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionEnablePostNonExistent")
    @Test
    public void testJobClusterActionEnablePostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterEnableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionEnablePostNonMatchedResource")
    @Test
    public void testJobClusterActionEnableGetNotAllowed() throws InterruptedException {
        testGet(getJobClusterEnableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

//    (dependsOnMethods = "testJobClusterActionEnableGetNotAllowed")
    @Test
    public void testJobClusterActionEnablePUTNotAllowed() throws InterruptedException {
        testPut(getJobClusterEnableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_ENABLE),
                StatusCodes.METHOD_NOT_ALLOWED, null);
    }


//    (dependsOnMethods = "testJobClusterActionEnablePUTNotAllowed")
    @Test
    public void testJobClusterActionEnableDELETENotAllowed() throws InterruptedException {
        testDelete(getJobClusterEnableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

    /** disable cluster action test **/


//    (dependsOnMethods = "testJobClusterActionEnableDELETENotAllowed")
    @Test
    public void testJobClusterActionDisablePost() throws InterruptedException {
        testPost(
                getJobClusterDisableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE),
                StatusCodes.NO_CONTENT,
                null
        );
    }


//    (dependsOnMethods = "testJobClusterActionDisablePost")
    @Test
    public void testJobClusterActionDisablePostNonExistent() throws InterruptedException {
        testPost(
                getJobClusterDisableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE_NONEXISTENT),
                StatusCodes.NOT_FOUND,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionDisablePostNonExistent")
    @Test
    public void testJobClusterActionDisablePostNonMatchedResource() throws InterruptedException {
        testPost(
                getJobClusterDisableEp("NonExistent"),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterActionDisablePostNonMatchedResource")
    @Test
    public void testJobClusterActionDisableGetNotAllowed() throws InterruptedException {
        testGet(getJobClusterDisableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

//    (dependsOnMethods = "testJobClusterActionDisableGetNotAllowed")
    @Test
    public void testJobClusterActionDisablePUTNotAllowed() throws InterruptedException {
        testPut(
                getJobClusterDisableEp(TEST_CLUSTER_NAME),
                HttpEntities.create(
                        ContentTypes.APPLICATION_JSON,
                        JobClusterPayloads.JOB_CLUSTER_DISABLE),
                StatusCodes.METHOD_NOT_ALLOWED,
                null);
    }


//    (dependsOnMethods = "testJobClusterActionDisablePUTNotAllowed")
    @Test
    public void testJobClusterActionDisableDELETENotAllowed() throws InterruptedException {
        testDelete(getJobClusterDisableEp(TEST_CLUSTER_NAME), StatusCodes.METHOD_NOT_ALLOWED, null);
    }

//    (dependsOnMethods = "testJobClusterActionDisableDELETENotAllowed")
    @Test
    public void testJobClusterDeleteWithoutRequiredParam() throws InterruptedException {
        testDelete(
                getJobClusterInstanceEndpoint("sine-function"),
                StatusCodes.BAD_REQUEST,
                null);
    }

//    (dependsOnMethods = "testJobClusterDeleteWithoutRequiredParam")
    @Test
    public void testJobClusterValidDelete() throws InterruptedException {
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
