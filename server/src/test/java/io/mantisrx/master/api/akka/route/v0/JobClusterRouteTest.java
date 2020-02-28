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

package io.mantisrx.master.api.akka.route.v0;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpCharsets;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.MediaTypes;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.payloads.JobClusterPayloads;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.MantisJobClusterMetadataView;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.SimpleCachedFileStorageProvider;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobClusterRouteTest {
    private final static Logger logger = LoggerFactory.getLogger(JobClusterRouteTest.class);
    private final ActorMaterializer materializer = ActorMaterializer.create(system);
    private final Http http = Http.get(system);
    private static Thread t;
    private static final int serverPort = 8301;

    private CompletionStage<String> processRespFut(final HttpResponse r, final int expectedStatusCode) {
        logger.info("headers {} {}", r.getHeaders(), r.status());
        assertEquals(expectedStatusCode, r.status().intValue());
        assert(r.getHeader("Access-Control-Allow-Origin").isPresent());
        assertEquals("*", r.getHeader("Access-Control-Allow-Origin").get().value());

        CompletionStage<HttpEntity.Strict> strictEntity = r.entity().toStrict(1000, materializer);
        return strictEntity.thenCompose(s ->
            s.getDataBytes()
                .runFold(ByteString.emptyByteString(), (acc, b) -> acc.concat(b), materializer)
                .thenApply(s2 -> s2.utf8String())
        );
    }

    private String getResponseMessage(final String msg, final Throwable t) {
        if (t != null) {
            logger.error("got err ", t);
            fail(t.getMessage());
        } else {
            return msg;
        }
        return "";
    }

    private static CompletionStage<ServerBinding> binding;
    private static ActorSystem system = ActorSystem.create("JobClusterRoutes");

    @BeforeClass
    public static void setup() throws Exception {
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);

                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

                ActorRef jobClustersManagerActor = system.actorOf(JobClustersManagerActor.props(new MantisJobStore(new SimpleCachedFileStorageProvider(true)), lifecycleEventPublisher), "jobClustersManager");
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                jobClustersManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(fakeScheduler, false), ActorRef.noSender());


                final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(jobClustersManagerActor);
                final JobRouteHandler jobRouteHandler = new JobRouteHandlerAkkaImpl(jobClustersManagerActor);

                final JobClusterRoute app = new JobClusterRoute(jobClusterRouteHandler, jobRouteHandler, system);
                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute(Function.identity()).flow(system, materializer);
                logger.info("starting test server on port {}", serverPort);
                binding = http.bindAndHandle(routeFlow,
                    ConnectHttp.toHost("localhost", serverPort), materializer);
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
    public static void teardown() {
        logger.info("V0JobClusterRouteTest teardown");
        binding
            .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
            .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }

    private String namedJobAPIEndpoint(final String endpoint) {
        return String.format("http://127.0.0.1:%d/api/namedjob/%s", serverPort, endpoint);
    }

    @Test
    public void cleanupExistingJobs() throws InterruptedException {
//        final CountDownLatch latch = new CountDownLatch(1);
//        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
//            HttpRequest.POST(namedJobAPIEndpoint("delete"))
//                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, JobClusterPayloads.JOB_CLUSTER_DELETE)));
//        responseFuture.thenCompose(r -> {
//            CompletionStage<HttpEntity.Strict> strictEntity = r.entity().toStrict(1000, materializer);
//            return strictEntity.thenCompose(s ->
//                s.getDataBytes()
//                    .runFold(ByteString.empty(), (acc, b) -> acc.concat(b), materializer)
//                    .thenApply(s2 -> s2.utf8String()));
//        }).whenComplete((msg, t) -> {
//                String responseMessage = getResponseMessage(msg, t);
//                logger.info("got response {}", responseMessage);
//                latch.countDown();
//            });
//        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "cleanupExistingJobs" })
    public void testJobClusterCreate() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("create"))
                .withMethod(HttpMethods.POST)
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_CREATE));

        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((r, t) -> {
                String responseMessage = getResponseMessage(r, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function created", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterCreate" })
    public void testDuplicateJobClusterCreateFails() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("create"))
                .withMethod(HttpMethods.POST)
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_CREATE));

        responseFuture
            .thenCompose(r -> processRespFut(r, 500))
            .whenComplete((r, t) -> {
                String responseMessage = getResponseMessage(r, t);
                logger.info("got response {}", responseMessage);
                assertTrue(responseMessage.startsWith("{\"error\":"));
                latch.countDown();
            });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testDuplicateJobClusterCreateFails" })
    public void testJobClusterDisable() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("disable"))
                .withMethod(HttpMethods.POST)
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_DISABLE));

        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((r, t) -> {
                String responseMessage = getResponseMessage(r, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function disabled", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterDisable" })
    public void testJobClusterEnable() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("enable"))
                .withMethod(HttpMethods.POST)
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_DISABLE));

        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((r, t) -> {
                String responseMessage = getResponseMessage(r, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function enabled", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterEnable" })
    public void testJobClusterUpdateArtifact() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("quickupdate"))
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function artifact updated", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterUpdateArtifact" })
    public void testJobClusterUpdateSLA() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("updatesla"))
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_UPDATE_SLA));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function SLA updated", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterUpdateSLA" })
    public void testJobClusterUpdateLabels() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("updatelabels"))
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_UPDATE_LABELS));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function labels updated", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterUpdateLabels" })
    public void testJobClusterUpdateMigrateStrategy() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("migratestrategy"))
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.MIGRATE_STRATEGY_UPDATE));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function worker migration config updated", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterUpdateMigrateStrategy" })
    public void testJobClusterQuickSubmit() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("quicksubmit"))
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.QUICK_SUBMIT));
        responseFuture
            .thenCompose(r -> processRespFut(r, 400))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertTrue(responseMessage.contains("Job Definition could not retrieved from a previous submission (There may not be a previous submission)"));
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterQuickSubmit" })
    public void testJobClustersList() throws InterruptedException {
        int numIter = 10;
        final CountDownLatch latch = new CountDownLatch(numIter);
        AtomicReference<String> prevResp = new AtomicReference<>(null);
        for (int i =0; i < numIter; i++) {
            final CompletionStage<HttpResponse> responseFuture2 = http.singleRequest(
                HttpRequest.GET(namedJobAPIEndpoint("list")));
            responseFuture2
                .thenCompose(r -> processRespFut(r, 200))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    try {
                        List<MantisJobClusterMetadataView> jobClusters = Jackson.fromJSON(responseMessage, new TypeReference<List<MantisJobClusterMetadataView>>() {
                        });
                        assertEquals(1, jobClusters.size());
                        MantisJobClusterMetadataView jobCluster = jobClusters.get(0);
                        assertEquals("sine-function", jobCluster.getName());
                    } catch (IOException e) {
                        fail("failed to parse response message " + e.getMessage());
                    }
                    if (prevResp.get() != null) {
                        assertEquals(prevResp.get(), responseMessage);
                    }
                    prevResp.set(responseMessage);
                    latch.countDown();
                });
        }
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClustersList" })
    public void testJobClusterGetDetail() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.GET(namedJobAPIEndpoint("list/sine-function")));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                try {
                    List<MantisJobClusterMetadataView> jobClusters = Jackson.fromJSON(responseMessage, new TypeReference<List<MantisJobClusterMetadataView>>() {});
                    assertEquals(1, jobClusters.size());
                    MantisJobClusterMetadataView jc = jobClusters.get(0);
                    assertEquals("sine-function", jc.getName());
                    // TODO fix Jars list
                    assertEquals(2, jc.getJars().size());
                    assertEquals(1, jc.getJars().get(0).getSchedulingInfo().getStages().size());
                    assertEquals(1, jc.getJars().get(0).getSchedulingInfo().getStages().get(1).getNumberOfInstances());
                    assertEquals(true, jc.getJars().get(0).getSchedulingInfo().getStages().get(1).getScalable());
                    assertEquals("sine-function", jc.getName());
                } catch (Exception e) {
                    logger.error("failed to deser json {}", responseMessage, e);
                    fail("failed to deser json "+responseMessage);
                }
                latch.countDown();
            });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterGetDetail" })
    public void testJobClusterGetJobIds() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.GET(namedJobAPIEndpoint("listJobIds/sine-function")));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                try {
                    String responseMessage = getResponseMessage(msg, t);

                    logger.info("got response {}", responseMessage);
                    List<JobClusterProtoAdapter.JobIdInfo> jobIdInfos = Jackson.fromJSON(responseMessage, new TypeReference<List<JobClusterProtoAdapter.JobIdInfo>>() {
                    });
                    assertEquals(0, jobIdInfos.size());
                } catch (Exception e) {
                    fail("unexpected error "+ e.getMessage());
                }
                latch.countDown();
            });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    @Test(dependsOnMethods = { "testJobClusterGetJobIds" })
    public void testJobClusterGetAllJobIds() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.GET(namedJobAPIEndpoint("listJobIds")));
        responseFuture
            .thenCompose(r -> processRespFut(r, 400))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertEquals("Specify the Job cluster name '/api/namedjob/listJobIds/<JobClusterName>' to list the job Ids", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterGetAllJobIds" })
    public void testJobClusterDisable2() throws InterruptedException {
        // Disable cluster to terminate all running jobs
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("disable"))
                .withMethod(HttpMethods.POST)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, JobClusterPayloads.JOB_CLUSTER_DISABLE)));

        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((r, t) -> {
                String responseMessage = getResponseMessage(r, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function disabled", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = { "testJobClusterDisable2" })
    public void testJobClusterDelete() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("delete"))
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, JobClusterPayloads.JOB_CLUSTER_DELETE)));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function deleted", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }
}
