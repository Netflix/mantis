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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.MediaTypes;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.Label;
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
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobClusterRouteTest {
    private static final Logger logger = LoggerFactory.getLogger(JobClusterRouteTest.class);
    private static final Duration latchTimeout = Duration.ofSeconds(10);
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

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);

                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

                ActorRef jobClustersManagerActor = system.actorOf(
                    JobClustersManagerActor.props(
                        new MantisJobStore(new FileBasedPersistenceProvider(new FileBasedStore(temporaryFolder.newFolder("test")))),
                        lifecycleEventPublisher,
                        CostsCalculator.noop(),
                        0),
                    "jobClustersManager");
                MantisSchedulerFactory fakeSchedulerFactory = mock(MantisSchedulerFactory.class);
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                when(fakeSchedulerFactory.forJob(any())).thenReturn(fakeScheduler);
                jobClustersManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(fakeSchedulerFactory, false), ActorRef.noSender());


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
    public void testIt() throws Exception {
        testJobClusterCreate();
        testDuplicateJobClusterCreateFails();
        testJobClusterDisable();
        testJobClusterEnable();
        testJobClusterUpdateArtifact();
        testJobClusterUpdateSLA();
        testJobClusterUpdateLabels();
        testJobClusterUpdateMigrateStrategy();
        testJobClusterQuickSubmit();
        testJobClustersList();
        testJobClusterGetDetail();
        testJobClusterGetJobIds();
        testJobClusterGetAllJobIds();
        testJobClusterDisable2();
        testJobClusterDelete();
        testJobClusterCreateOnRC();
        testJobClusterRCGetDetail();
    }

    private void testJobClusterCreate() throws InterruptedException {
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

    private void testJobClusterCreateOnRC() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("create"))
                .withMethod(HttpMethods.POST)
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.JOB_CLUSTER_CREATE_RC));

        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((r, t) -> {
                String responseMessage = getResponseMessage(r, t);
                logger.info("got response {}", responseMessage);
                assertEquals("sine-function-rc created", responseMessage);
                latch.countDown();
            });
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    private void testDuplicateJobClusterCreateFails() throws InterruptedException {
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

    private void testJobClusterDisable() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterEnable() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterUpdateArtifact() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterUpdateSLA() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterUpdateLabels() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterUpdateMigrateStrategy() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterQuickSubmit() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("quicksubmit"))
                .withEntity(ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED), JobClusterPayloads.QUICK_SUBMIT));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                assertTrue(responseMessage.contains("sine-function-1"));
                latch.countDown();
            });
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClustersList() throws InterruptedException {
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

    private void testJobClusterGetDetail() throws InterruptedException {
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
                    assertEquals(2, jc.getJars().get(0).getSchedulingInfo().getStages().size());
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

    private void testJobClusterRCGetDetail() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.GET(namedJobAPIEndpoint("list/sine-function-rc")));
        responseFuture
            .thenCompose(r -> processRespFut(r, 200))
            .whenComplete((msg, t) -> {
                String responseMessage = getResponseMessage(msg, t);
                logger.info("got response {}", responseMessage);
                try {
                    List<MantisJobClusterMetadataView> jobClusters = Jackson.fromJSON(responseMessage, new TypeReference<List<MantisJobClusterMetadataView>>() {});
                    assertEquals(1, jobClusters.size());
                    MantisJobClusterMetadataView jc = jobClusters.get(0);
                    assertEquals("sine-function-rc", jc.getName());
                    assertEquals(6, jc.getLabels().size());
                    List<Label> rcLabels = jc.getLabels().stream()
                        .filter(l -> l.getName().equals("_mantis.resourceCluster") && l.getValue().equals(
                            "mantisagent")).collect(Collectors.toList());
                    assertEquals(1, rcLabels.size());
                } catch (Exception e) {
                    logger.error("failed to deser json {}", responseMessage, e);
                    fail("failed to deser json "+responseMessage);
                }
                latch.countDown();
            });
        assertTrue(latch.await(4, TimeUnit.SECONDS));
    }

    private void testJobClusterGetJobIds() throws InterruptedException {
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
                    assertEquals(1, jobIdInfos.size());
                } catch (Exception e) {
                    fail("unexpected error "+ e.getMessage());
                }
                latch.countDown();
            });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    private void testJobClusterGetAllJobIds() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterDisable2() throws InterruptedException {
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
        assertTrue(latch.await(latchTimeout.getSeconds(), TimeUnit.SECONDS));
    }

    private void testJobClusterDelete() throws InterruptedException {
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.POST(namedJobAPIEndpoint("delete"))
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, JobClusterPayloads.JOB_CLUSTER_DELETE)));
        HttpResponse response = responseFuture.toCompletableFuture().join();
        assertEquals("sine-function deleted", processRespFut(response, 200).toCompletableFuture().join());
    }
}
