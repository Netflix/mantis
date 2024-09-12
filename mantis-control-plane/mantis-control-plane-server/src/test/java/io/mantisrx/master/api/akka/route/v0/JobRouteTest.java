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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.payloads.JobClusterPayloads;
import io.mantisrx.master.api.akka.payloads.JobPayloads;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.MantisMasterRoute;
import io.mantisrx.master.api.akka.route.handlers.JobArtifactRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobArtifactRouteHandlerImpl;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.api.akka.route.v1.AdminMasterRoute;
import io.mantisrx.master.api.akka.route.v1.JobArtifactsRoute;
import io.mantisrx.master.api.akka.route.v1.JobClustersRoute;
import io.mantisrx.master.api.akka.route.v1.JobDiscoveryStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobStatusStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobsRoute;
import io.mantisrx.master.api.akka.route.v1.LastSubmittedJobIdStreamRoute;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.LeadershipManagerLocalImpl;
import io.mantisrx.server.master.http.api.CompactJobInfo;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.server.master.store.MantisStageMetadataWritable;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observable;

public class JobRouteTest {
    private final static Logger logger = LoggerFactory.getLogger(JobRouteTest.class);
    private final ActorMaterializer materializer = ActorMaterializer.create(system);
    private final Http http = Http.get(system);
    private static Thread t;
    private static final int serverPort = 8203;
    private static final int targetEndpointPort = serverPort;
    private final TestMantisClient mantisClient = new TestMantisClient(serverPort);

    private CompletionStage<String> processRespFut(
            final HttpResponse r,
            final Optional<Integer> expectedStatusCode) {
        logger.info("headers {} {}", r.getHeaders(), r.status());
        expectedStatusCode.ifPresent(sc -> assertEquals(sc.intValue(), r.status().intValue()));
        assert (r.getHeader("Access-Control-Allow-Origin").isPresent());
        assertEquals("*", r.getHeader("Access-Control-Allow-Origin").get().value());

        CompletionStage<HttpEntity.Strict> strictEntity = r.entity().toStrict(1000, materializer);
        return strictEntity.thenCompose(s ->
                                                s.getDataBytes()
                                                 .runFold(
                                                         ByteString.emptyByteString(),
                                                         (acc, b) -> acc.concat(b),
                                                         materializer)
                                                 .thenApply(s2 -> s2.utf8String())
        );
    }

    private CompletionStage<String> processRespFutWithoutHeadersCheck(
            final HttpResponse r,
            final Optional<Integer> expectedStatusCode) {
        logger.info("headers {} {}", r.getHeaders(), r.status());
        expectedStatusCode.ifPresent(sc -> assertEquals(sc.intValue(), r.status().intValue()));

        CompletionStage<HttpEntity.Strict> strictEntity = r.entity().toStrict(1000, materializer);
        return strictEntity.thenCompose(s ->
                                                s.getDataBytes()
                                                 .runFold(
                                                         ByteString.emptyByteString(),
                                                         (acc, b) -> acc.concat(b),
                                                         materializer)
                                                 .thenApply(s2 -> s2.utf8String())
        );
    }

    private String getResponseMessage(final String msg, final Throwable t) {
        if (t != null) {
            logger.error("got err ", t);
            t.printStackTrace();
            fail(t.getMessage());
        } else {
            return msg;
        }
        return "";
    }

    private static CompletionStage<ServerBinding> binding;
    private static ActorSystem system = ActorSystem.create("JobRoutes");

    @BeforeClass
    public static void setup() throws Exception {
        JobTestHelper.deleteAllFiles();
        JobTestHelper.createDirsIfRequired();
        final CountDownLatch latch = new CountDownLatch(1);
        TestHelpers.setupMasterConfig();

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);


//                new File("/tmp/MantisSpool/namedJobs").mkdirs();
//                IMantisStorageProvider storageProvider = new MantisStorageProviderAdapter(simpleCachedFileStorageProvider);
                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
                        new AuditEventSubscriberLoggingImpl(),
                        new StatusEventSubscriberLoggingImpl(),
                        new WorkerEventSubscriberLoggingImpl());

                IMantisPersistenceProvider mantisStorageProvider = new KeyValueBasedPersistenceProvider(new FileBasedStore(), lifecycleEventPublisher);
                ActorRef jobClustersManagerActor = system.actorOf(
                    JobClustersManagerActor.props(
                        new MantisJobStore(new FileBasedPersistenceProvider(true)),
                        lifecycleEventPublisher,
                        CostsCalculator.noop(),
                        0),
                    "jobClustersManager");

                MantisSchedulerFactory fakeSchedulerFactory = mock(MantisSchedulerFactory.class);
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                when(fakeSchedulerFactory.forJob(any())).thenReturn(fakeScheduler);
                jobClustersManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
                        fakeSchedulerFactory,
                        false), ActorRef.noSender());

                final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(
                        jobClustersManagerActor);
                final JobArtifactRouteHandler jobArtifactRouteHandler = new JobArtifactRouteHandlerImpl(mantisStorageProvider);
                final JobRouteHandler jobRouteHandler = new JobRouteHandlerAkkaImpl(
                        jobClustersManagerActor);

                MasterDescription masterDescription = new MasterDescription(
                        "127.0.0.1",
                        "127.0.0.1",
                        serverPort,
                        serverPort,
                        serverPort,
                        "api/postjobstatus",
                        serverPort,
                        System.currentTimeMillis());
                Duration idleTimeout = system.settings()
                                             .config()
                                             .getDuration("akka.http.server.idle-timeout");
                logger.info("idle timeout {} sec ", idleTimeout.getSeconds());

                final JobDiscoveryRouteHandler jobDiscoveryRouteHandler = new JobDiscoveryRouteHandlerAkkaImpl(
                        jobClustersManagerActor,
                        idleTimeout);
                final MasterDescriptionRoute masterDescriptionRoute = new MasterDescriptionRoute(
                        masterDescription);
                final JobRoute v0JobRoute = new JobRoute(jobRouteHandler, system);

                final JobDiscoveryRoute v0JobDiscoveryRoute = new JobDiscoveryRoute(
                        jobDiscoveryRouteHandler);
                final JobClusterRoute v0JobClusterRoute = new JobClusterRoute(
                        jobClusterRouteHandler,
                        jobRouteHandler,
                        system);
                final JobClustersRoute v1JobClusterRoute = new JobClustersRoute(
                        jobClusterRouteHandler, system);
                final JobsRoute v1JobsRoute = new JobsRoute(
                        jobClusterRouteHandler,
                        jobRouteHandler,
                        system);
                final JobArtifactsRoute v1JobArtifactsRoute = new JobArtifactsRoute(jobArtifactRouteHandler);
                final AdminMasterRoute v1AdminMasterRoute = new AdminMasterRoute(masterDescription);

                final JobStatusRouteHandler jobStatusRouteHandler = mock(JobStatusRouteHandler.class);
                when(jobStatusRouteHandler.jobStatus(anyString())).thenReturn(Flow.create());
                final JobStatusRoute v0JobStatusRoute = new JobStatusRoute(jobStatusRouteHandler);

                final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute = new JobDiscoveryStreamRoute(jobDiscoveryRouteHandler);
                final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute = new LastSubmittedJobIdStreamRoute(jobDiscoveryRouteHandler);
                final JobStatusStreamRoute v1JobStatusStreamRoute = new JobStatusStreamRoute(jobStatusRouteHandler);
                final ResourceClusters resourceClusters = mock(ResourceClusters.class);

                LocalMasterMonitor localMasterMonitor = new LocalMasterMonitor(masterDescription);
                LeadershipManagerLocalImpl leadershipMgr = new LeadershipManagerLocalImpl(
                        masterDescription);
                leadershipMgr.setLeaderReady();
                LeaderRedirectionFilter leaderRedirectionFilter = new LeaderRedirectionFilter(
                        localMasterMonitor,
                        leadershipMgr);
                final MantisMasterRoute app = new MantisMasterRoute(
                        system,
                        leaderRedirectionFilter,
                        masterDescriptionRoute,
                        v0JobClusterRoute,
                        v0JobRoute,
                        v0JobDiscoveryRoute,
                        v0JobStatusRoute,
                        v1JobClusterRoute,
                        v1JobsRoute,
                        v1JobArtifactsRoute,
                        v1AdminMasterRoute,
                        v1JobDiscoveryStreamRoute,
                        v1LastSubmittedJobIdStreamRoute,
                        v1JobStatusStreamRoute,
                        resourceClusters,
                        mock(ResourceClusterRouteHandler.class));

                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute()
                                                                              .flow(system, materializer);
                logger.info("starting test server on port {}", serverPort);
                binding = http.bindAndHandle(
                        routeFlow,
                        ConnectHttp.toHost("localhost", serverPort),
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
        Thread.sleep(100);
    }

    @AfterClass
    public static void teardown() {
        logger.info("JobRouteTest teardown");
        binding
                .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
                .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }

    private String jobClusterAPIEndpoint(final String endpoint) {
        return String.format("http://127.0.0.1:%d/api/namedjob/%s", targetEndpointPort, endpoint);
    }

    private String jobAPIEndpoint(final String endpoint) {
        return String.format("http://127.0.0.1:%d/api/jobs/%s", targetEndpointPort, endpoint);
    }

    @Test
    public void cleanupExistingJobs() throws InterruptedException {
        // Disable cluster to terminate all running jobs
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobClusterAPIEndpoint("disable"))
                           .withMethod(HttpMethods.POST)
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobClusterPayloads.JOB_CLUSTER_DISABLE)));

        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.empty()))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        final CountDownLatch latch2 = new CountDownLatch(1);
        final CompletionStage<HttpResponse> respF = http.singleRequest(
                HttpRequest.POST(jobClusterAPIEndpoint("delete"))
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobClusterPayloads.JOB_CLUSTER_DELETE)));
        respF.thenCompose(r -> {
            CompletionStage<HttpEntity.Strict> strictEntity = r.entity()
                                                               .toStrict(1000, materializer);
            return strictEntity.thenCompose(s ->
                                                    s.getDataBytes()
                                                     .runFold(
                                                             ByteString.emptyByteString(),
                                                             (acc, b) -> acc.concat(b),
                                                             materializer)
                                                     .thenApply(s2 -> s2.utf8String()));
        }).whenComplete((msg, t) -> {
            String responseMessage = getResponseMessage(msg, t);
            logger.info("got response {}", responseMessage);
            latch2.countDown();
        });
        assertTrue(latch2.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"cleanupExistingJobs"})
    public void setupJobCluster() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobClusterAPIEndpoint("create"))
                           .withMethod(HttpMethods.POST)
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobClusterPayloads.JOB_CLUSTER_CREATE)));

        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals("sine-function created", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"setupJobCluster"})
    public void testJobSubmit() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(String.format(
                        "http://127.0.0.1:%d/api/submit",
                        targetEndpointPort))
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobClusterPayloads.JOB_CLUSTER_SUBMIT)));
        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals("sine-function-1", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobSubmit"})
    public void testJobClusterGetJobIds() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(jobClusterAPIEndpoint("listJobIds/sine-function?jobState=Active")));
        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((msg, t) -> {
                    try {
                        String responseMessage = getResponseMessage(msg, t);

                        logger.info("got response {}", responseMessage);
                        List<JobClusterProtoAdapter.JobIdInfo> jobIdInfos = Jackson.fromJSON(
                                responseMessage,
                                new TypeReference<List<JobClusterProtoAdapter.JobIdInfo>>() {
                                });
                        logger.info("jobInfos---> {}", jobIdInfos);
                        assertEquals(1, jobIdInfos.size());
                        JobClusterProtoAdapter.JobIdInfo jobIdInfo = jobIdInfos.get(0);

                        assertEquals("sine-function-1", jobIdInfo.getJobId());
                        //assertEquals("0.1.39 2018-03-13 09:40:53", jobIdInfo.getVersion());
                        assertEquals("", jobIdInfo.getTerminatedAt());
                        assertEquals("nmahilani", jobIdInfo.getUser());
                        assertTrue(jobIdInfo.getState().equals(MantisJobState.Accepted) ||
                                   jobIdInfo.getState().equals(MantisJobState.Launched));
                    } catch (Exception e) {
                        fail("unexpected error " + e.getMessage());
                    }
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    @Test(dependsOnMethods = {"testJobClusterGetJobIds"})
    public void testJobClusterGetJobsList() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(jobAPIEndpoint("list")));
        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);

                    logger.info("got response---> {}", responseMessage);
                    List<MantisJobMetadataView> jobInfos = Collections.emptyList();
                    try {
                        jobInfos = Jackson.fromJSON(
                                responseMessage,
                                new TypeReference<List<MantisJobMetadataView>>() {
                                });
                    } catch (IOException e) {
                        logger.error("failed to deser json {}", responseMessage, e);
                        fail("job list deser failed");
                    }
                    logger.info("jobInfos---> {}", jobInfos);
                    assertEquals(1, jobInfos.size());
                    MantisJobMetadataView mjm = jobInfos.get(0);
                    assertEquals(mjm.getJobMetadata().getJobId(), "sine-function-1");
                    assertEquals(mjm.getJobMetadata().getName(), "sine-function");

                    assertTrue(mjm.getStageMetadataList().size() > 0);
                    MantisStageMetadataWritable msm = mjm.getStageMetadataList().get(0);
                    assertEquals(1, msm.getNumWorkers());
                    assertTrue(mjm.getWorkerMetadataList().size() > 0);
                    MantisWorkerMetadataWritable mwm = mjm.getWorkerMetadataList().get(0);
                    assertEquals("sine-function-1", mwm.getJobId());
                    assertEquals(false, mwm.getCluster().isPresent());
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobClusterGetJobsList"})
    public void testJobClusterGetJobDetail() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(jobAPIEndpoint("list/sine-function-1")));
        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);

                    logger.info("got response---> {}", responseMessage);
                    MantisJobMetadataView mjm = null;
                    try {
                        mjm = Jackson.fromJSON(responseMessage, MantisJobMetadataView.class);
                    } catch (IOException e) {
                        logger.error("failed to deser json {}", responseMessage, e);
                        fail("job info deser failed");
                    }
                    logger.info("jobInfo---> {}", mjm);
                    assertNotNull(mjm);
                    assertEquals(mjm.getJobMetadata().getJobId(), "sine-function-1");
                    assertEquals(mjm.getJobMetadata().getName(), "sine-function");

                    assertTrue(mjm.getStageMetadataList().size() > 0);
                    MantisStageMetadataWritable msm = mjm.getStageMetadataList().get(0);
                    assertEquals(1, msm.getNumWorkers());
                    assertTrue(mjm.getWorkerMetadataList().size() > 0);
                    MantisWorkerMetadataWritable mwm = mjm.getWorkerMetadataList().get(0);
                    assertEquals("sine-function-1", mwm.getJobId());
                    assertEquals(false, mwm.getCluster().isPresent());
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    @Test(dependsOnMethods = {"testJobClusterGetJobDetail"})
    public void testJobClusterGetJobsCompact() throws InterruptedException {
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(jobAPIEndpoint("list?compact=true")));
        try {
            responseFuture
                    .thenCompose(r -> processRespFut(r, Optional.of(200)))
                    .whenComplete((msg, t) -> {
                        String responseMessage = getResponseMessage(msg, t);

                        logger.info("got response {}", responseMessage);
                        List<CompactJobInfo> jobIdInfos = Collections.emptyList();
                        try {
                            jobIdInfos = Jackson.fromJSON(
                                    responseMessage,
                                    new TypeReference<List<CompactJobInfo>>() {
                                    });
                        } catch (IOException e) {
                            logger.error(
                                    "failed to get CompactJobInfos from json response {}",
                                    responseMessage,
                                    e);
                            fail("compactJobInfo deser failed");
                        }
                        logger.info("got jobIdInfos {}", jobIdInfos);
                        assertEquals(1, jobIdInfos.size());

                        CompactJobInfo jobInfo = jobIdInfos.get(0);

                        assertEquals("sine-function-1", jobInfo.getJobId());

                        assertEquals("nmahilani", jobInfo.getUser());

                        assertEquals(7, jobInfo.getLabels().size());
                        assertEquals(2, jobInfo.getNumStages());
                        assertEquals(2, jobInfo.getNumWorkers());
                        assertTrue(jobInfo.getState().equals(MantisJobState.Accepted) ||
                                   jobInfo.getState().equals(MantisJobState.Launched));
                        assertEquals(2.0, jobInfo.getTotCPUs(), 0.0);

                        // TODO total memory is 400 for old master, 2048 for new master
                        //assertEquals(400.0, jobInfo.getTotMemory(), 0.0);
                        assertEquals(MantisJobDurationType.Perpetual, jobInfo.getType());
                        assertTrue(Collections.singletonMap("Started", 2)
                                              .equals(jobInfo.getStatesSummary()) ||
                                   Collections.singletonMap("StartInitiated", 2)
                                              .equals(jobInfo.getStatesSummary()) ||
                                   Collections.singletonMap("Launched", 2)
                                              .equals(jobInfo.getStatesSummary()) ||
                                   Collections.singletonMap("Accepted", 2)
                                              .equals(jobInfo.getStatesSummary()));
                    }).toCompletableFuture()
            .get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(dependsOnMethods = {"testJobClusterGetJobsCompact"})
    public void testNamedJobInfoStream() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String jobCluster = "sine-function";
        Observable<NamedJobInfo> namedJobInfo = mantisClient.namedJobInfo(jobCluster);
        namedJobInfo
                .doOnNext(lastSubmittedJobId -> {
                    logger.info(
                            "namedJobInfo {} {}",
                            lastSubmittedJobId.getName(),
                            lastSubmittedJobId.getJobId());
                    try {
                        lastSubmittedJobId.getName();
                        assertEquals("sine-function", lastSubmittedJobId.getName());
                        assertEquals("sine-function-1", lastSubmittedJobId.getJobId());
                    } catch (Exception e) {
                        logger.error("caught exception", e);
                        org.testng.Assert.fail(
                                "testNamedJobInfoStream test failed with exception " +
                                e.getMessage(),
                                e);
                    }
                    latch.countDown();
                })
                .doOnError(t -> logger.warn("onError", t))
                .doOnCompleted(() -> logger.info("onCompleted"))
                .doAfterTerminate(() -> latch.countDown())
                .subscribe();
        latch.await();
    }

    @Test(dependsOnMethods = {"testNamedJobInfoStream"})
    public void testSchedulingInfo() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String jobId = "sine-function-1";
//        final AtomicBoolean flag = new AtomicBoolean(false);
        Observable<JobSchedulingInfo> jobSchedulingInfoObservable = mantisClient.schedulingChanges(
                jobId);
        jobSchedulingInfoObservable
                .map(schedInfo -> {
                    logger.info("schedInfo {}", schedInfo);
                    try {
                        assertEquals(jobId, schedInfo.getJobId());
                        Map<Integer, WorkerAssignments> wa = schedInfo.getWorkerAssignments();
                        assertEquals(2, wa.size());
                        // 1 worker in stage 0
                        assertEquals(1, wa.get(0).getHosts().size());
                        assertEquals(0, wa.get(0).getHosts().get(1).getWorkerIndex());
                        assertEquals(1, wa.get(0).getHosts().get(1).getWorkerNumber());
                        assertEquals(
                                MantisJobState.Started,
                                wa.get(0).getHosts().get(1).getState());
                        // 1 worker in stage 1
                        assertEquals(1, wa.get(1).getHosts().size());
                        assertEquals(0, wa.get(1).getHosts().get(2).getWorkerIndex());
                        assertEquals(2, wa.get(1).getHosts().get(2).getWorkerNumber());
                        assertEquals(
                                MantisJobState.Started,
                                wa.get(1).getHosts().get(2).getState());
//                    if (flag.compareAndSet(false, true)) {
//                        testJobResubmitWorker();
//                    }
                    } catch (Exception e) {
                        logger.error("caught exception", e);
                        org.testng.Assert.fail(
                                "testSchedulingInfo test failed with exception " + e.getMessage(),
                                e);
                    }
                    latch.countDown();
                    return schedInfo;
                })
                .take(1)
                .doOnError(t -> logger.warn("onError", t))
                .doOnCompleted(() -> logger.info("onCompleted"))
                .doAfterTerminate(() -> latch.countDown())
                .subscribe();
        latch.await();
    }

    @Test(dependsOnMethods = {"testSchedulingInfo"})
    public void testJobResubmitWorker() throws InterruptedException {
        Thread.sleep(3000);
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobAPIEndpoint(JobRoute.RESUBMIT_WORKER_ENDPOINT))
                           .withMethod(HttpMethods.POST)
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobPayloads.RESUBMIT_WORKER)));

        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    assertTrue(responseMessage.startsWith(
                            "Worker 2 of job sine-function-1 resubmitted"));
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobResubmitWorker"})
    public void testJobClusterGetJobArchivedWorkersList() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(jobAPIEndpoint("archived/sine-function-1")));
        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);

                    logger.info("############################got response---> {}", responseMessage);
                    //JobArchivedWorkersResponse resp = null;
                    //JobArchivedWorkersResponse.Builder builder = JobArchivedWorkersResponse.newBuilder();
                    List<MantisWorkerMetadataWritable> workers = Collections.emptyList();
                    try {
//                    JsonFormat.parser().ignoringUnknownFields().merge(responseMessage, builder);
                        workers = Jackson.fromJSON(
                                responseMessage,
                                new TypeReference<List<MantisWorkerMetadataWritable>>() {
                                });
                      //  resp = builder.build();
                    } catch (IOException e) {
                        logger.error("failed to deser json {}", responseMessage, e);
                        fail("archived workers list deser failed");
                    }
                    logger.info("archived workers ---> {}", workers);
                    assertEquals(1, workers.size());
                    MantisWorkerMetadataWritable worker = workers.get(0);

//                WorkerMetadata worker = resp.getWorkers(0);
//                assertEquals(1, resp.getWorkersCount());
                    assertEquals("sine-function-1", worker.getJobId());
                    logger.info("no of ports " + worker.getNumberOfPorts());

                    assertEquals(5, worker.getNumberOfPorts());
                    logger.info("stage num " + worker.getStageNum());

                    assertEquals(1, worker.getStageNum());
                    logger.info("Reason " + worker.getReason().name());
                    //assertEquals("Relaunched", worker.getReason().name());
                    logger.info("state " + worker.getState().name());
                    assertEquals("Failed", worker.getState().name());
                    logger.info("index " + worker.getWorkerIndex());
                    assertEquals(0, worker.getWorkerIndex());
                    logger.info("worker no " + worker.getWorkerNumber());
                    assertEquals(2, worker.getWorkerNumber());
                    logger.info("resubmit cnt  " + worker.getTotalResubmitCount());
                    assertEquals(0, worker.getTotalResubmitCount());
                    latch.countDown();
                });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    @Test(dependsOnMethods = {"testJobClusterGetJobArchivedWorkersList"})
    public void testJobClusterScaleStage() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobAPIEndpoint(JobRoute.SCALE_STAGE_ENDPOINT))
                           .withMethod(HttpMethods.POST)
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobPayloads.SCALE_STAGE)));

        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals("Scaled stage 1 to 3 workers", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobClusterScaleStage"})
    public void testJobStatus() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(String.format(
                        "http://127.0.0.1:%d/api/postjobstatus",
                        targetEndpointPort))
                           .withMethod(HttpMethods.POST)
                           .withEntity(
                                   ContentTypes.create(
                                           MediaTypes.TEXT_PLAIN,
                                           HttpCharsets.ISO_8859_1),
                                   JobPayloads.JOB_STATUS));

        responseFuture
                .thenCompose(r -> processRespFutWithoutHeadersCheck(r, Optional.of(200)))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response '{}'", responseMessage);
                    assertEquals("forwarded worker status", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobStatus"})
    public void testJobKill() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobAPIEndpoint(JobRoute.KILL_ENDPOINT))
                           .withMethod(HttpMethods.POST)
                           .withEntity(
                                   ContentTypes.create(MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED)
                                   ,
                                   JobPayloads.KILL_JOB));

        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals("[\"sine-function-1 Killed\"]", responseMessage.trim());
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobKill"})
    public void testJobClusterDisable() throws InterruptedException {
        // Disable cluster to terminate all running jobs
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobClusterAPIEndpoint("disable"))
                           .withMethod(HttpMethods.POST)
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobClusterPayloads.JOB_CLUSTER_DISABLE)));

        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals("sine-function disabled", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testJobClusterDisable"})
    public void testJobClusterDelete() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(jobClusterAPIEndpoint("delete"))
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   JobClusterPayloads.JOB_CLUSTER_DELETE)));
        responseFuture
                .thenCompose(r -> processRespFut(r, Optional.of(200)))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals("sine-function deleted", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }
}
