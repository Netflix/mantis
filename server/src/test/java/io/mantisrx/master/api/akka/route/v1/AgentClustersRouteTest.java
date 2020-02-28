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
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;

import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.fenzo.AutoScaleAction;
import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.payloads.AgentClusterPayloads;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.master.scheduler.JobMessageRouterImpl;
import io.mantisrx.master.vm.AgentClusterOperations;
import io.mantisrx.master.vm.AgentClusterOperationsImpl;
import io.mantisrx.server.master.AgentClustersAutoScaler;
import io.mantisrx.server.master.persistence.IMantisStorageProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.SimpleCachedFileStorageProvider;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AgentClustersRouteTest extends RouteTestBase {
    private final static Logger logger = LoggerFactory.getLogger(AgentClustersRouteTest.class);
    private static Thread t;
    private static final int serverPort = 8202;
    private static final ObjectMapper mapper = new ObjectMapper().configure(
            DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false);
    private static String SERVER_ENDPOINT = String.format(
            "http://127.0.0.1:%d/api/v1/agentClusters",
            serverPort);


    private static CompletionStage<ServerBinding> binding;

    AgentClustersRouteTest(){
        super("AgentClusterRoutes", 8202);

    }
    @BeforeClass
    public void setup() throws InterruptedException {
        TestHelpers.setupMasterConfig();
        final CountDownLatch latch = new CountDownLatch(1);
        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);
                IMantisStorageProvider storageProvider = new SimpleCachedFileStorageProvider(true);
                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
                        new AuditEventSubscriberLoggingImpl(),
                        new StatusEventSubscriberLoggingImpl(),
                        new WorkerEventSubscriberLoggingImpl());

                ActorRef jobClustersManagerActor = system.actorOf(
                        JobClustersManagerActor.props(
                                new MantisJobStore(storageProvider),
                                lifecycleEventPublisher),
                        "jobClustersManager");


                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                jobClustersManagerActor.tell(
                        new JobClusterManagerProto.JobClustersManagerInitialize(
                                fakeScheduler,
                                false),
                        ActorRef.noSender());

                setupDummyAgentClusterAutoScaler();
                final AgentClustersRoute agentClusterV2Route = new AgentClustersRoute(
                        new AgentClusterOperationsImpl(
                                storageProvider,
                                new JobMessageRouterImpl(jobClustersManagerActor),
                                fakeScheduler,
                                lifecycleEventPublisher,
                                "cluster"));

                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = agentClusterV2Route.createRoute(
                        Function.identity()).flow(system, materializer);
                logger.info("test server starting on port {}", serverPort);
                latch.countDown();
                binding = http.bindAndHandle(routeFlow,
                                             ConnectHttp.toHost("localhost", serverPort),
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
        logger.info("V1AgentClusterRouteTest teardown");
        binding
                .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
                .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }

    private static void setupDummyAgentClusterAutoScaler() {
        final AutoScaleRule dummyAutoScaleRule = new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return "test";
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return 1;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return 10;
            }

            @Override
            public long getCoolDownSecs() {
                return 300;
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return false;
            }

            @Override
            public int getMinSize() {
                return 1;
            }

            @Override
            public int getMaxSize() {
                return 100;
            }
        };
        try {
            AgentClustersAutoScaler.initialize(() -> new HashSet<>(Collections.singletonList(
                    dummyAutoScaleRule)), new Observer<AutoScaleAction>() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {

                }

                @Override
                public void onNext(AutoScaleAction autoScaleAction) {

                }
            });
        } catch (Exception e) {
            logger.info("AgentClustersAutoScaler is already initialized by another test", e);
        }
    }

    @Test()
    public void testSetActiveAgentClusters() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.POST(SERVER_ENDPOINT)
                           .withEntity(HttpEntities.create(
                                   ContentTypes.APPLICATION_JSON,
                                   AgentClusterPayloads.SET_ACTIVE)));
        responseFuture
                .thenCompose(r -> processRespFut(r, 200))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }


    @Test(dependsOnMethods = {"testSetActiveAgentClusters"})
    public void testGetJobsOnAgentClusters() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(SERVER_ENDPOINT + "/jobs"));
        responseFuture
                .thenCompose(r -> processRespFut(r, 200))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    // TODO validate jobs on VM response
                    assertEquals("{}", responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testGetJobsOnAgentClusters"})
    public void testGetAutoScalePolicy() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(SERVER_ENDPOINT + "/autoScalePolicy"));
        responseFuture
                .thenCompose(r -> processRespFut(r, 200))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    try {
                        Map<String, AgentClusterOperations.AgentClusterAutoScaleRule> agentClusterAutoScaleRule = mapper
                                .readValue(
                                        responseMessage,
                                        new TypeReference<Map<String, AgentClusterOperations.AgentClusterAutoScaleRule>>() {
                                        });
                        agentClusterAutoScaleRule.values().forEach(autoScaleRule -> {
                            assertEquals("test", autoScaleRule.getName());
                            assertEquals(300, autoScaleRule.getCooldownSecs());
                            assertEquals(1, autoScaleRule.getMinIdle());
                            assertEquals(10, autoScaleRule.getMaxIdle());
                            assertEquals(1, autoScaleRule.getMinSize());
                            assertEquals(100, autoScaleRule.getMaxSize());
                        });
                    } catch (IOException e) {
                        logger.error("caught error", e);
                        fail("failed to deserialize response");
                    }
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test(dependsOnMethods = {"testGetAutoScalePolicy"})
    public void testGetActiveAgentClusters() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
                HttpRequest.GET(SERVER_ENDPOINT));
        responseFuture
                .thenCompose(r -> processRespFut(r, 200))
                .whenComplete((msg, t) -> {
                    String responseMessage = getResponseMessage(msg, t);
                    logger.info("got response {}", responseMessage);
                    assertEquals(AgentClusterPayloads.SET_ACTIVE, responseMessage);
                    latch.countDown();
                });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }
}
