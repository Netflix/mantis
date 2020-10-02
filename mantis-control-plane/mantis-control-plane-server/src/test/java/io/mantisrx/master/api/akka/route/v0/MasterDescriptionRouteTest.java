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
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpEntity;

import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.server.core.master.MasterDescription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MasterDescriptionRouteTest {
    private final static Logger logger = LoggerFactory.getLogger(MasterDescriptionRouteTest.class);
    private final ActorMaterializer materializer = ActorMaterializer.create(system);
    private final Http http = Http.get(system);
    private static Thread t;
    private static final int serverPort = 8205;
    private static final int targetEndpointPort = serverPort;
    private static final MasterDescription fakeMasterDesc = new MasterDescription(
        "localhost",
        "127.0.0.1", targetEndpointPort,
        targetEndpointPort + 2,
        targetEndpointPort + 4,
        "api/postjobstatus",
        targetEndpointPort + 6,
        System.currentTimeMillis());


    private CompletionStage<String> processRespFut(final HttpResponse r, final Optional<Integer> expectedStatusCode) {
        logger.info("headers {} {}", r.getHeaders(), r.status());
        expectedStatusCode.ifPresent(sc -> assertEquals(sc.intValue(), r.status().intValue()));
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
    private static ActorSystem system = ActorSystem.create("MasterDescriptionRouteTest");
    private static final MasterDescriptionRoute masterDescRoute;

    static {
        TestHelpers.setupMasterConfig();
        masterDescRoute = new MasterDescriptionRoute(fakeMasterDesc);
    }

    @BeforeClass
    public static void setup() throws Exception {
        JobTestHelper.deleteAllFiles();
        JobTestHelper.createDirsIfRequired();
        final CountDownLatch latch = new CountDownLatch(1);

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);

                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = masterDescRoute.createRoute(Function.identity()).flow(system, materializer);
                logger.info("starting test server on port {}", serverPort);
                latch.countDown();
                binding = http.bindAndHandle(routeFlow,
                    ConnectHttp.toHost("localhost", serverPort), materializer);
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
        logger.info("MasterDescriptionRouteTest teardown");
        binding
            .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
            .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }

    private String masterEndpoint(final String ep) {
        return String.format("http://127.0.0.1:%d/api/%s", targetEndpointPort, ep);
    }

    @Test
    public void testMasterInfoAPI() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.GET(masterEndpoint("masterinfo")));
        responseFuture
            .thenCompose(r -> processRespFut(r, Optional.of(200)))
            .whenComplete((msg, t) -> {
                try {
                    String responseMessage = getResponseMessage(msg, t);

                    logger.info("got response {}", responseMessage);
                    MasterDescription masterDescription = Jackson.fromJSON(responseMessage, MasterDescription.class);
                    logger.info("master desc ---> {}", masterDescription);
                    assertEquals(fakeMasterDesc, masterDescription);
                } catch (Exception e) {
                    fail("unexpected error "+ e.getMessage());
                }
                latch.countDown();
            });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testMasterConfigAPI() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(
            HttpRequest.GET(masterEndpoint("masterconfig")));
        responseFuture
            .thenCompose(r -> processRespFut(r, Optional.of(200)))
            .whenComplete((msg, t) -> {
                try {
                    String responseMessage = getResponseMessage(msg, t);

                    logger.info("got response {}", responseMessage);
                    List<MasterDescriptionRoute.Configlet> masterconfig = Jackson.fromJSON(responseMessage,
                        new TypeReference<List<MasterDescriptionRoute.Configlet>>() {});
                    logger.info("master config ---> {}", masterconfig);
                    assertEquals(masterDescRoute.getConfigs(), masterconfig);
                } catch (Exception e) {
                    fail("unexpected error "+ e.getMessage());
                }
                latch.countDown();
            });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }
}
