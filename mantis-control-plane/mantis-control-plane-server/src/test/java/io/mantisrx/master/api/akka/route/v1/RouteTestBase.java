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
import static org.junit.Assert.fail;

import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.HttpMethod;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.model.StatusCode;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.util.Strings;

public abstract class RouteTestBase {
    private final static Logger logger = LoggerFactory.getLogger(RouteTestBase.class);

    static ActorSystem system;
    static Materializer materializer;
    static Http http;
    private final String testName;
    final private int serverPort;

    static ResponseValidatorFunc EMPTY_RESPONSE_VALIDATOR = (msg) -> {

        assertTrue(String.format("response [%s] is not empty", msg), Strings.isNullOrEmpty(msg));
    };

    RouteTestBase(String testName, int port) {
        this.testName = testName;
        this.system = ActorSystem.create(testName);
        this.materializer = Materializer.createMaterializer(system);
        this.http = Http.get(system);
        this.serverPort = port;
    }

    @BeforeClass
    public static void setupActorSystem() {
        system = ActorSystem.create();
        materializer = Materializer.createMaterializer(system);
        http = Http.get(system);
    }

    @AfterClass
    public static void tearDownActorSystem() {
        try {
            http.shutdownAllConnectionPools();
        } catch (Exception e) {
            logger.error("Failed to close http", e);
        }

        try {
            materializer.shutdown();
        } catch (Exception e) {
            logger.error("Failed to shutdown materializer", e);
        }

        TestKit.shutdownActorSystem(system);
    }

    final String getJobClustersEndpoint() {
        return String.format(
                "http://127.0.0.1:%d/api/v1/jobClusters",
                serverPort);
    }

    final String getJobClusterInstanceEndpoint(String clusterName) {
        return String.format(
                "http://127.0.0.1:%d/api/v1/jobClusters/%s",
                serverPort,
                clusterName);
    }

    final String getJobClusterLatestJobDiscoveryInfoEp(String clusterName) {
        return String.format(
            "http://127.0.0.1:%d/api/v1/jobClusters/%s/latestJobDiscoveryInfo",
            serverPort,
            clusterName);
    }

    final String getJobClusterUpdateArtifactEp(String clusterName) {
        return getJobClusterInstanceEndpoint(clusterName) + "/actions/updateArtifact";
    }

    final String getJobClusterUpdateSlaEp(String clusterName) {
        return getJobClusterInstanceEndpoint(clusterName) + "/actions/updateSla";
    }

    final String getJobClusterUpdateMigrationStrategyEp(String clusterName) {
        return getJobClusterInstanceEndpoint(clusterName) + "/actions/updateMigrationStrategy";
    }

    final String getJobClusterUpdateLabelEp(String clusterName) {
        return getJobClusterInstanceEndpoint(clusterName) + "/actions/updateLabel";
    }

    final String getJobClusterEnableEp(String clusterName) {
        return getJobClusterInstanceEndpoint(clusterName) + "/actions/enableCluster";
    }

    final String getJobClusterDisableEp(String clusterName) {
        return getJobClusterInstanceEndpoint(clusterName) + "/actions/disableCluster";
    }

    final String getJobsEndpoint() {
        return String.format(
                "http://127.0.0.1:%d/api/v1/jobs",
                serverPort);
    }

    final String getClusterJobsEndpoint(String clusterName) {
        return String.format(
                "http://127.0.0.1:%d/api/v1/jobClusters/%s/jobs",
                serverPort,
                clusterName);
    }

    final String getJobInstanceEndpoint(String clusterName, String jobId) {
        return String.format(
                "http://127.0.0.1:%d/api/v1/jobClusters/%s/jobs/%s",
                serverPort,
                clusterName,
                jobId);
    }

    final String getJobInstanceEndpoint(String jobId) {
        return String.format(
                "http://127.0.0.1:%d/api/v1/jobs/%s",
                serverPort,
                jobId);
    }


    CompletionStage<String> processRespFut(
            final HttpResponse r,
            final int expectedStatusCode) {
        logger.info("headers {} {}", r.getHeaders(), r.status());
        logger.info("response entity: {}", r.entity());
        assertEquals(expectedStatusCode, r.status().intValue());

        if (r.getHeader("Access-Control-Allow-Origin").isPresent()) {
            assertEquals("*", r.getHeader("Access-Control-Allow-Origin").get().value());
        }

        CompletionStage<HttpEntity.Strict> strictEntity = r.entity().toStrict(1000, materializer);
        return strictEntity.thenCompose(s -> s.getDataBytes()
                                              .runFold(
                                                      ByteString.emptyByteString(),
                                                      ByteString::concat,
                                                      materializer)
                                              .thenApply(ByteString::utf8String)
        );
    }

    String getResponseMessage(final String msg, final Throwable t) {
        if (t != null) {
            logger.error("got err ", t);
            fail(t.getMessage());
        } else {
            logger.info("got response {}", msg);
            return msg;
        }
        logger.info("got empty response {}");
        return "";
    }


    boolean isClusterExist(String clusterName) {

        final boolean result =
                http.singleRequest(HttpRequest.GET(getJobClusterInstanceEndpoint(clusterName)))
                    .thenApply(r -> r.status().intValue() != 404)
                    .toCompletableFuture()
                    .handle((x, y) -> x)
                    .join();
        return result;

    }

    void deleteClusterIfExist(String clusterName) throws InterruptedException {

        if (isClusterExist(clusterName)) {
            final CountDownLatch latch = new CountDownLatch(1);
            http.singleRequest(HttpRequest.DELETE(getJobClusterInstanceEndpoint(clusterName)))
                .thenCompose(r -> processRespFut(r, 202))
                .whenComplete((r, t) -> {
                    String responseMessage = getResponseMessage(r, t);
                    logger.info("got response {}", responseMessage);
                    latch.countDown();
                });
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        } else {
            logger.info("Cluster {} does not exist, no need to delete", clusterName);
        }
    }


    void testGet(
            String endpoint,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {
        testHttpRequest(
                HttpMethods.GET,
                endpoint,
                expectedResponseCode,
                validatorFunc);
    }

    void testPost(
            String endpoint,
            RequestEntity requestEntity,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {
        testHttpRequest(
                HttpMethods.POST,
                endpoint,
                requestEntity,
                expectedResponseCode,
                validatorFunc);
    }

    void testPost(
            String endpoint,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {
        testHttpRequest(
                HttpMethods.POST,
                endpoint,
                expectedResponseCode,
                validatorFunc);
    }

    void testPut(
            String endpoint,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {
        testHttpRequest(
                HttpMethods.PUT,
                endpoint,
                expectedResponseCode,
                validatorFunc);
    }

    void testPut(
            String endpoint,
            RequestEntity requestEntity,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {
        testHttpRequest(
                HttpMethods.PUT,
                endpoint,
                requestEntity,
                expectedResponseCode,
                validatorFunc);
    }


    void testDelete(
            String endpoint,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {

        testHttpRequest(HttpMethods.DELETE, endpoint, expectedResponseCode, validatorFunc);
    }

    void testHttpRequest(
            HttpMethod httpMethod,
            String endpoint,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {

        testHttpRequest(
                HttpRequest.create().withMethod(httpMethod).withUri(endpoint),
                expectedResponseCode,
                validatorFunc);
    }

    private void testHttpRequest(
            HttpMethod httpMethod,
            String endpoint,
            RequestEntity requestEntity,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {

        testHttpRequest(
                HttpRequest.create()
                           .withMethod(httpMethod)
                           .withUri(endpoint)
                           .withEntity(requestEntity),
                expectedResponseCode,
                validatorFunc);
    }

    private void testHttpRequest(
            HttpRequest request,
            StatusCode expectedResponseCode,
            ResponseValidatorFunc validatorFunc) throws InterruptedException {
        assert request != null;
        logger.info(request.toString());
        final CountDownLatch latch = new CountDownLatch(1);
        final CompletionStage<HttpResponse> responseFuture = http.singleRequest(request);

        try {
            responseFuture
                .thenCompose(r -> processRespFut(r, expectedResponseCode.intValue()))
                .whenComplete((msg, t) -> {
                    logger.info("got response: {}", msg);
                    assert t == null;
                    if (null != validatorFunc) {
                        validatorFunc.validate(msg);
                    }
                    latch.countDown();
                })
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    interface ResponseValidatorFunc {
        void validate(String response);
    }

}
