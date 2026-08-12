/*
 * Copyright 2024 Netflix, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.RetryAfter;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import akka.http.javadsl.testkit.TestRouteResult;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.api.akka.payloads.JobClusterPayloads;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.utils.ApiRequestRateLimiter;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.JsonNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers the admission control on the v1 job-creation endpoints. All three are wired to the route's
 * single {@link ApiRequestRateLimiter}, so a storm on any one of them is capped by the same bucket.
 *
 * <p>The shed counters are not asserted on by value, and deliberately so: both metric singletons bind to
 * whichever registry {@code SpectatorRegistryFactory} holds when they are first touched, that factory
 * takes a registry once per JVM, and this module runs its whole suite in one JVM with no {@code forkEvery}
 * — so what a counter reads depends on which test class ran first. An assertion on the value would pass or
 * fail on test ordering rather than on this code. What is worth guarding does not need one: the endpoint
 * names are validated against a closed set and an unlisted one throws on the shed path, so the 429
 * assertions below are what stand between a bad endpoint constant and a 500 during a storm.
 */
public class JobsRouteThrottleTest extends JUnitRouteTest {

    private static final String SUBMIT_ENDPOINT = "/api/v1/jobs";
    private static final String CLUSTER_SUBMIT_ENDPOINT =
            "/api/v1/jobClusters/" + JobClusterPayloads.CLUSTER_NAME + "/jobs";
    private static final String QUICK_SUBMIT_ENDPOINT = "/api/v1/jobs/actions/quickSubmit";

    private final JobClusterRouteHandler clusterRouteHandler = mock(JobClusterRouteHandler.class);
    private final JobRouteHandler jobRouteHandler = mock(JobRouteHandler.class);

    @BeforeClass
    public static void init() {
        TestHelpers.setupMasterConfig();
    }

    private TestRoute routeWith(ApiRequestRateLimiter rateLimiter) {
        return testRoute(
                new JobsRoute(clusterRouteHandler, jobRouteHandler, system(), rateLimiter)
                        .createRoute(route -> route));
    }

    /** A limiter that sheds everything, i.e. the bucket is permanently empty. */
    private static ApiRequestRateLimiter denyAll() {
        return request -> false;
    }

    /**
     * Sheds everything and advertises {@code retryAfter}. A value the route layer could not have invented
     * on its own, so a passing assertion shows the hint came from the limiter rather than a constant.
     */
    private static ApiRequestRateLimiter denyAllRetryingAfter(Duration retryAfter) {
        return new ApiRequestRateLimiter() {
            @Override
            public boolean tryAcquire(HttpRequest request) {
                return false;
            }

            @Override
            public Duration getRetryAfter(HttpRequest request) {
                return retryAfter;
            }
        };
    }

    private static HttpRequest post(String uri, String payload) {
        return HttpRequest.POST(uri)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, payload));
    }

    @Test
    public void throttledJobSubmitIsRejectedWithTooManyRequests() {
        routeWith(denyAll())
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        // The point of shedding before unmarshalling: the storm never reaches the cluster actor.
        verifyNoInteractions(clusterRouteHandler, jobRouteHandler);
    }

    /**
     * A 429 with no {@code Retry-After} leaves a client guessing when to come back, which in practice
     * means immediately. The header is the machine-readable half of the answer.
     */
    @Test
    public void throttledResponseCarriesRetryAfterFromTheLimiter() {
        TestRouteResult response = routeWith(denyAllRetryingAfter(Duration.ofSeconds(7)))
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        // Asserted via the parsed header rather than the raw string: the point is that clients see a
        // well-formed Retry-After, and that the value is the limiter's rather than a constant.
        RetryAfter retryAfter = response.header(RetryAfter.class);
        assertNotNull("429 must carry a Retry-After header", retryAfter);
        assertEquals(Optional.of(7L), retryAfter.getDelaySeconds());
    }

    /**
     * {@code Retry-After} also admits an HTTP-date, and a sub-second hint would round to 0 — "retry now",
     * the opposite of shedding. Both are guarded by flooring the header at one second.
     */
    @Test
    public void subSecondRetryAfterIsFlooredToOneSecond() {
        TestRouteResult response = routeWith(denyAllRetryingAfter(Duration.ofMillis(200)))
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        assertEquals(Optional.of(1L), response.header(RetryAfter.class).getDelaySeconds());
    }

    /**
     * Truncating 1.4s to 1s sends the caller back before the permit it was waiting for exists, so a hint
     * that does not land on a whole second rounds up. The cost of being a fraction of a second late is a
     * fraction of a second; the cost of being early is another shed request.
     */
    @Test
    public void fractionalRetryAfterIsRoundedUp() {
        TestRouteResult response = routeWith(denyAllRetryingAfter(Duration.ofMillis(1400)))
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        assertEquals(Optional.of(2L), response.header(RetryAfter.class).getDelaySeconds());
    }

    /**
     * A keyed limiter with a slowly refilling per-caller bucket can compute a hint measured in minutes.
     * Handing that to a client on a submit path is an outage for them rather than useful advice, so the
     * header is capped and they come back to a fresh decision instead.
     */
    @Test
    public void implausiblyLongRetryAfterIsCapped() {
        TestRouteResult response = routeWith(denyAllRetryingAfter(Duration.ofHours(1)))
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        assertEquals(Optional.of(60L), response.header(RetryAfter.class).getDelaySeconds());
    }

    /**
     * A keyed limiter cannot answer per caller unless it is told who the caller is, and the shed path is
     * the one place the route layer has to hand it over. This pins that it does: the hint here is derived
     * from the request, so a route layer that passed anything else could not produce it.
     */
    @Test
    public void retryAfterIsAskedAboutTheRequestBeingShed() {
        ApiRequestRateLimiter perCaller = new ApiRequestRateLimiter() {
            @Override
            public boolean tryAcquire(HttpRequest request) {
                return false;
            }

            @Override
            public Duration getRetryAfter(HttpRequest request) {
                return Duration.ofSeconds(request.getUri().path().length());
            }
        };

        TestRouteResult response = routeWith(perCaller)
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        assertEquals(
                Optional.of((long) SUBMIT_ENDPOINT.length()),
                response.header(RetryAfter.class).getDelaySeconds());
    }

    /**
     * The header tells a client library what to do; the body tells the person reading a curl output why,
     * in the same JSON envelope as every other error on these routes so nothing needs a special case.
     */
    @Test
    public void throttledResponseBodyExplainsTheShed() throws Exception {
        String body = routeWith(denyAllRetryingAfter(Duration.ofSeconds(7)))
                .run(post(SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS)
                .assertMediaType(MediaTypes.APPLICATION_JSON)
                .entityString();

        JsonNode error = new ObjectMapper().readTree(body).get("error");
        assertNotNull("throttle body must use the standard failure envelope", error);
        String message = error.asText();
        // The endpoint, because a bare 429 does not say which of a client's calls was shed; the delay,
        // because the body is what a human reads; and the backoff advice, because every shed caller is
        // handed the same number and retrying in lockstep just reproduces the storm.
        assertTrue("should name the shed endpoint, was: " + message, message.contains(SUBMIT_ENDPOINT));
        assertTrue("should state the delay, was: " + message, message.contains("7 second(s)"));
        assertTrue("should ask for jitter, was: " + message, message.contains("jitter"));
    }

    @Test
    public void throttledClusterJobSubmitIsRejectedWithTooManyRequests() {
        routeWith(denyAll())
                .run(post(CLUSTER_SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        verifyNoInteractions(clusterRouteHandler, jobRouteHandler);
    }

    @Test
    public void throttledQuickSubmitIsRejectedWithTooManyRequests() {
        routeWith(denyAll())
                .run(post(QUICK_SUBMIT_ENDPOINT, JobClusterPayloads.QUICK_SUBMIT))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        verifyNoInteractions(clusterRouteHandler, jobRouteHandler);
    }

    @Test
    public void malformedBodyIsStillThrottled() {
        // Shedding happens before the entity is parsed, so even a request we would have rejected as a
        // 400 costs only a rate-limiter check.
        routeWith(denyAll())
                .run(post(SUBMIT_ENDPOINT, "not json"))
                .assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);
    }

    /**
     * The reads and the per-job actions on this route are deliberately not wrapped: only the creation
     * path drives a backlog on the shared cluster actor. A limiter that shed everything must therefore
     * still leave them reachable, or enabling the throttle would take down job monitoring with it.
     */
    @Test
    public void unguardedEndpointsAreNotThrottled() {
        when(jobRouteHandler.getJobDetails(any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new JobClusterManagerProto.GetJobDetailsResponse(
                                1L, BaseResponse.ResponseCode.CLIENT_ERROR_NOT_FOUND, "nope",
                                Optional.empty())));

        routeWith(denyAll())
                .run(HttpRequest.GET(SUBMIT_ENDPOINT + "/" + JobClusterPayloads.CLUSTER_NAME + "-1"));

        // Reaching the handler is the assertion: a throttled read would have been answered with a 429
        // before the handler was ever consulted.
        verify(jobRouteHandler).getJobDetails(any());
    }

    @Test
    public void admittedJobSubmitReachesTheHandler() {
        when(clusterRouteHandler.submit(any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new JobClusterManagerProto.SubmitJobResponse(
                                1L, BaseResponse.ResponseCode.CLIENT_ERROR, "nope", Optional.empty())));

        routeWith(ApiRequestRateLimiter.UNLIMITED)
                .run(post(CLUSTER_SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT));

        // Reaching the handler at all is the assertion: whatever the handler then answers is the
        // existing submit behaviour, covered by JobsRouteTest.
        verify(clusterRouteHandler).submit(any());
    }

    /**
     * The throttle is opt-in, so an operator who has not enabled it must see no behaviour change at
     * all. {@code MasterApiAkkaService} reads this flag to decide whether to build a limiter.
     */
    @Test
    public void throttleIsDisabledByDefault() {
        assertFalse(ConfigurationProvider.getConfig().isApiV1JobsThrottleEnabled());
    }

    /**
     * Guards against the limiter being consulted once when the route tree is built rather than on
     * every request — a mistake that would leave the endpoint either permanently open or permanently
     * shut after the first call.
     */
    @Test
    public void limiterIsConsultedOnEveryRequest() {
        AtomicInteger calls = new AtomicInteger();
        // Admits the first request, sheds every one after it.
        TestRoute route = routeWith(request -> calls.getAndIncrement() == 0);

        when(clusterRouteHandler.submit(any()))
                .thenReturn(CompletableFuture.completedFuture(
                        new JobClusterManagerProto.SubmitJobResponse(
                                1L, BaseResponse.ResponseCode.CLIENT_ERROR, "nope", Optional.empty())));

        HttpRequest request = post(CLUSTER_SUBMIT_ENDPOINT, JobClusterPayloads.JOB_CLUSTER_SUBMIT);

        route.run(request);
        route.run(request).assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);
        route.run(request).assertStatusCode(StatusCodes.TOO_MANY_REQUESTS);

        assertEquals(3, calls.get());
        // Only the first request got through, so the limiter is not being short-circuited either way.
        verify(clusterRouteHandler, times(1)).submit(any());
    }
}
