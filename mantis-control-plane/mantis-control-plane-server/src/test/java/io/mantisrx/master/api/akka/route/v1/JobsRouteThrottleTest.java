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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.api.akka.payloads.JobClusterPayloads;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.utils.ApiRequestRateLimiter;
import io.mantisrx.server.master.config.ConfigurationProvider;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers the admission control on the v1 job-creation endpoints. All three are wired to the route's
 * single {@link ApiRequestRateLimiter}, so a storm on any one of them is capped by the same bucket.
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
