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

import akka.actor.ActorSystem;
import akka.http.caching.LfuCache;
import akka.http.caching.javadsl.Cache;
import akka.http.caching.javadsl.CachingSettings;
import akka.http.caching.javadsl.LfuCacheSettings;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.model.headers.RetryAfter;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.RequestContext;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.server.RejectionHandler;
import akka.http.javadsl.server.ValidationRejection;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.japi.JavaPartialFunction;
import akka.japi.pf.PFBuilder;
import akka.pattern.AskTimeoutException;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.MasterApiMetrics;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.utils.ApiRequestRateLimiter;
import io.mantisrx.server.master.resourcecluster.RequestThrottledException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorNotFoundException;
import io.mantisrx.server.master.resourcecluster.TaskExecutorTaskCancelledException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.JsonNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.FilterProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.mantisrx.shaded.com.google.common.collect.Sets;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;


abstract class BaseRoute extends AllDirectives {

    private static final Logger logger = LoggerFactory.getLogger(BaseRoute.class);
    public static final String TOPLEVEL_FILTER = "topLevelFilter";
    public static final String JOBMETADATA_FILTER = "jobMetadata";
    public static final String STAGEMETADATA_FILTER = "stageMetadataList";
    public static final String WORKERMETADATA_FILTER = "workerMetadataList";

    /** Bounds on the {@code Retry-After} of a 429; see {@link #clampRetryAfterSeconds}. */
    private static final long MIN_RETRY_AFTER_SECONDS = 1;
    private static final long MAX_RETRY_AFTER_SECONDS = 60;

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
            HttpHeader.parse("Access-Control-Allow-Origin", "*");

    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS =
            Arrays.asList(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    protected final JavaPartialFunction<RequestContext, Uri> getRequestUriKeyer = new JavaPartialFunction<RequestContext, Uri>() {
        public Uri apply(RequestContext in, boolean isCheck) {
            final HttpRequest request = in.getRequest();
            final boolean isGet = request.method() == HttpMethods.GET;
            if (isGet) {
                return request.getUri();
            } else {
                throw noMatch();
            }
        }
    };

    private String hostName;

    BaseRoute() {
        try {
            this.hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            this.hostName = "unknown";
        }
    }

    protected Cache<Uri, RouteResult> createCache(ActorSystem actorSystem, int initialCapacity, int maxCapacity, int ttlMillis) {
        final CachingSettings defaultCachingSettings = CachingSettings.create(actorSystem);
        final LfuCacheSettings lfuCacheSettings = defaultCachingSettings.lfuCacheSettings()
            .withInitialCapacity(initialCapacity)
            .withMaxCapacity(maxCapacity)
            .withTimeToLive(Duration.create(ttlMillis, TimeUnit.MILLISECONDS));
        final CachingSettings cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings);
        return LfuCache.create(cachingSettings);
    }

    /**
     * Wraps {@code inner} in admission control: if {@code rateLimiter} has no permit left for the
     * request, it is shed with a 429 and {@code inner} never runs.
     *
     * <p>Intended for the mutating endpoints — job submit, cluster create/update/delete — where a
     * client storm turns into a backlog on an actor that reads do not touch. The limiter decides the
     * policy (node-wide bucket, per-client bucket, unlimited); this only decides where the check
     * happens.
     *
     * <p>Two properties matter at the call site. The check runs before the entity is unmarshalled, so
     * a storm costs a rate-limiter check rather than a parse plus an ask into an actor. And
     * {@code extractRequest} is evaluated per request — unlike the by-name {@code Supplier} that
     * {@code post} and friends take — which is what makes this a request-time check rather than a
     * one-off when the route tree is built.
     *
     * <p>Sheds are counted here rather than inside the limiter, and that placement is the point: which
     * limiter guards a route is a deployment's choice — see {@code ApiRequestRateLimiterProvider} — so a
     * counter owned by an implementation disappears the moment one is substituted, taking the dashboard
     * with it exactly when admission-control policy is being changed. Every shed passes through this
     * method whatever the mechanism, so counting here is the one placement no implementation can drop.
     * The numbers are the ones the rest of these routes already emit: {@code apiv1} tagged with the
     * endpoint and {@code responseCode=429}, so a shed reads as one more outcome of a call the operator
     * is already graphing, and {@code MasterApiMetrics}' untagged totals.
     *
     * @param rateLimiter the bucket to draw a permit from. Every endpoint handed the same instance
     *     shares its permits, so pass the limiter whose configured rate was sized for this endpoint's
     *     traffic; an endpoint given a limiter built for a different workload silently splits one
     *     budget between two.
     * @param endpointName the endpoint whose sheds are counted, one of
     *     {@link HttpRequestMetrics.Endpoints}. Endpoints are a closed set, which is what keeps the tag's
     *     cardinality bounded — the request path could not be used for this, since the cluster-submit path
     *     carries a cluster name and would open a time series per cluster.
     * @param verb the method the endpoint is reached by, tagged as on any other response
     * @param inner the route to run when a permit is available
     */
    protected Route withThrottle(
            ApiRequestRateLimiter rateLimiter,
            String endpointName,
            HttpRequestMetrics.HttpVerb verb,
            Supplier<Route> inner) {
        return extractRequest(request -> {
            if (!rateLimiter.tryAcquire(request)) {
                MasterApiMetrics.getInstance().incrementResp4xx();
                MasterApiMetrics.getInstance().incrementThrottledRequestCount();
                HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                        endpointName,
                        new BasicTag("verb", verb.toString()),
                        new BasicTag(
                                "responseCode",
                                String.valueOf(StatusCodes.TOO_MANY_REQUESTS.intValue())));
                return complete(throttledResponse(request, rateLimiter.getRetryAfter(request)));
            }
            return inner.get();
        });
    }

    /**
     * The 429 a shed caller gets: a {@code Retry-After} header carrying the limiter's hint, and the same
     * JSON failure body every other error on these routes uses, so a client already parsing {@code error}
     * needs no special case for this one.
     *
     * <p>{@code Retry-After} is what makes the shed actionable to a client that does not read prose — the
     * conventional signal, understood by most HTTP clients, that this is a wait-and-retry rather than a
     * malformed request to give up on. The body says the same thing in words for the human reading a curl
     * output, and names the endpoint, since a 429 says nothing about which of a client's calls was shed.
     *
     * <p>The hint is deliberately advisory: no permit is reserved, so honouring it exactly is not a
     * guarantee of admission and every shed caller is handed the same number. The message asks for
     * backoff and jitter on top rather than letting a fleet of clients synchronise on one instant.
     */
    // java.time.Duration is spelled out: this file already imports scala.concurrent.duration.Duration.
    private HttpResponse throttledResponse(HttpRequest request, java.time.Duration retryAfter) {
        final long retryAfterSeconds = clampRetryAfterSeconds(retryAfter);
        final String message = String.format(
                "Request throttled: %s %s exceeded the rate limit for this endpoint. Retry after %d "
                        + "second(s), with exponential backoff and jitter; retrying sooner or in lockstep "
                        + "with other clients will be shed again.",
                request.method().value(), request.getUri().path(), retryAfterSeconds);
        // Not logged: this path runs once per shed request, so under the storm it exists to survive a log
        // line per shed would add load rather than shed it. The counters withThrottle increments are the
        // signal to alert on.
        return HttpResponse.create()
                .withStatus(StatusCodes.TOO_MANY_REQUESTS)
                .addHeader(RetryAfter.create(retryAfterSeconds))
                .withEntity(
                        ContentTypes.APPLICATION_JSON,
                        // No request id: the request was shed before it became one.
                        generateFailureResponsePayload(message, -1));
    }

    /**
     * Turns a limiter's wait into the whole number of seconds {@code Retry-After} carries, clamped to a
     * range that means something on these routes. Doing it here rather than in each limiter is what lets
     * {@link ApiRequestRateLimiter#getRetryAfter} promise its implementors they need not round.
     *
     * <p>Rounds <em>up</em>: truncating 1.4s to 1s sends the caller back before the permit exists, turning
     * one shed request into two. The floor of one second follows, and also catches zero and negative waits,
     * which {@code Retry-After} renders as "retry now" — the opposite of what shedding was for.
     *
     * <p>The ceiling is the one that constrains a real implementation. A keyed limiter with a slowly
     * refilling per-caller bucket can compute a wait of many minutes, and while that number is arithmetically
     * right it is not useful advice on a submit path: a client told to wait a quarter of an hour has been
     * handed an outage, and the honest reading is that the bucket is mis-sized rather than that the caller
     * should sleep. Capping keeps the header actionable and turns the underlying problem into something the
     * shed counters show rather than something clients absorb silently.
     */
    private static long clampRetryAfterSeconds(java.time.Duration retryAfter) {
        final long ceiled = retryAfter.getNano() > 0 ? retryAfter.getSeconds() + 1 : retryAfter.getSeconds();
        return Math.min(MAX_RETRY_AFTER_SECONDS, Math.max(MIN_RETRY_AFTER_SECONDS, ceiled));
    }

    protected abstract Route constructRoutes();

    public Route createRoute(Function<Route, Route> routeFilter) {

        final ExceptionHandler jsonExceptionHandler = ExceptionHandler
                .newBuilder()
                .match(
                        Exception.class,
                        x -> {
                            logger.error("got exception", x);
                            return complete(
                                    StatusCodes.INTERNAL_SERVER_ERROR,
                                    generateFailureResponsePayload(
                                            "caught exception: " + x.toString(),
                                            -1)
                            );
                        })
                .build();

        final RejectionHandler jsonRejectionHandler = RejectionHandler
                .newBuilder()
                .handle(
                        ValidationRejection.class,
                        rejection -> {
                            logger.warn("Malformed request content: {}", rejection.message());
                            return complete(
                                    StatusCodes.BAD_REQUEST,
                                    HttpEntities.create(
                                            ContentTypes.APPLICATION_JSON,
                                            generateFailureResponsePayload(
                                                    rejection.message(),
                                                    -1))
                            );
                        })
                .build();

        return respondWithHeaders(
                DEFAULT_RESPONSE_HEADERS,
                () -> handleRejections(
                        jsonRejectionHandler,
                        () -> handleExceptions(
                                jsonExceptionHandler,
                                () -> routeFilter.apply(this.constructRoutes()))));

    }


    HttpResponse toDefaultHttpResponse(final BaseResponse r) {
        switch (r.responseCode) {
        case SUCCESS:
            return HttpResponse.create()
                    .withEntity(ContentTypes.APPLICATION_JSON, r.message)
                    .withStatus(StatusCodes.OK);

        case SUCCESS_CREATED:
            return HttpResponse.create()
                    .withEntity(ContentTypes.APPLICATION_JSON, r.message)
                    .withStatus(StatusCodes.CREATED);

        case CLIENT_ERROR:
            return HttpResponse.create()
                    .withEntity(
                            ContentTypes.APPLICATION_JSON,
                            generateFailureResponsePayload(r.message, r.requestId))
                    .withStatus(StatusCodes.BAD_REQUEST);

        case CLIENT_ERROR_NOT_FOUND:
            return HttpResponse.create()
                    .withEntity(
                            ContentTypes.APPLICATION_JSON,
                            generateFailureResponsePayload(r.message, r.requestId))
                    .withStatus(StatusCodes.NOT_FOUND);

        case CLIENT_ERROR_CONFLICT:
            return HttpResponse.create()
                    .withEntity(
                            ContentTypes.APPLICATION_JSON,
                            generateFailureResponsePayload(r.message, r.requestId))
                    .withStatus(StatusCodes.CONFLICT);

        case OPERATION_NOT_ALLOWED:
            return HttpResponse.create()
                    .withEntity(
                            ContentTypes.APPLICATION_JSON,
                            generateFailureResponsePayload(r.message, r.requestId))
                    .withStatus(StatusCodes.METHOD_NOT_ALLOWED);
        case SERVER_ERROR:
        default:
            return HttpResponse.create()
                    .withEntity(
                            ContentTypes.APPLICATION_JSON,
                            generateFailureResponsePayload(r.message, r.requestId))
                    .withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
        }
    }


    <T extends BaseResponse> RouteAdapter completeAsync(
            final CompletionStage<T> stage,
            final Function<T, RouteAdapter> successTransform,
            String endpointName,
            HttpRequestMetrics.HttpVerb verb) {

        return completeAsync(
                stage,
                successTransform,
                r -> {
                    HttpResponse response = toDefaultHttpResponse(r);
                    return complete(
                            response.status(),
                            HttpEntities.create(
                                    ContentTypes.APPLICATION_JSON,
                                    generateFailureResponsePayload(
                                            r.message,
                                            r.requestId))
                    );
                },
                endpointName,
                verb);
    }

    <T extends BaseResponse> RouteAdapter completeAsync(
            final CompletionStage<T> stage,
            final Function<T, RouteAdapter> successTransform,
            final Function<T, RouteAdapter> clientFailureTransform,
            String endpointName,
            HttpRequestMetrics.HttpVerb verb) {
        return onComplete(
                stage,
                resp -> resp
                        .map(r -> {
                            HttpRequestMetrics.getInstance()
                                    .incrementEndpointMetrics(
                                            endpointName,
                                            new BasicTag("verb", verb.toString()),
                                            new BasicTag(
                                                    "responseCode",
                                                    String.valueOf(r.responseCode.getValue())));
                            switch (r.responseCode) {
                            case SUCCESS:
                            case SUCCESS_CREATED:
                                MasterApiMetrics.getInstance().incrementResp2xx();
                                return successTransform.apply(r);
                            case CLIENT_ERROR:
                            case CLIENT_ERROR_CONFLICT:
                            case CLIENT_ERROR_NOT_FOUND:
                            case OPERATION_NOT_ALLOWED:
                                MasterApiMetrics.getInstance().incrementResp4xx();
                                return clientFailureTransform.apply(r);
                            case SERVER_ERROR:
                            default:
                                MasterApiMetrics.getInstance().incrementResp5xx();
                                logger.error("completeAsync default response code error: {}", r.message);
                                return complete(StatusCodes.INTERNAL_SERVER_ERROR, r.message);
                            }
                        })
                        .recover(
                                new PFBuilder<Throwable, Route>()
                                        .match(AskTimeoutException.class, te -> {
                                            MasterApiMetrics.getInstance()
                                                    .incrementAskTimeOutCount();
                                            MasterApiMetrics.getInstance().incrementResp5xx();
                                            return complete(
                                                    StatusCodes.INTERNAL_SERVER_ERROR,
                                                    generateFailureResponsePayload(
                                                            te.toString(),
                                                            -1));
                                        })
                                        .matchAny(ex -> {
                                            MasterApiMetrics.getInstance().incrementResp5xx();
                                            logger.error("completeAsync matchAny ex: ", ex);
                                            return complete(
                                                    StatusCodes.INTERNAL_SERVER_ERROR,
                                                    generateFailureResponsePayload(
                                                            ex.toString(),
                                                            -1));
                                        })
                                        .build()).get());
    }

    protected String generateFailureResponsePayload(String errorMsg, long requestId) {

        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("time", System.currentTimeMillis());
        node.put("host", this.hostName);
        node.put("error", errorMsg);
        node.put("requestId", requestId);
        return node.toString();
    }

    protected String generateFailureResponsePayload(JsonNode errorMsgNode, long requestId) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("time", System.currentTimeMillis());
        node.put("host", this.hostName);
        node.set("error", errorMsgNode);
        node.put("requestId", requestId);
        return node.toString();
    }

    FilterProvider parseFilter(String fields, String target) {
        if (Strings.isNullOrEmpty(fields)) {
            return null;
        }

        if (Strings.isNullOrEmpty(target)) {
            target = TOPLEVEL_FILTER;
        }
        Set<String> filtersSet = Sets.newHashSet();
        StringTokenizer st = new StringTokenizer(fields, ",");
        while (st.hasMoreTokens()) {
            filtersSet.add(st.nextToken().trim());
        }

        return new SimpleFilterProvider()
                .addFilter(TOPLEVEL_FILTER, TOPLEVEL_FILTER.equalsIgnoreCase(target) ? SimpleBeanPropertyFilter.filterOutAllExcept(filtersSet)
                        : SimpleBeanPropertyFilter.filterOutAllExcept(target))
                .addFilter(JOBMETADATA_FILTER,  JOBMETADATA_FILTER.equalsIgnoreCase(target) ? SimpleBeanPropertyFilter.filterOutAllExcept(filtersSet)
                        : SimpleBeanPropertyFilter.serializeAll())
                .addFilter(STAGEMETADATA_FILTER, STAGEMETADATA_FILTER.equalsIgnoreCase(target) ? SimpleBeanPropertyFilter.filterOutAllExcept(filtersSet)
                        : SimpleBeanPropertyFilter.serializeAll())
                .addFilter(WORKERMETADATA_FILTER, WORKERMETADATA_FILTER.equalsIgnoreCase(target) ? SimpleBeanPropertyFilter.filterOutAllExcept(filtersSet)
                        : SimpleBeanPropertyFilter.serializeAll());
    }

    Integer parseInteger(String val) {
        if (Strings.isNullOrEmpty(val)) {
            return null;
        } else {
            return Integer.valueOf(val);
        }
    }

    Boolean parseBoolean(String val) {
        if (Strings.isNullOrEmpty(val)) {
            return null;
        } else {
            return Boolean.valueOf(val);
        }
    }

    protected  <T> Route withFuture(CompletableFuture<T> tFuture) {
        return onComplete(tFuture,
            t -> t.fold(
                throwable -> {
                    if (throwable instanceof TaskExecutorNotFoundException) {
                        MasterApiMetrics.getInstance().incrementResp4xx();
                        return complete(
                            StatusCodes.NOT_FOUND,
                            HttpEntities.create(
                                ContentTypes.APPLICATION_JSON,
                                generateFailureResponsePayload(throwable.getMessage(), -1)));
                    }

                    if (throwable instanceof RequestThrottledException) {
                        MasterApiMetrics.getInstance().incrementResp4xx();
                        MasterApiMetrics.getInstance().incrementThrottledRequestCount();
                        return complete(
                            StatusCodes.TOO_MANY_REQUESTS,
                            HttpEntities.create(
                                ContentTypes.APPLICATION_JSON,
                                generateFailureResponsePayload(throwable.getMessage(), -1)));
                    }

                    if (throwable instanceof TaskExecutorTaskCancelledException) {
                        MasterApiMetrics.getInstance().incrementResp4xx();
                        TaskExecutorTaskCancelledException ex = (TaskExecutorTaskCancelledException) throwable;
                        return complete(
                            StatusCodes.NOT_ACCEPTABLE,
                            HttpEntities.create(
                                ContentTypes.APPLICATION_JSON,
                                generateFailureResponsePayload(ex.toJsonNode(), -1)));
                    }

                    if (throwable instanceof AskTimeoutException) {
                        MasterApiMetrics.getInstance().incrementAskTimeOutCount();
                    }

                    MasterApiMetrics.getInstance().incrementResp5xx();
                    logger.error("withFuture error: ", throwable);
                    return complete(
                        StatusCodes.INTERNAL_SERVER_ERROR,
                        HttpEntities.create(
                            ContentTypes.APPLICATION_JSON,
                            generateFailureResponsePayload(throwable.getMessage(), -1)));
                },
                r -> complete(StatusCodes.OK, r, Jackson.marshaller())));
    }
}
