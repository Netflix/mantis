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
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.RequestContext;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.japi.JavaPartialFunction;
import akka.japi.pf.PFBuilder;
import akka.pattern.AskTimeoutException;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.MasterApiMetrics;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.server.master.resourcecluster.RequestThrottledException;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorNotFoundException;
import io.mantisrx.server.master.resourcecluster.TaskExecutorTaskCancelledException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;


abstract class BaseRoute extends AllDirectives {

    private static final Logger logger = LoggerFactory.getLogger(BaseRoute.class);
    public static final String TOPLEVEL_FILTER = "topLevelFilter";
    public static final String JOBMETADATA_FILTER = "jobMetadata";
    public static final String STAGEMETADATA_FILTER = "stageMetadataList";
    public static final String WORKERMETADATA_FILTER = "workerMetadataList";

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

        return respondWithHeaders(
                DEFAULT_RESPONSE_HEADERS,
                () -> handleExceptions(
                        jsonExceptionHandler,
                        () -> routeFilter.apply(this.constructRoutes())));

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
                        return complete(StatusCodes.NOT_FOUND);
                    }

                    if (throwable instanceof RequestThrottledException) {
                        MasterApiMetrics.getInstance().incrementResp4xx();
                        MasterApiMetrics.getInstance().incrementThrottledRequestCount();
                        return complete(StatusCodes.TOO_MANY_REQUESTS);
                    }

                    if (throwable instanceof TaskExecutorTaskCancelledException) {
                        MasterApiMetrics.getInstance().incrementResp4xx();
                        return complete(StatusCodes.NOT_ACCEPTABLE, throwable, Jackson.marshaller() );
                    }

                    if (throwable instanceof AskTimeoutException) {
                        MasterApiMetrics.getInstance().incrementAskTimeOutCount();
                    }

                    MasterApiMetrics.getInstance().incrementResp5xx();
                    logger.error("withFuture error: ", throwable);
                    return complete(StatusCodes.INTERNAL_SERVER_ERROR, throwable, Jackson.marshaller());
                },
                r -> complete(StatusCodes.OK, r, Jackson.marshaller())));
    }
}
