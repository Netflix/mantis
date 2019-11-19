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

import akka.actor.ActorSystem;
import akka.http.caching.javadsl.Cache;
import akka.http.caching.javadsl.CachingSettings;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.RequestContext;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.japi.JavaPartialFunction;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.vm.AgentClusterOperations;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;
import static akka.http.javadsl.server.directives.CachingDirectives.alwaysCache;
import static akka.http.javadsl.server.directives.CachingDirectives.routeCache;

public class AgentClusterRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(AgentClusterRoute.class);
    private final AgentClusterOperations agentClusterOps;
    private final Cache<Uri, RouteResult> cache;
    private final JavaPartialFunction<RequestContext, Uri> requestUriKeyer = new JavaPartialFunction<RequestContext, Uri>() {
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

    private final Counter setActiveCount;
    private final Counter listActiveCount;
    private final Counter listJobsOnVMsCount;
    private final Counter listAgentClustersCount;


    public AgentClusterRoute(final AgentClusterOperations agentClusterOperations, final ActorSystem actorSystem) {
        Preconditions.checkNotNull(agentClusterOperations, "agentClusterOperations");
        this.agentClusterOps = agentClusterOperations;
        MasterConfiguration config = ConfigurationProvider.getConfig();
        this.cache = createCache(actorSystem, config.getApiCacheMinSize(), config.getApiCacheMaxSize(),
                config.getApiCacheTtlMilliseconds());

        Metrics m = new Metrics.Builder()
            .id("V0AgentClusterRoute")
            .addCounter("setActive")
            .addCounter("listActive")
            .addCounter("listJobsOnVMs")
            .addCounter("listAgentClusters")
            .build();
        this.setActiveCount = m.getCounter("setActive");
        this.listActiveCount = m.getCounter("listActive");
        this.listJobsOnVMsCount = m.getCounter("listJobsOnVMs");
        this.listAgentClustersCount = m.getCounter("listAgentClusters");
    }

    private static final PathMatcher0 API_VM_ACTIVEVMS = segment("api").slash("vm").slash("activevms");
    @VisibleForTesting
    public static final String LISTACTIVE ="listactive";
    @VisibleForTesting
    public static final String SETACTIVE ="setactive";
    @VisibleForTesting
    public static final String LISTJOBSONVMS="listjobsonvms";
    @VisibleForTesting
    public static final String LISTAGENTCLUSTERS = "listagentclusters";

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
        HttpHeader.parse("Access-Control-Allow-Origin", "*");
    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS = Arrays.asList(
        ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    private Route agentClusterRoutes() {
        return route(
            get(() -> route(
                path(API_VM_ACTIVEVMS.slash(LISTACTIVE), () -> {
                    logger.debug("/api/vm/activems/{} called", LISTACTIVE);
                    listActiveCount.increment();
                    return complete(StatusCodes.OK,
                                    agentClusterOps.getActiveVMsAttributeValues(),
                                    Jackson.marshaller());
                }),
                path(API_VM_ACTIVEVMS.slash(LISTJOBSONVMS), () -> {
                    logger.debug("/api/vm/activems/{} called", LISTJOBSONVMS);
                    listJobsOnVMsCount.increment();
                    return alwaysCache(cache, requestUriKeyer, () ->
                        extractUri(uri -> complete(StatusCodes.OK,
                        agentClusterOps.getJobsOnVMs(),
                        Jackson.marshaller())));
                }),
                path(API_VM_ACTIVEVMS.slash(LISTAGENTCLUSTERS), () -> {
                    logger.debug("/api/vm/activems/{} called", LISTAGENTCLUSTERS);
                    listAgentClustersCount.increment();
                    return complete(StatusCodes.OK,
                        agentClusterOps.getAgentClusterAutoScaleRules(),
                        Jackson.marshaller());
                })
            )),
            post(() -> route(
                path(API_VM_ACTIVEVMS.slash(SETACTIVE), () ->
                    decodeRequest(() ->
                        entity(Unmarshaller.entityToString(), req -> {
                            try {
                                setActiveCount.increment();
                                List<String> activeClustersList = Jackson.fromJSON(req, new TypeReference<List<String>>() {});
                                logger.info("POST /api/vm/activems/{} called {}", SETACTIVE, activeClustersList);
                                agentClusterOps.setActiveVMsAttributeValues(activeClustersList);
                            } catch (IOException e) {
                                return complete(StatusCodes.INTERNAL_SERVER_ERROR,
                                    "Failed to set active clusters to "+req);
                            }
                            return complete(StatusCodes.OK, req);
                        }))
                )
            ))
        );
    }

    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating routes");
        final ExceptionHandler jsonExceptionHandler = ExceptionHandler.newBuilder()
            .match(Exception.class, x -> {
                logger.error("got exception", x);
                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "{\"error\": \"" + x.getMessage() + "\"}");
            })
            .build();

        return
            respondWithHeaders(DEFAULT_RESPONSE_HEADERS,
                () -> handleExceptions(jsonExceptionHandler,
                    () -> routeFilter.apply(agentClusterRoutes())));
    }

}
