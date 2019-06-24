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

import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.Route;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.vm.AgentClusterOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;

/***
 * Agent clusters route
 * Defines the following end points:
 *    /api/v1/agentClusters                  (GET, POST)
 *    /api/v1/agentClusters/jobs             (GET)
 *    /api/v1/agentClusters/autoScalePolicy  (GET)
 */
public class AgentClustersRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(AgentClustersRoute.class);
    private final AgentClusterOperations agentClusterOps;

    public AgentClustersRoute(final AgentClusterOperations agentClusterOperations) {
        this.agentClusterOps = agentClusterOperations;
    }

    private static final PathMatcher0 API_V1_AGENT_CLUSTER = segment("api").slash("v1")
                                                                           .slash("agentClusters");

    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/agentClusters");
        return super.createRoute(routeFilter);
    }

    public Route constructRoutes() {
        return concat(
                pathPrefix(API_V1_AGENT_CLUSTER, () -> concat(
                        // api/v1/agentClusters
                        pathEndOrSingleSlash(() -> concat(
                                // GET - list all active agent clusters
                                get(this::getAgentClustersRoute),

                                // POST - activate/deactivate agent clusters
                                post(this::postAgentClustersRoute)
                        )),

                        // api/v1/agentClusters/jobs
                        path(
                                "jobs",
                                () -> pathEndOrSingleSlash(
                                        // GET - retrieve job detail by job ID
                                        () -> get(this::getAgentClustersJobsRoute)
                                )
                        ),
                        // api/v1/agentClusters/autoScalePolicy
                        path(
                                "autoScalePolicy",
                                () -> pathEndOrSingleSlash(
                                        // GET - retrieve job detail by job ID
                                        () -> get(this::getAgentClustersAutoScalePolicyRoute)
                                ))
                           )
                )
        );

    }

    private Route getAgentClustersRoute() {
        logger.info("GET /api/v1/agentClusters called");

        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                HttpRequestMetrics.Endpoints.AGENT_CLUSTERS,
                new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                new BasicTag("responseCode", String.valueOf(StatusCodes.OK.intValue())));

        return complete(
                StatusCodes.OK,
                agentClusterOps.getActiveVMsAttributeValues(),
                Jackson.marshaller());
    }

    private Route postAgentClustersRoute() {
        logger.info("POST /api/v1/agentClusters called");
        return entity(
                Jackson.unmarshaller(new TypeReference<List<String>>() {
                }),
                activeClustersList -> {
                    logger.info("POST {} called {}", API_V1_AGENT_CLUSTER, activeClustersList);
                    try {
                        agentClusterOps.setActiveVMsAttributeValues(activeClustersList);
                    } catch (IOException e) {
                        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                                HttpRequestMetrics.Endpoints.AGENT_CLUSTERS,
                                new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                                new BasicTag("responseCode", String.valueOf(StatusCodes.INTERNAL_SERVER_ERROR.intValue())));
                        return complete(
                                StatusCodes.INTERNAL_SERVER_ERROR,
                                "Failed to set active clusters to " +
                                activeClustersList.toString());
                    }

                    HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                            HttpRequestMetrics.Endpoints.AGENT_CLUSTERS,
                            new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                            new BasicTag("responseCode", String.valueOf(StatusCodes.OK.intValue())));

                    return complete(StatusCodes.OK, "");
                });
    }

    private Route getAgentClustersJobsRoute() {
        logger.info("GET /api/v1/agentClusters/jobs called");
        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                HttpRequestMetrics.Endpoints.AGENT_CLUSTERS_JOBS,
                new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                new BasicTag("responseCode", String.valueOf(StatusCodes.OK.intValue())));

        return complete(
                StatusCodes.OK,
                agentClusterOps.getJobsOnVMs(),
                Jackson.marshaller());

    }

    private Route getAgentClustersAutoScalePolicyRoute() {
        logger.info("GET /api/v1/agentClusters/autoScalePolicy called");
        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                HttpRequestMetrics.Endpoints.AGENT_CLUSTERS_AUTO_SCALE_POLICY,
                new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                new BasicTag("responseCode", String.valueOf(StatusCodes.OK.intValue())));

        return complete(
                StatusCodes.OK,
                agentClusterOps.getAgentClusterAutoScaleRules(),
                Jackson.marshaller());
    }
}
