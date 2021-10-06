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

import static akka.http.javadsl.server.PathMatchers.segment;

import akka.NotUsed;
import akka.http.javadsl.marshalling.sse.EventStreamMarshalling;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.sse.ServerSentEvent;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.stream.javadsl.Source;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterInfo;
import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto;
import io.mantisrx.master.api.akka.route.utils.StreamingUtils;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.RxReactiveStreams;

/***
 * LastSubmittedJobIdStreamRoute
 * Defines the following end points:
 *    /api/v1/lastSubmittedJobIdStream/{clusterName}        (GET)
 */
public class LastSubmittedJobIdStreamRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(LastSubmittedJobIdStreamRoute.class);

    private final JobDiscoveryRouteHandler jobDiscoveryRouteHandler;

    private static final PathMatcher0 JOBDISCOVERY_API_PREFIX = segment("api").slash("v1");

    public LastSubmittedJobIdStreamRoute(final JobDiscoveryRouteHandler jobDiscoveryRouteHandler) {
        this.jobDiscoveryRouteHandler = jobDiscoveryRouteHandler;
    }

    @Override
    protected Route constructRoutes() {
        return pathPrefix(
                JOBDISCOVERY_API_PREFIX,
                () -> concat(
                        path(
                                segment("lastSubmittedJobIdStream").slash(PathMatchers.segment()),
                                (clusterName) -> pathEndOrSingleSlash(
                                        () -> get(() -> getLastSubmittedJobIdStreamRoute(clusterName)))
                        )
                )
        );
    }


    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/jobDiscoveryStream routes");
        return super.createRoute(routeFilter);
    }

    private Route getLastSubmittedJobIdStreamRoute(String clusterName) {
        return parameterOptional(StringUnmarshallers.BOOLEAN, ParamName.SEND_HEARTBEAT,
                (sendHeartbeats) -> {
                    logger.info("GET /api/v1/lastSubmittedJobIdStream/{} called", clusterName);

                    CompletionStage<JobDiscoveryRouteProto.JobClusterInfoResponse> jobClusterInfoRespCS =
                            jobDiscoveryRouteHandler.lastSubmittedJobIdStream(
                                    new JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest(
                                            clusterName),
                                    sendHeartbeats.orElse(false));

                    return completeAsync(
                            jobClusterInfoRespCS,
                            resp -> {
                                Optional<Observable<JobClusterInfo>> jobClusterInfoO = resp.getJobClusterInfoObs();
                                if (jobClusterInfoO.isPresent()) {
                                    Observable<JobClusterInfo> jciStream = jobClusterInfoO.get();

                                    Source<ServerSentEvent, NotUsed> source = Source
                                            .fromPublisher(RxReactiveStreams.toPublisher(jciStream))
                                            .map(j -> StreamingUtils.from(j).orElse(null))
                                            .filter(Objects::nonNull);

                                    return completeOK(
                                            source,
                                            EventStreamMarshalling.toEventStream());
                                } else {
                                    logger.warn(
                                            "Failed to get last submitted jobId stream for {}",
                                            clusterName);
                                    return complete(
                                            StatusCodes.INTERNAL_SERVER_ERROR,
                                            "Failed to get last submitted jobId stream for " +
                                            clusterName);
                                }
                            },
                            HttpRequestMetrics.Endpoints.LAST_SUBMITTED_JOB_ID_STREAM,
                            HttpRequestMetrics.HttpVerb.GET);
                });
    }
}
