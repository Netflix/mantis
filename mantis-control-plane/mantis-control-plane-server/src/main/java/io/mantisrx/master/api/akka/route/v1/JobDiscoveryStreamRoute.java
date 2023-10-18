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
import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto;
import io.mantisrx.master.api.akka.route.utils.StreamingUtils;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.master.domain.JobId;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.RxReactiveStreams;

/***
 * JobDiscoveryStreamRoute - returns scheduling info stream for a given job.
 * Defines the following end points:
 *    /api/v1/jobDiscoveryStream/{jobId}        (GET)
 */public class JobDiscoveryStreamRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobDiscoveryStreamRoute.class);

    private final JobDiscoveryRouteHandler jobDiscoveryRouteHandler;

    private static final PathMatcher0 JOBDISCOVERY_API_PREFIX = segment("api").slash("v1");

    public JobDiscoveryStreamRoute(final JobDiscoveryRouteHandler jobDiscoveryRouteHandler) {
        this.jobDiscoveryRouteHandler = jobDiscoveryRouteHandler;
    }

    @Override
    protected Route constructRoutes() {
        return pathPrefix(
                JOBDISCOVERY_API_PREFIX,
                () -> concat(
                        path(
                                segment("jobDiscoveryStream").slash(PathMatchers.segment()),
                                (jobId) -> pathEndOrSingleSlash(
                                        () -> get(() -> getJobDiscoveryStreamRoute(
                                                jobId)))
                        )
                )
        );
    }


    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/jobDiscoveryStream routes");
        return super.createRoute(routeFilter);
    }


    private Route getJobDiscoveryStreamRoute(String jobId) {
        return parameterOptional(
                StringUnmarshallers.BOOLEAN, ParamName.SEND_HEARTBEAT,
                (sendHeartbeats) -> {

                    logger.info("GET /api/v1/jobStatusStream/{} called", jobId);
                    CompletionStage<JobDiscoveryRouteProto.SchedInfoResponse> schedulingInfoRespCS =
                            jobDiscoveryRouteHandler.schedulingInfoStream(
                                    new JobClusterManagerProto
                                            .GetJobSchedInfoRequest(JobId.fromId(jobId).get()),
                                    sendHeartbeats.orElse(false));

                    return completeAsync(
                            schedulingInfoRespCS,
                            resp -> {
                                Optional<Observable<JobSchedulingInfo>> siStream = resp.getSchedInfoStream();
                                if (siStream.isPresent()) {
                                    Observable<JobSchedulingInfo> schedulingInfoObs = siStream.get();

                                    Source<ServerSentEvent, NotUsed> schedInfoSource =
                                            Source.fromPublisher(RxReactiveStreams.toPublisher(
                                                    schedulingInfoObs))
                                                  .map(j -> StreamingUtils.from(j).orElse(null))
                                                  .filter(Objects::nonNull);
                                    return completeOK(
                                            schedInfoSource,
                                            EventStreamMarshalling.toEventStream());
                                } else {
                                    logger.warn(
                                            "Failed to get sched info stream for job {}",
                                            jobId);
                                    return complete(
                                            StatusCodes.INTERNAL_SERVER_ERROR,
                                            "Failed to get sched info stream for job " +
                                            jobId);
                                }
                            },
                            HttpRequestMetrics.Endpoints.JOB_STATUS_STREAM,
                            HttpRequestMetrics.HttpVerb.GET
                            );
                });
    }
}
