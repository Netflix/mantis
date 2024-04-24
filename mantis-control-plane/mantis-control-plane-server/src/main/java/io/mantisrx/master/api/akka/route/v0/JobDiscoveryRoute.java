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

import static akka.http.javadsl.server.PathMatchers.segment;

import akka.NotUsed;
import akka.http.javadsl.marshalling.sse.EventStreamMarshalling;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.sse.ServerSentEvent;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.stream.javadsl.Source;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.proto.JobClusterInfo;
import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto;
import io.mantisrx.master.api.akka.route.utils.StreamingUtils;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.master.domain.JobId;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.RxReactiveStreams;

public class JobDiscoveryRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobDiscoveryRoute.class);
    private final JobDiscoveryRouteHandler jobDiscoveryRouteHandler;

    private final Metrics metrics;
    private final Counter schedulingInfoStreamGET;
    private final Counter jobClusterInfoStreamGET;

    public JobDiscoveryRoute(final JobDiscoveryRouteHandler jobDiscoveryRouteHandler) {
        this.jobDiscoveryRouteHandler = jobDiscoveryRouteHandler;
        Metrics m = new Metrics.Builder()
                .id("JobDiscoveryRoute")
                .addCounter("schedulingInfoStreamGET")
                .addCounter("jobClusterInfoStreamGET")
                .build();
        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.schedulingInfoStreamGET = metrics.getCounter("schedulingInfoStreamGET");
        this.jobClusterInfoStreamGET = metrics.getCounter("jobClusterInfoStreamGET");
    }

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
            HttpHeader.parse("Access-Control-Allow-Origin", "*");
    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS = Arrays.asList(
            ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    private Route getJobDiscoveryRoutes() {
        return route(
                get(() -> route(
                        path(segment("assignmentresults").slash(PathMatchers.segment()), (jobId) ->
                                extractClientIP(clientIp ->
                                    parameterOptional(
                                        StringUnmarshallers.BOOLEAN,
                                        "sendHB",
                                        (sendHeartbeats) -> {
                                            logger.debug(
                                                    "/assignmentresults/{} called by {}",
                                                    jobId, clientIp);
                                            schedulingInfoStreamGET.increment();
                                            JobClusterManagerProto.GetJobSchedInfoRequest req =
                                                    new JobClusterManagerProto.GetJobSchedInfoRequest(
                                                            JobId.fromId(jobId).get());

                                            CompletionStage<JobDiscoveryRouteProto.SchedInfoResponse> schedulingInfoRespCS =
                                                    jobDiscoveryRouteHandler.schedulingInfoStream(
                                                            req,
                                                            sendHeartbeats.orElse(false));

                                            return completeAsync(
                                                    schedulingInfoRespCS,
                                                    r -> {
                                                        if (r.responseCode.equals(ResponseCode.CLIENT_ERROR_NOT_FOUND)) {
                                                            logger.warn(
                                                                "Sched info stream not found for job {}",
                                                                jobId);
                                                            return complete(
                                                                StatusCodes.NOT_FOUND,
                                                                "Sched info stream not found for job " +
                                                                    jobId);
                                                        }

                                                        Optional<Observable<JobSchedulingInfo>> schedInfoStreamO = r
                                                                .getSchedInfoStream();
                                                        if (schedInfoStreamO.isPresent()) {
                                                            Observable<JobSchedulingInfo> schedulingInfoObs = schedInfoStreamO
                                                                    .get();
                                                            Source<ServerSentEvent, NotUsed> schedInfoSource =
                                                                    Source.fromPublisher(
                                                                            RxReactiveStreams.toPublisher(
                                                                                    schedulingInfoObs))
                                                                          .map(j -> StreamingUtils.from(
                                                                                  j)
                                                                                                  .orElse(null))
                                                                          .filter(sse -> sse !=
                                                                                         null);
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
                                                    });
                                        }))
                        ),
                        path(segment("namedjobs").slash(PathMatchers.segment()), (jobCluster) ->
                                parameterOptional(
                                        StringUnmarshallers.BOOLEAN,
                                        "sendHB",
                                        (sendHeartbeats) -> {
                                            logger.debug(
                                                    "/namedjobs/{} called",
                                                    jobCluster);
                                            jobClusterInfoStreamGET.increment();
                                            JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest req =
                                                    new JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest(
                                                            jobCluster);

                                            CompletionStage<JobDiscoveryRouteProto.JobClusterInfoResponse> jobClusterInfoRespCS =
                                                    jobDiscoveryRouteHandler.lastSubmittedJobIdStream(
                                                            req,
                                                            sendHeartbeats.orElse(false));
                                            return completeAsync(
                                                    jobClusterInfoRespCS,
                                                    r -> {
                                                        Optional<Observable<JobClusterInfo>> jobClusterInfoO = r
                                                                .getJobClusterInfoObs();
                                                        if (jobClusterInfoO.isPresent()) {
                                                            Observable<JobClusterInfo> jobClusterInfoObs = jobClusterInfoO
                                                                    .get();

                                                            Source<ServerSentEvent, NotUsed> source = Source
                                                                    .fromPublisher(RxReactiveStreams
                                                                                           .toPublisher(
                                                                                                   jobClusterInfoObs))
                                                                    .map(j -> StreamingUtils.from(j)
                                                                                            .orElse(null))
                                                                    .filter(sse -> sse != null);
                                                            return completeOK(
                                                                    source,
                                                                    EventStreamMarshalling.toEventStream());
                                                        } else {
                                                            logger.warn(
                                                                    "Failed to get last submitted jobId stream for {}",
                                                                    jobCluster);
                                                            return complete(
                                                                    StatusCodes.INTERNAL_SERVER_ERROR,
                                                                    "Failed to get last submitted jobId stream for " +
                                                                    jobCluster);
                                                        }
                                                    });
                                        })
                        )
                ))
        );
    }

    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating routes");
        final ExceptionHandler jsonExceptionHandler =
                ExceptionHandler.newBuilder()
                                .match(Exception.class, x -> {
                                    logger.error("got exception", x);
                                    return complete(
                                            StatusCodes.INTERNAL_SERVER_ERROR,
                                            "{\"error\": \"" + x.getMessage() + "\"}");
                                })
                                .build();


        return respondWithHeaders(
                DEFAULT_RESPONSE_HEADERS,
                () -> handleExceptions(
                        jsonExceptionHandler,
                        () -> routeFilter.apply(getJobDiscoveryRoutes())));
    }

}
