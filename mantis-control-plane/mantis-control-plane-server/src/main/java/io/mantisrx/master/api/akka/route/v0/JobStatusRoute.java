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

import akka.NotUsed;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.stream.javadsl.Flow;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;

public class JobStatusRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobStatusRoute.class);
    private final JobStatusRouteHandler jobStatusRouteHandler;

    public JobStatusRoute(final JobStatusRouteHandler jobStatusRouteHandler) {
        this.jobStatusRouteHandler = jobStatusRouteHandler;
    }

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
        HttpHeader.parse("Access-Control-Allow-Origin", "*");
    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS = Arrays.asList(
        ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    private Route getJobStatusRoutes() {
        return route(
            get(() -> route(
                path(segment("job").slash("status").slash(PathMatchers.segment()), (jobId) -> {
                    logger.info("/job/status/{} called", jobId);
                    Flow<Message, Message, NotUsed> webSocketFlow = jobStatusRouteHandler.jobStatus(jobId);
                    return handleWebSocketMessages(webSocketFlow);
                })
            ))
        );
    }

    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating routes");
        final ExceptionHandler jsonExceptionHandler = ExceptionHandler.newBuilder()
            .match(IOException.class, x -> {
                logger.error("got exception", x);
                return complete(StatusCodes.BAD_REQUEST, "caught exception " + x.getMessage());
            })
            .build();


        return respondWithHeaders(DEFAULT_RESPONSE_HEADERS, () -> handleExceptions(jsonExceptionHandler, () -> routeFilter.apply(getJobStatusRoutes())));
    }
}
