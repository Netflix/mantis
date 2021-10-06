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
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.stream.javadsl.Flow;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * JobStatusStreamRoute
 * Defines the following end points:
 *    /api/v1/jobStatusStream/{jobId}        (websocket)
 */
public class JobStatusStreamRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobStatusStreamRoute.class);
    private final JobStatusRouteHandler jobStatusRouteHandler;

    private static final PathMatcher0 JOBSTATUS_API_PREFIX = segment("api").slash("v1");

    public JobStatusStreamRoute(final JobStatusRouteHandler jobStatusRouteHandler) {
        this.jobStatusRouteHandler = jobStatusRouteHandler;
    }

    @Override
    protected Route constructRoutes() {
        return pathPrefix(
                JOBSTATUS_API_PREFIX,
                () -> concat(
                        path(segment("jobStatusStream").slash(PathMatchers.segment()), (jobId) ->
                                get(() -> getJobStatusStreamRoute(jobId))
                        )
                )
        );
    }


    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/jobStatusStream routes");
        return super.createRoute(routeFilter);
    }

    private Route getJobStatusStreamRoute(String jobId) {
        logger.info("/api/v1/jobStatusStream/{} called", jobId);

        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                HttpRequestMetrics.Endpoints.JOB_STATUS_STREAM);

        Flow<Message, Message, NotUsed> webSocketFlow = jobStatusRouteHandler.jobStatus(jobId);
        return handleWebSocketMessages(webSocketFlow);
    }
}
