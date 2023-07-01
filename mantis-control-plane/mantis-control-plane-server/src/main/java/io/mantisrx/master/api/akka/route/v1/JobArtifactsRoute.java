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

import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.Route;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.handlers.JobArtifactRouteHandler;
import io.mantisrx.master.jobcluster.proto.JobArtifactProto;
import io.mantisrx.master.jobcluster.proto.JobArtifactProto.SearchJobArtifactsRequest;
import io.mantisrx.master.jobcluster.proto.JobArtifactProto.UpsertJobArtifactResponse;
import io.mantisrx.server.core.domain.JobArtifact;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * JobArtifactsRoute endpoints:
 *  - /api/v1/jobArtifacts (GET, POST)
 *  - /api/v1/jobArtifacts/names (GET)
 */
public class JobArtifactsRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(JobArtifactsRoute.class);
    private static final PathMatcher0 JOB_ARTIFACTS_API_PREFIX = segment("api").slash("v1").slash("jobArtifacts");

    private final JobArtifactRouteHandler jobArtifactRouteHandler;

    // TODO(fdichiara): consolidate object mappers. This is needed because
    //  Instant cannot be serialized by the existing ObjectMappers
    private static final ObjectMapper JAVA_TIME_COMPATIBLE_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule());
    public static final SimpleFilterProvider DEFAULT_FILTER_PROVIDER;

    static {
        DEFAULT_FILTER_PROVIDER = new SimpleFilterProvider();
        DEFAULT_FILTER_PROVIDER.setFailOnUnknownId(false);
    }

    public JobArtifactsRoute(final JobArtifactRouteHandler jobArtifactRouteHandler) {
        this.jobArtifactRouteHandler = jobArtifactRouteHandler;
    }

    public Route constructRoutes() {
        return concat(
                pathPrefix(JOB_ARTIFACTS_API_PREFIX, () -> concat(
                        // /api/v1/jobArtifacts
                        pathEndOrSingleSlash(() -> concat(
                                // GET - search job artifacts by name and version (optional)
                                get(this::getJobArtifactsRoute),

                                // POST - register new job artifact
                                post(this::postJobArtifactRoute)
                        )),
                        // /api/v1/jobArtifacts/names
                        path(
                            "names",
                            () -> pathEndOrSingleSlash(
                                // GET - search job artifacts names by prefix
                                () -> get(this::listJobArtifactsByNameRoute)
                            )
                        )
                    )
                )
        );
    }

    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/jobArtifacts routes");
        return super.createRoute(routeFilter);
    }

    private Route getJobArtifactsRoute() {
        logger.trace("GET /api/v1/jobArtifacts called");
        return parameterMap(param -> completeAsync(
            jobArtifactRouteHandler.search(new SearchJobArtifactsRequest(param.get("name"), param.get("version"))),
            resp -> completeOK(
                resp.getJobArtifacts(),
                Jackson.marshaller(JAVA_TIME_COMPATIBLE_MAPPER)),
            HttpRequestMetrics.Endpoints.JOB_ARTIFACTS,
            HttpRequestMetrics.HttpVerb.GET));
    }

    private Route postJobArtifactRoute() {
        return entity(
            Jackson.unmarshaller(JAVA_TIME_COMPATIBLE_MAPPER, JobArtifact.class),
            jobArtifact -> {
                logger.trace("POST /api/v1/jobArtifacts called with payload: {}", jobArtifact);
                try {
                    final CompletionStage<UpsertJobArtifactResponse> response = jobArtifactRouteHandler.upsert(new JobArtifactProto.UpsertJobArtifactRequest(jobArtifact));

                    return completeAsync(
                        response,
                        resp -> complete(
                                StatusCodes.CREATED,
                                resp.getArtifactID(),
                                Jackson.marshaller(JAVA_TIME_COMPATIBLE_MAPPER)),
                        HttpRequestMetrics.Endpoints.JOB_ARTIFACTS,
                        HttpRequestMetrics.HttpVerb.POST);
                } catch (Exception e) {
                    return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Failed to store job artifact");
                }
            }
        );
    }

    private Route listJobArtifactsByNameRoute() {
        logger.trace("GET /api/v1/jobArtifacts/names called");
        return parameterMap(param -> completeAsync(
            jobArtifactRouteHandler.listArtifactsByName(new JobArtifactProto.ListJobArtifactsByNameRequest(param.getOrDefault("prefix", ""), param.getOrDefault("contains", ""))),
            resp -> completeOK(
                resp.getNames(),
                Jackson.marshaller(JAVA_TIME_COMPATIBLE_MAPPER)),
            HttpRequestMetrics.Endpoints.JOB_ARTIFACTS_NAMES,
            HttpRequestMetrics.HttpVerb.GET));
    }
}
