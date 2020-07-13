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

import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.Route;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;

public class MasterDescriptionRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(MasterDescriptionRoute.class);
    private static final ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final MasterDescription masterDesc;
    private String masterDescStr;
    private final List<Configlet> configs = new ArrayList<>();

    public static class Configlet {
        private final String name;
        private final String value;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown=true)
        public Configlet(@JsonProperty("name") String name, @JsonProperty("value") String value) {
            this.name = name;
            this.value = value;
        }
        public String getName() {
            return name;
        }
        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Configlet configlet = (Configlet) o;
            return Objects.equals(name, configlet.name) &&
                Objects.equals(value, configlet.value);
        }

        @Override
        public int hashCode() {

            return Objects.hash(name, value);
        }

        @Override
        public String toString() {
            return "Configlet{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
        }
    }

    static class WorkerResourceLimits {
        private final int maxCpuCores;
        private final int maxMemoryMB;
        private final int maxNetworkMbps;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public WorkerResourceLimits(@JsonProperty("maxCpuCores") final int maxCpuCores,
                                    @JsonProperty("maxMemoryMB") final int maxMemoryMB,
                                    @JsonProperty("maxNetworkMbps") final int maxNetworkMbps) {
            this.maxCpuCores = maxCpuCores;
            this.maxMemoryMB = maxMemoryMB;
            this.maxNetworkMbps = maxNetworkMbps;
        }

        public int getMaxCpuCores() {
            return maxCpuCores;
        }

        public int getMaxMemoryMB() {
            return maxMemoryMB;
        }

        public int getMaxNetworkMbps() {
            return maxNetworkMbps;
        }
    }


    public MasterDescriptionRoute(final MasterDescription masterDescription) {
        this.masterDesc = masterDescription;

        try {
            this.masterDescStr = mapper.writeValueAsString(masterDesc);
        } catch (JsonProcessingException e) {
            logger.error("failed to create json for master desc {}", masterDesc);
            this.masterDescStr = masterDesc.toString();
        }
        try {
            configs.add(new Configlet(JobConstraints.class.getSimpleName(), mapper.writeValueAsString(JobConstraints.values())));
            configs.add(new Configlet(StageScalingPolicy.ScalingReason.class.getSimpleName(), mapper.writeValueAsString(StageScalingPolicy.ScalingReason.values())));
            configs.add(new Configlet(WorkerMigrationConfig.MigrationStrategyEnum.class.getSimpleName(), mapper.writeValueAsString(WorkerMigrationConfig.MigrationStrategyEnum.values())));
            MasterConfiguration config = ConfigurationProvider.getConfig();
            int maxCpuCores = config.getWorkerMachineDefinitionMaxCpuCores();
            int maxMemoryMB = config.getWorkerMachineDefinitionMaxMemoryMB();
            int maxNetworkMbps = config.getWorkerMachineDefinitionMaxNetworkMbps();
            configs.add(new Configlet(WorkerResourceLimits.class.getSimpleName(), mapper.writeValueAsString(new WorkerResourceLimits(maxCpuCores, maxMemoryMB, maxNetworkMbps))));
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN_HEADER =
        HttpHeader.parse("Access-Control-Allow-Origin", "*");
    private static final Iterable<HttpHeader> DEFAULT_RESPONSE_HEADERS = Arrays.asList(
        ACCESS_CONTROL_ALLOW_ORIGIN_HEADER);

    public List<Configlet> getConfigs() {
        return configs;
    }

    private Route getMasterDescRoute() {
        return route(
            get(() -> route(
                path(segment("api").slash("masterinfo"), () -> completeOK(masterDesc, Jackson.marshaller())),
                path(segment("api").slash("masterinfostr"), () -> complete(StatusCodes.OK, masterDescStr)),
                path(segment("api").slash("masterconfig"), () -> completeOK(configs, Jackson.marshaller()))
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


        return respondWithHeaders(DEFAULT_RESPONSE_HEADERS, () -> handleExceptions(jsonExceptionHandler, () -> routeFilter.apply(getMasterDescRoute())));
    }

}
