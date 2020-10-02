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

import akka.http.javadsl.server.PathMatcher0;
import akka.http.javadsl.server.Route;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;

/***
 * Master description route
 * Defines the following end points:
 *    /api/v1/masterInfo     (GET)
 *    /api/v1/masterConfigs  (GET)
 */
public class AdminMasterRoute extends BaseRoute {
    private static final Logger logger = LoggerFactory.getLogger(AdminMasterRoute.class);
    private static final PathMatcher0 MASTER_API_PREFIX = segment("api").slash("v1");
    private static final ObjectMapper mapper = new ObjectMapper().configure(
            DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false);
    private final MasterDescription masterDesc;
    private final List<Configlet> configs = new ArrayList<>();


    public static class Configlet {
        private final String name;
        private final String value;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
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
        public WorkerResourceLimits(
                @JsonProperty("maxCpuCores") final int maxCpuCores,
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

    public AdminMasterRoute(final MasterDescription masterDescription) {
        //TODO: hardcode some V1 admin master info, this should be cleaned up once v0 apis
        // are deprecated
        this.masterDesc = new MasterDescription(masterDescription.getHostname(),
                                                masterDescription.getHostIP(),
                                                masterDescription.getApiPort(),
                                                masterDescription.getSchedInfoPort(),
                                                -1,
                                                "api/v1/jobs/actions/postJobStatus",
                                                -1,
                                                masterDescription.getCreateTime());

        try {
            configs.add(new Configlet(
                    JobConstraints.class.getSimpleName(),
                    mapper.writeValueAsString(JobConstraints.values())));
            configs.add(new Configlet(
                    StageScalingPolicy.ScalingReason.class.getSimpleName(),
                    mapper.writeValueAsString(StageScalingPolicy.ScalingReason.values())));
            configs.add(new Configlet(
                    WorkerMigrationConfig.MigrationStrategyEnum.class.getSimpleName(),
                    mapper.writeValueAsString(WorkerMigrationConfig.MigrationStrategyEnum.values())));
            MasterConfiguration config = ConfigurationProvider.getConfig();
            int maxCpuCores = config.getWorkerMachineDefinitionMaxCpuCores();
            int maxMemoryMB = config.getWorkerMachineDefinitionMaxMemoryMB();
            int maxNetworkMbps = config.getWorkerMachineDefinitionMaxNetworkMbps();
            configs.add(new Configlet(
                    WorkerResourceLimits.class.getSimpleName(),
                    mapper.writeValueAsString(new WorkerResourceLimits(
                            maxCpuCores,
                            maxMemoryMB,
                            maxNetworkMbps))));
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public List<Configlet> getConfigs() {
        return configs;
    }


    @Override
    protected Route constructRoutes() {
        return pathPrefix(
                MASTER_API_PREFIX,
                () -> concat(
                        // GET api/v1/masterInfo
                        path(segment("masterInfo"), () -> pathEndOrSingleSlash(() -> concat(
                                get(this::getMasterInfo)))),

                        // GET api/v1/masterConfigs
                        path(segment("masterConfigs"), () -> pathEndOrSingleSlash(() -> concat(
                                get(this::getMasterConfigs))))
                ));
    }

    @Override
    public Route createRoute(Function<Route, Route> routeFilter) {
        logger.info("creating /api/v1/masterInfo routes");
        logger.info("creating /api/v1/masterConfigs routes");
        return super.createRoute(routeFilter);
    }


    private Route getMasterInfo() {
        logger.info("GET /api/v1/masterInfo called");
        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                HttpRequestMetrics.Endpoints.MASTER_INFO,
                new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                new BasicTag("responseCode", "200"));

        return completeOK(masterDesc, Jackson.marshaller());

    }

    private Route getMasterConfigs() {

        logger.info("GET /api/v1/masterConfigs called");
        HttpRequestMetrics.getInstance().incrementEndpointMetrics(
                HttpRequestMetrics.Endpoints.MASTER_CONFIGS,
                new BasicTag("verb", HttpRequestMetrics.HttpVerb.GET.toString()),
                new BasicTag("responseCode", "200"));

        return completeOK(configs, Jackson.marshaller());

    }
}