/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.common.metrics;

import io.mantisrx.shaded.com.google.common.base.Joiner;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * This implementation will print the selected metric groups in log.
 * To use this publisher override config "mantis.metricsPublisher.class" in the master-*.properties file.
 */
@Slf4j
public class LoggingMetricsPublisher extends MetricsPublisher {
    public static final String LOGGING_ENABLED_METRICS_GROUP_ID_LIST_KEY =
            "MANTIS_LOGGING_ENABLED_METRICS_GROUP_ID_LIST";
    private Set<String> loggingEnabledMetricsGroupId = new HashSet<>();

    public LoggingMetricsPublisher(Properties properties) {
        super(properties);

        String key = properties.getProperty(LOGGING_ENABLED_METRICS_GROUP_ID_LIST_KEY,
                System.getenv(LOGGING_ENABLED_METRICS_GROUP_ID_LIST_KEY));
        log.info("LOGGING_ENABLED_METRICS_GROUP_ID_LIST_KEY: {}", key);

        if (key != null) {
            this.loggingEnabledMetricsGroupId =
                    Arrays.stream(key
                            .toLowerCase().split(";")).collect(Collectors.toSet());
            log.info("[Metrics Publisher] enable logging for: {}",
                    Joiner.on(',').join(this.loggingEnabledMetricsGroupId));
        }
    }

    @Override
    public void publishMetrics(long timestamp,
            Collection<Metrics> currentMetricsRegistered) {
        log.info("Printing metrics from: {}", Instant.ofEpochMilli(timestamp));
        currentMetricsRegistered.stream()
                .filter(ms -> this.loggingEnabledMetricsGroupId.contains(ms.getMetricGroupId().id().toLowerCase()))
                .map(ms -> ms.counters().entrySet())
                .flatMap(Collection::stream)
                .forEach(m -> log.info("[METRICS] {} : {}", m.getKey(), m.getValue().value()));
    }
}
