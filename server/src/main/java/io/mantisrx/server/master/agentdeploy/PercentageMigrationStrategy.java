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

package io.mantisrx.server.master.agentdeploy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.runtime.MigrationStrategy;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.utils.MantisClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PercentageMigrationStrategy extends MigrationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(PercentageMigrationStrategy.class);

    private static final int DEFAULT_PERCENT_WORKERS = 10;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final MantisClock clock;
    private final String jobId;
    private final Configuration configuration;
    public PercentageMigrationStrategy(final MantisClock clock,
                                       final String jobId,
                                       final WorkerMigrationConfig config) {
        super(config);
        this.clock = clock;
        this.jobId = jobId;
        long defaultMigrationIntervalMs;
        try {
            defaultMigrationIntervalMs = ConfigurationProvider.getConfig().getIntervalMoveWorkersOnDisabledVMsMillis();
        } catch (IllegalStateException ise) {
            logger.warn("Error reading intervalMoveWorkersOnDisabledVMsMillis from config Provider, will default to 1 minute");
            defaultMigrationIntervalMs = 60_000L;
        }
        configuration = parseConfig(config.getConfigString(), defaultMigrationIntervalMs);
    }

    Configuration parseConfig(final String configuration, final long defaultMigrationIntervalMs) {
        try {
            return objectMapper.readValue(configuration, Configuration.class);
        } catch (IOException e) {
            logger.error("failed to parse config '{}' for job {}, default to {} percent workers migrated every {} millis", configuration, jobId, DEFAULT_PERCENT_WORKERS, defaultMigrationIntervalMs);
            return new Configuration(DEFAULT_PERCENT_WORKERS, defaultMigrationIntervalMs);
        }
    }

    @Override
    public List<Integer> execute(final ConcurrentSkipListSet<Integer> workersOnDisabledVms,
                                 final int numRunningWorkers,
                                 final int totalNumWorkers,
                                 final long lastWorkerMigrationTimestamp) {
        if (lastWorkerMigrationTimestamp > (clock.now() - configuration.getIntervalMs())) {
            return Collections.emptyList();
        }

        if (workersOnDisabledVms.isEmpty()) {
            return Collections.emptyList();
        }
        final int numWorkersOnDisabledVM = workersOnDisabledVms.size();

        final int numInactiveWorkers = totalNumWorkers - numRunningWorkers;
        int numWorkersToMigrate = Math.min(numWorkersOnDisabledVM, Math.max(1, (int) Math.ceil(totalNumWorkers * configuration.getPercentToMove() / 100.0)));

        // If we already have inactive workers for the job, don't migrate more workers as we could end up with all workers in not running state for a job
        if (numInactiveWorkers >= numWorkersToMigrate) {
            logger.debug("[{}] num inactive workers {} > num workers to migrate {}, suppressing percent migrate", jobId, numInactiveWorkers, numWorkersToMigrate);
            return Collections.emptyList();
        } else {
            // ensure no more than percentToMove workers for the job are in inactive state
            numWorkersToMigrate = numWorkersToMigrate - numInactiveWorkers;
        }

        final List<Integer> workersToMigrate = new ArrayList<>(numWorkersToMigrate);
        for (int i = numWorkersToMigrate; i > 0; i--) {
            final Integer workerToMigrate = workersOnDisabledVms.pollFirst();
            if (workerToMigrate != null) {
                workersToMigrate.add(workerToMigrate);
            }
        }

        if (workersToMigrate.size() > 0) {
            logger.debug("migrating jobId {} workers {}", jobId, workersToMigrate);
        }

        return workersToMigrate;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    static class Configuration {

        private final int percentToMove;
        private final long intervalMs;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Configuration(@JsonProperty("percentToMove") final int percentToMove,
                             @JsonProperty("intervalMs") final long intervalMs) {
            this.percentToMove = percentToMove;
            this.intervalMs = intervalMs;
        }

        public int getPercentToMove() {
            return percentToMove;
        }

        public long getIntervalMs() {
            return intervalMs;
        }
    }
}
