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

import io.mantisrx.runtime.MigrationStrategy;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.server.master.utils.MantisSystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MigrationStrategyFactory {

    private static final Logger logger = LoggerFactory.getLogger(MigrationStrategyFactory.class);

    public static MigrationStrategy getStrategy(final String jobId, final WorkerMigrationConfig config) {
        switch (config.getStrategy()) {
        case PERCENTAGE:
            return new PercentageMigrationStrategy(MantisSystemClock.INSTANCE, jobId, config);

        case ONE_WORKER:
            return new OneWorkerPerTickMigrationStrategy(MantisSystemClock.INSTANCE, jobId, config);

        default:
            logger.error("unknown strategy type {} in config {}, using default strategy to migrate 25 percent every 1 min", config.getStrategy(), config);
            return new PercentageMigrationStrategy(MantisSystemClock.INSTANCE, jobId,
                    new WorkerMigrationConfig(
                            WorkerMigrationConfig.MigrationStrategyEnum.PERCENTAGE,
                            "{\"percentToMove\":25,\"intervalMs\":60000}"));
        }
    }
}
