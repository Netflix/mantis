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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import io.mantisrx.runtime.MigrationStrategy;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.utils.MantisClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OneWorkerPerTickMigrationStrategy extends MigrationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(OneWorkerPerTickMigrationStrategy.class);

    private final String jobId;
    private final MantisClock clock;
    private long intervalMoveWorkersOnDisabledVMsMillis;

    public OneWorkerPerTickMigrationStrategy(final MantisClock clock,
                                             final String jobId,
                                             final WorkerMigrationConfig config) {
        super(config);
        this.clock = clock;
        this.jobId = jobId;
        try {
            this.intervalMoveWorkersOnDisabledVMsMillis = ConfigurationProvider.getConfig().getIntervalMoveWorkersOnDisabledVMsMillis();
        } catch (IllegalStateException ise) {
            logger.warn("[{}] Error reading intervalMoveWorkersOnDisabledVMsMillis from config Provider, will default to 1 minute", jobId);
            this.intervalMoveWorkersOnDisabledVMsMillis = 60_000L;
        }
    }

    @Override
    public List<Integer> execute(final ConcurrentSkipListSet<Integer> workersOnDisabledVms,
                                 final int numRunningWorkers,
                                 final int totalNumWorkers,
                                 final long lastMovedWorkerOnDisabledVM) {
        if (lastMovedWorkerOnDisabledVM > (clock.now() - intervalMoveWorkersOnDisabledVMsMillis)) {
            return Collections.emptyList();
        }

        final Integer workerNumber = workersOnDisabledVms.pollFirst();
        if (workerNumber != null) {
            return Collections.singletonList(workerNumber);
        } else {
            return Collections.emptyList();
        }
    }
}
