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

import io.mantisrx.master.jobcluster.job.JobSettings;
import io.mantisrx.runtime.MigrationStrategy;
import io.mantisrx.runtime.WorkerMigrationConfig;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;


public class OneWorkerPerTickMigrationStrategy extends MigrationStrategy {

    private final Clock clock;
    private final Duration workerOnDisabledVMsCheckInterval;

    public OneWorkerPerTickMigrationStrategy(final Clock clock,
                                             final String jobId,
                                             final WorkerMigrationConfig config,
                                             final JobSettings jobSettings) {
        super(config);
        this.clock = clock;
        this.workerOnDisabledVMsCheckInterval = jobSettings.getWorkerOnDisabledVMsCheckInterval();
    }

    @Override
    public List<Integer> execute(final ConcurrentSkipListSet<Integer> workersOnDisabledVms,
                                 final int numRunningWorkers,
                                 final int totalNumWorkers,
                                 final long lastMovedWorkerOnDisabledVM) {
        if (Duration.between(Instant.ofEpochMilli(lastMovedWorkerOnDisabledVM), clock.instant()).compareTo(workerOnDisabledVMsCheckInterval) < 0) {
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
