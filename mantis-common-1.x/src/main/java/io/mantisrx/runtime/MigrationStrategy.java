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

package io.mantisrx.runtime;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;


public abstract class MigrationStrategy {

    private final WorkerMigrationConfig config;

    public MigrationStrategy(final WorkerMigrationConfig config) {
        this.config = config;
    }

    /**
     * @param workersOnDisabledVms         set of WorkerNumber on disabled VM
     * @param numRunningWorkers            total number of running workers for this job
     * @param totalNumWorkers              total number of workers for this job
     * @param lastWorkerMigrationTimestamp last timestamp at which a worker was migrated for this job
     *
     * @return list of WorkerNumber to migrate in this iteration
     */
    abstract public List<Integer> execute(final ConcurrentSkipListSet<Integer> workersOnDisabledVms,
                                          final int numRunningWorkers,
                                          final int totalNumWorkers,
                                          final long lastWorkerMigrationTimestamp);
}
