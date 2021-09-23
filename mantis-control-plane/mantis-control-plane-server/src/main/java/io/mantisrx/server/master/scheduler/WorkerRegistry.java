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

package io.mantisrx.server.master.scheduler;

import io.mantisrx.server.core.domain.WorkerId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public interface WorkerRegistry {

    /* Returns number of workers in LAUNCHED, START_INITIATED and STARTED state */
    int getNumRunningWorkers();


    /* Returns the set of all workers in LAUNCHED, START_INITIATED and STARTED state */
    Set<WorkerId> getAllRunningWorkers();

    /* Returns the map of all workers to SlaveId in LAUNCHED, START_INITIATED and STARTED state */
    Map<WorkerId, String> getAllRunningWorkerSlaveIdMappings();

    /**
     * @param workerId id to check
     *
     * @return false is job/worker is in Terminal State, otherwise true
     */
    boolean isWorkerValid(final WorkerId workerId);

    /**
     * Get time at which the worker was Accepted
     *
     * @param workerId Worker ID
     *
     * @return time when worker was Accepted
     */
    Optional<Long> getAcceptedAt(final WorkerId workerId);
}