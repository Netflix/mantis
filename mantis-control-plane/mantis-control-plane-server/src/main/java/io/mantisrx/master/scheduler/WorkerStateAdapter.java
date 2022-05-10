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

package io.mantisrx.master.scheduler;

import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerStateAdapter {
    private static final Logger logger = LoggerFactory.getLogger(WorkerStateAdapter.class);
    // Mark constructor private as this class is not intended to be instantiated
    private WorkerStateAdapter() {}

    public static WorkerState from(final WorkerResourceStatus.VMResourceState resourceState) {
        final WorkerState state;
        switch (resourceState) {
            case START_INITIATED:
                state = WorkerState.StartInitiated;
                break;
            case STARTED:
                state = WorkerState.Started;
                break;
            case FAILED:
                state = WorkerState.Failed;
                break;
            case COMPLETED:
                state = WorkerState.Completed;
                break;
            default:
                logger.error("Missing WorkerState mapping for VMResourceState {}", resourceState);
                throw new IllegalArgumentException("unknown enum value for VMResourceState " + resourceState);
        }
        return state;
    }
}
