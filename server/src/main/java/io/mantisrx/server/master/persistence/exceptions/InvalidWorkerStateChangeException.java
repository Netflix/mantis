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

package io.mantisrx.server.master.persistence.exceptions;

import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.core.domain.WorkerId;


public class InvalidWorkerStateChangeException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = 6997193965197779136L;

    public InvalidWorkerStateChangeException(String jobId, WorkerState state) {
        super("Unexpected state " + state + " for job " + jobId);
    }

    public InvalidWorkerStateChangeException(String jobId, WorkerState state, Throwable t) {
        super("Unexpected state " + state + " for job " + jobId, t);
    }

    public InvalidWorkerStateChangeException(String jobId, WorkerId workerId, WorkerState fromState, WorkerState toState) {
        super("Invalid worker state transition of " + workerId.getId() + " from state " + fromState + " to " + toState);
    }

    public InvalidWorkerStateChangeException(String jobId, WorkerState fromState, WorkerState toState, Throwable cause) {
        super("Invalid worker state transition of job " + jobId + " from state " + fromState + " to " + toState, cause);
    }
}
