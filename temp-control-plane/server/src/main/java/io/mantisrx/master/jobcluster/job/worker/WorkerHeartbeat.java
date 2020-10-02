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

package io.mantisrx.master.jobcluster.job.worker;

import java.time.Instant;

import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.scheduler.WorkerEvent;


/**
 * A WorkerHeartbeat object encapsulates the heart beat data sent by the worker to the master.
 */
public class WorkerHeartbeat implements WorkerEvent {

    private WorkerId workerId;
    private Status heartBeat;
    private WorkerState workerState;
    private long time;

    /**
     * Creates an instance of this class from a {@link Status} object.
     * @param hb
     */
    public WorkerHeartbeat(Status hb) {
        this(hb, Instant.ofEpochMilli(hb.getTimestamp()));
    }

    /**
     * For testing only.
     *
     * @param hb
     * @param time
     */
    public WorkerHeartbeat(Status hb, Instant time) {
        this.heartBeat = hb;
        String jobId = heartBeat.getJobId();
        int index = heartBeat.getWorkerIndex();
        int number = heartBeat.getWorkerNumber();
        this.time = time.toEpochMilli();
        workerId = new WorkerId(jobId, index, number);
        workerState = setWorkerState(heartBeat.getState());
    }

    private WorkerState setWorkerState(MantisJobState state) {

        switch (state) {

        case Launched:
            return WorkerState.Launched;

        case Started:
            return WorkerState.Started;

        case StartInitiated:
            return WorkerState.StartInitiated;

        case Completed:
            return WorkerState.Completed;

        case Failed:
            return WorkerState.Failed;

        case Noop:
            return WorkerState.Noop;
        default:
            return WorkerState.Unknown;
        }
    }

    @Override
    public WorkerId getWorkerId() {
        return this.workerId;
    }

    public Status getStatus() {
        return this.heartBeat;
    }

    public WorkerState getState() {
        return workerState;
    }

    @Override
    public long getEventTimeMs() {
        return this.time;
    }

    @Override
    public String toString() {
        return "WorkerHeartbeat [workerId=" + workerId + ", heartBeat="
                + heartBeat + ", workerState=" + workerState
                + ", time=" + time + "]";
    }


}
