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

import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.scheduler.WorkerEvent;


/**
 * Encapsulates a worker terminated event.
 */
public class WorkerTerminate implements WorkerEvent {

    private final JobCompletedReason reason;
    private final WorkerId workerId;
    private final long eventTime;

    private final WorkerState finalState;

    /**
     * Creates an instance of this class.
     * @param workerId
     * @param state
     * @param reason
     * @param time
     */
    public WorkerTerminate(WorkerId workerId, WorkerState state, JobCompletedReason reason, long time) {
        this.workerId = workerId;
        this.reason = reason;
        this.finalState = state;
        this.eventTime = time;
    }

    /**
     * Creates an instance of this class. auto-populates the current time.
     * @param workerId
     * @param state
     * @param reason
     */
    public WorkerTerminate(WorkerId workerId, WorkerState state, JobCompletedReason reason) {
        this(workerId, state, reason, System.currentTimeMillis());
    }

    @Override
    public WorkerId getWorkerId() {
        return workerId;
    }

    @Override
    public long getEventTimeMs() {

        return this.eventTime;
    }

    public JobCompletedReason getReason() {
        return reason;
    }

    public WorkerState getFinalState() {
        return finalState;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (eventTime ^ (eventTime >>> 32));
        result = prime * result + ((finalState == null) ? 0 : finalState.hashCode());
        result = prime * result + ((reason == null) ? 0 : reason.hashCode());
        result = prime * result + ((workerId == null) ? 0 : workerId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WorkerTerminate other = (WorkerTerminate) obj;
        if (eventTime != other.eventTime)
            return false;
        if (finalState != other.finalState)
            return false;
        if (reason != other.reason)
            return false;
        if (workerId == null) {
            if (other.workerId != null)
                return false;
        } else if (!workerId.equals(other.workerId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "WorkerTerminate [reason=" + reason + ", workerId=" + workerId + ", eventTime=" + eventTime
                + ", finalState=" + finalState + "]";
    }


}
