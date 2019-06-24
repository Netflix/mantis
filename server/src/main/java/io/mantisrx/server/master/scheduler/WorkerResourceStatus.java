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

import java.util.Objects;

import io.mantisrx.server.core.domain.WorkerId;


public class WorkerResourceStatus implements WorkerEvent {

    private final WorkerId workerId;
    private final String message;
    private final VMResourceState state;
    private final long eventTimeMs = System.currentTimeMillis();
    public WorkerResourceStatus(final WorkerId workerId,
                                final String message,
                                final VMResourceState state) {
        this.workerId = workerId;
        this.message = message;
        this.state = state;
    }

    @Override
    public WorkerId getWorkerId() {
        return workerId;
    }

    public String getMessage() {
        return message;
    }

    public VMResourceState getState() {
        return state;
    }

    @Override
    public long getEventTimeMs() {
        return eventTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerResourceStatus that = (WorkerResourceStatus) o;
        return eventTimeMs == that.eventTimeMs &&
                Objects.equals(workerId, that.workerId) &&
                Objects.equals(message, that.message) &&
                state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId, message, state, eventTimeMs);
    }

    @Override
    public String toString() {
        return "WorkerResourceStatus{" +
                "workerId=" + workerId +
                ", message='" + message + '\'' +
                ", state=" + state +
                ", eventTimeMs=" + eventTimeMs +
                '}';
    }

    public enum VMResourceState {
        STARTED,
        START_INITIATED,
        COMPLETED,
        FAILED
    }
}
