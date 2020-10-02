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


public class WorkerUnscheduleable implements WorkerEvent {

    private final WorkerId workerId;
    private final int stageNum;
    private final long eventTimeMs = System.currentTimeMillis();

    public WorkerUnscheduleable(final WorkerId workerId,
                                final int stageNum) {
        this.workerId = workerId;
        this.stageNum = stageNum;
    }

    @Override
    public WorkerId getWorkerId() {
        return workerId;
    }

    public int getStageNum() {
        return stageNum;
    }

    @Override
    public long getEventTimeMs() {
        return eventTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerUnscheduleable that = (WorkerUnscheduleable) o;
        return stageNum == that.stageNum &&
                eventTimeMs == that.eventTimeMs &&
                Objects.equals(workerId, that.workerId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(workerId, stageNum, eventTimeMs);
    }

    @Override
    public String toString() {
        return "WorkerUnscheduleable{" +
                "workerId=" + workerId +
                ", stageNum=" + stageNum +
                ", eventTimeMs=" + eventTimeMs +
                '}';
    }
}
