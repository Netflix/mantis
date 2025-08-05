/*
 * Copyright 2024 Netflix, Inc.
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
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import java.util.Objects;

/**
 * WorkerEvent indicating that a TaskExecutor that was previously running a worker
 * has reconnected after a failure. This allows JobActors to refresh their scheduling
 * info to prevent stale task executor mappings.
 */
public class TaskExecutorReconnectedEvent implements WorkerEvent {

    private final WorkerId previousWorkerId;
    private final TaskExecutorID taskExecutorID;
    private final long eventTimeMs = System.currentTimeMillis();

    public TaskExecutorReconnectedEvent(final WorkerId previousWorkerId, final TaskExecutorID taskExecutorID) {
        this.previousWorkerId = previousWorkerId;
        this.taskExecutorID = taskExecutorID;
    }

    @Override
    public WorkerId getWorkerId() {
        return previousWorkerId;
    }

    @Override
    public long getEventTimeMs() {
        return eventTimeMs;
    }

    public TaskExecutorID getTaskExecutorID() {
        return taskExecutorID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskExecutorReconnectedEvent that = (TaskExecutorReconnectedEvent) o;
        return eventTimeMs == that.eventTimeMs &&
                Objects.equals(previousWorkerId, that.previousWorkerId) &&
                Objects.equals(taskExecutorID, that.taskExecutorID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previousWorkerId, taskExecutorID, eventTimeMs);
    }

    @Override
    public String toString() {
        return "TaskExecutorReconnectedEvent{" +
                "previousWorkerId=" + previousWorkerId +
                ", taskExecutorID=" + taskExecutorID +
                ", eventTimeMs=" + eventTimeMs +
                '}';
    }
}
