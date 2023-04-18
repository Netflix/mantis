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

package io.mantisrx.server.worker.scheduling;

import io.mantisrx.server.core.WorkerHost;


public class WorkerIndexChange {

    private final int workerIndex;
    private final WorkerHost newState;
    private final WorkerHost oldState;

    public WorkerIndexChange(int workerIndex, WorkerHost newState,
                             WorkerHost oldState) {
        this.workerIndex = workerIndex;
        this.newState = newState;
        this.oldState = oldState;
    }

    public WorkerHost getNewState() {
        return newState;
    }

    public WorkerHost getOldState() {
        return oldState;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    @Override
    public String toString() {
        return "WorkerIndexChange [workerIndex=" + workerIndex + ", newState="
                + newState + ", oldState=" + oldState + "]";
    }
}
