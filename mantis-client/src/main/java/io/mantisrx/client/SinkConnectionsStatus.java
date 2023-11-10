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

package io.mantisrx.client;

public class SinkConnectionsStatus {

    private final long numConnected;
    private final long total;
    private final long recevingDataFrom;
    private final long runningCount;

    public SinkConnectionsStatus(long recevingDataFrom, long numConnected, long total, long runningCount) {
        this.recevingDataFrom = recevingDataFrom;
        this.numConnected = numConnected;
        this.total = total;
        this.runningCount = runningCount;
    }

    public long getRecevingDataFrom() {
        return recevingDataFrom;
    }

    public long getNumConnected() {
        return numConnected;
    }

    public long getTotal() {
        return total;
    }

    public long getRunningCount() { return runningCount; }
}
