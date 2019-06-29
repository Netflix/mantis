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

package io.mantisrx.server.worker;

import java.util.HashSet;
import java.util.Set;


public class WorkerIndexHistory {

    final Set<Integer> runningWorkerIndex = new HashSet<Integer>();
    final Set<Integer> terminalWorkerIndex = new HashSet<Integer>();

    public synchronized void addToRunningIndex(int workerIndex) {
        runningWorkerIndex.add(workerIndex);
    }

    public synchronized void addToTerminalIndex(int workerIndex) {
        terminalWorkerIndex.add(workerIndex);
    }

    public synchronized boolean isRunningOrTerminal(int workerIndex) {
        return runningWorkerIndex.contains(workerIndex)
                || terminalWorkerIndex.contains(workerIndex);
    }

    public synchronized void clearHistory() {
        runningWorkerIndex.clear();
        terminalWorkerIndex.clear();
    }
}
