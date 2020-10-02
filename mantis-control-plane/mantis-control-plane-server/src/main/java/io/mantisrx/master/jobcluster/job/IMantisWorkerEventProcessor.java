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

package io.mantisrx.master.jobcluster.job;


import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.WorkerEvent;


/**
 * Declares behavior the Mantis worker event processor which is responsible for managing
 * Worker state.
 */
public interface IMantisWorkerEventProcessor {

    /**
     * Handles state transition for a worker.
     * @param event
     * @param jobStore
     * @throws Exception
     */
    public void processEvent(WorkerEvent event, MantisJobStore jobStore) throws Exception;
}
