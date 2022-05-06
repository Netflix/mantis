/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.master.client;

import io.mantisrx.server.core.Status;

/**
 * TaskStatusUpdateHandler is responsible for handling updates to task statuses as the task is being run.
 */
public interface TaskStatusUpdateHandler {
    void onStatusUpdate(Status status);

    /**
     * Creates a task status update handler that keeps the mantis master up to date on the task's progress.
     * @param gateway gateway that needs to kept upto date.
     * @return created instance of TaskStatusUpdateHandler
     */
    static TaskStatusUpdateHandler forReportingToGateway(MantisMasterGateway gateway) {
        return new TaskStatusUpdateHandlerImpl(gateway);
    }
}
