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

package io.mantisrx.server.worker.mesos;

public class VirtualMachineTaskStatus {

    private final String taskId;
    private final TYPE type;
    private final String message;
    public VirtualMachineTaskStatus(String taskId, TYPE type, String message) {
        this.taskId = taskId;
        this.type = type;
        this.message = message;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getMessage() {
        return message;
    }

    public TYPE getType() {
        return type;
    }

    public enum TYPE {
        STARTED, COMPLETED, ERROR
    }
}
