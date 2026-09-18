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

package io.mantisrx.server.master.resourcecluster;

import io.mantisrx.server.core.domain.WorkerId;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.JsonNode;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

/**
 * Exception to represent a task executor's assigned task has been cancelled.
 */
@Getter
public class TaskExecutorTaskCancelledException extends Exception {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final WorkerId workerId;

    public TaskExecutorTaskCancelledException(String msg, WorkerId workerId) {
        super(msg);
        this.workerId = workerId;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // Do not include stack trace to be returned to clients
        return this;
    }

    /**
     * Serializes the full exception object including workerId to JsonNode.
     * @return JsonNode representation of this exception
     */
    public JsonNode toJsonNode() {
        return mapper.valueToTree(this);
    }
}
