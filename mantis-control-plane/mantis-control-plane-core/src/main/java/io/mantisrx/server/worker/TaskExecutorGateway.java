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
package io.mantisrx.server.worker;

import io.mantisrx.common.Ack;
import io.mantisrx.server.core.CacheJobArtifactsRequest;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.domain.WorkerId;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.runtime.rpc.RpcGateway;

/**
 * Gateway to talk to the task executor running on the mantis-agent.
 */
public interface TaskExecutorGateway extends RpcGateway {
    /**
     * submit a new task to be run on the task executor. The definition of the task is represented by
     * {@link ExecuteStageRequest}.
     *
     * @param request Task that needs to be run on the executor.
     * @return Ack to indicate that the gateway was able to receive the task.
     * @throws TaskAlreadyRunningException wrapped inside {@link java.util.concurrent.CompletionException}
     *                                     in case there's already an existing task that's running on the task executor.
     */
    CompletableFuture<Ack> submitTask(ExecuteStageRequest request);

    /**
     * instruct the task executor on which job artifacts to cache in order to speed up job initialization time.
     *
     * @param request List of job artifacts that need to be cached.
     * @return Ack in any case (this task is best effort).
     */
    CompletableFuture<Ack> cacheJobArtifacts(CacheJobArtifactsRequest request);

    /**
     * cancel the currently running task and get rid of all of the associated resources.
     *
     * @param workerId of the task that needs to be cancelled.
     * @return Ack to indicate that the gateway was able to receive the request and the worker ID represents the currently
     * running task.
     * @throws TaskNotFoundException wrapped inside a {@link java.util.concurrent.CompletionException} in case
     *                               workerId is not running on the executor.
     */
    CompletableFuture<Ack> cancelTask(WorkerId workerId);

    /**
     * request a thread dump on the worker to see what threads are running on it.
     *
     * @return thread dump in the string format.
     */
    CompletableFuture<String> requestThreadDump();

    CompletableFuture<Boolean> isRegistered();

    class TaskAlreadyRunningException extends Exception {
        private static final long serialVersionUID = 1L;

        private final WorkerId currentlyRunningWorkerTask;

        public TaskAlreadyRunningException(WorkerId workerId) {
            this(workerId, null);
        }

        public TaskAlreadyRunningException(WorkerId workerId, Throwable cause) {
            super(cause);
            this.currentlyRunningWorkerTask = workerId;
        }
    }

    class TaskNotFoundException extends Exception {
        private static final long serialVersionUID = 1L;

        public TaskNotFoundException(WorkerId workerId) {
            this(workerId, null);
        }

        public TaskNotFoundException(WorkerId workerId, Throwable cause) {
            super(String.format("Task %s not found", workerId.toString()), cause);
        }
    }
}
