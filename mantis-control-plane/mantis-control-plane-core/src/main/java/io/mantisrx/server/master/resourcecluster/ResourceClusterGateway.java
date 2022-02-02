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
package io.mantisrx.server.master.resourcecluster;

import io.mantisrx.common.Ack;
import java.util.concurrent.CompletableFuture;

public interface ResourceClusterGateway {
  // triggered at the start of a resource manager connection
  CompletableFuture<Ack> registerTaskExecutor(TaskExecutorRegistration registration);

  // triggered every epoch after registration
  CompletableFuture<Ack> heartBeatFromTaskExecutor(TaskExecutorHeartbeat heartbeat);

  // triggered whenever the task executor gets occupied with a worker request or is available to do some work
  CompletableFuture<Ack> notifyTaskExecutorStatusChange(TaskExecutorStatusChange taskExecutorStatusChange);

  CompletableFuture<Ack> disconnectTaskExecutor(TaskExecutorDisconnection taskExecutorDisconnection);

  class InvalidStateTransitionException extends Exception {

    public InvalidStateTransitionException(String message) {
      super(message);
    }
  }
}
