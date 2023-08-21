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

/**
 * Gateway for performing all actions corresponding to the resource cluster from the task executor.
 */
public interface ResourceClusterGateway {
  /**
   * triggered at the start of a task executor or when the task executor has lost connection to the resource cluster
   * previously
   */
  CompletableFuture<Ack> registerTaskExecutor(TaskExecutorRegistration registration);

  /**
   * Triggered when the task executor needs to send a heartbeat every epoch after registration.
   * Absence of a heartbeat from the task executor implies loss of the task executor or a network partition.
   */
  CompletableFuture<Ack> heartBeatFromTaskExecutor(TaskExecutorHeartbeat heartbeat);

  /**
   * Triggered whenever the task executor gets occupied with a worker request or is available to do some work
   */
  CompletableFuture<Ack> notifyTaskExecutorStatusChange(TaskExecutorStatusChange taskExecutorStatusChange);

  /**
   * Triggered by the task executor when it's about to shut itself down.
   */
  CompletableFuture<Ack> disconnectTaskExecutor(TaskExecutorDisconnection taskExecutorDisconnection);

  /**
   * Exception thrown by the resource cluster whenever anyone of the state transitions of the task executor are invalid.
   * Note that the exception generally gets wrapped inside {@link java.util.concurrent.CompletionException}.
   */
  class InvalidStateTransitionException extends Exception {

    public InvalidStateTransitionException(String message) {
      super(message);
    }
  }
}
