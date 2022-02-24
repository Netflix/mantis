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

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.worker.TaskExecutorGateway;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ResourceCluster extends ResourceClusterGateway {
  CompletableFuture<List<TaskExecutorID>> getRegisteredTaskExecutors();

  CompletableFuture<List<TaskExecutorID>> getAvailableTaskExecutors();

  CompletableFuture<List<TaskExecutorID>> getBusyTaskExecutors();

  CompletableFuture<List<TaskExecutorID>> getUnregisteredTaskExecutors();

  CompletableFuture<ResourceOverview> resourceOverview();

  // Can throw NoResourceAvailableException wrapped within the CompletableFuture.
  CompletableFuture<TaskExecutorID> getTaskExecutorFor(MachineDefinition machineDefinition, WorkerId workerId);

  CompletableFuture<TaskExecutorGateway> getTaskExecutorGateway(TaskExecutorID taskExecutorID);

  CompletableFuture<TaskExecutorRegistration> getTaskExecutorInfo(String hostName);

  CompletableFuture<TaskExecutorRegistration> getTaskExecutorInfo(TaskExecutorID taskExecutorID);

  CompletableFuture<TaskExecutorStatus> getTaskExecutorState(TaskExecutorID taskExecutorID);

  class NoResourceAvailableException extends Exception {

    public NoResourceAvailableException(String message) {
      super(message);
    }
  }
}
