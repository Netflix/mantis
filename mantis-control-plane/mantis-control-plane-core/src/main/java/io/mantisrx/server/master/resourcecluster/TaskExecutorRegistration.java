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

import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Data structure used at the time of registration by the task executor.
 * Different fields help identify the task executor in different dimensions.
 */
@Value
@Builder
public class TaskExecutorRegistration {
    @NonNull
  TaskExecutorID taskExecutorID;

    @NonNull
  ClusterID clusterID;

  // RPC address that's used to talk to the task executor
  @NonNull
  String taskExecutorAddress;

  // host name of the task executor
  @NonNull
  String hostname;

  // ports used by the task executor for various purposes.
  @NonNull
  WorkerPorts workerPorts;

  // machine information identifies the cpu/mem/disk/network resources of the task executor.
  @NonNull
  MachineDefinition machineDefinition;

  // custom attributes describing the task executor
  @NonNull
  Map<String, String> taskExecutorAttributes;

  public Optional<String> getTaskExecutorContainerDefinitionId() {
      return Optional.ofNullable(this.getTaskExecutorAttributes() == null ? null : this.getTaskExecutorAttributes()
              .getOrDefault(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID, null));
  }
}
