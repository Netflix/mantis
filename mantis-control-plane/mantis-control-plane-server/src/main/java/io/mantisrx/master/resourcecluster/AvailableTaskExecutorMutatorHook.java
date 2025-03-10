/*
 * Copyright 2025 Netflix, Inc.
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

package io.mantisrx.master.resourcecluster;

import io.mantisrx.master.resourcecluster.ExecutorStateManagerImpl.TaskExecutorHolder;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import java.util.stream.Stream;

public interface AvailableTaskExecutorMutatorHook {
    Stream<TaskExecutorHolder> mutate(Stream<TaskExecutorHolder> taskExecutorHolderStream, TaskExecutorBatchAssignmentRequest request, SchedulingConstraints schedulingConstraints);
}
