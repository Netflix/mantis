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

import java.util.UUID;
import lombok.Value;

/**
 * ID of the task executor. Reason this is wrapped inside a type is to have strong typing when dealing
 * with different types of IDs within the system.
 */
@Value
public class TaskExecutorID {
  String resourceId;

  public static TaskExecutorID of(String id) {
    return new TaskExecutorID(id);
  }

  public static TaskExecutorID generate() {
    return new TaskExecutorID(UUID.randomUUID().toString());
  }
}
