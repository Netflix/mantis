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

import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.client.TaskExecutorReport.Available;
import io.mantisrx.server.master.client.TaskExecutorReport.Occupied;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Value;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes({
    @Type(value = Available.class, name = "available"),
    @Type(value = Occupied.class, name = "occupied")
})
public interface TaskExecutorReport {
  @Value
  class Available implements TaskExecutorReport {}

  @Value
  class Occupied implements TaskExecutorReport {
    WorkerId workerId;
  }

  static Available available() {
    return new Available();
  }

  static Occupied occupied(WorkerId workerId) {
    return new Occupied(workerId);
  }
}
