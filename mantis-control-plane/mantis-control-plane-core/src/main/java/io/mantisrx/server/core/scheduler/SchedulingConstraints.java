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

package io.mantisrx.server.core.scheduler;

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * A class that represents scheduling constraints. These constraints include the resource constraints, and a map of scheduling attributes (e.g. jdkVersion:17).
 */
@AllArgsConstructor(staticName = "of")
@Value
public class SchedulingConstraints {
    // Defines the resource constraints for scheduling
    MachineDefinition machineDefinition;

    // Additional attributes for scheduling (ie. jdkVersion:17)
    Map<String, String> schedulingAttributes;

    @VisibleForTesting
    public static SchedulingConstraints of(MachineDefinition machineDefinition) {
        return SchedulingConstraints.of(machineDefinition, ImmutableMap.of());
    }
}
