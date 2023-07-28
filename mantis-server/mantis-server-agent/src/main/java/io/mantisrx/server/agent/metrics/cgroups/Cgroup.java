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

package io.mantisrx.server.agent.metrics.cgroups;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

interface Cgroup {
    Boolean isV1();

    Boolean isV2();

    List<Long> getMetrics(@Nullable String subsystem, String metricName) throws IOException;
    Long getMetric(@Nullable String subsystem, String metricName) throws IOException;
    Map<String, Long> getStats(@Nullable String subsystem, String stat) throws IOException;
}
