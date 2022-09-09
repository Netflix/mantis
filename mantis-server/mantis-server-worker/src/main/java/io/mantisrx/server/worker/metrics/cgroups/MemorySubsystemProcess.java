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

package io.mantisrx.server.worker.metrics.cgroups;

import io.mantisrx.server.worker.metrics.Usage.UsageBuilder;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class MemorySubsystemProcess implements SubsystemProcess {
    private final Cgroup cgroup;

    @Override
    public void getUsage(UsageBuilder usageBuilder) throws IOException {
        Long memLimit = cgroup.getMetric("memory", "memory.limit_in_bytes");
        usageBuilder.memLimit(memLimit);

        Map<String, Long> stats = cgroup.getStats("memory", "memory.stat");
        Optional<Long> totalRss = Optional.ofNullable(stats.get("total_rss"));
        if (totalRss.isPresent()) {
            usageBuilder.memAnonBytes(totalRss.get());
            usageBuilder.memRssBytes(totalRss.get());
        } else {
            log.warn("stats for memory not found; stats={}", stats);
        }
    }
}
