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

package io.mantisrx.runtime.loader.cgroups;

import io.mantisrx.runtime.loader.config.Usage.UsageBuilder;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Uses cgroup memory.stat for collecting various metrics about the memory usage of the container.
 * Implementation uses ideas from the <a href="https://github.com/apache/mesos/blob/96339efb53f7cdf1126ead7755d2b83b435e3263/src/slave/containerizer/mesos/isolators/cgroups/subsystems/memory.cpp#L474-L499">actual mesos implementation</a> used in the statistics.json endpoint.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class MemorySubsystemProcess implements SubsystemProcess {
    private final Cgroup cgroup;

    @Override
    public void getUsage(UsageBuilder usageBuilder) throws IOException {
        if (cgroup.isV1()) {
            handleV1(usageBuilder);
        } else {
            handleV2(usageBuilder);
        }
    }

    private void handleV1(UsageBuilder usageBuilder) throws IOException {
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

    private void handleV2(UsageBuilder usageBuilder) throws IOException {
        Long memLimit = cgroup.getMetric("", "memory.max");
        usageBuilder.memLimit(memLimit);

        Long memCurrent = cgroup.getMetric("", "memory.current");
        usageBuilder.memRssBytes(memCurrent);

        Map<String, Long> stats = cgroup.getStats("", "memory.stat");
        Optional<Long> anon = Optional.ofNullable(stats.get("anon"));
        if (anon.isPresent()) {
            usageBuilder.memAnonBytes(anon.get());
        } else {
            log.warn("stats for memory not found; stats={}", stats);
        }
    }
}
