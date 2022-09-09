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

package io.mantisrx.server.worker.metrics;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@RequiredArgsConstructor
@Value
@Builder
public class Usage {

    double cpusLimit;
    /**
     * // Add the cpuacct.stat information.
     * Try<hashmap<string, uint64_t>> stat = cgroups::stat(
     * hierarchy,
     * cgroup,
     * "cpuacct.stat");
     * <p>
     * if (stat.isError()) {
     * return Failure("Failed to read 'cpuacct.stat': " + stat.error());
     * }
     * <p>
     * // TODO(bmahler): Add namespacing to cgroups to enforce the expected
     * // structure, e.g., cgroups::cpuacct::stat.
     * Option<uint64_t> user = stat->get("user");
     * Option<uint64_t> system = stat->get("system");
     * <p>
     * if (user.isSome() && system.isSome()) {
     * result.set_cpus_user_time_secs((double) user.get() / (double) ticks);
     * result.set_cpus_system_time_secs((double) system.get() / (double) ticks);
     * }
     */
    double cpusSystemTimeSecs;
    double cpusUserTimeSecs;
    double memLimit;
    /**
     * // TODO(bmahler): Add namespacing to cgroups to enforce the expected
     * // structure, e.g, cgroups::memory::stat.
     * Try<hashmap<string, uint64_t>> stat = cgroups::stat(
     * hierarchy,
     * cgroup,
     * "memory.stat");
     * <p>
     * Option<uint64_t> total_rss = stat->get("total_rss");
     * if (total_rss.isSome()) {
     * // TODO(chzhcn): mem_anon_bytes is deprecated in 0.23.0 and will
     * // be removed in 0.24.0.
     * result.set_mem_anon_bytes(total_rss.get());
     * result.set_mem_rss_bytes(total_rss.get());
     * }
     */
    double memRssBytes;
    double memAnonBytes;
    double networkReadBytes;
    double networkWriteBytes;
}
