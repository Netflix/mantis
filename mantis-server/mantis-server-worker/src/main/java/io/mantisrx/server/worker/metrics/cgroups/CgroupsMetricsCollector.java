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

import io.mantisrx.server.master.client.config.MetricsCollector;
import io.mantisrx.server.master.client.config.Usage;
import java.io.IOException;
import java.util.Properties;
import lombok.RequiredArgsConstructor;

/**
 * Cgroups based metrics collector.
 * This assumes that the worker task is being run on a cgroup based on container.
 */
@RequiredArgsConstructor
public class CgroupsMetricsCollector implements MetricsCollector {
    private final CpuAcctsSubsystemProcess cpu;
    private final MemorySubsystemProcess memory;
    private final NetworkSubsystemProcess network;

    @SuppressWarnings("unused")
    public static CgroupsMetricsCollector valueOf(Properties properties) {
        String cgroupPath = properties.getProperty("mantis.cgroups.path", "/sys/fs/cgroup");
        String networkIfacePath = properties.getProperty("mantis.cgroups.networkPath", "/proc/net/dev");
        String interfaceName = properties.getProperty("mantis.cgroups.interface", "eth0:");
        return new CgroupsMetricsCollector(cgroupPath, networkIfacePath, interfaceName);
    }

    public CgroupsMetricsCollector(String cgroupPath, String networkIfacePath, String interfaceName) {
        Cgroup cgroup = new CgroupImpl(cgroupPath);
        this.cpu = new CpuAcctsSubsystemProcess(cgroup);
        this.memory = new MemorySubsystemProcess(cgroup);
        this.network = new NetworkSubsystemProcess(networkIfacePath, interfaceName);
    }

    @Override
    public Usage get() throws IOException {
        Usage.UsageBuilder usageBuilder = Usage.builder();
        cpu.getUsage(usageBuilder);
        memory.getUsage(usageBuilder);
        network.getUsage(usageBuilder);
        return usageBuilder.build();
    }
}
