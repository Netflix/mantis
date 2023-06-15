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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.runtime.loader.config.Usage;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCpuAcctsSubsystemProcess {
    private final Cgroup cgroup = mock(Cgroup.class);
    private final CpuAcctsSubsystemProcess process = new CpuAcctsSubsystemProcess(cgroup);

    @Test
    public void testWhenCgroupsReturnsCorrectData() throws Exception {
        when(cgroup.getStats("cpuacct", "cpuacct.stat"))
            .thenReturn(ImmutableMap.<String, Long>of("user", 43873627L, "system", 4185541L));
        when(cgroup.getMetric("cpuacct", "cpu.cfs_quota_us"))
            .thenReturn(400000L);
        when(cgroup.getMetric("cpuacct", "cpu.cfs_period_us"))
            .thenReturn(100000L);

        final Usage.UsageBuilder usageBuilder = Usage.builder();
        process.getUsage(usageBuilder);
        final Usage usage = usageBuilder.build();
        assertEquals(4L, (long) usage.getCpusLimit());
        assertEquals(438736L, (long) usage.getCpusUserTimeSecs());
        assertEquals(41855L, (long) usage.getCpusSystemTimeSecs());
    }
}
