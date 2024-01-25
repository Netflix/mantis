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

import io.mantisrx.runtime.loader.config.Usage;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.io.Resources;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCpuAcctsSubsystemProcess {

    @Test
    public void testWhenCgroupsReturnsCorrectData() throws Exception {
        final Cgroup cgroup = Mockito.mock(Cgroup.class);
        final CpuAcctsSubsystemProcess process = new CpuAcctsSubsystemProcess(cgroup);

        Mockito.when(cgroup.isV1()).thenReturn(true);
        Mockito.when(cgroup.getStats("cpuacct", "cpuacct.stat"))
            .thenReturn(ImmutableMap.<String, Long>of("user", 43873627L, "system", 4185541L));
        Mockito.when(cgroup.getStats("cpuacct", "cpuacct.stat"))
            .thenReturn(ImmutableMap.<String, Long>of("user", 43873627L, "system", 4185541L));
        Mockito.when(cgroup.getMetric("cpuacct", "cpu.cfs_quota_us"))
            .thenReturn(400000L);
        Mockito.when(cgroup.getMetric("cpuacct", "cpu.cfs_period_us"))
            .thenReturn(100000L);

        final Usage.UsageBuilder usageBuilder = Usage.builder();
        process.getUsage(usageBuilder);
        final Usage usage = usageBuilder.build();
        Assert.assertEquals(4L, (long) usage.getCpusLimit());
        Assert.assertEquals(438736L, (long) usage.getCpusUserTimeSecs());
        Assert.assertEquals(41855L, (long) usage.getCpusSystemTimeSecs());
    }

    @Test
    public void testCgroupsV2() throws IOException {
        final Cgroup cgroupv2 =
            new CgroupImpl(Resources.getResource("example2").getPath());
        Assert.assertFalse(cgroupv2.isV1());

        final CpuAcctsSubsystemProcess process = new CpuAcctsSubsystemProcess(cgroupv2);
        final Usage.UsageBuilder usageBuilder = Usage.builder();
        process.getUsage(usageBuilder);
        final Usage usage = usageBuilder.build();
        Assert.assertEquals(2L, (long) usage.getCpusLimit());
        Assert.assertEquals(4231L, (long) usage.getCpusUserTimeSecs());
        Assert.assertEquals(1277L, (long) usage.getCpusSystemTimeSecs());
    }
}
