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

public class TestMemorySubsystemProcess {
    private final Cgroup cgroup = Mockito.mock(Cgroup.class);

    private final MemorySubsystemProcess process = new MemorySubsystemProcess(cgroup);

    @Test
    public void testHappyPath() throws Exception {
        Mockito.when(cgroup.isV1()).thenReturn(true);
        Mockito.when(cgroup.getMetric("memory", "memory.limit_in_bytes"))
            .thenReturn(17179869184L);
        Mockito.when(cgroup.getStats("memory", "memory.stat"))
            .thenReturn(
                ImmutableMap.<String, Long>builder()
                    .put("cache", 233472L)
                    .put("rss", 38641664L)
                    .put("rss_huge", 0L)
                    .put("shmem", 110592L)
                    .put("mapped_file", 0L)
                    .put("dirty", 0L)
                    .put("writeback", 0L)
                    .put("swap", 0L)
                    .put("pgpgin", 18329978L)
                    .put("pgpgout", 18320487L)
                    .put("pgfault", 20573333L)
                    .put("pgmajfault", 0L)
                    .put("inactive_anon", 36065280L)
                    .put("active_anon", 65536L)
                    .put("inactive_file", 2715648L)
                    .put("active_file", 28672L)
                    .put("unevictable", 0L)
                    .put("hierarchical_memory_limit", 17179869184L)
                    .put("hierarchical_memsw_limit", 34359738368L)
                    .put("total_cache", 1014173696L)
                    .put("total_rss", 14828109824L)
                    .put("total_rss_huge", 0L)
                    .put("total_shmem", 2998272L)
                    .put("total_mapped_file", 72470528L)
                    .put("total_dirty", 217088L)
                    .put("total_writeback", 0L)
                    .put("total_swap", 0L)
                    .put("total_pgpgin", 519970432L)
                    .put("total_pgpgout", 516102687L)
                    .put("total_pgfault", 1741668505L)
                    .put("total_pgmajfault", 34L)
                    .put("total_inactive_anon", 14828208128L)
                    .put("total_active_anon", 278528L)
                    .put("total_inactive_file", 972009472L)
                    .put("total_active_file", 41783296L)
                    .put("total_unevictable", 0L)
                    .build());

        final Usage.UsageBuilder usageBuilder = Usage.builder();
        process.getUsage(usageBuilder);
        final Usage usage = usageBuilder.build();
        Assert.assertEquals(17179869184L, (long) usage.getMemLimit());
        Assert.assertEquals(14828109824L, (long) usage.getMemRssBytes());
        Assert.assertEquals(14828109824L, (long) usage.getMemAnonBytes());
    }

    @Test
    public void testCgroupv2() throws IOException {
        final Cgroup cgroupv2 =
            new CgroupImpl(Resources.getResource("example2").getPath());

        final MemorySubsystemProcess process = new MemorySubsystemProcess(cgroupv2);
        final Usage.UsageBuilder usageBuilder = Usage.builder();
        process.getUsage(usageBuilder);
        final Usage usage = usageBuilder.build();
        Assert.assertEquals(2147483648L, (long) usage.getMemLimit());
        Assert.assertEquals(1693843456L, (long) usage.getMemRssBytes());
        Assert.assertEquals(945483776L, (long) usage.getMemAnonBytes());
    }
}
