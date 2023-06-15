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

public class TestMemorySubsystemProcess {
    private final Cgroup cgroup = mock(Cgroup.class);

    private final MemorySubsystemProcess process = new MemorySubsystemProcess(cgroup);

    @Test
    public void testHappyPath() throws Exception {
        when(cgroup.getMetric("memory", "memory.limit_in_bytes"))
            .thenReturn(17179869184L);
        when(cgroup.getStats("memory", "memory.stat"))
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
        assertEquals(17179869184L, (long) usage.getMemLimit());
        assertEquals(14828109824L, (long) usage.getMemRssBytes());
        assertEquals(14828109824L, (long) usage.getMemAnonBytes());
    }
}
