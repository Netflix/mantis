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

import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.io.Resources;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class TestCgroup {

    private final Cgroup cgroup =
        new CgroupImpl(Resources.getResource("example1").getPath());

    @Test
    public void testReadingStatFiles() throws IOException {
        Assert.assertEquals(
            ImmutableMap.of("user", 49692738L, "system", 4700825L),
            cgroup.getStats("cpuacct", "cpuacct.stat"));
        Assert.assertTrue(cgroup.isV1());
    }

    @Test
    public void testReadingMetrics() throws IOException {
        Assert.assertEquals(
            400000L,
            cgroup.getMetric("cpuacct", "cpu.cfs_quota_us").longValue()
        );
    }

    @Test
    public void testLongOverflow() throws IOException {
        Assert.assertEquals(
            Long.MAX_VALUE,
            cgroup.getMetric("testlongoverflow", "verylongvalue").longValue()
        );
    }
}
