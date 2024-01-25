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
import io.mantisrx.shaded.com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

public class TestNetworkSubsystemProcess {
    @Test
    public void testValidPath() throws Exception {
        String path = Resources.getResource("example1/network/dev").getPath();
        NetworkSubsystemProcess process = new NetworkSubsystemProcess(path, "eth0:");

        final Usage.UsageBuilder usageBuilder = Usage.builder();
        process.getUsage(usageBuilder);
        final Usage usage = usageBuilder.build();
        Assert.assertEquals(2861321009430L, (long) usage.getNetworkReadBytes());
        Assert.assertEquals(2731791728959L, (long) usage.getNetworkWriteBytes());
    }
}
