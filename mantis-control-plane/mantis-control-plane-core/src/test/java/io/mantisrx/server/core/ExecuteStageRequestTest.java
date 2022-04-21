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

package io.mantisrx.server.core;

import static org.junit.Assert.assertTrue;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.shaded.com.google.common.base.Optional;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.net.URL;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

public class ExecuteStageRequestTest {
    @Test
    public void testSerialization() throws Exception {
        ExecuteStageRequest request =
            new ExecuteStageRequest("jobName", "jobId-0", 0, 1,
                new URL("http://datamesh/whatever"),
                1, 1,
                ImmutableList.of(1, 2, 3, 4, 5), 100L, 1,
                ImmutableList.of(new Parameter("name", "value")),
                new SchedulingInfo.Builder().numberOfStages(1)
                    .singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2),
                        Lists.newArrayList(), Lists.newArrayList()).build(),
                MantisJobDurationType.Perpetual,
                1000L,
                1L,
                new WorkerPorts(2, 3, 4, 5, 6),
                Optional.absent());

        byte[] serializedBytes = SerializationUtils.serialize(request);
        assertTrue(serializedBytes.length > 0);
    }
}
