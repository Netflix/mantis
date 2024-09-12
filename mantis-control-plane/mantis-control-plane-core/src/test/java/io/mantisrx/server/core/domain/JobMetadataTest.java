/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.core.domain;

import static org.junit.Assert.assertEquals;

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.net.URL;
import org.junit.Test;

public class JobMetadataTest {
    @Test
    public void testGetJobArtifact() throws Exception {
        MachineDefinition machineDefinition = new MachineDefinition(1.0, 1.0, 1.0, 1.0, 3);
        SchedulingInfo schedulingInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .singleWorkerStageWithConstraints(machineDefinition,
                        Lists.newArrayList(),
                        Lists.newArrayList()).build();
        JobMetadata jobMetadata = new JobMetadata(
                "testId", new URL("http://artifact.zip"), "111", 1,"testUser",schedulingInfo, Lists.newArrayList(),0,10, 0);
        assertEquals(jobMetadata.getJobArtifact(), ArtifactID.of("artifact.zip"));
    }
}
