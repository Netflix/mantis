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

package io.mantisrx.server.master.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;

public class KeyValueBasedPersistenceProviderTest {

    @Test
    public void testLoadJobWithDupeWorker() throws IOException, InvalidJobException {
        IKeyValueStore kvStore = Mockito.mock(IKeyValueStore.class);
        when(kvStore.getAllRows("MantisWorkers"))
            .thenReturn(ImmutableMap.of(
                "testjob-1-1000", ImmutableMap.of(
                    "1-0-2",
                    "{\"workerIndex\":0,\"workerNumber\":2,\"jobId\":\"testjob-1\",\"stageNum\":1,"
                        + "\"numberOfPorts\":5,\"metricsPort\":1051,\"consolePort\":1053,\"debugPort\":1052,"
                        + "\"customPort\":1054,\"ports\":[1055]}"),
                "testjob-1-2000", ImmutableMap.of(
                    "1-0-1",
                    "{\"workerIndex\":0,\"workerNumber\":1,\"jobId\":\"testjob-1\",\"stageNum\":1,"
                        + "\"numberOfPorts\":5,\"metricsPort\":1051,\"consolePort\":1053,\"debugPort\":1052,"
                        + "\"customPort\":1054,\"ports\":[1055]}")
            ));

        when(kvStore.getAllRows("MantisJobStageData"))
            .thenReturn(ImmutableMap.of(
                "testjob-1",
                ImmutableMap.of(
                    "stageMetadata-1",
                    "{\"jobId\":\"testjob-1\",\"stageNum\":1,\"numStages\":1,\"numWorkers\":1,"
                        + "\"machineDefinition\":{\"cpuCores\":2.0,\"memoryMB\":5000.0,\"networkMbps\":1000.0,"
                        + "\"diskMB\":8192.0,\"numPorts\":1}}",
                    "jobMetadata",
                    "{\"jobId\":\"testjob-1\",\"name\":\"testjob\",\"user\":\"mantisoss\","
                        + "\"submittedAt\":1691103290000,\"startedAt\":1691103300002,\"jarUrl\":\"http://testjob1.zip\","
                        + "\"numStages\":1,\"state\":\"Launched\",\"parameters\":[],\"nextWorkerNumberToUse\":3,"
                        + "\"sla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\","
                        + "\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"}"
                        + "}"
                )));

        LifecycleEventPublisher eventPublisher = Mockito.mock(LifecycleEventPublisher.class);

        KeyValueBasedPersistenceProvider kvProvider =
            new KeyValueBasedPersistenceProvider(kvStore, eventPublisher);

        // loading two workers on same index should retain the one with newer worker number.
        List<IMantisJobMetadata> iMantisJobMetadata = kvProvider.loadAllJobs();
        assertNotNull(iMantisJobMetadata);
        assertEquals(1, iMantisJobMetadata.size());
        assertEquals("testjob-1", iMantisJobMetadata.get(0).getJobId().getId());
        assertTrue(iMantisJobMetadata.get(0).getStageMetadata(1).isPresent());
        assertEquals(2,
            iMantisJobMetadata.get(0).getStageMetadata(1).get().getWorkerByIndex(0).getMetadata().getWorkerNumber());
    }
}
