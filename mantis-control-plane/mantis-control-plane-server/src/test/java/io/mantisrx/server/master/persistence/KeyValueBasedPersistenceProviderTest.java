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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.Mockito;

public class KeyValueBasedPersistenceProviderTest {

    private static final String JOB_STAGE_DATA =
        "{\"jobId\":\"testjob-1\",\"name\":\"testjob\",\"user\":\"mantisoss\","
            + "\"submittedAt\":1691103290000,\"startedAt\":1691103300002,\"jarUrl\":\"http://testjob1.zip\","
            + "\"numStages\":1,\"state\":\"Launched\",\"parameters\":[],\"nextWorkerNumberToUse\":4,"
            + "\"sla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\","
            + "\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"}"
            + "}";

    private static final String STAGE_METADATA =
        "{\"jobId\":\"testjob-1\",\"stageNum\":1,\"numStages\":1,\"numWorkers\":1,"
            + "\"machineDefinition\":{\"cpuCores\":2.0,\"memoryMB\":5000.0,\"networkMbps\":1000.0,"
            + "\"diskMB\":8192.0,\"numPorts\":1}}";

    private static String workerJson(int workerIndex, int workerNumber) {
        return "{\"workerIndex\":" + workerIndex + ",\"workerNumber\":" + workerNumber
            + ",\"jobId\":\"testjob-1\",\"stageNum\":1,"
            + "\"numberOfPorts\":5,\"metricsPort\":1051,\"consolePort\":1053,\"debugPort\":1052,"
            + "\"customPort\":1054,\"ports\":[1055]}";
    }

    private Map<String, Map<String, String>> jobStageRows() {
        Map<String, Map<String, String>> rows = new HashMap<>();
        Map<String, String> jobData = new HashMap<>();
        jobData.put("stageMetadata-1", STAGE_METADATA);
        jobData.put("jobMetadata", JOB_STAGE_DATA);
        rows.put("testjob-1", jobData);
        return rows;
    }

    @Test
    public void testLoadJobWithDupeWorker() throws IOException, InvalidJobException {
        IKeyValueStore kvStore = Mockito.mock(IKeyValueStore.class);
        when(kvStore.getAllRows("MantisWorkers"))
            .thenReturn(ImmutableMap.of(
                "testjob-1-1000", ImmutableMap.of(
                    "1-0-2", workerJson(0, 2)),
                "testjob-1-2000", ImmutableMap.of(
                    "1-0-1", workerJson(0, 1))
            ));

        when(kvStore.getAllRows("MantisJobStageData")).thenReturn(jobStageRows());

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

        // Verify stale worker (workerNumber=1) is archived asynchronously:
        // deleted from active namespace and upserted to archive namespace.
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(kvStore).delete("MantisWorkers", "testjob-1-1000", "1-0-1");
            verify(kvStore).upsert(
                eq("MantisArchivedWorkers"),
                eq("testjob-1-1000"),
                eq("1-0-1"),
                anyString(),
                any(Duration.class));
        });
    }

    @Test
    public void testLoadJobWithNoDuplicates() throws IOException, InvalidJobException {
        IKeyValueStore kvStore = Mockito.mock(IKeyValueStore.class);
        when(kvStore.getAllRows("MantisWorkers"))
            .thenReturn(ImmutableMap.of(
                "testjob-1-1000", ImmutableMap.of(
                    "1-0-1", workerJson(0, 1),
                    "1-1-2", workerJson(1, 2))
            ));

        when(kvStore.getAllRows("MantisJobStageData")).thenReturn(jobStageRows());

        LifecycleEventPublisher eventPublisher = Mockito.mock(LifecycleEventPublisher.class);
        KeyValueBasedPersistenceProvider kvProvider =
            new KeyValueBasedPersistenceProvider(kvStore, eventPublisher);

        List<IMantisJobMetadata> jobs = kvProvider.loadAllJobs();
        assertEquals(1, jobs.size());
        assertEquals(1,
            jobs.get(0).getStageMetadata(1).get().getWorkerByIndex(0).getMetadata().getWorkerNumber());
        assertEquals(2,
            jobs.get(0).getStageMetadata(1).get().getWorkerByIndex(1).getMetadata().getWorkerNumber());

        // No archival calls when there are no duplicates
        verify(kvStore, never()).delete(eq("MantisWorkers"), anyString(), anyString());
        verify(kvStore, never()).upsert(
            eq("MantisArchivedWorkers"), anyString(), anyString(), anyString(), any(Duration.class));
    }

    @Test
    public void testLoadJobWithMultipleDuplicatesAtSameIndex() throws IOException, InvalidJobException {
        IKeyValueStore kvStore = Mockito.mock(IKeyValueStore.class);

        // 3 workers at index 0: workerNumbers 1, 2, 3. Only 3 should be retained.
        Map<String, Map<String, String>> workersRows = new HashMap<>();
        workersRows.put("testjob-1-1000", ImmutableMap.of(
            "1-0-1", workerJson(0, 1),
            "1-0-2", workerJson(0, 2),
            "1-0-3", workerJson(0, 3)));

        when(kvStore.getAllRows("MantisWorkers")).thenReturn(workersRows);
        when(kvStore.getAllRows("MantisJobStageData")).thenReturn(jobStageRows());

        LifecycleEventPublisher eventPublisher = Mockito.mock(LifecycleEventPublisher.class);
        KeyValueBasedPersistenceProvider kvProvider =
            new KeyValueBasedPersistenceProvider(kvStore, eventPublisher);

        List<IMantisJobMetadata> jobs = kvProvider.loadAllJobs();
        assertEquals(1, jobs.size());
        assertEquals(3,
            jobs.get(0).getStageMetadata(1).get().getWorkerByIndex(0).getMetadata().getWorkerNumber());

        // Both stale workers (1 and 2) should be archived
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(kvStore).delete("MantisWorkers", "testjob-1-1000", "1-0-1");
            verify(kvStore).delete("MantisWorkers", "testjob-1-1000", "1-0-2");
            verify(kvStore).upsert(
                eq("MantisArchivedWorkers"), eq("testjob-1-1000"), eq("1-0-1"),
                anyString(), any(Duration.class));
            verify(kvStore).upsert(
                eq("MantisArchivedWorkers"), eq("testjob-1-1000"), eq("1-0-2"),
                anyString(), any(Duration.class));
        });
    }

    @Test
    public void testLoadJobWithDupeWorkerArchiveFailure() throws IOException, InvalidJobException {
        IKeyValueStore kvStore = Mockito.mock(IKeyValueStore.class);
        when(kvStore.getAllRows("MantisWorkers"))
            .thenReturn(ImmutableMap.of(
                "testjob-1-1000", ImmutableMap.of(
                    "1-0-2", workerJson(0, 2)),
                "testjob-1-2000", ImmutableMap.of(
                    "1-0-1", workerJson(0, 1))
            ));

        when(kvStore.getAllRows("MantisJobStageData")).thenReturn(jobStageRows());

        // Make the delete call throw to simulate archive failure
        doThrow(new IOException("KV store unavailable"))
            .when(kvStore).delete("MantisWorkers", "testjob-1-1000", "1-0-1");

        LifecycleEventPublisher eventPublisher = Mockito.mock(LifecycleEventPublisher.class);
        KeyValueBasedPersistenceProvider kvProvider =
            new KeyValueBasedPersistenceProvider(kvStore, eventPublisher);

        // Job should still load correctly despite archive failure
        List<IMantisJobMetadata> jobs = kvProvider.loadAllJobs();
        assertNotNull(jobs);
        assertEquals(1, jobs.size());
        assertEquals("testjob-1", jobs.get(0).getJobId().getId());
        assertEquals(2,
            jobs.get(0).getStageMetadata(1).get().getWorkerByIndex(0).getMetadata().getWorkerNumber());

        // Verify the delete was attempted (even though it failed)
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(kvStore).delete("MantisWorkers", "testjob-1-1000", "1-0-1"));
    }
}
