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

package io.mantisrx.master.jobcluster.job.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.Status;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobWorkerTest {

    private static Registry registry;

    @BeforeClass
    public static void setup() {
        registry = new DefaultRegistry();
        SpectatorRegistryFactory.setRegistry(registry);
    }

    @Test
    public void testWorkerAcceptedToStartedMsRecordedOnWorkerStatus() throws Exception {
        final long acceptedAt = 1000L;
        final long startedAt = 6000L;
        final long expectedLatencyNanos = TimeUnit.MILLISECONDS.toNanos(startedAt - acceptedAt);

        // Use a unique job ID (format: clusterName-jobNum) to avoid registry collisions
        final String jobId = "test-startedMs-" + System.nanoTime() + "-1";

        // Build worker in Launched state so we can transition to Started via WorkerStatus
        JobWorker worker = new JobWorker.Builder()
                .withJobId(jobId)
                .withWorkerIndex(0)
                .withWorkerNumber(1)
                .withStageNum(1)
                .withNumberOfPorts(1)
                .withAcceptedAt(acceptedAt)
                .withState(WorkerState.Launched)
                .withLifecycleEventsPublisher(LifecycleEventPublisher.noop())
                .build();

        // Send WorkerStatus with Started state — this is the path we're testing
        Status startedStatus = new Status(
                jobId, 1, 0, 1,
                Status.TYPE.INFO, "started", MantisJobState.Started, startedAt);
        WorkerStatus startedEvent = new WorkerStatus(startedStatus, Instant.ofEpochMilli(startedAt));
        boolean persistRequired = worker.processEvent(startedEvent);

        assertTrue("processEvent should return true for Started status", persistRequired);

        // Query the Spectator registry directly for the timer
        // The MetricId formats it as "metricGroup_metricName"
        com.netflix.spectator.api.Timer spectatorTimer = registry.timer(
                registry.createId("JobWorker_workerAcceptedToStartedMs").withTag("jobId", jobId));

        assertEquals("workerAcceptedToStartedMs timer should have been recorded once",
                1, spectatorTimer.count());
        assertEquals("workerAcceptedToStartedMs should record the correct latency",
                expectedLatencyNanos, spectatorTimer.totalTime());
    }
}
