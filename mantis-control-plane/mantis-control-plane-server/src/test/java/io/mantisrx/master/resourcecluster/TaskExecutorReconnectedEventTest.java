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

package io.mantisrx.master.resourcecluster;

import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.TaskExecutorReconnectedEvent;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TaskExecutorReconnectedEventTest {

    @Test
    public void testTaskExecutorReconnectedEvent() {
        WorkerId testWorkerId = new WorkerId("testCluster", "testCluster-1", 1, 1);
        TaskExecutorID testExecutorId = TaskExecutorID.of("testExecutor");

        TaskExecutorReconnectedEvent event = new TaskExecutorReconnectedEvent(testWorkerId, testExecutorId);

        assertEquals(testWorkerId, event.getWorkerId());
        assertEquals(testExecutorId, event.getTaskExecutorID());
        assertTrue(event.getEventTimeMs() > 0);
    }
}
