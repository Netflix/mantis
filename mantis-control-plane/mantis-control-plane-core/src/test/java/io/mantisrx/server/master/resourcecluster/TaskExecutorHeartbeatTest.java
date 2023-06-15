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

package io.mantisrx.server.master.resourcecluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.mantisrx.common.JsonSerializer;
import org.junit.jupiter.api.Test;

public class TaskExecutorHeartbeatTest {
    private final JsonSerializer serializer = new JsonSerializer();

    @Test
    public void testHeartbeat() throws Exception {
        TaskExecutorHeartbeat heartbeat =
                new TaskExecutorHeartbeat(
                        TaskExecutorID.generate(),
                        ClusterID.of("cluster"),
                        TaskExecutorReport.available());
        String encoded = serializer.toJson(heartbeat);

        assertEquals(serializer.fromJSON(encoded, TaskExecutorHeartbeat.class), heartbeat);
    }
}
