/*
 * Copyright 2021 Netflix, Inc.
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class JobSchedulingInfoTest {
    @Test
    public void testEquals() {
        WorkerHost w1 = new WorkerHost("localhost", 1, ImmutableList.of(7001, 7002), MantisJobState.Accepted, 2, 7003, 7004);
        WorkerHost w2 = new WorkerHost("localhost", 1, ImmutableList.of(7001, 7002), MantisJobState.Accepted, 2, 7003, 7004);
        WorkerHost w3 = new WorkerHost("localhost", 1, ImmutableList.of(7001, 7002), MantisJobState.Accepted, 2, 7003, 7005);

        WorkerAssignments a1 = new WorkerAssignments(1, 1, ImmutableMap.of(0, w1));
        WorkerAssignments a2 = new WorkerAssignments(1, 1, ImmutableMap.of(0, w2));
        WorkerAssignments a3 = new WorkerAssignments(1, 1, ImmutableMap.of(0, w3));

        JobSchedulingInfo s1 = new JobSchedulingInfo("job-1", ImmutableMap.of(1, a1));
        JobSchedulingInfo s2 = new JobSchedulingInfo("job-1", ImmutableMap.of(1, a2));
        JobSchedulingInfo s3 = new JobSchedulingInfo("job-1", ImmutableMap.of(1, a3));

        assertTrue(s1.equals(s2));
        assertFalse(s1.equals(s3));
    }
}
