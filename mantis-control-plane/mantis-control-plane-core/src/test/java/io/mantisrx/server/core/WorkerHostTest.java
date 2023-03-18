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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class WorkerHostTest {
    @Test
    public void testEquals() {
        WorkerHost w1 = new WorkerHost("localhost", 1, ImmutableList.of(7001, 7002), MantisJobState.Accepted, 2, 7003, 7004);
        WorkerHost w2 = new WorkerHost("localhost", 1, ImmutableList.of(7001, 7002), MantisJobState.Accepted, 2, 7003, 7004);
        WorkerHost w3 = new WorkerHost("localhost", 1, ImmutableList.of(7001, 7002), MantisJobState.Accepted, 2, 7003, 7005);

        // required?
        assertAll(
            () -> assertTrue(w1.equals(w2)),
            () -> assertFalse(w1.equals(w3))
        );
    }
}
