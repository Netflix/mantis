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

package io.mantisrx.master.resourcecluster;

import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorState;
import io.mantisrx.server.core.TestingRpcService;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class TaskExecutorStateTest {
    private final AtomicReference<Clock> actual =
        new AtomicReference<>(Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));

    private final Clock clock = new DelegateClock(actual);

    private final TestingRpcService rpc = new TestingRpcService();

    private final TaskExecutorState state = TaskExecutorState.of(clock, rpc);

//    @Test
//    public void
}
