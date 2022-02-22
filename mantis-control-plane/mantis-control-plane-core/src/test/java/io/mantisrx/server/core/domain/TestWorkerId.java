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

package io.mantisrx.server.core.domain;

import static org.junit.Assert.assertEquals;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class TestWorkerId {
  @Test
  public void testSerializationAndDeserialization() throws Exception {
    WorkerId workerId = WorkerId.fromId("late-sine-function-tutorial-1-worker-0-1").get();
    ObjectMapper objectMapper = new ObjectMapper();
    String s = objectMapper.writeValueAsString(workerId);
    assertEquals(s, "{\"jobCluster\":\"late-sine-function-tutorial\",\"jobId\":\"late-sine-function-tutorial-1\",\"workerIndex\":0,\"workerNum\":1}");

    WorkerId actual = objectMapper.readValue(s, WorkerId.class);
    assertEquals(workerId, actual);
  }
}
