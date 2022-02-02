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

package io.mantisrx.common;

import static org.junit.Assert.assertEquals;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Test;

public class TestAck {

  @Test
  public void testSerializationAndDeserialization() throws Exception {
    Ack ack = Ack.getInstance();
    ObjectMapper objectMapper = new ObjectMapper().configure(
        SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    String serialized =
        objectMapper.writeValueAsString(ack);
    assertEquals(Ack.getInstance(), objectMapper.readValue(serialized, Ack.class));
  }
}
