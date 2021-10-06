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
package io.mantisrx.publish.netty.proto;

import static org.junit.jupiter.api.Assertions.fail;

import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import org.junit.jupiter.api.Test;


public class MantisEnvelopeTest {

    @Test
    public void deserTest() {
        String data = "{\"ts\":1571174446676,\"originServer\":\"origin\",\"eventList\":[{\"id\":1,\"data\":\"{\\\"mantisStream\\\":\\\"defaultStream\\\",\\\"matched-clients\\\":[\\\"MantisPushRequestEvents_PushRequestEventSourceJobLocal-1_nj3\\\"],\\\"id\\\":44,\\\"type\\\":\\\"EVENT\\\"}\"}]}";
        final ObjectMapper mapper = new ObjectMapper();
        ObjectReader mantisEventEnvelopeReader = mapper.readerFor(MantisEventEnvelope.class);
        try {
            MantisEventEnvelope envelope = mantisEventEnvelopeReader.readValue(data);
            System.out.println("Envelope=>" + envelope);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            fail();
        }


    }
}
