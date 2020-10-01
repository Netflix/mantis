/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.kafka.source.serde;

import java.util.Map;

import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleJsonDeserializer extends MapDeserializerBase {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleJsonDeserializer.class);
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> typeRef =
        new TypeReference<Map<String, Object>>() {};


    @Override
    public boolean canParse(byte[] message) {
        // no easy way of pre-determine if the json is valid without actually parsing it (unlike chaski format message).
        // so we'll always assume the message can be parsed and move onto deserialization phase
        return true;
    }

    @Override
    public Map<String, Object> parseMessage(byte[] message) throws ParseException {
        Map<String, Object> result;

        try {
            result = jsonMapper.readValue(message, typeRef);
        } catch (Exception ex) {
            LOGGER.error("Json parser failed to parse message! PAYLOAD:" + getPartialPayLoadForLogging(message), ex);
            throw new ParseException("Json not able to parse raw message", ex);
        }
        return result;
    }
}
