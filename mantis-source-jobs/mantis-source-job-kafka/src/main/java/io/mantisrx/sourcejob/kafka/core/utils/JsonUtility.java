/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.sourcejob.kafka.core.utils;

import java.io.IOException;
import java.util.Map;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectWriter;


public class JsonUtility {

    private static final JsonUtility INSTANCE = new JsonUtility();

    private JsonUtility() {}

    public static Map<String, Object> jsonToMap(String jsonString) {
        return INSTANCE._jsonToMap(jsonString);
    }

    public static String mapToJson(Map<String, ? extends Object> map) {
        return INSTANCE._mapToJson(map);
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectReader objectReader = objectMapper.readerFor(Map.class);

    private Map<String, Object> _jsonToMap(String jsonString) {
        try {
            return objectReader.readValue(jsonString);
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse JSON", e);
        }
    }

    private final ObjectWriter objectWriter = objectMapper.writerFor(Map.class);

    private String _mapToJson(Map<String, ? extends Object> map) {
        try {
            return objectWriter.writeValueAsString(map);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write JSON", e);
        }
    }
}  