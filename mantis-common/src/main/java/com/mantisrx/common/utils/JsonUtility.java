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

package com.mantisrx.common.utils;

import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonUtility {

    private static final JsonUtility INSTANCE = new JsonUtility();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter objectWriter = objectMapper.writerFor(Map.class);
    private final ObjectWriter listWriter = objectMapper.writerFor(List.class);

    private JsonUtility() {}

    public static Map<String, Object> jsonToMap(String jsonString) {
        return INSTANCE._jsonToMap(jsonString);
    }

    public static String mapToJson(Map<String, ? extends Object> map) {
        return INSTANCE._mapToJson(map);
    }

    public static List<Object> jsonToList(String jsonString) {
        return INSTANCE._jsonToList(jsonString);
    }

    public static String listToJson(List<? extends Object> list) {
        return INSTANCE._listToJson(list);
    }

    private Map<String, Object> _jsonToMap(String jsonString) {
        try {
            return objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse JSON", e);
        }
    }

    private List<Object> _jsonToList(String jsonString) {
        try {
            return objectMapper.readValue(jsonString, new TypeReference<List<Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse JSON", e);
        }
    }

    private String _mapToJson(Map<String, ? extends Object> map) {
        try {
            return objectWriter.writeValueAsString(map);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write JSON", e);
        }
    }

    private String _listToJson(List<? extends Object> list) {
        try {
            return listWriter.writeValueAsString(list);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write JSON", e);
        }
    }
}
