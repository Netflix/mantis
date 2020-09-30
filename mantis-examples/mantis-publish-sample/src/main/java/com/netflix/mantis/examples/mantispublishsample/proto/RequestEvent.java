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

package com.netflix.mantis.examples.mantispublishsample.proto;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import lombok.Builder;
import lombok.Data;


/**
 * Represents a Request Event a service may receive.
 */
@Data
@Builder
public class RequestEvent {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectReader requestEventReader = mapper.readerFor(RequestEvent.class);

    private final String userId;
    private final String uri;
    private final int status;
    private final String country;
    private final String deviceType;

    public Map<String,Object> toMap() {
        Map<String,Object> data = new HashMap<>();
        data.put("userId", userId);
        data.put("uri", uri);
        data.put("status", status);
        data.put("country", country);
        data.put("deviceType", deviceType);
        return data;
    }

    public String toJsonString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }



}
