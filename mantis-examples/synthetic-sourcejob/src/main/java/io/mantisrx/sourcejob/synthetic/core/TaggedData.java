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

package io.mantisrx.sourcejob.synthetic.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.runtime.codec.JsonType;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;

public class TaggedData implements JsonType {

    private final Set<String> matchedClients = new HashSet<String>();
    private Map<String, Object> payLoad;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public TaggedData(@JsonProperty("data") Map<String, Object> data) {
        this.payLoad = data;
    }

    public Set<String> getMatchedClients() {
        return matchedClients;
    }

    public boolean matchesClient(String clientId) {
        return matchedClients.contains(clientId);
    }

    public void addMatchedClient(String clientId) {
        matchedClients.add(clientId);
    }

    public Map<String, Object> getPayload() {
        return this.payLoad;
    }

    public void setPayload(Map<String, Object> newPayload) {
        this.payLoad = newPayload;
    }


    public static Codec<TaggedData> taggedDataCodec() {

        return new Codec<TaggedData>() {
            @Override
            public TaggedData decode(byte[] bytes) {
                return new TaggedData(new HashMap<>());
            }

            @Override
            public byte[] encode(final TaggedData value) {
                return new byte[128];
            }
        };
    }



}
