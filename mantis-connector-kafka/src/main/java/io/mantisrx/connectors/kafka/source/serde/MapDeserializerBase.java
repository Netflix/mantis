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

package io.mantisrx.connectors.kafka.source.serde;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public abstract class MapDeserializerBase implements Parser, Deserializer<Map<String, Object>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Map<String, Object> deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        else if (canParse(data))
            return parseMessage(data);
        else throw new UnsupportedOperationException("Message cannot be deserialized with parser");
    }

    @Override
    public void close() {

    }
}
