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

package io.mantisrx.runtime.codec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import io.mantisrx.shaded.com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.module.afterburner.AfterburnerModule;

/**
 * @deprecated instead use {@link io.mantisrx.runtime.common.codec.JacksonCodecs}
 */
@Deprecated
public class JacksonCodecs {

    private final static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper(new CBORFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new AfterburnerModule());
    }

    public static <T> Codec<T> pojo(final Class<T> clazz) {

        return new Codec<T>() {

            @Override
            public byte[] encode(T value) {
                try {
                    return mapper.writeValueAsBytes(value);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to write bytes for value: " + value, e);
                }
            }

            @Override
            public T decode(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, clazz);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert to type: " + clazz.toString(), e);
                }
            }
        };
    }

    public static <T> Codec<List<T>> list() {
        return new Codec<List<T>>() {
            @Override
            public byte[] encode(List<T> value) {
                try {
                    return mapper.writeValueAsBytes(value);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to write list to bytes", e);
                }
            }

            @Override
            public List<T> decode(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, new TypeReference<List<T>>() {});
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert bytes to list", e);
                }
            }

        };
    }

    public static Codec<Map<String, Object>> mapStringObject() {
        return new Codec<Map<String, Object>>() {
            private final ObjectReader reader = mapper.readerFor(Map.class);

            @Override
            public byte[] encode(Map<String, Object> map) {
                try {
                    return mapper.writeValueAsBytes(map);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write bytes for map: " + map, e);
                }
            }

            @Override
            public Map<String, Object> decode(byte[] bytes) {
                try {
                    return reader.readValue(bytes);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert bytes to map", e);
                }
            }
        };
    }

}
