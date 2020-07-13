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

package io.mantisrx.master.api.akka.route;

import java.io.IOException;

import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.marshalling.Marshaller;
import akka.http.javadsl.unmarshalling.Unmarshaller;

import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.FilterProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.PropertyFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.google.common.base.Strings;
import com.netflix.spectator.impl.Preconditions;

public class Jackson {
    private static final ObjectMapper defaultObjectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(new Jdk8Module());
    public static final SimpleFilterProvider DEFAULT_FILTER_PROVIDER;

    static {
        DEFAULT_FILTER_PROVIDER = new SimpleFilterProvider();
        DEFAULT_FILTER_PROVIDER.setFailOnUnknownId(false);
    }


    public static <T> Marshaller<T, RequestEntity> marshaller() {
        return marshaller(defaultObjectMapper, null);
    }

    public static <T> Marshaller<T, RequestEntity> marshaller(FilterProvider filterProvider) {
        return marshaller(defaultObjectMapper, filterProvider);
    }

    public static <T> Marshaller<T, RequestEntity> marshaller(ObjectMapper mapper) {
        return Marshaller.wrapEntity(
                u -> {
                    try {
                        return toJSON(mapper, null, u);
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException("cannot marshall to Json " + u);
                    }
                },
                Marshaller.stringToEntity(),
                MediaTypes.APPLICATION_JSON
        );
    }

    public static <T> Marshaller<T, RequestEntity> marshaller(
            ObjectMapper mapper,
            FilterProvider filterProvider) {
        return Marshaller.wrapEntity(
                u -> {
                    try {
                        return toJSON(mapper, filterProvider, u);
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException("cannot marshall to Json " + u);
                    }
                },
                Marshaller.stringToEntity(),
                MediaTypes.APPLICATION_JSON
        );
    }

//    public static <T> Unmarshaller<ByteString, T> byteStringUnmarshaller(Class<T> expectedType) {
//        return byteStringUnmarshaller(defaultObjectMapper, expectedType);
//    }

    public static <T> Unmarshaller<HttpEntity, T> unmarshaller(Class<T> expectedType) {
        return unmarshaller(defaultObjectMapper, expectedType);
    }

    public static <T> Unmarshaller<HttpEntity, T> unmarshaller(TypeReference<T> expectedType) {
        return unmarshaller(defaultObjectMapper, expectedType);
    }

    public static <T> Unmarshaller<HttpEntity, T> unmarshaller(
            ObjectMapper mapper,
            Class<T> expectedType) {
        return Unmarshaller.forMediaType(MediaTypes.APPLICATION_JSON, Unmarshaller.entityToString())
                           .thenApply(s -> {
                               try {
                                   return fromJSON(mapper, s, expectedType);
                               } catch (IOException e) {
                                   throw new IllegalArgumentException("cannot unmarshall Json as " +
                                                                      expectedType.getSimpleName());
                               }
                           });
    }

    public static <T> Unmarshaller<HttpEntity, T> unmarshaller(
            ObjectMapper mapper,
            TypeReference<T> expectedType) {
        return Unmarshaller.forMediaType(MediaTypes.APPLICATION_JSON, Unmarshaller.entityToString())
                           .thenApply(s -> {
                               try {
                                   return fromJSON(mapper, s, expectedType);
                               } catch (IOException e) {
                                   throw new IllegalArgumentException("cannot unmarshall Json as " +
                                                                      expectedType.getType()
                                                                                  .getTypeName());
                               }
                           });
    }

//    public static <T> Unmarshaller<ByteString, T> byteStringUnmarshaller(ObjectMapper mapper, Class<T> expectedType) {
//        return Unmarshaller.sync(s -> fromJSON(mapper, s.utf8String(), expectedType));
//    }

    private static String toJSON(
            ObjectMapper mapper,
            FilterProvider filters,
            Object object) throws JsonProcessingException {
        if (filters == null) {
            filters = DEFAULT_FILTER_PROVIDER;
        }
        return mapper.writer(filters).writeValueAsString(object);

    }

    public static <T> T fromJSON(
            ObjectMapper mapper,
            String json,
            TypeReference<T> expectedType) throws IOException {
        return mapper.readerFor(expectedType).readValue(json);
    }

    public static <T> T fromJSON(
            ObjectMapper mapper,
            String json,
            Class<T> expectedType) throws IOException {
        return mapper.readerFor(expectedType).readValue(json);
    }

    public static <T> T fromJSON(String json, Class<T> expectedType) throws IOException {
        return defaultObjectMapper.readerFor(expectedType).readValue(json);
    }

    public static <T> T fromJSON(String json, TypeReference<T> expectedType) throws IOException {
        return defaultObjectMapper.readerFor(expectedType).readValue(json);
    }

    public static String toJson(Object object) throws IOException {
        return defaultObjectMapper.writeValueAsString(object);
    }
}
