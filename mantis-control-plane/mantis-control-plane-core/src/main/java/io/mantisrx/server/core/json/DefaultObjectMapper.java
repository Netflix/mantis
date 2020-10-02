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

package io.mantisrx.server.core.json;

import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.core.Version;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.MapperFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.module.SimpleModule;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;


public class DefaultObjectMapper extends ObjectMapper {

    private static DefaultObjectMapper INSTANCE = new DefaultObjectMapper();

    private DefaultObjectMapper() {
        this(null);
    }

    public DefaultObjectMapper(JsonFactory factory) {
        super(factory);
        SimpleModule serializerModule = new SimpleModule("Mantis Default JSON Serializer", new Version(1, 0, 0, null));
        registerModule(serializerModule);

        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        configure(MapperFeature.AUTO_DETECT_FIELDS, false);
        registerModule(new Jdk8Module());
    }

    public static DefaultObjectMapper getInstance() {
        return INSTANCE;
    }
}
