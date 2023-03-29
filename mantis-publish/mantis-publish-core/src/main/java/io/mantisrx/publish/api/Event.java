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

package io.mantisrx.publish.api;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonValue;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Event {

    private static final Logger LOG = LoggerFactory.getLogger(Event.class);

    private static final AtomicBoolean ERROR_LOG_ENABLED = new AtomicBoolean(true);

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();
    static {
        JACKSON_MAPPER
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());
    }

    private final Map<String, Object> attributes;

    public Event() {
        this(null);
    }

    public Event(Map<String, Object> attributes) {
        this(attributes, true);
    }

    /**
     * @deprecated
     * Use {@link #Event(Map)} instead where <code>deepCopy</code> is default to true. Always creates a new top level
     * Map to avoid any exceptions due to immutable map.
     */
    @Deprecated
    public Event(Map<String, Object> attributes, boolean deepCopy) {
        if (attributes == null || deepCopy) {
            this.attributes = new HashMap<>();
            if (attributes != null) {
                this.attributes.putAll(attributes);
            }
        } else {
            this.attributes = attributes;
        }
    }

    public Event set(String key, Object value) {
        attributes.put(key, value);
        return this;
    }

    public Object get(String key) {
        return attributes.get(key);
    }

    public boolean has(String key) {
        return attributes.containsKey(key);
    }

    public Iterator<String> keys() {
        return attributes.keySet().iterator();
    }

    public Set<Map.Entry<String, Object>> entries() {
        return attributes.entrySet();
    }

    @JsonValue
    public Map<String, Object> getMap() {
        return attributes;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return attributes.isEmpty();
    }

    public String toJsonString() {
        try {
            return JACKSON_MAPPER.writeValueAsString(attributes);
        } catch (JsonProcessingException e) {
            if (ERROR_LOG_ENABLED.get()) {
                LOG.error("failed to serialize Event to json {}", attributes.toString(), e);
                ERROR_LOG_ENABLED.set(false);
            }
            LOG.debug("failed to serialize Event to json {}", attributes.toString(), e);
            return "";
        }
    }

    public Map<String, String> toStringMap() {
        final Map<String, String> m = new HashMap<>();

        for (Map.Entry<String, Object> entry : this.entries()) {
            final Object val = entry.getValue();
            if (val != null) {
                m.put(entry.getKey(), String.valueOf(val));
            }
        }

        return m;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(attributes, event.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }

    @Override
    public String toString() {
        return String.format("%s%s", this.getClass().getSimpleName(), toJsonString());
    }
}
