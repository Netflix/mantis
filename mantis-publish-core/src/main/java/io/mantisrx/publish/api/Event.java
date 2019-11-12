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

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Event {

    private static final Logger LOG = LoggerFactory.getLogger(Event.class);

    private static final AtomicBoolean ERROR_LOG_ENABLED = new AtomicBoolean(true);

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();
    private final Map<String, Object> attributes;

    public Event() {
        this(null, false);
    }

    public Event(Map<String, Object> attributes) {
        this(attributes, true);
    }

    public Event(Map<String, Object> attributes, boolean deepCopy) {
        if (attributes == null || deepCopy) {
            this.attributes = new HashMap<>();
            if (attributes != null) {
                this.attributes.putAll(attributes);
            }
        } else {
            this.attributes = attributes;
        }

        JACKSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
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
