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

package io.mantisrx.common.metrics.measurement;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;

import io.micrometer.core.instrument.Meter;

import java.util.Map;


public class MicrometerMeasurements {

    private final Map<String, String> tags;
    private String name;
    private long timestamp;
    private Meter.Type type;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MicrometerMeasurements(
            @JsonProperty("name") String name,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("type") Meter.Type type,
            @JsonProperty("tags") Map<String, String> tags) {
        this.name = name;
        this.timestamp = timestamp;
        this.type = type;
        this.tags = tags;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Meter.Type getType() {
        return type;
    }
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "Measurements{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", type=" + type + "" +
                ", model = v2" +
                ", tags=" + tags +
                '}';
    }
}
