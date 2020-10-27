/*
 *
 * Copyright 2020 Netflix, Inc.
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
 *
 */

package io.mantisrx.common.metrics.measurement;

import java.util.Collection;
import java.util.Map;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class Measurements {

    private final Map<String, String> tags;
    private String name;
    private long timestamp;
    private Collection<CounterMeasurement> counters;
    private Collection<GaugeMeasurement> gauges;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public Measurements(
            @JsonProperty("name") String name,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("counters") Collection<CounterMeasurement> counters,
            @JsonProperty("gauges") Collection<GaugeMeasurement> gauges,
            @JsonProperty("tags") Map<String, String> tags) {
        this.name = name;
        this.timestamp = timestamp;
        this.counters = counters;
        this.gauges = gauges;
        this.tags = tags;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Collection<CounterMeasurement> getCounters() {
        return counters;
    }

    public Collection<GaugeMeasurement> getGauges() {
        return gauges;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Measurements{");
        sb.append("tags=").append(tags);
        sb.append(", name='").append(name).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", counters=").append(counters);
        sb.append(", gauges=").append(gauges);
        sb.append('}');
        return sb.toString();
    }
}
