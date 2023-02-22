/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.discovery.proto;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;


public class StreamJobClusterMap {

    public static final String DEFAULT_STREAM_KEY = "__default__";
    private String appName;
    private Map<String, String> mappings;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StreamJobClusterMap(final String appName,
                               final Map<String, String> mappings) {
        this.appName = appName;
        this.mappings = mappings;
    }

    // default constructor for Json deserialize
    public StreamJobClusterMap() {
    }
    public String getAppName() {
        return appName;
    }

    @JsonIgnore
    public String getJobCluster(String streamName) {
        return mappings.getOrDefault(streamName, mappings.get(DEFAULT_STREAM_KEY));
    }

    public Map<String, String> getStreamJobClusterMap() {
        return Collections.unmodifiableMap(mappings);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StreamJobClusterMap that = (StreamJobClusterMap) o;
        return Objects.equals(appName, that.appName) &&
                Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appName, mappings);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StreamJobClusterMap{");
        sb.append("appName='").append(appName).append('\'');
        sb.append(", mappings=").append(mappings);
        sb.append('}');
        return sb.toString();
    }
}
