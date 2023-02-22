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
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class AppJobClustersMap {

    public static final String VERSION_1 = "1";
    public static final String DEFAULT_APP_KEY = "__default__";
    /**
     * time stamp associated with the mapping when the mapping was created/updated
     */
    private long timestamp;
    private final Map<String, Map<String, String>> mappings = new HashMap<>();
    private String version = VERSION_1;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public AppJobClustersMap(@JsonProperty("version") String version,
                             @JsonProperty("timestamp") long ts,
                             @JsonProperty("mappings") Map<String, Object> mappings) {
        checkNotNull(mappings, "mappings");
        checkNotNull(version, "version");
        this.timestamp = ts;
        if (!version.equals(VERSION_1)) {
            throw new IllegalArgumentException("version " + version + " is not supported");
        }
        this.version = version;

        mappings.entrySet()
                .stream()
                .forEach(e -> this.mappings.put(e.getKey(), (Map<String, String>) e.getValue()));
    }

    // default constructor for Json deserialize
    public AppJobClustersMap() {
    }

    /**
     * Ensures the object reference is not null.
     */
    @JsonIgnore
    public static <T> T checkNotNull(T obj, String name) {
        if (obj == null) {
            String msg = String.format("parameter '%s' cannot be null", name);
            throw new NullPointerException(msg);
        }
        return obj;
    }

    public String getVersion() {
        return version;
    }

    public Map<String, Map<String, String>> getMappings() {
        return Collections.unmodifiableMap(mappings);
    }

    public long getTimestamp() {
        return timestamp;
    }

    @JsonIgnore
    private Map<String, String> defaultStreamJobClusterMap() {
        return mappings.getOrDefault(DEFAULT_APP_KEY, Collections.emptyMap());
    }

    @JsonIgnore
    public StreamJobClusterMap getStreamJobClusterMap(final String appName) {
        Map<String, String> mappings = this.mappings.getOrDefault(appName, defaultStreamJobClusterMap());
        return new StreamJobClusterMap(appName, mappings);
    }

    @JsonIgnore
    public AppJobClustersMap getFilteredAppJobClustersMap(final List<String> apps) {
        AppJobClustersMap.Builder builder = new AppJobClustersMap.Builder();
        builder = builder.withVersion(version);
        builder = builder.withTimestamp(timestamp);
        if (apps != null) {
            for (final String appName : apps) {
                builder = builder.withAppJobCluster(appName, getStreamJobClusterMap(appName));
            }
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppJobClustersMap that = (AppJobClustersMap) o;
        return timestamp == that.timestamp &&
                version.equals(that.version) &&
                mappings.equals(that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, timestamp, mappings);
    }

    @Override
    public String toString() {
        return "AppJobClustersMap{" +
                "version='" + version + '\'' +
                ", timestamp=" + timestamp +
                ", mappings=" + mappings +
                '}';
    }

    static class Builder {

        private final Map<String, Object> mappings = new HashMap<>();
        private String version;
        private long ts = -1;

        Builder withVersion(String version) {
            this.version = version;
            return this;
        }

        Builder withTimestamp(long ts) {
            this.ts = ts;
            return this;
        }

        Builder withAppJobCluster(String appName, StreamJobClusterMap streamJobCluster) {
            mappings.put(appName, streamJobCluster.getStreamJobClusterMap());
            return this;
        }

        public AppJobClustersMap build() {
            if (version == null) {
                throw new IllegalArgumentException("version cannot be null when creating AppJobClustersMap");
            }
            if (ts == -1) {
                throw new IllegalArgumentException("timestamp not specified when creating AppJobClustersMap");
            }
            return new AppJobClustersMap(version, ts, mappings);
        }
    }
}
