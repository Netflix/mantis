/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.master.resourcecluster.writable;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * Persistency contract of registered resource clusters.
 */
@Value
@Builder(toBuilder = true)
public class RegisteredResourceClustersWritable {
    @Singular
    Map<String, ClusterRegistration> clusters;

    @JsonCreator
    public RegisteredResourceClustersWritable(
            @JsonProperty("clusters") final Map<String, ClusterRegistration> clusters) {
        this.clusters = clusters;
    }

    @Value
    @Builder
    public static class ClusterRegistration {
        String clusterId;

        String version;

        /** [Note] The @JsonCreator + @JasonProperty is needed when using this class with mixed shaded/non-shaded Jackson.
         * The new @Jacksonized annotation is currently not usable with shaded Jackson here.
         */
        @JsonCreator
        public ClusterRegistration(
                @JsonProperty("clusterId") final String clusterId,
                @JsonProperty("version") final String version) {
            this.clusterId = clusterId;
            this.version = version;
        }
    }
}
