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

import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

/**
 * Persistence contract of resource cluster spec.
 */
@Value
@Builder
public class ResourceClusterSpecWritable {
    String version;

    ClusterID id;

    MantisResourceClusterSpec clusterSpec;

    /** [Note] The @JsonCreator + @JasonProperty is needed when using this class with mixed shaded/non-shaded Jackson.
     * The new @Jacksonized annotation is currently not usable with shaded Jackson here.
     */
    @JsonCreator
    public ResourceClusterSpecWritable(
            @JsonProperty("version") final String version,
            @JsonProperty("id") final ClusterID id,
            @JsonProperty("clusterSpec") final MantisResourceClusterSpec clusterSpec) {
        this.version = version;
        this.id = id;
        this.clusterSpec = clusterSpec;
    }
}
