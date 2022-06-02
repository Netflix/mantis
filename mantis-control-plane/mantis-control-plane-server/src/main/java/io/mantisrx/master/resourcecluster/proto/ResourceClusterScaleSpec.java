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

package io.mantisrx.master.resourcecluster.proto;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ResourceClusterScaleSpec {
    String clusterId;
    String skuId;
    int minIdleToKeep;
    int minSize;
    int maxIdleToKeep;
    int maxSize;
    long coolDownSecs;

    @JsonCreator
    public ResourceClusterScaleSpec(
        @JsonProperty("clusterId") final String clusterId,
        @JsonProperty("skuId") final String skuId,
        @JsonProperty("minIdleToKeep") final int minIdleToKeep,
        @JsonProperty("minSize") final int minSize,
        @JsonProperty("maxIdleToKeep") final int maxIdleToKeep,
        @JsonProperty("maxSize") final int maxSize,
        @JsonProperty("coolDownSecs") final long coolDownSecs) {
        this.clusterId = clusterId;
        this.skuId = skuId;
        this.minIdleToKeep = minIdleToKeep;
        this.minSize = minSize;
        this.maxIdleToKeep = maxIdleToKeep;
        this.maxSize = maxSize;
        this.coolDownSecs = coolDownSecs;
    }
}
