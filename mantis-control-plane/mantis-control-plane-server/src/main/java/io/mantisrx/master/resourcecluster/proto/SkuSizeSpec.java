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

import io.mantisrx.server.master.resourcecluster.SkuSizeID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Builder
@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class SkuSizeSpec {
    @EqualsAndHashCode.Include
    SkuSizeID skuSizeID; // ie. small-v0, small-v1

    String skuSizeName; // ie. small

    int cpuCoreCount;

    int memorySizeInMB;

    int networkMbps;

    int diskSizeInMB;

    @JsonCreator
    public SkuSizeSpec(
        @JsonProperty("skuSizeID") final SkuSizeID skuSizeID,
        @JsonProperty("skuSizeName") final String skuSizeName,
        @JsonProperty("cpuCoreCount") final int cpuCoreCount,
        @JsonProperty("memorySizeInMB") final int memorySizeInMB,
        @JsonProperty("networkMbps") final int networkMbps,
        @JsonProperty("diskSizeInMB") final int diskSizeInMB) {
        this.skuSizeID = skuSizeID;
        this.skuSizeName = skuSizeName;
        this.cpuCoreCount = cpuCoreCount;
        this.memorySizeInMB = memorySizeInMB;
        this.networkMbps = networkMbps;
        this.diskSizeInMB = diskSizeInMB;
    }

    public boolean isSizeValid() {
        return cpuCoreCount >= 1 && diskSizeInMB >= 1 && memorySizeInMB >= 1 && networkMbps >= 1;
    }
}
