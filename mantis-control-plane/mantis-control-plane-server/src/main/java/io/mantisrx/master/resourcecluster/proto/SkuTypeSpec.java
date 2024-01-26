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

import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.SkuSizeID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.Value;

@Builder
@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class SkuTypeSpec {
    @EqualsAndHashCode.Include
    ContainerSkuID skuId; // user-defined ID

    String imageId;

    SkuCapacity capacity;

    // TODO(fdichiara): drop these 4 values in favor of sizeID. kept for backward compatibility during sizeID rollout.
    int cpuCoreCount;
    int memorySizeInMB;
    int networkMbps;
    int diskSizeInMB;

    @Singular
    Map<String, String> skuMetadataFields; // ie. jdk17, sbn3

    @Nullable
    SkuSizeID sizeId; // ie. small-v0, large-v1

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonCreator
    public SkuTypeSpec(
        @JsonProperty("skuId") final ContainerSkuID skuId,
        @JsonProperty("imageId") final String imageId,
        @JsonProperty("capacity") final SkuCapacity capacity,
        @JsonProperty("cpuCoreCount") final int cpuCoreCount,
        @JsonProperty("memorySizeInBytes") final int memorySizeInMB,
        @JsonProperty("networkMbps") final int networkMbps,
        @JsonProperty("diskSizeInBytes") final int diskSizeInMB,
        @JsonProperty("skuMetadataFields") final Map<String, String> skuMetadataFields,
        @JsonProperty("sizeId") final SkuSizeID sizeId) {
        this.skuId = skuId;
        this.imageId = imageId;
        this.capacity = capacity;
        this.cpuCoreCount = cpuCoreCount;
        this.memorySizeInMB = memorySizeInMB;
        this.networkMbps = networkMbps;
        this.diskSizeInMB = diskSizeInMB;
        this.skuMetadataFields = skuMetadataFields;
        this.sizeId = sizeId;
    }

    @Builder
    @Value
    public static class SkuCapacity {
        ContainerSkuID skuId;

        int minSize;

        int maxSize;

        int desireSize;

        @JsonCreator
        public SkuCapacity(
            @JsonProperty("skuId") final ContainerSkuID skuId,
            @JsonProperty("minSize") final int minSize,
            @JsonProperty("maxSize") final int maxSize,
            @JsonProperty("desireSize") final int desireSize
        ) {
            this.skuId = skuId;
            this.minSize = minSize;
            this.maxSize = maxSize;
            this.desireSize = desireSize;
        }
    }
}
