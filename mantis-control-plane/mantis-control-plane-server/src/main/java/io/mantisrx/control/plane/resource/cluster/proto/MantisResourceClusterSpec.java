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

package io.mantisrx.control.plane.resource.cluster.proto;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Contract class to define a Mantis resource cluster. This contrace provides the abstraction to provide a generic
 * definition from Mantis control perspective, and it's up to the implementations of each
 * {@link io.mantisrx.control.plane.resource.cluster.resourceprovider.IResourceClusterProvider} to translate this spec
 * to corresponding framework's cluster/node(s) definition.
 */
@Value
@Builder
public class MantisResourceClusterSpec {

    @NonNull
    String name;

    /**
     * ID fields maps to cluster name or spinnaker app name.
     */
    @NonNull
    String id;

    @NonNull
    String ownerName;

    @NonNull
    String ownerEmail;

    @NonNull
    MantisResourceClusterEnvType envType;

    @Singular
    Set<SkuTypeSpec> skuSpecs;

    @Singular
    Map<String, String> clusterMetadataFields;

    @JsonCreator
    public MantisResourceClusterSpec(
            @JsonProperty("name") final String name,
            @JsonProperty("id") final String id,
            @JsonProperty("ownerName") final String ownerName,
            @JsonProperty("ownerEmail") final String ownerEmail,
            @JsonProperty("envType") final MantisResourceClusterEnvType envType,
            @JsonProperty("skuSpecs") final Set<SkuTypeSpec> skuSpecs,
            @JsonProperty("clusterMetadataFields") final Map<String, String> clusterMetadataFields) {
        this.name = name;
        this.id = id;
        this.ownerName = ownerName;
        this.ownerEmail = ownerEmail;
        this.envType = envType;
        this.skuSpecs = skuSpecs;
        this.clusterMetadataFields = clusterMetadataFields;
    }

    @Builder
    @Value
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static class SkuTypeSpec {
        @NonNull
        @EqualsAndHashCode.Include
        String skuId;

        @NonNull
        SkuCapacity capacity;

        @NonNull
        String imageId;

        int cpuCoreCount;

        int memorySizeInBytes;

        int networkMbps;

        int diskSizeInBytes;

        @Singular
        Map<String, String> skuMetadataFields;

        @JsonCreator
        public SkuTypeSpec(
                @JsonProperty("skuId") final String skuId,
                @JsonProperty("capacity") final SkuCapacity capacity,
                @JsonProperty("imageId") final String imageId,
                @JsonProperty("cpuCoreCount") final int cpuCoreCount,
                @JsonProperty("memorySizeInBytes") final int memorySizeInBytes,
                @JsonProperty("networkMbps") final int networkMbps,
                @JsonProperty("diskSizeInBytes") final int diskSizeInBytes,
                @JsonProperty("skuMetadataFields") final Map<String, String> skuMetadataFields) {
            this.skuId = skuId;
            this.capacity = capacity;
            this.imageId = imageId;
            this.cpuCoreCount = cpuCoreCount;
            this.memorySizeInBytes = memorySizeInBytes;
            this.networkMbps = networkMbps;
            this.diskSizeInBytes = diskSizeInBytes;
            this.skuMetadataFields = skuMetadataFields;
        }
    }

    @Builder
    @Value
    public static class SkuCapacity {
        @NonNull
        String skuId;

        int minSize;

        int maxSize;

        int desireSize;

        @JsonCreator
        public SkuCapacity(
                @JsonProperty("skuId") final String skuId,
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
