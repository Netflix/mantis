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

import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.Value;

/**
 * Contract class to define a Mantis resource cluster. This contract provides the abstraction to provide a generic
 * definition from Mantis control perspective, and it's up to the implementations of each
 * {@link ResourceClusterProvider} to translate this spec
 * to corresponding framework's cluster/node(s) definition.
 */
@Value
@Builder
public class MantisResourceClusterSpec {

    String name;

    /**
     * ID fields maps to cluster name or spinnaker app name.
     */
    ClusterID id;

    String ownerName;

    String ownerEmail;

    MantisResourceClusterEnvType envType;

    @Singular
    Set<SkuTypeSpec> skuSpecs;

    @Singular
    Map<String, String> clusterMetadataFields;

    /** [Note] The @JsonCreator + @JasonProperty is needed when using this class with mixed shaded/non-shaded Jackson.
     * The new @Jacksonized annotation is currently not usable with shaded Jackson here.
     */
    @JsonCreator
    public MantisResourceClusterSpec(
            @JsonProperty("name") final String name,
            @JsonProperty("id") final ClusterID id,
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
        @EqualsAndHashCode.Include
        ContainerSkuID skuId;

        SkuCapacity capacity;

        String imageId;

        @Deprecated
        int cpuCoreCount;

        @Deprecated
        int memorySizeInMB;

        @Deprecated
        int networkMbps;

        @Deprecated
        int diskSizeInMB;

        @Singular
        Map<String, String> skuMetadataFields;

        // TODO(fdichiara): Currently nullable for backward compatibility.
        // Plan to remove nullable after upgrading all existing SKU specs.
        @Nullable
        SkuSizeSpec size;

        @JsonCreator
        public SkuTypeSpec(
                @JsonProperty("skuId") final ContainerSkuID skuId,
                @JsonProperty("capacity") final SkuCapacity capacity,
                @JsonProperty("imageId") final String imageId,
                @JsonProperty("cpuCoreCount") final int cpuCoreCount,
                @JsonProperty("memorySizeInBytes") final int memorySizeInMB,
                @JsonProperty("networkMbps") final int networkMbps,
                @JsonProperty("diskSizeInBytes") final int diskSizeInMB,
                @JsonProperty("skuMetadataFields") final Map<String, String> skuMetadataFields,
                @JsonProperty("size") final SkuSizeSpec size) {
            this.skuId = skuId;
            this.capacity = capacity;
            this.imageId = imageId;
            this.cpuCoreCount = cpuCoreCount;
            this.memorySizeInMB = memorySizeInMB;
            this.networkMbps = networkMbps;
            this.diskSizeInMB = diskSizeInMB;
            this.skuMetadataFields = skuMetadataFields;
            this.size = size;
        }
    }

    /**
     * This class defined the capacity required for the given skuId mapping to hosting framework nodes
     * e.g. containers/virtual machines.
     */
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
