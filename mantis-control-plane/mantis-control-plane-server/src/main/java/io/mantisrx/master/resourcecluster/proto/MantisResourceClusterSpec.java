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
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
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
}
