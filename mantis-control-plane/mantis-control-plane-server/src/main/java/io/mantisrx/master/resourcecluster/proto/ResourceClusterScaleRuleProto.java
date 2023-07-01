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

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.jobcluster.proto.BaseRequest;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;


public class ResourceClusterScaleRuleProto {

    @Builder
    @Value
    public static class GetResourceClusterScaleRulesRequest {
        ClusterID clusterId;
    }

    @Value
    public static class GetResourceClusterScaleRulesResponse extends BaseResponse {
        ClusterID clusterId;

        @Singular
        List<ResourceClusterScaleRule> rules;

        @Builder
        @JsonCreator
        public GetResourceClusterScaleRulesResponse(
            @JsonProperty("requestId") final long requestId,
            @JsonProperty("responseCode") final ResponseCode responseCode,
            @JsonProperty("message") final String message,
            @JsonProperty("clusterId") final ClusterID clusterId,
            @JsonProperty("rules") final List<ResourceClusterScaleRule> rules) {
            super(requestId, responseCode, message);
            this.rules = rules;
            this.clusterId = clusterId;
        }
    }

    @Builder
    @Value
    public static class CreateResourceClusterScaleRuleRequest {
        ClusterID clusterId;
        ResourceClusterScaleRule rule;
    }

    /**
     * Create all scale rules to the given cluster id. This shall override/delete any existing rules.
     */
    @Builder
    @Value
    public static class CreateAllResourceClusterScaleRulesRequest {
        ClusterID clusterId;

        @Singular
        @NonNull
        List<ResourceClusterScaleRule> rules;
    }

    @Value
    @Builder
    public static class ResourceClusterScaleRule {
        ClusterID clusterId;
        ContainerSkuID skuId;
        int minIdleToKeep;
        int minSize;
        int maxIdleToKeep;
        int maxSize;
        long coolDownSecs;
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class JobArtifactsToCacheRequest extends BaseRequest {
        ClusterID clusterID;

        @Singular
        @NonNull
        List<ArtifactID> artifacts;

        public JobArtifactsToCacheRequest(@JsonProperty("clusterID") ClusterID clusterID, @JsonProperty("artifacts") List<ArtifactID> artifacts) {
            super();
            Preconditions.checkNotNull(clusterID, "clusterID cannot be null");
            Preconditions.checkNotNull(artifacts, "artifacts cannot be null");
            this.clusterID = clusterID;
            this.artifacts = artifacts;
        }
    }
}
