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

import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

public class ResourceClusterAPIProto {

    // TODO: @Value generated equals doesn't include base class fields.
    @Value
    public static class ListResourceClustersResponse extends BaseResponse {

        @Singular
        List<RegisteredResourceCluster> registeredResourceClusters;

        /** [Note] The @JsonCreator + @JasonProperty is needed when using this class with mixed shaded/non-shaded Jackson.
         * The new @Jacksonized annotation is currently not usable with shaded Jackson here.
         */
        @Builder
        @JsonCreator
        public ListResourceClustersResponse(
                @JsonProperty("requestId") final long requestId,
                @JsonProperty("responseCode") final ResponseCode responseCode,
                @JsonProperty("message") final String message,
                @JsonProperty("registeredResourceClusters") final List<RegisteredResourceCluster> registeredResourceClusters) {
            super(requestId, responseCode, message);
            this.registeredResourceClusters = registeredResourceClusters;
        }

        @Value
        @Builder
        public static class RegisteredResourceCluster {
            String id;
            String version;

            @JsonCreator
            public RegisteredResourceCluster(
                @JsonProperty("id") final String id,
                @JsonProperty("version") final String version) {
                this.id = id;
                this.version = version;
            }
        }
    }

    @Value
    public static class GetResourceClusterResponse extends BaseResponse {

        MantisResourceClusterSpec clusterSpec;

        @Builder
        @JsonCreator
        public GetResourceClusterResponse(
            @JsonProperty("requestId") final long requestId,
            @JsonProperty("responseCode") final ResponseCode responseCode,
            @JsonProperty("message") final String message,
            @JsonProperty("clusterSpec") final MantisResourceClusterSpec clusterSpec) {
            super(requestId, responseCode, message);
            this.clusterSpec = clusterSpec;
        }
    }

    @Value
    public static class DeleteResourceClusterResponse extends BaseResponse {
        @Builder
        @JsonCreator
        public DeleteResourceClusterResponse(
            @JsonProperty("requestId") final long requestId,
            @JsonProperty("responseCode") final ResponseCode responseCode,
            @JsonProperty("message") final String message) {
            super(requestId, responseCode, message);
        }
    }

    @Builder
    @Value
    public static class DeleteResourceClusterRequest {
        String clusterId;
    }
}
