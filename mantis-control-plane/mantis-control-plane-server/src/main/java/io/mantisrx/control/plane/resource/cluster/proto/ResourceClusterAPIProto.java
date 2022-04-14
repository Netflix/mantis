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

import io.mantisrx.master.jobcluster.proto.BaseResponse;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

public class ResourceClusterAPIProto {

    @Value
    public static final class ListResourceClustersResponse extends BaseResponse {

        @Singular
        List<RegisteredResourceCluster> registeredResourceClusters;

        @Builder
        public ListResourceClustersResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final List<RegisteredResourceCluster> registeredResourceClusters) {
            super(requestId, responseCode, message);
            this.registeredResourceClusters = registeredResourceClusters;
        }

        @Value
        @Builder
        public static class RegisteredResourceCluster {
            String id;
            String version;
        }
    }

    @Value
    public static final class GetResourceClusterResponse extends BaseResponse {

        MantisResourceClusterSpec clusterSpec;

        @Builder
        public GetResourceClusterResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final MantisResourceClusterSpec clusterSpec) {
            super(requestId, responseCode, message);
            this.clusterSpec = clusterSpec;
        }
    }
}
