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
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Value
@ToString(callSuper = true)
public class UpgradeClusterContainersResponse extends BaseResponse {
    ClusterID clusterId;

    String region;

    String optionalSkuId;

    MantisResourceClusterEnvType optionalEnvType;

    @JsonCreator
    public UpgradeClusterContainersResponse(
        @JsonProperty("requestId") final long requestId,
        @JsonProperty("responseCode") final ResponseCode responseCode,
        @JsonProperty("message") final String message,
        @JsonProperty("clusterId") final ClusterID clusterId,
        @JsonProperty("region") final String region,
        @JsonProperty("optionalSkuId") String optionalSkuId,
        @JsonProperty("optionalEnvType") MantisResourceClusterEnvType optionalEnvType) {
        super(requestId, responseCode, message);
        this.clusterId = clusterId;
        this.optionalSkuId = optionalSkuId;
        this.region = region;
        this.optionalEnvType = optionalEnvType;
    }
}
