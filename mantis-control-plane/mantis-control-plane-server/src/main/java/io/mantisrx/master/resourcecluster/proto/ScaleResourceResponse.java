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
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Value
@ToString(callSuper = true)
public class ScaleResourceResponse extends BaseResponse {
    ClusterID clusterId;

    ContainerSkuID skuId;

    String region;

    MantisResourceClusterEnvType envType;

    int desireSize;

    @JsonCreator
    public ScaleResourceResponse(
            @JsonProperty("requestId") final long requestId,
            @JsonProperty("responseCode") final ResponseCode responseCode,
            @JsonProperty("message") final String message,
            @JsonProperty("clusterId") final ClusterID clusterId,
            @JsonProperty("skuId") ContainerSkuID skuId,
            @JsonProperty("region") String region,
            @JsonProperty("envType") MantisResourceClusterEnvType envType,
            @JsonProperty("desireSize") int desireSize) {
        super(requestId, responseCode, message);
        this.clusterId = clusterId;
        this.skuId = skuId;
        this.region = region;
        this.envType = envType;
        this.desireSize = desireSize;
    }
}
