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
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
public class ScaleResourceResponse extends BaseResponse {
    @NonNull
    String clusterId;

    @NonNull
    String skuId;

    @NonNull
    String region;

    @NonNull
    MantisResourceClusterEnvType envType;

    int desireSize;

    @Builder
    public ScaleResourceResponse(
            final long requestId,
            final ResponseCode responseCode,
            final String message,
            final String clusterId,
            String skuId,
            String region,
            MantisResourceClusterEnvType envType,
            int desireSize) {
        super(requestId, responseCode, message);
        this.clusterId = clusterId;
        this.skuId = skuId;
        this.region = region;
        this.envType = envType;
        this.desireSize = desireSize;
    }
}
