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
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;


public class ResourceClusterSkuSizeProto {

    @Builder
    @Value
    public static class GetResourceClusterSkuSizesRequest {
    }

    @Builder
    @Value
    public static class CreateResourceClusterSkuSizeRequest {
        SkuSizeSpec skuSizeSpec;
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class GetResourceClusterSkuSizesResponse extends BaseResponse {
        @Singular
        @NonNull
        List<SkuSizeSpec> skuSizeSpecs;

        @Builder
        @JsonCreator
        public GetResourceClusterSkuSizesResponse(
            @JsonProperty("requestId") final long requestId,
            @JsonProperty("responseCode") final ResponseCode responseCode,
            @JsonProperty("message") final String message,
            @JsonProperty("skuSizeSpecs") final List<SkuSizeSpec> skuSizeSpecs) {
            super(requestId, responseCode, message);
            this.skuSizeSpecs = skuSizeSpecs;
        }
    }
}
