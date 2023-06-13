/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.master.resourcecluster.resourceprovider;

import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersRequest;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class ResourceClusterProviderUpgradeRequest {
    ClusterID clusterId;

    String region;

    @Nullable
    String optionalImageId;

    @Nullable
    String optionalSkuId;

    MantisResourceClusterEnvType optionalEnvType;

    int optionalBatchMaxSize;

    boolean forceUpgradeOnSameImage;

    boolean enableSkuSpecUpgrade;

    @Nullable
    MantisResourceClusterSpec resourceClusterSpec;

    public static ResourceClusterProviderUpgradeRequest from(
        UpgradeClusterContainersRequest req) {
        return from(req, null);
    }

    public static ResourceClusterProviderUpgradeRequest from(
        UpgradeClusterContainersRequest req,
        MantisResourceClusterSpec resourceClusterSpec) {
        return ResourceClusterProviderUpgradeRequest.builder()
            .clusterId(req.getClusterId())
            .region(req.getRegion())
            .optionalImageId(req.getOptionalImageId())
            .optionalSkuId(req.getOptionalSkuId())
            .optionalEnvType(req.getOptionalEnvType())
            .optionalBatchMaxSize(req.getOptionalBatchMaxSize())
            .forceUpgradeOnSameImage(req.isForceUpgradeOnSameImage())
            .enableSkuSpecUpgrade(req.isEnableSkuSpecUpgrade())
            .resourceClusterSpec(resourceClusterSpec)
            .build();
    }
}
