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

package io.mantisrx.master.api.akka.payloads;

public class ResourceClustersPayloads {
    public static final String CLUSTER_ID = "mantisResourceClusterUT1";

    public static final String RESOURCE_CLUSTER_CREATE = "{\"clusterId\":\"mantisResourceClusterUT1\",\"clusterSpec\":"
            + "{\"name\":\"mantisResourceClusterUT1\",\"id\":\"mantisResourceClusterUT1\",\"ownerName\":"
            + "\"mantisrx@netflix.com\",\"ownerEmail\":\"mantisrx@netflix.com\",\"envType\":\"Prod\","
            + "\"skuSpecs\":[{\"skuId\":\"small\",\"capacity\":{\"skuId\":\"small\",\"minSize\":1,"
            + "\"maxSize\":3,\"desireSize\":2},\"imageId\":\"dev/mantistaskexecutor:main.test\","
            + "\"cpuCoreCount\":2,\"memorySizeInBytes\":16384,\"networkMbps\":700,\"diskSizeInBytes\":"
            + "81920,\"skuMetadataFields\":{\"skuRegionNameKey\":\"us-east-1\",\"skuSecurityGroupListKey\":"
            + "\"sg-1, sg-2, sg-3, sg-4\"}}],\"clusterMetadataFields\":{}}}";

    public static final String RESOURCE_CLUSTER_SKU_SCALE = "{\"clusterId\":\"mantisResourceClusterUT1\","
            + "\"skuId\":\"small\",\"region\":\"us-east-1\",\"envType\":\"Prod\",\"desireSize\":11}\n";
}
