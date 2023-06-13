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

package io.mantisrx.master.resourcecluster.resourceprovider;

import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterProvisionSubmissionResponse;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceResponse;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Default Resource cluster provider implementation. This needs to be replaced by OSS implementation provider e.g. k8s.
 */
public class NoopResourceClusterProvider implements ResourceClusterProvider {
    @Override
    public CompletionStage<ResourceClusterProvisionSubmissionResponse> provisionClusterIfNotPresent(ProvisionResourceClusterRequest clusterSpec) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<ScaleResourceResponse> scaleResource(ScaleResourceRequest scaleRequest) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<UpgradeClusterContainersResponse> upgradeContainerResource(
        ResourceClusterProviderUpgradeRequest request) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public ResourceClusterResponseHandler getResponseHandler() {
        return new NoopResourceClusterResponseHandler();
    }
}
