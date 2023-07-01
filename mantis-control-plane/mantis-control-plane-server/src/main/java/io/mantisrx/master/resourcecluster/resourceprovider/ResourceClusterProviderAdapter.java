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

import akka.actor.ActorSystem;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterProvisionSubmissionResponse;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceResponse;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

/**
 * Adapter to bind the implementation of {@link ResourceClusterProvider} using class name specified in
 * {@link io.mantisrx.server.master.config.MasterConfiguration}.
 * <p>
 *     This adapter requires the implementation of {@link ResourceClusterProvider} to have a ctor with
 *     {@link akka.actor.ActorSystem} param.
 * </p>
 */
@Slf4j
public class ResourceClusterProviderAdapter implements ResourceClusterProvider {
    private final ResourceClusterProvider providerImpl;

    public ResourceClusterProviderAdapter(String providerClassStr, ActorSystem system) {
        boolean fallBackToEmptyCtor = false;
        ResourceClusterProvider provider = null;
        try {
            provider = (ResourceClusterProvider) Class.forName(providerClassStr)
                .getConstructor(ActorSystem.class).newInstance(system);
        } catch (NoSuchMethodException ex) {
            log.warn("Could not find ctor with actorSystem param: {}", providerClassStr);
            fallBackToEmptyCtor = true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ResourceClusterProvider from " + providerClassStr,  e);
        }

        if (fallBackToEmptyCtor) {
            try {
                log.info("Building ResourceClusterProvider with empty ctor: {}", providerClassStr);
                provider = (ResourceClusterProvider) Class.forName(providerClassStr)
                    .getConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create ResourceClusterProvider from " + providerClassStr,  e);
            }
        }

        this.providerImpl = provider;
    }

    @Override
    public CompletionStage<ResourceClusterProvisionSubmissionResponse> provisionClusterIfNotPresent(ProvisionResourceClusterRequest clusterSpec) {
        return providerImpl.provisionClusterIfNotPresent(clusterSpec);
    }

    @Override
    public CompletionStage<ScaleResourceResponse> scaleResource(ScaleResourceRequest scaleRequest) {
        return providerImpl.scaleResource(scaleRequest);
    }

    @Override
    public CompletionStage<UpgradeClusterContainersResponse> upgradeContainerResource(
        ResourceClusterProviderUpgradeRequest request) {
        return providerImpl.upgradeContainerResource(request);
    }

    @Override
    public ResourceClusterResponseHandler getResponseHandler() {
        return providerImpl.getResponseHandler();
    }
}
