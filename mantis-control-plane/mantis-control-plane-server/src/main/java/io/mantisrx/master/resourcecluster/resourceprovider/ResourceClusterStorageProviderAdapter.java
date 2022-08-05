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
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

/**
 * Adapter to bind the implementation of {@link ResourceClusterStorageProvider} using class name specified in
 * {@link io.mantisrx.server.master.config.MasterConfiguration}.
 * <p>
 *     This adapter requires the implementation of {@link ResourceClusterStorageProvider} to have a ctor with
 *     {@link akka.actor.ActorSystem} param or empty ctor.
 * </p>
 */
@Slf4j
public class ResourceClusterStorageProviderAdapter implements ResourceClusterStorageProvider {
    private final ResourceClusterStorageProvider providerImpl;

    public ResourceClusterStorageProviderAdapter(String providerClassStr, ActorSystem system) {
        boolean fallBackToEmptyCtor = false;
        ResourceClusterStorageProvider provider = null;
        try {
            provider = (ResourceClusterStorageProvider) Class.forName(providerClassStr)
                .getConstructor(ActorSystem.class).newInstance(system);
        } catch (NoSuchMethodException ex) {
            log.warn("Could not find ctor with actorSystem param: {}", providerClassStr);
            fallBackToEmptyCtor = true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ResourceClusterStorageProvider from " + providerClassStr,  e);
        }

        if (fallBackToEmptyCtor) {
            try {
                log.info("Building ResourceClusterProvider with empty ctor: {}", providerClassStr);
                provider = (ResourceClusterStorageProvider) Class.forName(providerClassStr)
                    .getConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create ResourceClusterStorageProvider from " + providerClassStr,  e);
            }
        }

        this.providerImpl = provider;
    }

    @Override
    public CompletionStage<ResourceClusterSpecWritable> registerAndUpdateClusterSpec(ResourceClusterSpecWritable spec) {
        return this.providerImpl.registerAndUpdateClusterSpec(spec);
    }

    @Override
    public CompletionStage<RegisteredResourceClustersWritable> deregisterCluster(ClusterID clusterId) {
        return this.providerImpl.deregisterCluster(clusterId);
    }

    @Override
    public CompletionStage<RegisteredResourceClustersWritable> getRegisteredResourceClustersWritable() {
        return this.providerImpl.getRegisteredResourceClustersWritable();
    }

    @Override
    public CompletionStage<ResourceClusterSpecWritable> getResourceClusterSpecWritable(ClusterID id) {
        return this.providerImpl.getResourceClusterSpecWritable(id);
    }

    @Override
    public CompletionStage<ResourceClusterScaleRulesWritable> getResourceClusterScaleRules(ClusterID clusterId) {
        return this.providerImpl.getResourceClusterScaleRules(clusterId);
    }

    @Override
    public CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(
        ResourceClusterScaleRulesWritable ruleSpec) {
        return this.providerImpl.registerResourceClusterScaleRule(ruleSpec);
    }

    @Override
    public CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(ResourceClusterScaleSpec rule) {
        return this.providerImpl.registerResourceClusterScaleRule(rule);
    }

}
