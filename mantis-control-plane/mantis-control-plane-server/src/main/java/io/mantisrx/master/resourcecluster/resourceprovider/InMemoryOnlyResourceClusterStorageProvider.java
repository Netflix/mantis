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

/**
 * [Test only] Store resource storage data in memory only for testing.
 */
//@RequiredArgsConstructor
//public class InMemoryOnlyResourceClusterStorageProvider implements ResourceClusterStorageProvider {
//    private final Map<ClusterID, ResourceClusterSpecWritable> clusters = new ConcurrentHashMap<>();
//    private final Map<ClusterID, ResourceClusterScaleRulesWritable> clusterRules = new ConcurrentHashMap<>();
//
//    @Override
//    public CompletionStage<ResourceClusterSpecWritable> registerAndUpdateClusterSpec(ResourceClusterSpecWritable spec) {
//        this.clusters.put(spec.getId(), spec);
//        return CompletableFuture.completedFuture(spec);
//    }
//
//    @Override
//    public CompletionStage<RegisteredResourceClustersWritable> deregisterCluster(ClusterID clusterId) {
//        this.clusters.remove(clusterId);
//        return getRegisteredResourceClustersWritable();
//    }
//
//    @Override
//    public CompletionStage<RegisteredResourceClustersWritable> getRegisteredResourceClustersWritable() {
//        RegisteredResourceClustersWritable.RegisteredResourceClustersWritableBuilder builder =
//                RegisteredResourceClustersWritable.builder();
//
//        this.clusters.forEach((key, value) ->
//            builder.cluster(
//                key.getResourceID(),
//                RegisteredResourceClustersWritable.ClusterRegistration.builder()
//                    .clusterId(key).version(value.getVersion()).build()));
//
//        return CompletableFuture.completedFuture(builder.build());
//    }
//
//    @Override
//    public CompletionStage<ResourceClusterSpecWritable> getResourceClusterSpecWritable(ClusterID id) {
//        return CompletableFuture.completedFuture(this.clusters.getOrDefault(id, null));
//    }
//
//    @Override
//    public CompletionStage<ResourceClusterScaleRulesWritable> getResourceClusterScaleRules(ClusterID clusterId) {
//        return CompletableFuture.completedFuture(
//            this.clusterRules.getOrDefault(
//                clusterId,
//                ResourceClusterScaleRulesWritable.builder().clusterId(clusterId).build()));
//    }
//
//    @Override
//    public CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(
//        ResourceClusterScaleSpec rule) {
//        if (this.clusterRules.containsKey(rule.getClusterId())) {
//            this.clusterRules.computeIfPresent(rule.getClusterId(), (k, v) ->
//                v.toBuilder().scaleRule(rule.getSkuId().getResourceID(), rule).build());
//        }
//        else {
//            this.clusterRules.put(
//                rule.getClusterId(),
//                ResourceClusterScaleRulesWritable.builder()
//                    .clusterId(rule.getClusterId())
//                    .scaleRule(rule.getSkuId().getResourceID(), rule).build());
//        }
//
//        return CompletableFuture.completedFuture(this.clusterRules.get(rule.getClusterId()));
//    }
//
//    public CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(
//        ResourceClusterScaleRulesWritable rule) {
//        this.clusterRules.put(rule.getClusterId(), rule);
//        return CompletableFuture.completedFuture(this.clusterRules.get(rule.getClusterId()));
//    }
//}
