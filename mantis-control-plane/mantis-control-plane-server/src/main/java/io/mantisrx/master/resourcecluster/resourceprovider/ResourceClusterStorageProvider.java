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

import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.util.concurrent.CompletionStage;

/**
 * Interface for persisting resource cluster related data.
 */
public interface ResourceClusterStorageProvider {
    /**
     * Register and save the given cluster spec. Once the returned CompletionStage
     * finishes successfully the given cluster should be available in list cluster response.
     */
    CompletionStage<ResourceClusterSpecWritable> registerAndUpdateClusterSpec(ResourceClusterSpecWritable spec);

    CompletionStage<RegisteredResourceClustersWritable> deregisterCluster(ClusterID clusterId);

    CompletionStage<RegisteredResourceClustersWritable> getRegisteredResourceClustersWritable();

    CompletionStage<ResourceClusterSpecWritable> getResourceClusterSpecWritable(ClusterID id);

    CompletionStage<ResourceClusterScaleRulesWritable> getResourceClusterScaleRules(ClusterID clusterId);

    CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(
        ResourceClusterScaleRulesWritable ruleSpec);

    CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(
        ResourceClusterScaleSpec rule);
}
