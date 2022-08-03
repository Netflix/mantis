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

package io.mantisrx.master.api.akka.route.handlers;

import io.mantisrx.master.resourcecluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.master.resourcecluster.proto.ListResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.DeleteResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateAllResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateResourceClusterScaleRuleRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesResponse;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceResponse;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersRequest;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.util.concurrent.CompletionStage;

public interface ResourceClusterRouteHandler {
    CompletionStage<ListResourceClustersResponse> get(final ListResourceClusterRequest request);

    CompletionStage<GetResourceClusterResponse> create(final ProvisionResourceClusterRequest request);

    CompletionStage<DeleteResourceClusterResponse> delete(final ClusterID clusterId);

    CompletionStage<GetResourceClusterResponse> get(final GetResourceClusterSpecRequest request);

    CompletionStage<ScaleResourceResponse> scale(final ScaleResourceRequest request);

    CompletionStage<UpgradeClusterContainersResponse> upgrade(final UpgradeClusterContainersRequest request);

    CompletionStage<GetResourceClusterScaleRulesResponse> createSingleScaleRule(
        CreateResourceClusterScaleRuleRequest request);

    CompletionStage<GetResourceClusterScaleRulesResponse> createAllScaleRule(
        CreateAllResourceClusterScaleRulesRequest rule);

    CompletionStage<GetResourceClusterScaleRulesResponse> getClusterScaleRules(
        GetResourceClusterScaleRulesRequest request);
}
