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

import static akka.pattern.Patterns.ask;

import akka.actor.ActorRef;
import io.mantisrx.master.resourcecluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.master.resourcecluster.proto.ListResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.DeleteResourceClusterRequest;
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
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceClusterRouteHandlerAkkaImpl implements ResourceClusterRouteHandler {

    private final ActorRef resourceClustersHostManagerActor;
    private final Duration timeout;
    private final Duration longOperationTimeout;

    public ResourceClusterRouteHandlerAkkaImpl(ActorRef resourceClustersHostManagerActor) {
        this.resourceClustersHostManagerActor = resourceClustersHostManagerActor;
        this.timeout = Duration.ofMillis(ConfigurationProvider.getConfig().getMasterApiAskTimeoutMs());
        this.longOperationTimeout = Duration.ofMillis(
            ConfigurationProvider.getConfig().getMasterApiLongOperationAskTimeoutMs());
    }

    @Override
    public CompletionStage<ListResourceClustersResponse> get(ListResourceClusterRequest request) {
        CompletionStage<ListResourceClustersResponse> response =
                ask(this.resourceClustersHostManagerActor, request, timeout)
                        .thenApply(ListResourceClustersResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<GetResourceClusterResponse> create(
            ProvisionResourceClusterRequest request) {
        CompletionStage<GetResourceClusterResponse> response =
                ask(this.resourceClustersHostManagerActor, request, timeout)
                        .thenApply(GetResourceClusterResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<DeleteResourceClusterResponse> delete(ClusterID clusterId) {
        CompletionStage<DeleteResourceClusterResponse> response =
            ask(this.resourceClustersHostManagerActor,
                DeleteResourceClusterRequest.builder().clusterId(clusterId).build(),
                timeout)
                    .thenApply(DeleteResourceClusterResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<GetResourceClusterResponse> get(GetResourceClusterSpecRequest request) {
        CompletionStage<GetResourceClusterResponse> response =
                ask(this.resourceClustersHostManagerActor, request, timeout)
                        .thenApply(GetResourceClusterResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<ScaleResourceResponse> scale(ScaleResourceRequest request) {
        CompletionStage<ScaleResourceResponse> response =
                ask(this.resourceClustersHostManagerActor, request, this.longOperationTimeout)
                        .thenApply(ScaleResourceResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<UpgradeClusterContainersResponse> upgrade(UpgradeClusterContainersRequest request) {
        CompletionStage<UpgradeClusterContainersResponse> response =
            ask(this.resourceClustersHostManagerActor, request, this.longOperationTimeout)
                .thenApply(UpgradeClusterContainersResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<GetResourceClusterScaleRulesResponse> createSingleScaleRule(
        CreateResourceClusterScaleRuleRequest request) {
        return ask(this.resourceClustersHostManagerActor, request, timeout)
            .thenApply(GetResourceClusterScaleRulesResponse.class::cast);
    }

    @Override
    public CompletionStage<GetResourceClusterScaleRulesResponse> createAllScaleRule(
        CreateAllResourceClusterScaleRulesRequest request) {
        return ask(this.resourceClustersHostManagerActor, request, timeout)
            .thenApply(GetResourceClusterScaleRulesResponse.class::cast);
    }

    @Override
    public CompletionStage<GetResourceClusterScaleRulesResponse> getClusterScaleRules(
        GetResourceClusterScaleRulesRequest request) {
        return ask(this.resourceClustersHostManagerActor, request, timeout)
            .thenApply(GetResourceClusterScaleRulesResponse.class::cast);
    }
}
