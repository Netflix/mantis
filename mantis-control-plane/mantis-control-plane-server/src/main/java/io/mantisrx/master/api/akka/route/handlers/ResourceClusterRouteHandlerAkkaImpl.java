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
import io.mantisrx.control.plane.resource.cluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ListResourceClusterRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse;
import io.mantisrx.control.plane.resource.cluster.proto.ScaleResourceRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ScaleResourceResponse;
import io.mantisrx.server.master.config.ConfigurationProvider;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceClusterRouteHandlerAkkaImpl implements ResourceClusterRouteHandler {

    private final ActorRef resourceClustersManagerActor;
    private final Duration timeout;

    public ResourceClusterRouteHandlerAkkaImpl(ActorRef resourceClustersManagerActor) {
        this.resourceClustersManagerActor = resourceClustersManagerActor;
        long timeoutMs = Optional.ofNullable(ConfigurationProvider.getConfig().getMasterApiAskTimeoutMs()).orElse(1000L);
        this.timeout = Duration.ofMillis(timeoutMs);
        // Metrics m = new Metrics.Builder()
        //         .id("JobClusterRouteHandler")
        //         .addCounter("allJobClustersGET")
        //         .build();
        // Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        // allJobClustersGET = metrics.getCounter("allJobClustersGET");
    }

    @Override
    public CompletionStage<ListResourceClustersResponse> get(ListResourceClusterRequest request) {
        CompletionStage<ListResourceClustersResponse> response =
                ask(this.resourceClustersManagerActor, request, timeout)
                        .thenApply(ListResourceClustersResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<GetResourceClusterResponse> create(
            ProvisionResourceClusterRequest request) {
        CompletionStage<GetResourceClusterResponse> response =
                ask(this.resourceClustersManagerActor, request, timeout)
                        .thenApply(GetResourceClusterResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<GetResourceClusterResponse> get(GetResourceClusterSpecRequest request) {
        CompletionStage<GetResourceClusterResponse> response =
                ask(this.resourceClustersManagerActor, request, timeout)
                        .thenApply(GetResourceClusterResponse.class::cast);
        return response;
    }

    @Override
    public CompletionStage<ScaleResourceResponse> scale(ScaleResourceRequest request) {
        CompletionStage<ScaleResourceResponse> response =
                ask(this.resourceClustersManagerActor, request, timeout)
                        .thenApply(ScaleResourceResponse.class::cast);
        return response;
    }
}
