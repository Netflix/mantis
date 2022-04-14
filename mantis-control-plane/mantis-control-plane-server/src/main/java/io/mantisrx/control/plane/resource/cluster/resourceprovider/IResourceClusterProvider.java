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

package io.mantisrx.control.plane.resource.cluster.resourceprovider;

import io.mantisrx.control.plane.resource.cluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ResourceClusterProvisionSubmissiomResponse;
import io.mantisrx.control.plane.resource.cluster.proto.ScaleResourceRequest;
import io.mantisrx.control.plane.resource.cluster.proto.ScaleResourceResponse;
import java.util.concurrent.CompletionStage;

/**
 * This interface provides the API to connect resource cluster management actor to actual
 * implmentations of different resource cluster clients e.g k8s.
 */
public interface IResourceClusterProvider {
    /**
    * Provision a new resource cluster using the given spec. This operation should be idempotent.
    * The returned CompletionStage instance is to indicate whether the provision has been
    * accepted/started and doesn't need to represent the whole provisioning completion(s) of
    * every nodes in the cluster.
    */
    CompletionStage<ResourceClusterProvisionSubmissiomResponse> provisionClusterIfNotPresent(
            ProvisionResourceClusterRequest clusterSpec);

    /**
    * Request to scale an existing resource cluster using the given spec. This operation should be idempotent.
    * The returned CompletionStage instance is to indicate whether the operation has been
    * accepted/started and doesn't need to represent the full completion(s) of
    * every nodes.
    */
    CompletionStage<ScaleResourceResponse> scaleResource(ScaleResourceRequest scaleRequest);

    IResourceClusterResponseHandler getResponseHandler();
}
