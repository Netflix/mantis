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
import java.util.concurrent.CompletionStage;

/**
 * This interface provides the API to connect resource cluster management actor to actual
 * implementations of different resource cluster clients e.g. k8s.
 *
 * <p>
 *     To implement and integrate this interface, the {@link ResourceClusterProviderAdapter} is used to wire actual
 *     implementation to the main entrypoint and currently this adapter requires an {@link akka.actor.ActorSystem} to be
 *     passed into the constructor.
 * </p>
 */
public interface ResourceClusterProvider {
    /**
    * Provision a new resource cluster using the given spec. This operation should be idempotent.
    * The returned CompletionStage instance is to indicate whether the provision has been
    * accepted/started and doesn't need to represent the whole provisioning completion(s) of
    * every nodes in the cluster.
    */
    CompletionStage<ResourceClusterProvisionSubmissionResponse> provisionClusterIfNotPresent(
            ProvisionResourceClusterRequest clusterSpec);

    /**
    * Request to scale an existing resource cluster using the given spec. This operation should be idempotent.
    * The returned CompletionStage instance is to indicate whether the operation has been
    * accepted/started and doesn't need to represent the full completion(s) of
    * every nodes.
    */
    CompletionStage<ScaleResourceResponse> scaleResource(ScaleResourceRequest scaleRequest);

    /**
     * To upgrade cluster containers: each container running task executor is using docker image tag based image version.
     * In regular case the upgrade is to refresh the container to re-deploy with latest digest associated with the image
     * tag (e.g. latest).
     * If multiple image digest versions need to be ran/hosted at the same time, it is recommended to create a separate
     * sku id in addition to the existing sku(s).
     */
    CompletionStage<UpgradeClusterContainersResponse> upgradeContainerResource(ResourceClusterProviderUpgradeRequest request);

    ResourceClusterResponseHandler getResponseHandler();
}
