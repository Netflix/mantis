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
package io.mantisrx.master.resourcecluster;

import static akka.pattern.Patterns.pipe;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode;
import io.mantisrx.master.resourcecluster.proto.GetResourceClusterSpecRequest;
import io.mantisrx.master.resourcecluster.proto.ListResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec.SkuTypeSpec;
import io.mantisrx.master.resourcecluster.proto.ProvisionResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.DeleteResourceClusterRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.DeleteResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.GetResourceClusterResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterAPIProto.ListResourceClustersResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterProvisionSubmissionResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateAllResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.CreateResourceClusterScaleRuleRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesRequest;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesResponse;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersRequest;
import io.mantisrx.master.resourcecluster.resourceprovider.InMemoryOnlyResourceClusterStorageProvider;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProvider;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable.ResourceClusterScaleRulesWritableBuilder;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * This actor is responsible to translate requests for resource cluster related operations from API server and other
 * actors to binded resource cluster provider implementation.
 */
@Slf4j
public class ResourceClustersHostManagerActor extends AbstractActorWithTimers {

    @VisibleForTesting
    static Props props(
            final ResourceClusterProvider resourceClusterProvider) {
        return Props.create(
                ResourceClustersHostManagerActor.class,
                resourceClusterProvider,
                new InMemoryOnlyResourceClusterStorageProvider());
    }

    public static Props props(
            final ResourceClusterProvider resourceClusterProvider,
            final ResourceClusterStorageProvider resourceStorageProvider) {
        // TODO(andyz): investigate atlas metered-mailbox.
        return Props.create(ResourceClustersHostManagerActor.class, resourceClusterProvider, resourceStorageProvider);
    }

    private final ResourceClusterProvider resourceClusterProvider;
    private final ResourceClusterStorageProvider resourceClusterStorageProvider;

    public ResourceClustersHostManagerActor(
            final ResourceClusterProvider resourceClusterProvider,
            final ResourceClusterStorageProvider resourceStorageProvider) {
        this.resourceClusterProvider = resourceClusterProvider;
        this.resourceClusterStorageProvider = resourceStorageProvider;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
            .match(ProvisionResourceClusterRequest.class, this::onProvisionResourceClusterRequest)
            .match(ListResourceClusterRequest.class, this::onListResourceClusterRequest)
            .match(GetResourceClusterSpecRequest.class, this::onGetResourceClusterSpecRequest)
            .match(ResourceClusterProvisionSubmissionResponse.class, this::onResourceClusterProvisionResponse)
            .match(DeleteResourceClusterRequest.class, this::onDeleteResourceCluster)

            // Scale rule section
            .match(CreateAllResourceClusterScaleRulesRequest.class, this::onCreateAllResourceClusterScaleRulesRequest)
            .match(CreateResourceClusterScaleRuleRequest.class, this::onCreateResourceClusterScaleRuleRequest)
            .match(GetResourceClusterScaleRulesRequest.class, this::onGetResourceClusterScaleRulesRequest)
            .match(ScaleResourceRequest.class, this::onScaleResourceClusterRequest)

            // Upgrade section
            .match(UpgradeClusterContainersRequest.class, this::onUpgradeClusterContainersRequest)

            .build();
    }

    private void onCreateResourceClusterScaleRuleRequest(CreateResourceClusterScaleRuleRequest req) {
        ResourceClusterScaleSpec ruleSpec = ResourceClusterScaleSpec.builder()
            .maxSize(req.getRule().getMaxSize())
            .minSize(req.getRule().getMinSize())
            .minIdleToKeep(req.getRule().getMinIdleToKeep())
            .maxIdleToKeep(req.getRule().getMaxIdleToKeep())
            .coolDownSecs(req.getRule().getCoolDownSecs())
            .skuId(req.getRule().getSkuId())
            .clusterId(req.getRule().getClusterId())
            .build();

        pipe(this.resourceClusterStorageProvider.registerResourceClusterScaleRule(ruleSpec)
                .thenApply(this::toGetResourceClusterScaleRulesResponse)
                .exceptionally(err -> {
                    log.error("Error from registerResourceClusterScaleRule: {}, {}", err.getMessage(), req);
                    return GetResourceClusterScaleRulesResponse.builder()
                        .message(err.getMessage())
                        .responseCode(ResponseCode.SERVER_ERROR).build();
                }),
            getContext().dispatcher())
            .to(getSender());
    }

    private void onGetResourceClusterScaleRulesRequest(GetResourceClusterScaleRulesRequest req) {
        pipe(this.resourceClusterStorageProvider.getResourceClusterScaleRules(req.getClusterId())
                .thenApply(this::toGetResourceClusterScaleRulesResponse)
                .exceptionally(err -> {
                    log.error("Error from getResourceClusterScaleRules: {}, {}", err, req);
                    return GetResourceClusterScaleRulesResponse.builder()
                        .message(err.getMessage())
                        .responseCode(ResponseCode.SERVER_ERROR).build();
                }),
            getContext().dispatcher())
            .to(getSender());
    }

    private void onCreateAllResourceClusterScaleRulesRequest(CreateAllResourceClusterScaleRulesRequest req) {
        ResourceClusterScaleRulesWritableBuilder rulesBuilder = ResourceClusterScaleRulesWritable.builder()
            .clusterId(req.getClusterId());
        req.getRules().forEach(r -> rulesBuilder.scaleRule(
            r.getSkuId().getResourceID(),
            ResourceClusterScaleSpec.builder()
                .maxSize(r.getMaxSize())
                .minSize(r.getMinSize())
                .minIdleToKeep(r.getMinIdleToKeep())
                .maxIdleToKeep(r.getMaxIdleToKeep())
                .coolDownSecs(r.getCoolDownSecs())
                .skuId(r.getSkuId())
                .clusterId(r.getClusterId())
                .build()));

        pipe(this.resourceClusterStorageProvider.registerResourceClusterScaleRule(rulesBuilder.build())
            .thenApply(this::toGetResourceClusterScaleRulesResponse)
                .exceptionally(err -> {
                    log.error("Error from registerResourceClusterScaleRule: {}, {}", err, req);
                    return GetResourceClusterScaleRulesResponse.builder()
                        .message(err.getMessage())
                        .responseCode(ResponseCode.SERVER_ERROR).build();
                }),
            getContext().dispatcher())
            .to(getSender());
    }

    private ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesResponse toGetResourceClusterScaleRulesResponse(
        ResourceClusterScaleRulesWritable rules) {
        return GetResourceClusterScaleRulesResponse.builder()
            .responseCode(ResponseCode.SUCCESS)
            .clusterId(rules.getClusterId())
            .rules(rules.getScaleRules().entrySet().stream().map(kv ->
                ResourceClusterScaleRuleProto.ResourceClusterScaleRule.builder()
                    .clusterId(kv.getValue().getClusterId())
                    .coolDownSecs(kv.getValue().getCoolDownSecs())
                    .maxIdleToKeep(kv.getValue().getMaxIdleToKeep())
                    .minIdleToKeep(kv.getValue().getMinIdleToKeep())
                    .maxSize(kv.getValue().getMaxSize())
                    .minSize(kv.getValue().getMinSize())
                    .skuId(kv.getValue().getSkuId())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private void onDeleteResourceCluster(DeleteResourceClusterRequest req) {
        /**
         * Proper cluster deletion requires handling various cleanups e.g.:
         * * Migrate existing jobs.
         * * Un-provision cluster resources (nodes, network, storage e.g.).
         * * Update internal tracking state and persistent data.
         * For now this API will only serve the persistence layer update.
         */

        pipe(this.resourceClusterStorageProvider.deregisterCluster(req.getClusterId())
                .thenApply(clustersW ->
                    DeleteResourceClusterResponse.builder()
                        .responseCode(ResponseCode.SUCCESS)
                        .build())
                .exceptionally(err ->
                    DeleteResourceClusterResponse.builder()
                        .message(err.getMessage())
                        .responseCode(ResponseCode.SERVER_ERROR).build()),
            getContext().dispatcher())
            .to(getSender());
    }

    private void onResourceClusterProvisionResponse(ResourceClusterProvisionSubmissionResponse resp) {
        this.resourceClusterProvider.getResponseHandler().handleProvisionResponse(resp);
    }

    private void onListResourceClusterRequest(ListResourceClusterRequest req) {
        pipe(this.resourceClusterStorageProvider.getRegisteredResourceClustersWritable()
                .thenApply(clustersW ->
                        ListResourceClustersResponse.builder()
                                .responseCode(ResponseCode.SUCCESS)
                                .registeredResourceClusters(clustersW.getClusters().entrySet().stream().map(
                                        kv -> ListResourceClustersResponse.RegisteredResourceCluster.builder()
                                                .id(kv.getValue().getClusterId())
                                                .version(kv.getValue().getVersion())
                                                .build())
                                        .collect(Collectors.toList()))
                                .build()
                ).exceptionally(err ->
                                ListResourceClustersResponse.builder()
                                        .message(err.getMessage())
                                        .responseCode(ResponseCode.SERVER_ERROR).build()),
                getContext().dispatcher())
                .to(getSender());
    }

    private void onGetResourceClusterSpecRequest(GetResourceClusterSpecRequest req) {
        pipe(this.resourceClusterStorageProvider.getResourceClusterSpecWritable(req.getId())
                        .thenApply(specW -> {
                            if (specW == null) {
                                return GetResourceClusterResponse.builder()
                                        .responseCode(ResponseCode.CLIENT_ERROR_NOT_FOUND)
                                        .build();
                            }
                            return GetResourceClusterResponse.builder()
                                    .responseCode(ResponseCode.SUCCESS)
                                    .clusterSpec(specW.getClusterSpec())
                                    .build();
                        })
                        .exceptionally(err ->
                                GetResourceClusterResponse.builder()
                                        .responseCode(ResponseCode.SERVER_ERROR)
                                        .message(err.getMessage())
                                        .build()),
                getContext().dispatcher())
                .to(getSender());
    }

    private void onProvisionResourceClusterRequest(ProvisionResourceClusterRequest req) {
        /*
        For a provision request, the following steps will be taken:
        1. Persist the cluster request with spec to the resource storage provider.
        2. Once persisted, reply to sender (e.g. http server route) to confirm the accepted request.
        3. Queue the long-running provision task via resource cluster provider and register callback to self.
        4. Handle provision callback and error handling.
            (only logging for now as agent registration will happen directly inside agent).
         */
        log.info("Entering onProvisionResourceClusterRequest: " + req);

        // For now only full spec is supported during provision stage.
        if (!validateClusterSpec(req)) {
            pipe(
                CompletableFuture.completedFuture(GetResourceClusterResponse.builder()
                    .responseCode(ResponseCode.CLIENT_ERROR)
                    .message("No cluster spec found in provision request.")
                    .build()),
                getContext().dispatcher())
                .to(getSender());
            log.info("Invalid cluster spec, return client error. Req: {}", req.getClusterId());
            log.debug("Full invalid Req: {}", req);
            return;
        }

        ResourceClusterSpecWritable specWritable = ResourceClusterSpecWritable.builder()
                .clusterSpec(req.getClusterSpec())
                .version("")
                .id(req.getClusterId())
                .build();

        // Cluster spec is returned for API request.
        CompletionStage<GetResourceClusterResponse> updateSpecToStoreFut =
                this.resourceClusterStorageProvider.registerAndUpdateClusterSpec(specWritable)
                        .thenApply(specW -> GetResourceClusterResponse.builder()
                                        .responseCode(ResponseCode.SUCCESS)
                                        .clusterSpec(specW.getClusterSpec())
                                        .build())
                        .exceptionally(err ->
                                GetResourceClusterResponse.builder()
                                .responseCode(ResponseCode.SERVER_ERROR)
                                .message(err.getMessage())
                                .build());
        pipe(updateSpecToStoreFut, getContext().dispatcher()).to(getSender());
        log.debug("[Pipe finish] storing cluster spec.");

        // Provision response is directed back to this actor to handle its submission result.
        CompletionStage<ResourceClusterProvisionSubmissionResponse> provisionFut =
                updateSpecToStoreFut
                        .thenCompose(resp -> {
                            if (resp.responseCode.equals(ResponseCode.SUCCESS)) {
                                return this.resourceClusterProvider.provisionClusterIfNotPresent(req);
                            }
                            return CompletableFuture.completedFuture(
                                ResourceClusterProvisionSubmissionResponse.builder().response(resp.message).build());
                        })
                        .exceptionally(err -> ResourceClusterProvisionSubmissionResponse.builder().error(err).build());
        pipe(provisionFut, getContext().dispatcher()).to(getSelf());
        log.debug("[Pipe finish 2]: returned provision fut.");
    }

    private void onScaleResourceClusterRequest(ScaleResourceRequest req) {
        log.info("Entering onScaleResourceClusterRequest: " + req);
        // [Notes] for scaling-up the request can go straight into provider to increase desire size.
        // FOr scaling-down the decision requires getting idle hosts first.

        pipe(this.resourceClusterProvider.scaleResource(req), getContext().dispatcher()).to(getSender());
    }

    private void onUpgradeClusterContainersRequest(UpgradeClusterContainersRequest req) {
        log.info("Entering onScaleResourceClusterRequest: " + req);
        // [Notes] for scaling-up the request can go straight into provider to increase desire size.
        // FOr scaling-down the decision requires getting idle hosts first.

        pipe(this.resourceClusterProvider.upgradeContainerResource(req), getContext().dispatcher()).to(getSender());

    }

    private static boolean validateClusterSpec(ProvisionResourceClusterRequest req) {
        if (req.getClusterSpec() == null) {
            log.info("Empty request without cluster spec: {}", req.getClusterId());
            return false;
        }

        if (!req.getClusterId().equals(req.getClusterSpec().getId())) {
            log.info("Mismatch cluster id: {}, {}", req.getClusterId(), req.getClusterSpec().getId());
            return false;
        }

        Optional<SkuTypeSpec> invalidSku = req.getClusterSpec().getSkuSpecs().stream().filter(sku ->
            sku.getSkuId() == null || sku.getCapacity() == null || sku.getCpuCoreCount() < 1 ||
                sku.getDiskSizeInMB() < 1 || sku.getMemorySizeInMB() < 1 || sku.getNetworkMbps() < 1 ||
                Strings.isNullOrEmpty(sku.getImageId())).findAny();

        if (invalidSku.isPresent()) {
            log.info("Empty request without cluster spec: {}, {}", req.getClusterId(), invalidSku.get());
            return false;
        }

        return true;
    }
}
