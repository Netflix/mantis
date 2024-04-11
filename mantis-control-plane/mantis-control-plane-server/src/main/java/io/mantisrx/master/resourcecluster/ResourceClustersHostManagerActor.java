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
import io.mantisrx.master.resourcecluster.proto.UpgradeClusterContainersResponse;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProvider;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProviderUpgradeRequest;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable.ResourceClusterScaleRulesWritableBuilder;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.shaded.com.google.common.base.Strings;
import java.io.IOException;
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

    public static Props props(
            final ResourceClusterProvider resourceClusterProvider,
            final IMantisPersistenceProvider persistenceProvider) {
        // TODO(andyz): investigate atlas metered-mailbox.
        return Props.create(ResourceClustersHostManagerActor.class, resourceClusterProvider, persistenceProvider);
    }

    private final ResourceClusterProvider resourceClusterProvider;
    private final IMantisPersistenceProvider resourceClusterStorageProvider;

    public ResourceClustersHostManagerActor(
            final ResourceClusterProvider resourceClusterProvider,
            final IMantisPersistenceProvider resourceStorageProvider) {
        this.resourceClusterProvider = resourceClusterProvider;
        this.resourceClusterStorageProvider = resourceStorageProvider;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
            .match(ProvisionResourceClusterRequest.class, this::onProvisionResourceClusterRequest)
            .match(ListResourceClusterRequest.class, this::onListResourceClusterRequest)
            .match(GetResourceClusterSpecRequest.class, this::onGetResourceClusterSpecRequest)
            .match(DeleteResourceClusterRequest.class, this::onDeleteResourceCluster)

            // Scale rule section
            .match(CreateAllResourceClusterScaleRulesRequest.class, this::onCreateAllResourceClusterScaleRulesRequest)
            .match(CreateResourceClusterScaleRuleRequest.class, this::onCreateResourceClusterScaleRuleRequest)
            .match(GetResourceClusterScaleRulesRequest.class, this::onGetResourceClusterScaleRulesRequest)
            .match(ResourceClusterProvisionSubmissionResponse.class, this::onResourceClusterProvisionResponse)
            .match(ScaleResourceRequest.class, this::onScaleResourceClusterRequest)

            // Upgrade section
            .match(UpgradeClusterContainersRequest.class, this::onUpgradeClusterContainersRequest)
            .match(UpgradeClusterContainersResponse.class, this::onUpgradeClusterContainersResponse)

            .build();
    }

    private void onCreateResourceClusterScaleRuleRequest(CreateResourceClusterScaleRuleRequest req) {
        try {
            ResourceClusterScaleSpec ruleSpec = ResourceClusterScaleSpec.builder()
                .maxSize(req.getRule().getMaxSize())
                .minSize(req.getRule().getMinSize())
                .minIdleToKeep(req.getRule().getMinIdleToKeep())
                .maxIdleToKeep(req.getRule().getMaxIdleToKeep())
                .coolDownSecs(req.getRule().getCoolDownSecs())
                .skuId(req.getRule().getSkuId())
                .clusterId(req.getRule().getClusterId())
                .build();

            getSender().tell(toGetResourceClusterScaleRulesResponse(resourceClusterStorageProvider.registerResourceClusterScaleRule(ruleSpec)), getSelf());
        } catch (Exception err) {
            log.error("Error from registerResourceClusterScaleRule: {}", req, err);
            GetResourceClusterScaleRulesResponse response =
                GetResourceClusterScaleRulesResponse
                    .builder()
                    .message(err.getMessage())
                    .responseCode(ResponseCode.SERVER_ERROR)
                    .build();
            getSender().tell(response, getSelf());
        }
    }

    private void onGetResourceClusterScaleRulesRequest(GetResourceClusterScaleRulesRequest req) {
        try {
            final GetResourceClusterScaleRulesResponse response =
                toGetResourceClusterScaleRulesResponse(
                    resourceClusterStorageProvider.getResourceClusterScaleRules(req.getClusterId()));
            getSender().tell(response, getSelf());
        } catch (IOException e) {
            log.error("Error from getResourceClusterScaleRules: {}", req, e);
            GetResourceClusterScaleRulesResponse errorResponse =
                GetResourceClusterScaleRulesResponse
                    .builder()
                    .message(e.getMessage())
                    .responseCode(ResponseCode.SERVER_ERROR)
                    .build();
            getSender().tell(errorResponse, getSelf());
        }
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

        GetResourceClusterScaleRulesResponse response;
        try {
             response =
                 toGetResourceClusterScaleRulesResponse(
                     resourceClusterStorageProvider.registerResourceClusterScaleRule(rulesBuilder.build()));
        } catch (IOException e) {
            log.error("Error from registerResourceClusterScaleRule: {}", req, e);
            response =
                GetResourceClusterScaleRulesResponse.builder()
                    .message(e.getMessage())
                    .responseCode(ResponseCode.SERVER_ERROR)
                    .build();
        }
        sender().tell(response, self());
    }

    private static ResourceClusterScaleRuleProto.GetResourceClusterScaleRulesResponse toGetResourceClusterScaleRulesResponse(
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

    private void onResourceClusterProvisionResponse(ResourceClusterProvisionSubmissionResponse resp) {
        this.resourceClusterProvider.getResponseHandler().handleProvisionResponse(resp);
    }

    private void onUpgradeClusterContainersResponse(UpgradeClusterContainersResponse resp) {
        if (resp.responseCode.getValue() >= 300) {
            log.error("Unexpected error response from upgradeClusterContainers: {}", resp);
        } else {
            log.info("Success response from upgradeClusterContainers request: {}", resp);
        }
    }

    private void onDeleteResourceCluster(DeleteResourceClusterRequest req) {
        /*
          Proper cluster deletion requires handling various cleanups e.g.:
          * Migrate existing jobs.
          * Un-provision cluster resources (nodes, network, storage e.g.).
          * Update internal tracking state and persistent data.
          For now this API will only serve the persistence layer update.
         */

        try {
            this.resourceClusterStorageProvider.deregisterCluster(req.getClusterId());
            DeleteResourceClusterResponse response =
                DeleteResourceClusterResponse
                    .builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .build();
            getSender().tell(response, getSelf());
        } catch (IOException err) {
            DeleteResourceClusterResponse response =
                DeleteResourceClusterResponse
                    .builder()
                    .message(err.getMessage())
                    .responseCode(ResponseCode.SERVER_ERROR)
                    .build();
            getSender().tell(response, getSelf());
        }
    }

    private void onListResourceClusterRequest(ListResourceClusterRequest req) {
        try {
            RegisteredResourceClustersWritable clustersW =
                this.resourceClusterStorageProvider.getRegisteredResourceClustersWritable();
            ListResourceClustersResponse response =
                ListResourceClustersResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .registeredResourceClusters(clustersW.getClusters().entrySet().stream().map(
                            kv -> ListResourceClustersResponse.RegisteredResourceCluster.builder()
                                .id(kv.getValue().getClusterId())
                                .version(kv.getValue().getVersion())
                                .build())
                        .collect(Collectors.toList()))
                    .build();
            getSender().tell(response, getSelf());
        } catch (IOException err) {
            ListResourceClustersResponse response =
                ListResourceClustersResponse
                    .builder()
                    .message(err.getMessage())
                    .responseCode(ResponseCode.SERVER_ERROR)
                    .build();
            getSender().tell(response, getSelf());
        }
    }

    private void onGetResourceClusterSpecRequest(GetResourceClusterSpecRequest req) {
        try {
            ResourceClusterSpecWritable specW =
                this.resourceClusterStorageProvider.getResourceClusterSpecWritable(req.getId());
            final GetResourceClusterResponse response;
            if (specW == null) {
                response = GetResourceClusterResponse.builder()
                    .responseCode(ResponseCode.CLIENT_ERROR_NOT_FOUND)
                    .build();
            } else {
                response = GetResourceClusterResponse.builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterSpec(specW.getClusterSpec())
                    .build();
            }
            getSender().tell(response, getSelf());
        } catch (IOException err) {
            GetResourceClusterResponse response = GetResourceClusterResponse.builder()
                .responseCode(ResponseCode.SERVER_ERROR)
                .message(err.getMessage())
                .build();
            getSender().tell(response, getSelf());
        }
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
        Optional<String> validationResultO = validateClusterSpec(req);
        if (validationResultO.isPresent()) {
            pipe(
                CompletableFuture.completedFuture(GetResourceClusterResponse.builder()
                    .responseCode(ResponseCode.CLIENT_ERROR)
                    .message(validationResultO.get())
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
        GetResourceClusterResponse response;
        try {
            ResourceClusterSpecWritable specW = this.resourceClusterStorageProvider.registerAndUpdateClusterSpec(specWritable);
            response =
                GetResourceClusterResponse
                    .builder()
                    .responseCode(ResponseCode.SUCCESS)
                    .clusterSpec(specW.getClusterSpec())
                    .build();
        } catch (IOException err) {
            response = GetResourceClusterResponse.builder()
                .responseCode(ResponseCode.SERVER_ERROR)
                .message(err.getMessage())
                .build();
        }
        getSender().tell(response, getSelf());

        if (response.responseCode.equals(ResponseCode.SUCCESS)) {
            // Provision response is directed back to this actor to handle its submission result.
            CompletionStage<ResourceClusterProvisionSubmissionResponse> provisionFut =
                this.resourceClusterProvider
                    .provisionClusterIfNotPresent(req)
                    .exceptionally(err -> ResourceClusterProvisionSubmissionResponse.builder().error(err).build());

            pipe(provisionFut, getContext().getDispatcher()).to(getSelf());
        }
    }

    private void onScaleResourceClusterRequest(ScaleResourceRequest req) {
        log.info("Entering onScaleResourceClusterRequest: " + req);
        // [Notes] for scaling-up the request can go straight into provider to increase desire size.
        // FOr scaling-down the decision requires getting idle hosts first.

        pipe(this.resourceClusterProvider.scaleResource(req), getContext().dispatcher()).to(getSender());
    }

    private void onUpgradeClusterContainersRequest(UpgradeClusterContainersRequest req) {
        log.info("Entering onUpgradeClusterContainersRequest: {}", req);
        // [Notes] for scaling-up the request can go straight into provider to increase desire size.
        // For scaling-down the decision requires getting idle hosts first.
        // if enableSkuSpecUpgrade is true, first fetch the latest spec to override the sku spec during upgrade
        // workflow.

        CompletionStage<UpgradeClusterContainersResponse> upgradeFut;
        if (req.isEnableSkuSpecUpgrade()) {
            try {
                ResourceClusterSpecWritable specW = this.resourceClusterStorageProvider.getResourceClusterSpecWritable(req.getClusterId());
                if (specW == null) {
                    upgradeFut =
                        CompletableFuture.completedFuture(UpgradeClusterContainersResponse
                            .builder()
                            .responseCode(ResponseCode.CLIENT_ERROR_NOT_FOUND)
                            .build());
                } else {
                    ResourceClusterProviderUpgradeRequest enrichedReq =
                        ResourceClusterProviderUpgradeRequest.from(req, specW.getClusterSpec());
                    upgradeFut = this.resourceClusterProvider.upgradeContainerResource(enrichedReq);
                }
            } catch (IOException err) {
                upgradeFut = CompletableFuture.completedFuture(
                    UpgradeClusterContainersResponse
                        .builder()
                        .responseCode(ResponseCode.SERVER_ERROR)
                        .message(err.getMessage())
                        .build());
            }
        }
        else {
            log.info("Upgrading cluster image only: {}", req.getClusterId());
            upgradeFut =
                this.resourceClusterProvider.upgradeContainerResource(ResourceClusterProviderUpgradeRequest.from(req));
        }

        pipe(upgradeFut, getContext().getDispatcher()).to(getSelf());

        getSender().tell(
            UpgradeClusterContainersResponse
                .builder()
                .responseCode(ResponseCode.SUCCESS)
                .message("Upgrade request submitted")
                .clusterId(req.getClusterId())
                .optionalSkuId(req.getOptionalSkuId())
                .optionalEnvType(req.getOptionalEnvType())
                .region(req.getRegion())
                .build(),
            getSelf());
    }

    private static Optional<String> validateClusterSpec(ProvisionResourceClusterRequest req) {
        if (req.getClusterSpec() == null) {
            log.error("Empty request without cluster spec: {}", req.getClusterId());
            return Optional.of("cluster spec cannot be null");
        }

        if (!req.getClusterId().equals(req.getClusterSpec().getId())) {
            log.error("Mismatch cluster id: {}, {}", req.getClusterId(), req.getClusterSpec().getId());
            return Optional.of("cluster spec id doesn't match cluster id");
        }

        Optional<SkuTypeSpec> invalidSku = req.getClusterSpec().getSkuSpecs().stream().filter(sku ->
            sku.getSkuId() == null || sku.getCapacity() == null || sku.getCpuCoreCount() < 1 ||
                sku.getDiskSizeInMB() < 1 || sku.getMemorySizeInMB() < 1 || sku.getNetworkMbps() < 1 ||
                Strings.isNullOrEmpty(sku.getImageId()))
            .findAny();

        if (invalidSku.isPresent()) {
            log.error("Invalid request for cluster spec: {}, {}", req.getClusterId(), invalidSku.get());
            return Optional.of("Invalid sku definition");
        }

        Optional<SkuTypeSpec> invalidSkuNameSpec = req.getClusterSpec().getSkuSpecs().stream().filter(sku ->
                Character.isDigit(sku.getSkuId().getResourceID().charAt(sku.getSkuId().getResourceID().length() - 1)))
            .findAny();

        if (invalidSkuNameSpec.isPresent()) {
            log.error("Invalid request for cluster spec sku id (cannot end with number): {}, {}", req.getClusterId(),
                invalidSkuNameSpec.get());
            return Optional.of("Invalid skuID (cannot end with number): " + invalidSkuNameSpec.get().getSkuId());
        }

        return Optional.empty();
    }
}
