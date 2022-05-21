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

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.mantisrx.common.Ack;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec.SkuTypeSpec;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This actor is responsible to handle message regarding cluster usage and makes scaling decisions.
 * [Notes] There can be two communication model between the scaler actor and resource cluster actor. If the state is
 * pushed from resource cluster actor to the scaler actor, the downside is we need to ensure all changes are properly
 * handled and can trigger the push, while pulling state from scaler actor requires explicit timer firing.
 */
@Slf4j
public class ResourceClusterScalerActor extends AbstractActorWithTimers {
    private final ClusterID clusterId;

    private final Duration scalerPullThreshold;

    private final Duration scalerTimerTimeout;

    private final ActorRef resourceClusterActor;
    private final ActorRef resourceClusterHostActor;
    private final ResourceClusterStorageProvider storageProvider;

    private Map<String, ResourceClusterScaleSpec> skuToRuleMap;

    private final Clock clock;
    private Instant lastPullActivity;
    private Instant lastActionActivity;

    // TODO: scaler ruleSet; coolDown tracker;

    public static Props props(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        Duration scalerTimerTimeout,
        ResourceClusterStorageProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor) {
        return Props.create(
            ResourceClusterScalerActor.class,
            clusterId,
            clock,
            scalerPullThreshold,
            scalerTimerTimeout,
            storageProvider,
            resourceClusterHostActor,
            resourceClusterActor);
    }

    public ResourceClusterScalerActor(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        Duration scalerTimerTimeout,
        ResourceClusterStorageProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor) {
        this.clusterId = clusterId;
        this.resourceClusterActor = resourceClusterActor;
        this.resourceClusterHostActor = resourceClusterHostActor;
        this.storageProvider = storageProvider;
        this.clock = clock;
        this.scalerPullThreshold = scalerPullThreshold;
        this.scalerTimerTimeout = scalerTimerTimeout;

        // TODO load ruleset from storage provider.
        init();
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(TriggerClusterUsageRequest.class, this::onTriggerClusterUsageRequest)
                .match(GetClusterUsageResponse.class, this::onGetClusterUsageResponse)
                .match(Ack.class, ack -> log.info("Received ack from {}", sender()))
                .build();
    }

    private void init() {
        this.storageProvider.getResourceClusterScaleRules(this.clusterId.toString())
                .thenAccept(rules -> {
                    this.skuToRuleMap = rules
                        .getScaleRules()
                        .stream()
                        .collect(Collectors.toMap(ResourceClusterScaleSpec::getSkuId, Function.identity()));
                });

        getTimers().startTimerWithFixedDelay(
            "ClusterScaler-" + this.clusterId,
            new TriggerClusterUsageRequest(this.clusterId),
            scalerPullThreshold);
    }

    private void onGetClusterUsageResponse(GetClusterUsageResponse usageResponse) {
        log.info("Getting cluster usage: {}", usageResponse);

        // TODO Check last activity time and skip if within coolDown
        // get usage by mDef
        // for each mdef: locate rule for the mdef, apply rule if under coolDown.
        // update coolDown timer.

        // 1 matcher for usage and rule.
        // 2 rule apply to usage.
        // 3 translate between decision to scale request. (inline for now)

        getSender().tell(Ack.class, self());
    }

    private void onTriggerClusterUsageRequest(TriggerClusterUsageRequest req) {
        log.trace("Requesting cluster usage: {}", this.clusterId);
        this.resourceClusterActor.tell(new GetClusterUsageRequest(this.clusterId), self());
    }

    private ScaleResourceRequest translateScaleDecision(ScaleDecision decision) {
        return ScaleResourceRequest.builder()
            .clusterId(this.clusterId.getResourceID())
            .skuId(decision.getSkuId())
            .desireSize(decision.getDesireSize())
            .build();
    }

    @Value
    @Builder
    static class TriggerClusterUsageRequest {
        // TODO: cluster id might not be needed if connecting to each resCluster actor directly.
        ClusterID clusterID;
    }



    // mapping: clusterId -> skuId -> (ASG level) -> scale rule;
    // work: add resource persistence in kvdal provider + file provider;
    // data format: "resclusterprefix-" + clusterId: List of rules

    static class MachineDefinitionToSkuMapper {
        Map<MachineDefinition, String> mDefToSkuMap;

        public MachineDefinitionToSkuMapper(MantisResourceClusterSpec clusterSpec) {
            this.mDefToSkuMap = new HashMap<>();

            clusterSpec.getSkuSpecs().forEach(skuSpec -> {
                this.mDefToSkuMap.put(skuToMachineDefinition(skuSpec), skuSpec.getSkuId());
            });
        }

        /**
         * Map given {@link MachineDefinition} to the cluster SKU id.
         * @param mDef Given Machine definition.
         * @return skuId as string. Empty Optional if not found.
         */
        public Optional<String> map(MachineDefinition mDef)
        {
            return Optional.ofNullable(this.mDefToSkuMap.getOrDefault(mDef, null));
        }

        private static MachineDefinition skuToMachineDefinition(SkuTypeSpec sku) {
            //TODO validate rounding error from TaskExecutor registration.
            return new MachineDefinition(
                sku.getCpuCoreCount(),
                sku.getMemorySizeInBytes(),
                sku.getNetworkMbps(),
                sku.getDiskSizeInBytes(),
                5 // num of ports is currently a hardcoded value from {@link TaskExecutor}.
            );
        }
    }

    enum ScaleAction {
        NoAction,
        ScaleUp,
        ScaleDown
    }

    static class ClusterAvailabilityRule {
        private final ActorRef actuatorRef;
        private final ResourceClusterScaleSpec scaleSpec;
        private final Clock clock;
        private Instant lastActionInstant;

        public ClusterAvailabilityRule(ActorRef actuatorRef, ResourceClusterScaleSpec scaleSpec, Clock clock) {
            this.actuatorRef = actuatorRef;
            this.scaleSpec = scaleSpec;
            this.clock = clock;

            this.lastActionInstant = Instant.MIN;
        }
        public Optional<ScaleDecision> apply(GetClusterUsageResponse.UsageByMachineDefinition usage) {
            // assume matched with mDef already.
            ScaleAction action = ScaleAction.NoAction;

            // Cool down check
            if (this.lastActionInstant.plusSeconds(this.scaleSpec.getCoolDownSecs()).compareTo(clock.instant()) > 0) {
                log.debug("Action under coolDown, skip: {}, {}", this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
            }

            if (usage.getIdleCount() > scaleSpec.getMaxIdleToKeep()) {
                // too many idle agents, scale down.
                action = ScaleAction.ScaleDown;
            }
            else if (usage.getIdleCount() < scaleSpec.getMinIdleToKeep()) {
                // scale up
                action = ScaleAction.ScaleUp;
            }

            Optional<ScaleDecision> decision = Optional.empty();
            switch (action) {
                case ScaleDown:
                    int newSize = Math.min(
                        usage.getTotalCount() - this.scaleSpec.getStepSize(), this.scaleSpec.getMinSize());
                    decision = Optional.of(ScaleDecision.builder()
                        .desireSize(usage.getTotalCount() - this.scaleSpec.getStepSize())
                        .maxSize(newSize)
                        .minSize(newSize)
                        .build());
                    break;

                case ScaleUp:
                    newSize = Math.max(
                        usage.getTotalCount() + this.scaleSpec.getStepSize(), this.scaleSpec.getMaxSize());
                    decision = Optional.of(ScaleDecision.builder()
                        .desireSize(usage.getTotalCount() - this.scaleSpec.getStepSize())
                        .maxSize(newSize)
                        .minSize(newSize)
                        .build());
                    break;

                case NoAction:
                    break;
            }

            log.info("Scale Decision for {}-{}: {}",
                this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId(), decision.get());
            return decision;
        }
    }

    @Value
    @Builder
    static class ScaleDecision {
        String skuId;
        String clusterId;
        int maxSize;
        int minSize;
        int desireSize;
    }
}
