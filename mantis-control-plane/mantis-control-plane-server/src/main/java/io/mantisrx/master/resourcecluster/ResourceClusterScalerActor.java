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
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.Ack;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse.UsageByGroupKey;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.proto.SetResourceClusterScalerStatusRequest;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    // Timer threshold of pulling cluster usage.
    private final Duration scalerPullThreshold;

    // Timer threshold of refreshing cluster scale rules from storage provider.
    private final Duration ruleSetRefreshThreshold;

    private final ActorRef resourceClusterActor;
    private final ActorRef resourceClusterHostActor;
    private final IMantisPersistenceProvider storageProvider;

    private final ConcurrentMap<ContainerSkuID, ClusterAvailabilityRule> skuToRuleMap = new ConcurrentHashMap<>();

    private final Clock clock;

    private final Counter numScaleUp;
    private final Counter numScaleDown;
    private final Counter numReachScaleMaxLimit;
    private final Counter numReachScaleMinLimit;
    private final Counter numScaleRuleTrigger;

    public static Props props(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        Duration ruleRefreshThreshold,
        IMantisPersistenceProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor) {
        return Props.create(
            ResourceClusterScalerActor.class,
            clusterId,
            clock,
            scalerPullThreshold,
            ruleRefreshThreshold,
            storageProvider,
            resourceClusterHostActor,
            resourceClusterActor);
    }

    public ResourceClusterScalerActor(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        Duration ruleRefreshThreshold,
        IMantisPersistenceProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor) {
        this.clusterId = clusterId;
        this.resourceClusterActor = resourceClusterActor;
        this.resourceClusterHostActor = resourceClusterHostActor;
        this.storageProvider = storageProvider;
        this.clock = clock;
        this.scalerPullThreshold = scalerPullThreshold;
        this.ruleSetRefreshThreshold = ruleRefreshThreshold;

        MetricGroupId metricGroupId = new MetricGroupId(
            "ResourceClusterScalerActor",
            new BasicTag("resourceCluster", this.clusterId.getResourceID()));

        Metrics m = new Metrics.Builder()
            .id(metricGroupId)
            .addCounter("numScaleDown")
            .addCounter("numReachScaleMaxLimit")
            .addCounter("numScaleUp")
            .addCounter("numReachScaleMinLimit")
            .addCounter("numScaleRuleTrigger")
            .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        this.numScaleDown = m.getCounter("numScaleDown");
        this.numReachScaleMaxLimit = m.getCounter("numReachScaleMaxLimit");
        this.numScaleUp = m.getCounter("numScaleUp");
        this.numReachScaleMinLimit = m.getCounter("numReachScaleMinLimit");
        this.numScaleRuleTrigger = m.getCounter("numScaleRuleTrigger");
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(TriggerClusterUsageRequest.class, this::onTriggerClusterUsageRequest)
                .match(TriggerClusterRuleRefreshRequest.class, this::onTriggerClusterRuleRefreshRequest)
                .match(QueueClusterRuleRefreshRequest.class, this::onQueueClusterRuleRefreshRequest)
                .match(GetRuleSetRequest.class,
                    req -> getSender().tell(
                        GetRuleSetResponse.builder().rules(ImmutableMap.copyOf(this.skuToRuleMap)).build(), self()))
                .match(GetClusterUsageResponse.class, this::onGetClusterUsageResponse)
                .match(GetClusterIdleInstancesResponse.class, this::onGetClusterIdleInstancesResponse)
                .match(GetRuleSetResponse.class,
                    s -> log.debug("[{}] Refreshed rule size: {}", s.getClusterID(), s.getRules().size()))
                .match(SetResourceClusterScalerStatusRequest.class, req -> {
                    onSetScalerStatus(req);
                    getSender().tell(Ack.getInstance(), self());
                })
                .match(ExpireSetScalerStatusRequest.class, this::onExpireSetScalerStatus)
                .match(Ack.class, ack -> log.debug("Received ack from {}", sender()))
                .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("ResourceClusterScaler Actor {} starting", this.clusterId);
        this.fetchRuleSet();

        getTimers().startTimerWithFixedDelay(
            "ClusterScaler-" + this.clusterId,
            new TriggerClusterUsageRequest(this.clusterId),
            scalerPullThreshold);

        getTimers().startTimerWithFixedDelay(
            "ClusterScalerRuleFetcher-" + this.clusterId,
            new TriggerClusterRuleRefreshRequest(this.clusterId),
            this.ruleSetRefreshThreshold);
    }

    private void onGetClusterUsageResponse(GetClusterUsageResponse usageResponse) {
        log.info("Getting cluster usage: {}", usageResponse);
        this.numScaleRuleTrigger.increment();

        // get usage by mDef
        // for each mdef: locate rule for the mdef, apply rule if under coolDown.
        // update coolDown timer.

        // 1 matcher for usage and rule.
        // 2 rule apply to usage.
        // 3 translate between decision to scale request. (inline for now)

        usageResponse.getUsages().forEach(usage -> {
            ContainerSkuID skuId = ContainerSkuID.of(usage.getUsageGroupKey());

            if (this.skuToRuleMap.containsKey(skuId) && skuToRuleMap.get(skuId).isEnabled()) {
                Optional<ScaleDecision> decisionO = this.skuToRuleMap.get(skuId).apply(usage);
                if (decisionO.isPresent()) {
                    log.info("Informing scale decision: {}", decisionO.get());
                    switch (decisionO.get().getType()) {
                        case ScaleDown:
                            log.info("Scaling down, fetching idle instances: {}.", decisionO.get());
                            this.numScaleDown.increment();
                            this.resourceClusterActor.tell(
                                GetClusterIdleInstancesRequest.builder()
                                    .clusterID(this.clusterId)
                                    .skuId(skuId)
                                    .desireSize(decisionO.get().getDesireSize())
                                    .maxInstanceCount(
                                        Math.max(0, usage.getTotalCount() - decisionO.get().getDesireSize()))
                                    .build(),
                                self());
                            break;
                        case ScaleUp:
                            log.info("Scaling up, informing host actor: {}", decisionO.get());
                            this.numScaleUp.increment();
                            this.resourceClusterHostActor.tell(translateScaleDecision(decisionO.get()), self());
                            break;
                        case NoOpReachMax:
                            this.numReachScaleMaxLimit.increment();
                            break;
                        case NoOpReachMin:
                            this.numReachScaleMinLimit.increment();
                            break;
                        default:
                            throw new RuntimeException("Invalid scale type: " + decisionO);
                    }
                }
            } else {
                log.info("Either scaling is disabled for sku or no sku rule is available for {}: {}", this.clusterId, usage.getUsageGroupKey());
            }
        });

        getSender().tell(Ack.getInstance(), self());
    }


    private void onGetClusterIdleInstancesResponse(GetClusterIdleInstancesResponse response) {
        log.info("On GetClusterIdleInstancesResponse, informing host actor: {}", response);
        this.resourceClusterHostActor.tell(
            ScaleResourceRequest.builder()
                .clusterId(this.clusterId)
                .skuId(response.getSkuId())
                .desireSize(response.getDesireSize())
                .idleInstances(response.getInstanceIds())
                .build(),
            self());

        // also disable the scale down targets to avoid them being used during the scale down process.
        response.getInstanceIds().forEach(id ->
            this.resourceClusterActor.tell(new DisableTaskExecutorsRequest(
                Collections.emptyMap(),
                this.clusterId,
                Instant.now().plus(Duration.ofMinutes(60)),
                Optional.of(id)),
                self()
        ));
    }

    private void onTriggerClusterUsageRequest(TriggerClusterUsageRequest req) {
        log.trace("Requesting cluster usage: {}", this.clusterId);
        if (this.skuToRuleMap.isEmpty()) {
            log.info("{} scaler is disabled due to no rules", this.clusterId);
            return;
        }
        this.resourceClusterActor.tell(
            new GetClusterUsageRequest(
                this.clusterId, ResourceClusterScalerActor.groupKeyFromTaskExecutorDefinitionIdFunc),
            self());
    }

    private void onTriggerClusterRuleRefreshRequest(TriggerClusterRuleRefreshRequest req) {
        log.debug("{}: Requesting cluster rule refresh", this.clusterId);
        this.fetchRuleSet();
    }

    private void onQueueClusterRuleRefreshRequest(QueueClusterRuleRefreshRequest req) {
        log.debug("{}: Queue a request to refresh cluster rules", this.clusterId);
        self().tell(new TriggerClusterRuleRefreshRequest(this.clusterId), self());
        getSender().tell(Ack.getInstance(), self());
    }

    private void fetchRuleSet() {
        try {
            ResourceClusterScaleRulesWritable rules =
                this.storageProvider.getResourceClusterScaleRules(this.clusterId);
            Set<ContainerSkuID> removedKeys = new HashSet<>(this.skuToRuleMap.keySet());
            final Set<ContainerSkuID> preservedKeys = rules.getScaleRules().keySet().stream()
                .map(ContainerSkuID::of).collect(Collectors.toSet());
            removedKeys.removeAll(preservedKeys);
            removedKeys.forEach(this.skuToRuleMap::remove);

            rules
                .getScaleRules().values()
                .forEach(scaleRule -> {
                    log.info("Cluster [{}]: Adding scaleRule: {}", this.clusterId, scaleRule);
                    final ClusterAvailabilityRule clusterAvailabilityRule = createClusterAvailabilityRule(
                        scaleRule, this.skuToRuleMap.get(scaleRule.getSkuId()));
                    this.skuToRuleMap.put(scaleRule.getSkuId(), clusterAvailabilityRule);
                });
            GetRuleSetResponse fetchFut =
                GetRuleSetResponse.builder()
                    .rules(ImmutableMap.copyOf(this.skuToRuleMap))
                    .clusterID(this.clusterId)
                    .build();

            self().tell(fetchFut, self());
        } catch (IOException e) {
            log.error("Failed to fetch rule set for cluster: {}", this.clusterId, e);
        }
    }

    private ClusterAvailabilityRule createClusterAvailabilityRule(ResourceClusterScaleSpec scaleSpec, ClusterAvailabilityRule existingRule) {
        if (existingRule == null) {
            return new ClusterAvailabilityRule(scaleSpec, this.clock, Instant.MIN, true);
        }
        // If rule exists already, port over lastActionInstant and enabled from existing rule
        return new ClusterAvailabilityRule(scaleSpec, this.clock, existingRule.lastActionInstant, existingRule.enabled);
    }

    private void onSetScalerStatus(SetResourceClusterScalerStatusRequest req) {
        if (skuToRuleMap.containsKey(req.getSkuId())) {
            skuToRuleMap.get(req.getSkuId()).setEnabled(req.getEnabled());

            if (!req.getEnabled()) {
                // setup a timer to re-enable autoscaling after a given period
                setExpireSetScalerStatusRequestTimer(new ExpireSetScalerStatusRequest(req));
            }
        }
    }

    private void onExpireSetScalerStatus(ExpireSetScalerStatusRequest req) {
        log.info("Expiration set scaler status request: {}", req);

        // re-enable autoscaling if it's been disabled for longer than threshold
        final ContainerSkuID skuID = req.request.getSkuId();
        final ClusterAvailabilityRule rule = skuToRuleMap.get(skuID);
        if (rule != null && !rule.isEnabled()) {
            if (!skuToRuleMap.get(skuID).isLastActionOlderThan(req.getRequest().getExpirationDurationInSeconds())) {
                skuToRuleMap.get(skuID).setEnabled(true);
            } else {
                // try again later
                setExpireSetScalerStatusRequestTimer(req);
            }
        }
    }

    private void setExpireSetScalerStatusRequestTimer(ExpireSetScalerStatusRequest req) {
        getTimers().startSingleTimer(
            "ExpireSetScalerStatusRequest-" + clusterId,
            req,
            Duration.ofSeconds(req.getRequest().getExpirationDurationInSeconds()));
    }

    private ScaleResourceRequest translateScaleDecision(ScaleDecision decision) {
        return ScaleResourceRequest.builder()
            .clusterId(this.clusterId)
            .skuId(decision.getSkuId())
            .desireSize(decision.getDesireSize())
            .build();
    }

    @Value
    @Builder
    static class TriggerClusterUsageRequest {
        ClusterID clusterID;
    }

    @Value
    static class ExpireSetScalerStatusRequest {
        SetResourceClusterScalerStatusRequest request;
    }

    @Value
    @Builder
    static class TriggerClusterRuleRefreshRequest {
        ClusterID clusterID;
    }

    @Value
    @Builder
    static class QueueClusterRuleRefreshRequest {
        ClusterID clusterID;
    }

    @Value
    @Builder
    static class GetRuleSetRequest {
        ClusterID clusterID;
    }

    @Value
    @Builder
    static class GetRuleSetResponse {
        ClusterID clusterID;
        ImmutableMap<ContainerSkuID, ClusterAvailabilityRule> rules;
    }

    static class ClusterAvailabilityRule {
        private final ResourceClusterScaleSpec scaleSpec;
        private final Clock clock;
        private Instant lastActionInstant;
        private boolean enabled;

        public ClusterAvailabilityRule(ResourceClusterScaleSpec scaleSpec, Clock clock, Instant lastActionInstant, Boolean enabled) {
            this.scaleSpec = scaleSpec;
            this.clock = clock;

            // TODO: probably we should use current time
            this.lastActionInstant = lastActionInstant;
            this.enabled = enabled;
        }

        private void resetLastActionInstant() {
            log.debug("resetLastActionInstant: {}, {}", this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());

            lastActionInstant = clock.instant();
        }

        public void setEnabled(boolean enabled) {
            log.debug("setEnabled: {}, {}, {}", enabled, this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());

            this.enabled = enabled;
            resetLastActionInstant();
        }

        public boolean isEnabled() { return enabled; }

        public boolean isLastActionOlderThan(long secondsSinceLastAction) {
            log.debug("[isLastActionOlderThan] secondsSinceLastAction: {}, {}, {}", secondsSinceLastAction, this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
            log.debug("[isLastActionOlderThan] lastActionInstant: {}, {}, {}", lastActionInstant, this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
            log.debug("[isLastActionOlderThan] lastActionInstant + secondsSinceLastAction: {}, {}, {}", lastActionInstant.plusSeconds(secondsSinceLastAction), this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
            log.debug("[isLastActionOlderThan] comp: {}, {}, {}", lastActionInstant.plusSeconds(secondsSinceLastAction).compareTo(clock.instant()) > 0, this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());

            return lastActionInstant.plusSeconds(secondsSinceLastAction).compareTo(clock.instant()) > 0;
        }

        public Optional<ScaleDecision> apply(UsageByGroupKey usage) {
            Optional<ScaleDecision> decision = Optional.empty();
            if (usage.getIdleCount() > scaleSpec.getMaxIdleToKeep()) {
                // Cool down check: for scaling down we want to wait 5x the nominal cool down period
                if (isLastActionOlderThan(scaleSpec.getCoolDownSecs() * 5)) {
                    log.debug("Scale Down CoolDown skip: {}, {}", this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
                    return Optional.empty();
                }

                // too many idle agents, scale down.
                int step = usage.getIdleCount() - scaleSpec.getMaxIdleToKeep();
                int newSize = Math.max(
                    usage.getTotalCount() - step, this.scaleSpec.getMinSize());
                decision = Optional.of(
                    ScaleDecision.builder()
                        .clusterId(this.scaleSpec.getClusterId())
                        .skuId(this.scaleSpec.getSkuId())
                        .desireSize(newSize)
                        .maxSize(newSize)
                        .minSize(newSize)
                        .type(newSize == usage.getTotalCount() ? ScaleType.NoOpReachMin : ScaleType.ScaleDown)
                        .build());
            }
            else if (usage.getIdleCount() < scaleSpec.getMinIdleToKeep()) {
                // Cool down check
                if (isLastActionOlderThan(scaleSpec.getCoolDownSecs())) {
                    log.debug("Scale Up CoolDown skip: {}, {}", this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
                    return Optional.empty();
                }

                // scale up
                int step = scaleSpec.getMinIdleToKeep() - usage.getIdleCount();
                int newSize = Math.min(
                    usage.getTotalCount() + step, this.scaleSpec.getMaxSize());
                decision = Optional.of(
                    ScaleDecision.builder()
                        .clusterId(this.scaleSpec.getClusterId())
                        .skuId(this.scaleSpec.getSkuId())
                        .desireSize(newSize)
                        .maxSize(newSize)
                        .minSize(newSize)
                        .type(newSize == usage.getTotalCount() ? ScaleType.NoOpReachMax : ScaleType.ScaleUp)
                        .build());
            }

            log.info("Scale Decision for {}-{}: {}",
                this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId(), decision);

            // reset last action only if we decided to scale up or down
            if (decision.isPresent() && (decision.get().type.equals(ScaleType.ScaleDown) || decision.get().type.equals(ScaleType.ScaleUp))) {
                log.debug("Ongoing scale operation. Resetting last action timer: {}, {}", this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
                resetLastActionInstant();
            }
            return decision;
        }
    }

    enum ScaleType {
        NoOpReachMax,
        NoOpReachMin,
        ScaleUp,
        ScaleDown,
    }

    @Value
    @Builder
    static class ScaleDecision {
        ContainerSkuID skuId;
        ClusterID clusterId;
        int maxSize;
        int minSize;
        int desireSize;
        ScaleType type;
    }

    /**
     * {@link TaskExecutorRegistration} holds task attribute map in which the container sku ID's resource id is stored
     * as a string. Here the key function is used to retrieve the map this string as grouping kye.
     */
    static Function<TaskExecutorRegistration, Optional<String>> groupKeyFromTaskExecutorDefinitionIdFunc =
        reg -> reg.getTaskExecutorContainerDefinitionId().map(id -> id.getResourceID());
}
