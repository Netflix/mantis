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
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.time.Clock;
import java.time.Duration;
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

    private final Clock clock;

    // TODO: scaler ruleSet; cooldown tracker;

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
                .match(GetClusterUsageResponse.class, this::onGetClusterUsageResponse)
                .match(Ack.class, ack -> log.info("Received ack from {}", sender()))
                .build();
    }

    private void init() {
        getTimers().startSingleTimer(
            "ClusterScaler-" + this.clusterId.toString(),
            new TriggerClusterUsageRequest(this.clusterId),
            scalerTimerTimeout);
        // sender().tell(null, self());
    }

    private void onGetClusterUsageResponse(GetClusterUsageResponse usageResponse) {
        log.info("Getting cluster usage: {}", usageResponse);

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
}
