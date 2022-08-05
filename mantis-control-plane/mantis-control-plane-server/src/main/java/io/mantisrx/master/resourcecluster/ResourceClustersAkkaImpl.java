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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusterTaskExecutorMapper;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * This class is an implementation of {@link ResourceClusters} that uses the Akka actor implementation under the hood.
 * You can think of this class as a more java typed-way of sharing the functionalities of the akka actor.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ResourceClustersAkkaImpl implements ResourceClusters {

    private final ActorRef resourceClustersManagerActor;
    private final Duration askTimeout;
    private final ResourceClusterTaskExecutorMapper mapper;
    private final ConcurrentMap<ClusterID, ResourceCluster> cache =
        new ConcurrentHashMap<>();

    @Override
    public ResourceCluster getClusterFor(ClusterID clusterID) {
        cache.computeIfAbsent(
            clusterID,
            dontCare ->
                new ResourceClusterAkkaImpl(
                    resourceClustersManagerActor,
                    askTimeout,
                    clusterID,
                    mapper));
        return cache.get(clusterID);
    }

    @Override
    public CompletableFuture<Set<ClusterID>> listActiveClusters() {
        return
            Patterns.ask(resourceClustersManagerActor,
                    new ResourceClustersManagerActor.ListActiveClusters(), askTimeout)
                .toCompletableFuture()
                .thenApply(ResourceClustersManagerActor.ClusterIdSet.class::cast)
                .thenApply(clusterIdSet -> clusterIdSet.getClusterIDS());
    }

    public static ResourceClusters load(
        MasterConfiguration masterConfiguration,
        RpcService rpcService,
        ActorSystem actorSystem,
        MantisJobStore mantisJobStore,
        JobMessageRouter jobMessageRouter,
        ActorRef resourceClusterHostActorRef,
        ResourceClusterStorageProvider resourceStorageProvider) {
        final ActorRef resourceClusterManagerActor =
            actorSystem.actorOf(
                ResourceClustersManagerActor.props(masterConfiguration, Clock.systemDefaultZone(),
                    rpcService, mantisJobStore, resourceClusterHostActorRef, resourceStorageProvider,
                    jobMessageRouter));
        final ResourceClusterTaskExecutorMapper globalMapper =
            ResourceClusterTaskExecutorMapper.inMemory();

        final Duration askTimeout = java.time.Duration.ofMillis(
            ConfigurationProvider.getConfig().getMasterApiAskTimeoutMs());
        return new ResourceClustersAkkaImpl(resourceClusterManagerActor, askTimeout, globalMapper);
    }
}
