/*
 * Copyright 2023 Netflix, Inc.
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

import akka.japi.pf.FI;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import io.mantisrx.common.metrics.spectator.MetricId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.CacheJobArtifactsOnTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.HeartbeatTimeout;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.InitializeTaskExecutorRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorBatchAssignmentRequest;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.TaskExecutorGatewayRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.vavr.Tuple2;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ResourceClusterActorMetrics {

    private static final String METRIC_GROUP_ID = "ResourceClusterActor";

    public static final String NUM_REGISTERED_TE = "numRegisteredTaskExecutors";
    public static final String NUM_BUSY_TE = "numBusyTaskExecutors";
    public static final String NUM_AVAILABLE_TE = "numAvailableTaskExecutors";
    public static final String NUM_DISABLED_TE = "numDisabledTaskExecutors";
    public static final String NUM_UNREGISTERED_TE = "numUnregisteredTaskExecutors";
    public static final String NUM_ASSIGNED_TE = "numAssignedTaskExecutors";
    public static final String NO_RESOURCES_AVAILABLE = "noResourcesAvailable";
    public static final String HEARTBEAT_TIMEOUT = "taskExecutorHeartbeatTimeout";

    public static final String TE_CONNECTION_FAILURE = "taskExecutorConnectionFailure";

    public static final String RC_ACTOR_RESTART = "resourceClusterActorRestart";
    public static final String MAX_JOB_ARTIFACTS_TO_CACHE_REACHED = "maxJobArtifactsToCacheReached";

    private final Registry registry;
    private final Map<Class<?>, Tuple2<Counter, Timer>> messageMetrics;
    private final Tuple2<Counter, Timer> unknownMessageMetrics;

    private Id getMessageReceivedId(String messageName) {
        return new MetricId(METRIC_GROUP_ID, "messagesReceived",
            Tag.of("messageType", messageName)).getSpectatorId(registry);
    }

    private Id getMessageProcessingLatencyId(String messageName) {
        return new MetricId(METRIC_GROUP_ID, "messageProcessingLatency",
            Tag.of("messageType", messageName)).getSpectatorId(registry);
    }

    private Tuple2<Counter, Timer> getBoth(String messageName) {
        return new Tuple2<>(
            registry.counter(getMessageReceivedId(messageName)),
            registry.timer(getMessageProcessingLatencyId(messageName)));
    }

    public ResourceClusterActorMetrics() {
        this.registry = SpectatorRegistryFactory.getRegistry();
        this.messageMetrics = ImmutableMap.of(
            TaskExecutorRegistration.class, getBoth("TaskExecutorRegistration"),
            InitializeTaskExecutorRequest.class, getBoth("InitializeTaskExecutorRequest"),
            TaskExecutorHeartbeat.class, getBoth("TaskExecutorHeartbeat"),
            TaskExecutorDisconnection.class, getBoth("TaskExecutorDisconnection"),
            HeartbeatTimeout.class, getBoth("HeartbeatTimeout"),
            TaskExecutorBatchAssignmentRequest.class, getBoth("TaskExecutorBatchAssignmentRequest"),
            TaskExecutorGatewayRequest.class, getBoth("TaskExecutorGatewayRequest"),
            CacheJobArtifactsOnTaskExecutorRequest.class,
            getBoth("CacheJobArtifactsOnTaskExecutorRequest"),
            GetClusterUsageRequest.class, getBoth("GetClusterUsageRequest"),
            GetClusterIdleInstancesRequest.class, getBoth("GetClusterIdleInstancesRequest")
        );
        this.unknownMessageMetrics = getBoth("UnknownMessage");
    }

    public void setGauge(final String metric, final long value, final Iterable<Tag> tags) {
        registry.gauge(new MetricId(METRIC_GROUP_ID, metric, tags).getSpectatorId(registry))
            .set(value);
    }

    public void incrementCounter(final String metric, final Iterable<Tag> tags) {
        registry.counter(new MetricId(METRIC_GROUP_ID, metric, tags).getSpectatorId(registry))
            .increment();
    }

    public <P> FI.UnitApply<P> withTracking(final FI.UnitApply<P> apply) {
        return p -> {
            final long start = System.nanoTime();
            try {
                apply.apply(p);
            } finally {
                final Class<?> pClass = p.getClass();
                messageMetrics.getOrDefault(pClass, unknownMessageMetrics)
                    .apply((counter, timer) -> {
                        counter.increment();
                        timer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                        return null;
                    });
            }
        };
    }
}
