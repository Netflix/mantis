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
import io.vavr.Tuple2;
import java.util.concurrent.ConcurrentHashMap;
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
    public static final String TASK_EXECUTOR_ASSIGNMENT_FAILURE = "taskExecutorAssignmentFailure";

    public static final String RC_ACTOR_RESTART = "resourceClusterActorRestart";
    public static final String MAX_JOB_ARTIFACTS_TO_CACHE_REACHED = "maxJobArtifactsToCacheReached";
    public static final String RESERVATION_PROCESSED = "reservationProcessed";
    public static final String RESERVATION_UPSERTED = "reservationUpserted";
    public static final String RESERVATION_INFLIGHT_TIMEOUT = "reservationInFlightTimeout";
    public static final String NUM_PENDING_RESERVATIONS = "numPendingReservations";
    public static final String RESERVATION_FULFILLMENT_LATENCY = "reservationFulfillmentLatency";

    private final Registry registry;
    private final ConcurrentHashMap<String, Tuple2<Counter, Timer>> messageMetrics;
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
        this.messageMetrics = new ConcurrentHashMap<>();
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

    public void recordTimer(final String metric, final long nanos, final Iterable<Tag> tags) {
        registry.timer(new MetricId(METRIC_GROUP_ID, metric, tags).getSpectatorId(registry))
            .record(nanos, TimeUnit.NANOSECONDS);
    }

    public <P> FI.UnitApply<P> withTracking(final FI.UnitApply<P> apply) {
        return p -> {
            final long start = System.nanoTime();
            try {
                apply.apply(p);
            } finally {
                final String messageName = p.getClass().getSimpleName();
                messageMetrics.computeIfAbsent(messageName, this::getBoth)
                    .apply((counter, timer) -> {
                        counter.increment();
                        timer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                        return null;
                    });
            }
        };
    }
}
