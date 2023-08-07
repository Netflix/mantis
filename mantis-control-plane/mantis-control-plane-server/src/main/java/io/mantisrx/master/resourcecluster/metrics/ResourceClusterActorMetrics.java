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

package io.mantisrx.master.resourcecluster.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceClusterActorMetrics {

    public static final String METRIC_GROUP_ID = "ResourceClusterActor";

    public static final String NUM_REGISTERED_TE = "numRegisteredTaskExecutors";
    public static final String NUM_BUSY_TE = "numBusyTaskExecutors";
    public static final String NUM_AVAILABLE_TE = "numAvailableTaskExecutors";
    public static final String NUM_DISABLED_TE = "numDisabledTaskExecutors";
    public static final String NUM_UNREGISTERED_TE = "numUnregisteredTaskExecutors";
    public static final String NUM_ASSIGNED_TE = "numAssignedTaskExecutors";
    public static final String NO_RESOURCES_AVAILABLE = "noResourcesAvailable";
    public static final String HEARTBEAT_TIMEOUT = "taskExecutorHeartbeatTimeout";

    public static final String TE_CONNECTION_FAILURE = "taskExecutorConnectionFailure";
    public static final String TE_RECONNECTION_FAILURE = "taskExecutorReconnectionFailure";

    private final MeterRegistry meterRegistry;

    public ResourceClusterActorMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void setGauge(final String metric, final long value, final Tags tags) {
        meterRegistry.gauge(METRIC_GROUP_ID + "_" + metric, tags, value);
    }

    public void incrementCounter(final String metric, final Tags tags) {
        meterRegistry.counter(METRIC_GROUP_ID + "_" + metric, tags).increment();
    }
}
