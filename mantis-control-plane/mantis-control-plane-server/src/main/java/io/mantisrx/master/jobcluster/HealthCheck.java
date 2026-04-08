/*
 * Copyright 2026 Netflix, Inc.
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

package io.mantisrx.master.jobcluster;

import io.mantisrx.master.jobcluster.proto.HealthCheckResponse;
import java.util.List;
import java.util.Map;

/**
 * Extension point for job cluster health checks. Implementations are composed into an ordered
 * list and executed sequentially — the chain stops at the first failure.
 *
 * <p>The {@code context} map carries caller-provided parameters (e.g., alert group names)
 * without the interface needing to know about caller-specific concepts.
 */
public interface HealthCheck {

    /**
     * Run a health check against the given job cluster.
     *
     * @param clusterName the job cluster name
     * @param jobIds specific job IDs to check, or empty to use the latest active job
     * @param context arbitrary caller-provided parameters
     * @return a healthy response, or an unhealthy response with the appropriate {@link
     *     HealthCheckResponse.FailureReason}
     */
    HealthCheckResponse check(String clusterName, List<String> jobIds, Map<String, Object> context);
}
