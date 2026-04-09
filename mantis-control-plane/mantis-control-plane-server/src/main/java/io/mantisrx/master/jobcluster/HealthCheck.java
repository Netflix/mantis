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
 * <p>Each implementation declares a {@link #contextId()} that acts as a namespace prefix.
 * The handler extracts query params prefixed with this ID (e.g., {@code radar.alertGroups})
 * and passes them to the check with the prefix stripped.
 */
public interface HealthCheck {

    /**
     * Namespace prefix for context parameters belonging to this health check.
     * For example, a check with contextId "radar" receives query params like
     * {@code radar.alertGroups} as {@code alertGroups} in its context map.
     *
     * @return the context namespace, or empty string to receive unnamespaced params
     */
    String contextId();

    /**
     * Run a health check against the given job cluster.
     *
     * @param clusterName the job cluster name
     * @param jobIds specific job IDs to check, or empty to use the latest active job
     * @param context parameters namespaced to this check (prefix already stripped)
     * @return a healthy response, or an unhealthy response with the appropriate {@link
     *     HealthCheckResponse.FailureReason}
     */
    HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context);
}
