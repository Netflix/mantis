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

package io.mantisrx.master.resourcecluster.proto;

import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class GetClusterUsageResponse {
    ClusterID clusterID;

    @Singular
    List<UsageByGroupKey> usages;

    @Value
    @Builder
    public static class UsageByGroupKey {
        String usageGroupKey;
        int idleCount;
        int totalCount;

        @Builder.Default
        int pendingReservationCount = 0;

        /**
         * Effective idle count accounting for pending reservations.
         * If pending reservations exist, those "idle" TEs will soon be consumed.
         */
        public int getEffectiveIdleCount() {
            return Math.max(0, idleCount - pendingReservationCount);
        }
    }
}
