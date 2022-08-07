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

import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Value;

@Value
public class DisableTaskExecutorsRequest {
    Map<String, String> attributes;

    ClusterID clusterID;

    Instant expiry;

    boolean isExpired(Instant now) {
        return expiry.compareTo(now) <= 0;
    }

    boolean targetsSameTaskExecutorsAs(DisableTaskExecutorsRequest another) {
        return this.attributes.entrySet().containsAll(another.attributes.entrySet());
    }

    boolean covers(@Nullable TaskExecutorRegistration registration) {
        return registration != null && registration.containsAttributes(this.attributes);
    }
}
