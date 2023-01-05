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

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ResourceClusterSettings {
    Duration askTimeout;
    Duration taskExecutorHeartbeatTimeout;
    Duration taskExecutorAssignmentTimeout;
    Duration disabledTaskExecutorsCheckInterval;

    public static ResourceClusterSettings fromConfig(Config config) {
        return
            ResourceClusterSettings
                .builder()
                .askTimeout(config.getDuration("askTimeout"))
                .disabledTaskExecutorsCheckInterval(config.getDuration("disabledTaskExecutorsCheckInterval"))
                .taskExecutorHeartbeatTimeout(config.getDuration("taskExecutor.heartbeatTimeout"))
                .taskExecutorAssignmentTimeout(config.getDuration("taskExecutor.assignmentTimeout"))
                .build();
    }
}
