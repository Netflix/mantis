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

package io.mantisrx.server.master.mesos;

import com.typesafe.config.Config;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class AgentSettings {
    double binPackingWeight;
    double preferredClusterWeight;
    double goodEnoughThreshold;
    double durationTypeWeight;
    String clusterAttribute;

    public static AgentSettings fromConfig(Config config) {
        return
            AgentSettings
                .builder()
                .binPackingWeight(config.getDouble("fitness.binPackingWeight"))
                .preferredClusterWeight(config.getDouble("fitness.preferredClusterWeight"))
                .goodEnoughThreshold(config.getDouble("fitness.goodEnoughThreshold"))
                .durationTypeWeight(config.getDouble("fitness.durationTypeWeight"))
                .clusterAttribute(config.getString("clusterAttribute"))
                .build();
    }
}
