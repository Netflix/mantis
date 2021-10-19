/*
 * Copyright 2021 Netflix, Inc.
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

package io.mantisrx.runtime.descriptor;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.ToString;

@Builder
@EqualsAndHashCode
@ToString
public class DeploymentStrategy {
    @Singular(ignoreNullCollections = true) private Map<Integer, StageDeploymentStrategy> stages;

    public DeploymentStrategy(
            @JsonProperty("stageDeploymentStrategyMap") Map<Integer, StageDeploymentStrategy> stages) {
        this.stages = stages;
    }

    public StageDeploymentStrategy forStage(int stageNum) {
        if (!this.stages.containsKey(stageNum)) { return null; }
        return stages.get(stageNum);
    }

    public boolean requireInheritInstanceCheck() {
        return this.stages != null && this.stages.values().stream().anyMatch(StageDeploymentStrategy::isInheritInstanceCount);
    }

    public boolean requireInheritInstanceCheck(int stageNum) {
        return this.stages != null && this.stages.containsKey(stageNum) && this.stages.get(stageNum).isInheritInstanceCount();
    }
}
