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

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonInclude;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

@Builder
@EqualsAndHashCode
@ToString
public class DeploymentStrategy {
    @Singular(ignoreNullCollections = true, value = "stage")
    @Getter
    private final Map<Integer, StageDeploymentStrategy> stageDeploymentStrategyMap;

    /**
     * If this field is not empty, it's used to indicate this is a resource cluster stack deployment and will be hosted
     * to the given resource cluster Id.
     */
    @Getter
    @JsonInclude(Include.NON_NULL)
    private final String resourceClusterId;

    public DeploymentStrategy(
        @JsonProperty("stageDeploymentStrategyMap") Map<Integer, StageDeploymentStrategy> stageDeploymentStrategyMap,
        @JsonProperty("resourceClusterId") String resourceClusterId) {
        this.stageDeploymentStrategyMap = stageDeploymentStrategyMap;
        this.resourceClusterId = resourceClusterId;
    }

    public StageDeploymentStrategy forStage(int stageNum) {
        if (!this.stageDeploymentStrategyMap.containsKey(stageNum)) { return null; }
        return stageDeploymentStrategyMap.get(stageNum);
    }

    public boolean requireInheritInstanceCheck() {
        return this.stageDeploymentStrategyMap != null && this.stageDeploymentStrategyMap.values().stream().anyMatch(StageDeploymentStrategy::isInheritInstanceCount);
    }

    public boolean requireInheritInstanceCheck(int stageNum) {
        return this.stageDeploymentStrategyMap
            != null && this.stageDeploymentStrategyMap.containsKey(stageNum) && this.stageDeploymentStrategyMap.get(stageNum).isInheritInstanceCount();
    }
}
