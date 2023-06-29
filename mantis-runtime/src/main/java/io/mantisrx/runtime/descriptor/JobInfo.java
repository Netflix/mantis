/*
 * Copyright 2019 Netflix, Inc.
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

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


public class JobInfo extends MetadataInfo {

    private final int numberOfStages;
    private final MetadataInfo sourceInfo;
    private final MetadataInfo sinkInfo;
    private final Map<Integer, StageInfo> stages;
    private final Map<String, ParameterInfo> parameterInfo;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobInfo(
            @JsonProperty("name") final String name,
            @JsonProperty("description") final String description,
            @JsonProperty("numberOfStages") final int numberOfStages,
            @JsonProperty("parameterInfo") final Map<String, ParameterInfo> parameterInfo,
            @JsonProperty("sourceInfo") final MetadataInfo sourceInfo,
            @JsonProperty("sinkInfo") final MetadataInfo sinkInfo,
            @JsonProperty("stages") final Map<Integer, StageInfo> stages) {
        super(name, description);
        this.numberOfStages = numberOfStages;
        this.parameterInfo = parameterInfo;
        this.sourceInfo = sourceInfo;
        this.sinkInfo = sinkInfo;
        this.stages = stages;
    }

    public int getNumberOfStages() {
        return numberOfStages;
    }

    public Map<String, ParameterInfo> getParameterInfo() {
        return parameterInfo;
    }

    public MetadataInfo getSourceInfo() {
        return sourceInfo;
    }

    public MetadataInfo getSinkInfo() {
        return sinkInfo;
    }

    public Map<Integer, StageInfo> getStages() {
        return stages;
    }
}
