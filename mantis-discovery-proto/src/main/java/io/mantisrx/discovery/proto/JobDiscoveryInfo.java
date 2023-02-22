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

package io.mantisrx.discovery.proto;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;


public class JobDiscoveryInfo {

    @JsonIgnore
    private static final int INGEST_STAGE = 1;
    private final String jobCluster;
    private final String jobId;
    private final Map<Integer, StageWorkers> stageWorkersMap;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobDiscoveryInfo(@JsonProperty("jobCluster") final String jobCluster,
                            @JsonProperty("jobId") final String jobId,
                            @JsonProperty("stageWorkersMap") final Map<Integer, StageWorkers> stageWorkersMap) {
        this.jobCluster = jobCluster;
        this.jobId = jobId;
        this.stageWorkersMap = stageWorkersMap;
    }

    public String getJobCluster() {
        return jobCluster;
    }

    public String getJobId() {
        return jobId;
    }

    public Map<Integer, StageWorkers> getStageWorkersMap() {
        return stageWorkersMap;
    }

    @JsonIgnore
    public StageWorkers getIngestStageWorkers() {
        return stageWorkersMap.getOrDefault(INGEST_STAGE, new StageWorkers(jobCluster, jobId, INGEST_STAGE, Collections.emptyList()));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final JobDiscoveryInfo that = (JobDiscoveryInfo) o;
        return Objects.equals(jobId, that.jobId) &&
                Objects.equals(stageWorkersMap, that.stageWorkersMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, stageWorkersMap);
    }

    @Override
    public String toString() {
        return "JobDiscoveryInfo{"
                + " jobCluster='" + jobCluster + '\''
                + ", jobId='" + jobId + '\''
                + ", stageWorkersMap=" + stageWorkersMap
                + '}';
    }
}
