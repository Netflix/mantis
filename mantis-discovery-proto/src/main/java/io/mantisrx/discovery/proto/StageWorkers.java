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
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;


public class StageWorkers {

    private final String jobCluster;
    private final String jobId;
    private final int stageNum;
    private final List<MantisWorker> workers;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageWorkers(@JsonProperty("jobCluster") String jobCluster,
                        @JsonProperty("jobId") String jobId,
                        @JsonProperty("stageNum") int stageNum,
                        @JsonProperty("workers") List<MantisWorker> workers) {
        this.jobCluster = jobCluster;
        this.jobId = jobId;
        this.stageNum = stageNum;
        this.workers = workers;
    }

    public String getJobCluster() {
        return jobCluster;
    }

    public String getJobId() {
        return jobId;
    }

    public int getStageNum() {
        return stageNum;
    }

    public List<MantisWorker> getWorkers() {
        return workers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StageWorkers that = (StageWorkers) o;
        return stageNum == that.stageNum &&
                Objects.equals(jobId, that.jobId) &&
                Objects.equals(workers, that.workers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, stageNum, workers);
    }

    @Override
    public String toString() {
        return "StageWorkers{"
                + "jobCluster='" + jobCluster + '\''
                + ", jobId='" + jobId + '\''
                + ", stageNum=" + stageNum
                + ", workers=" + workers
                + '}';
    }
}
