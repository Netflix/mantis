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

package io.mantisrx.server.master.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


public class StageScaleRequest {

    @JsonProperty("JobId")
    private final String jobId;
    @JsonProperty("StageNumber")
    private final int stageNumber;
    @JsonProperty("NumWorkers")
    private final int numWorkers;
    @JsonProperty("Reason")
    private final String reason;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageScaleRequest(final String jobId,
                             final int stageNumber,
                             final int numWorkers,
                             final String reason) {
        this.jobId = jobId;
        this.stageNumber = stageNumber;
        this.numWorkers = numWorkers;
        this.reason = reason;
    }

    public String getJobId() {
        return jobId;
    }

    public int getStageNumber() {
        return stageNumber;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "StageScaleRequest{" +
                "jobId='" + jobId + '\'' +
                ", stageNumber=" + stageNumber +
                ", numWorkers=" + numWorkers +
                ", reason='" + reason + '\'' +
                '}';
    }
}
