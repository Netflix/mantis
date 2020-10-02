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

package io.mantisrx.server.core;

import java.util.Map;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.runtime.codec.JsonType;


public class JobSchedulingInfo implements JsonType {

    public static final String HB_JobId = "HB_JobId";
    public static final String SendHBParam = "sendHB";


    private String jobId;
    private Map<Integer, WorkerAssignments> workerAssignments; // index by stage num

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobSchedulingInfo(@JsonProperty("jobId") String jobId,
                             @JsonProperty("workerAssignments")
                                     Map<Integer, WorkerAssignments> workerAssignments) {
        this.jobId = jobId;
        this.workerAssignments = workerAssignments;
    }

    public String getJobId() {
        return jobId;
    }

    public Map<Integer, WorkerAssignments> getWorkerAssignments() {
        return workerAssignments;
    }

    @Override
    public String toString() {
        return "SchedulingChange [jobId=" + jobId + ", workerAssignments="
                + workerAssignments + "]";
    }
}
