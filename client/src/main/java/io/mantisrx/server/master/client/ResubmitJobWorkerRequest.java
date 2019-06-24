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


public class ResubmitJobWorkerRequest {

    @JsonProperty("JobId")
    private final String jobId;
    @JsonProperty("user")
    private final String user;
    @JsonProperty("workerNumber")
    private final int workerNumber;
    @JsonProperty("reason")
    private final String reason;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public ResubmitJobWorkerRequest(final String jobId,
                                    final String user,
                                    final int workerNumber,
                                    final String reason) {
        this.jobId = jobId;
        this.user = user;
        this.workerNumber = workerNumber;
        this.reason = reason;
    }

    public String getJobId() {
        return jobId;
    }

    public String getUser() {
        return user;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ResubmitJobWorkerRequest{" +
                "jobId='" + jobId + '\'' +
                ", user='" + user + '\'' +
                ", workerNumber=" + workerNumber +
                ", reason='" + reason + '\'' +
                '}';
    }
}
