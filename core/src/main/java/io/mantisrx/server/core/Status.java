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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.domain.WorkerId;

//import io.mantisrx.server.master.domain.WorkerId;


public class Status {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @JsonIgnore
    private final Optional<WorkerId> workerId;
    private String jobId;
    private int stageNum;
    private int workerIndex;
    private int workerNumber;
    private String hostname = null;
    private TYPE type;
    private String message;
    private long timestamp;
    private MantisJobState state;
    private JobCompletedReason reason = JobCompletedReason.Normal;
    private List<Payload> payloads;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public Status(@JsonProperty("jobId") String jobId, @JsonProperty("stageNum") int stageNum, @JsonProperty("workerIndex") int workerIndex,
                  @JsonProperty("workerNumber") int workerNumber,
                  @JsonProperty("type") TYPE type, @JsonProperty("message") String message, @JsonProperty("state") MantisJobState state) {
        this(jobId, stageNum, workerIndex, workerNumber, type, message, state, System.currentTimeMillis());
    }
    public Status(String jobId, int stageNum, int workerIndex,
                  int workerNumber,
                  TYPE type, String message, MantisJobState state, long ts) {
        this.jobId = jobId;
        this.stageNum = stageNum;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        if (workerIndex >= 0 && workerNumber >= 0) {
            this.workerId = Optional.of(new WorkerId(jobId, workerIndex, workerNumber));
        } else {
            this.workerId = Optional.empty();
        }
        this.type = type;
        this.message = message;
        this.state = state;
        timestamp = ts;
        this.payloads = new ArrayList<>();
    }

    public String getJobId() {
        return jobId;
    }

    public int getStageNum() {
        return stageNum;
    }

    public void setStageNum(int stageNum) {
        this.stageNum = stageNum;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public void setWorkerIndex(int workerIndex) {
        this.workerIndex = workerIndex;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public Optional<WorkerId> getWorkerId() {
        return workerId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public TYPE getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public MantisJobState getState() {
        return state;
    }

    public JobCompletedReason getReason() {
        return reason;
    }

    public void setReason(JobCompletedReason reason) {
        this.reason = reason;
    }

    public List<Payload> getPayloads() {
        return payloads;
    }

    public void setPayloads(List<Payload> payloads) {
        this.payloads = payloads;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Error getting string for status on job " + jobId;
        }
    }

    public enum TYPE {
        ERROR, WARN, INFO, DEBUG, HEARTBEAT
    }

    public static class Payload {

        private final String type;
        private final String data;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Payload(@JsonProperty("type") String type, @JsonProperty("data") String data) {
            this.type = type;
            this.data = data;
        }

        public String getType() {
            return type;
        }

        public String getData() {
            return data;
        }
    }
}
