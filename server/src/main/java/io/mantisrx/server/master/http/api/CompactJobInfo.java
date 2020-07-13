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

package io.mantisrx.server.master.http.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.master.store.MantisJobMetadata;
import io.mantisrx.server.master.store.MantisStageMetadata;
import io.mantisrx.server.master.store.MantisWorkerMetadata;


public class CompactJobInfo {

    private final String jobId;
    private final long submittedAt;
    private final String user;
    private final MantisJobState state;
    private final MantisJobDurationType type;
    private final int numStages;
    private final int numWorkers;
    private final double totCPUs;
    private final double totMemory;
    private final Map<String, Integer> statesSummary;
    private final List<Label> labels;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public CompactJobInfo(
            @JsonProperty("jobID") String jobId,
            @JsonProperty("submittedAt") long submittedAt,
            @JsonProperty("user") String user,
            @JsonProperty("state") MantisJobState state,
            @JsonProperty("type") MantisJobDurationType type,
            @JsonProperty("numStages") int numStages,
            @JsonProperty("numWorkers") int numWorkers,
            @JsonProperty("totCPUs") double totCPUs,
            @JsonProperty("totMemory") double totMemory,
            @JsonProperty("statesSummary") Map<String, Integer> statesSummary,
            @JsonProperty("labels") List<Label> labels
    ) {
        this.jobId = jobId;
        this.submittedAt = submittedAt;
        this.user = user;
        this.state = state;
        this.type = type;
        this.numStages = numStages;
        this.numWorkers = numWorkers;
        this.totCPUs = totCPUs;
        this.totMemory = totMemory;
        this.statesSummary = statesSummary;
        this.labels = labels;
    }

    static CompactJobInfo fromJob(MantisJobMetadata job) {
        if (job == null)
            return null;
        int workers = 0;
        double totCPUs = 0.0;
        double totMem = 0.0;
        Map<String, Integer> stSmry = new HashMap<>();
        for (MantisStageMetadata s : job.getStageMetadata()) {
            workers += s.getNumWorkers();
            totCPUs += s.getNumWorkers() * s.getMachineDefinition().getCpuCores();
            totMem += s.getNumWorkers() * s.getMachineDefinition().getMemoryMB();
            for (MantisWorkerMetadata w : s.getWorkerByIndexMetadataSet()) {
                final Integer prevVal = stSmry.get(w.getState() + "");
                if (prevVal == null)
                    stSmry.put(w.getState() + "", 1);
                else
                    stSmry.put(w.getState() + "", prevVal + 1);
            }
        }
        return new CompactJobInfo(
                job.getJobId(), job.getSubmittedAt(), job.getUser(), job.getState(),
                job.getSla().getDurationType(), job.getNumStages(), workers, totCPUs, totMem, stSmry, job.getLabels()
        );
    }

    public String getJobId() {
        return jobId;
    }

    public long getSubmittedAt() {
        return submittedAt;
    }

    public String getUser() {
        return user;
    }

    public MantisJobState getState() {
        return state;
    }

    public MantisJobDurationType getType() {
        return type;
    }

    public int getNumStages() {
        return numStages;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public double getTotCPUs() {
        return totCPUs;
    }

    public double getTotMemory() {
        return totMemory;
    }

    public Map<String, Integer> getStatesSummary() {
        return statesSummary;
    }

    public List<Label> getLabels() {
        return this.labels;
    }
}
