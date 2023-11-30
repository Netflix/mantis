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

package io.mantisrx.server.master.store;

import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.google.common.collect.Lists;


public class MantisJobMetadataWritable implements MantisJobMetadata {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobMetadataWritable.class);

    @Getter
    private final String user;
    @Getter
    private final JobSla sla;
    @Getter
    private final long subscriptionTimeoutSecs;
    @Getter
    private final List<Label> labels;
    @JsonIgnore
    private final ConcurrentMap<Integer, MantisStageMetadataWritable> stageMetadataMap;
    @JsonIgnore
    private final ConcurrentMap<Integer, Integer> workerNumberToStageMap;
    @JsonIgnore
    private final ReentrantLock lock = new ReentrantLock();
    @Getter
    private String jobId;
    @Getter
    private String name;
    @Getter
    private long submittedAt;
    @Getter
    private long startedAt = DEFAULT_STARTED_AT_EPOCH;

    @Getter
    private URL jarUrl;
    @Getter
    private volatile MantisJobState state;
    @Getter
    private final int numStages;
    @Getter
    private final List<Parameter> parameters;
    @Getter
    private int nextWorkerNumberToUse = 1;
    @Getter
    private final WorkerMigrationConfig migrationConfig;
    @JsonIgnore
    private Object sink; // ToDo need to figure out what object we store for sink
    @Getter
    private final long heartbeatIntervalSecs;
    @Getter
    private final long workerTimeoutSecs;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MantisJobMetadataWritable(@JsonProperty("jobId") String jobId,
                                     @JsonProperty("name") String name,
                                     @JsonProperty("user") String user,
                                     @JsonProperty("submittedAt") long submittedAt,
                                     @JsonProperty("startedAt") long startedAt,
                                     @JsonProperty("jarUrl") URL jarUrl,
                                     @JsonProperty("numStages") int numStages,
                                     @JsonProperty("sla") JobSla sla,
                                     @JsonProperty("state") MantisJobState state,
                                     @JsonProperty("workerTimeoutSecs") long workerTimeoutSecs,
                                     @JsonProperty("heartbeatIntervalSecs") long heartbeatIntervalSecs,
                                     @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                                     @JsonProperty("parameters") List<Parameter> parameters,
                                     @JsonProperty("nextWorkerNumberToUse") int nextWorkerNumberToUse,
                                     @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                                     @JsonProperty("labels") List<Label> labels) {
        this.jobId = jobId;
        this.name = name;
        this.user = user;
        this.submittedAt = submittedAt;
        this.startedAt = startedAt;

        this.jarUrl = jarUrl;
        this.numStages = numStages;
        this.sla = sla;
        this.state = state == null ? MantisJobState.Accepted : state;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.heartbeatIntervalSecs = heartbeatIntervalSecs;
        this.workerTimeoutSecs = workerTimeoutSecs;
        this.stageMetadataMap = new ConcurrentHashMap<>();
        this.workerNumberToStageMap = new ConcurrentHashMap<>();
        if (parameters == null) {
            this.parameters = new LinkedList<Parameter>();
        } else {
            this.parameters = parameters;
        }
        if (labels == null) {
            this.labels = new LinkedList<>();
        } else {
            this.labels = labels;
        }
        this.nextWorkerNumberToUse = nextWorkerNumberToUse;
        this.migrationConfig = Optional.ofNullable(migrationConfig).orElse(WorkerMigrationConfig.DEFAULT);
    }

    @Override
    public AutoCloseable obtainLock() {
        lock.lock();
        return new AutoCloseable() {
            @Override
            public void close() throws IllegalMonitorStateException {
                lock.unlock();
            }
        };
    }

    public void setNextWorkerNumberToUse(int n) {
        this.nextWorkerNumberToUse = n;
    }

    void setJobState(MantisJobState state) throws InvalidJobStateChangeException {
        if (!this.state.isValidStateChgTo(state))
            throw new InvalidJobStateChangeException(jobId, this.state, state);
        this.state = state;
    }

    @JsonIgnore
    @Override
    public Collection<? extends MantisStageMetadata> getStageMetadata() {
        return stageMetadataMap.values();
    }

    @JsonIgnore
    @Override
    public MantisStageMetadata getStageMetadata(int stageNum) {
        return stageMetadataMap.get(stageNum);
    }

    /**
     * Add job stage if absent, returning true if it was actually added.
     *
     * @param msmd The stage's metadata object.
     *
     * @return true if actually added, false otherwise.
     */
    public boolean addJobStageIfAbsent(MantisStageMetadataWritable msmd) {
        return stageMetadataMap.putIfAbsent(msmd.getStageNum(), msmd) == null;
    }

    /**
     * Add the given MantisWorkerMetadata instance to the corresponding stage.
     * If the stage worker index already exists, replace it only when the given worker has higher worker number.
     * @param stageNum target stage number.
     * @param workerMetadata new worker metadata instance.
     * @return null if the given worker metadata is added to this job. Otherwise, return the existing worker with
     * newer number.
     */
    public MantisWorkerMetadata tryAddOrReplaceWorker(int stageNum, MantisWorkerMetadata workerMetadata) {
        final boolean result =
            stageMetadataMap.get(stageNum)
                .replaceWorkerIndex(workerMetadata);

        if (result) {
            Integer integer = workerNumberToStageMap.put(workerMetadata.getWorkerNumber(), stageNum);
            if (integer != null && integer != stageNum) {
                logger.error(String.format("Unexpected to put worker number mapping from %d to stage %d for job %s, prev mapping to stage %d",
                    workerMetadata.getWorkerNumber(), stageNum, workerMetadata.getJobId(), integer));
            }
            return null;
        } else {
            try {
                return stageMetadataMap.get(stageNum).getWorkerByIndex(workerMetadata.getWorkerIndex());
            } catch (InvalidJobException e) {
                logger.error("Failed to fetch existing worker when new worker got rejected: {}", workerMetadata, e);
                throw new RuntimeException("Failed to fetch existing worker when new worker got rejected", e);
            }
        }
    }

    @JsonIgnore
    @Override
    public MantisWorkerMetadata getWorkerByIndex(int stageNumber, int workerIndex) throws InvalidJobException {
        MantisStageMetadata stage = stageMetadataMap.get(stageNumber);
        if (stage == null)
            throw new InvalidJobException(jobId, stageNumber, workerIndex);
        return stage.getWorkerByIndex(workerIndex);
    }

    @JsonIgnore
    @Override
    public MantisWorkerMetadata getWorkerByNumber(int workerNumber) throws InvalidJobException {
        Integer stageNumber = workerNumberToStageMap.get(workerNumber);
        if (stageNumber == null)
            throw new InvalidJobException(jobId, -1, workerNumber);
        MantisStageMetadata stage = stageMetadataMap.get(stageNumber);
        if (stage == null)
            throw new InvalidJobException(jobId, stageNumber, workerNumber);
        return stage.getWorkerByWorkerNumber(workerNumber);
    }

    @JsonIgnore
    public int getMaxWorkerNumber() {
        // Expected to be called only during initialization, no need to synchronize/lock.
        // Resubmitted workers are expected to have a worker number greater than those they replace.
        int max = -1;
        for (int id : workerNumberToStageMap.keySet())
            if (max < id) max = id;
        return max;
    }

    @Override
    public String toString() {
        return "MantisJobMetadataWritable{" +
                "user='" + user + '\'' +
                ", sla=" + sla +
                ", subscriptionTimeoutSecs=" + subscriptionTimeoutSecs +
                ", labels=" + labels +
                ", stageMetadataMap=" + stageMetadataMap +
                ", workerNumberToStageMap=" + workerNumberToStageMap +
                ", jobId='" + jobId + '\'' +
                ", name='" + name + '\'' +
                ", submittedAt=" + submittedAt +
                ", startedAt=" + startedAt +
                ", jarUrl=" + jarUrl +
                ", state=" + state +
                ", numStages=" + numStages +
                ", parameters=" + parameters +
                ", nextWorkerNumberToUse=" + nextWorkerNumberToUse +
                ", migrationConfig=" + migrationConfig +
                '}';
    }
}
