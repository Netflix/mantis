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

import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MantisStageMetadataWritable implements MantisStageMetadata {

    private static final Logger logger = LoggerFactory.getLogger(MantisStageMetadataWritable.class);
    @JsonIgnore
    private final ConcurrentMap<Integer, MantisWorkerMetadata> workerByIndexMetadataSet;
    @JsonIgnore
    private final ConcurrentMap<Integer, MantisWorkerMetadata> workerByNumberMetadataSet;
    private String jobId;
    private int stageNum;
    private int numStages;
    private MachineDefinition machineDefinition;
    private int numWorkers;
    private List<JobConstraints> hardConstraints;
    private List<JobConstraints> softConstraints;
    private StageScalingPolicy scalingPolicy;
    private boolean scalable;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MantisStageMetadataWritable(@JsonProperty("jobId") String jobId,
                                       @JsonProperty("stageNum") int stageNum,
                                       @JsonProperty("numStages") int numStages,
                                       @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                                       @JsonProperty("numWorkers") int numWorkers,
                                       @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                                       @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                                       @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                                       @JsonProperty("scalable") boolean scalable) {
        this.jobId = jobId;
        this.stageNum = stageNum;
        this.numStages = numStages;
        this.machineDefinition = machineDefinition;
        this.numWorkers = numWorkers;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.scalingPolicy = scalingPolicy;
        this.scalable = scalable;
        workerByIndexMetadataSet = new ConcurrentHashMap<>();
        workerByNumberMetadataSet = new ConcurrentHashMap<>();
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public int getStageNum() {
        return stageNum;
    }

    @Override
    public int getNumStages() {
        return numStages;
    }

    @Override
    public int getNumWorkers() {
        return numWorkers;
    }

    @JsonIgnore
    public int getNumActiveWorkers() {
        // we traverse the current worker for each index
        int active = 0;
        for (MantisWorkerMetadata w : workerByIndexMetadataSet.values()) {
            if (!MantisJobState.isTerminalState(w.getState()))
                active++;
        }
        return active;
    }

    // This call is unsafe to be called by itself. Typically this is called from within a block that
    // locks the corresponding job metadata object and also does the right things for reflecting upon the change. E.g., if increasing
    // the number, then create the new workers. When decrementing, call the unsafeRemoveWorker() to remove
    // the additional workers.
    public void unsafeSetNumWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    public boolean unsafeRemoveWorker(int index, int number) {
        final MantisWorkerMetadata removedIdx = workerByIndexMetadataSet.remove(index);
        final MantisWorkerMetadata removedNum = workerByNumberMetadataSet.remove(number);
        return removedIdx != null && removedNum != null && removedIdx.getWorkerNumber() == number &&
                removedNum.getWorkerIndex() == index;
    }

    @Override
    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    @Override
    public StageScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }

    public void setScalingPolicy(StageScalingPolicy scalingPolicy) {
        this.scalingPolicy = scalingPolicy;
    }

    @Override
    public boolean getScalable() {
        return scalable;
    }

    public void setScalable(boolean scalable) {
        this.scalable = scalable;
    }

    @Override
    public MachineDefinition getMachineDefinition() {
        return machineDefinition;
    }

    @JsonIgnore
    @Override
    public Collection<MantisWorkerMetadata> getWorkerByIndexMetadataSet() {
        return workerByIndexMetadataSet.values();
    }

    @JsonIgnore
    @Override
    public Collection<MantisWorkerMetadata> getAllWorkers() {
        return workerByNumberMetadataSet.values();
    }

    @JsonIgnore
    @Override
    public MantisWorkerMetadata getWorkerByIndex(int workerId) throws InvalidJobException {
        MantisWorkerMetadata mwmd = workerByIndexMetadataSet.get(workerId);
        if (mwmd == null)
            throw new InvalidJobException(jobId, -1, workerId);
        return mwmd;
    }

    @JsonIgnore
    @Override
    public MantisWorkerMetadata getWorkerByWorkerNumber(int workerNumber) throws InvalidJobException {
        MantisWorkerMetadata mwmd = workerByNumberMetadataSet.get(workerNumber);
        if (mwmd == null)
            throw new InvalidJobException(jobId, -1, workerNumber);
        return mwmd;
    }

    MantisWorkerMetadataWritable removeWorkerInErrorState(int workerNumber) {
        MantisWorkerMetadataWritable mwmd = (MantisWorkerMetadataWritable) workerByNumberMetadataSet.get(workerNumber);
        if (mwmd != null && MantisJobState.isErrorState(mwmd.getState())) {
            workerByNumberMetadataSet.remove(workerNumber);
            return mwmd;
        }
        return null;
    }

    Collection<MantisWorkerMetadataWritable> removeArchiveableWorkers() {
        Collection<MantisWorkerMetadataWritable> removedWorkers = new LinkedList<>();
        Set<Integer> workerNumbers = new HashSet<>(workerByNumberMetadataSet.keySet());
        for (Integer w : workerNumbers) {
            MantisWorkerMetadata mwmd = workerByNumberMetadataSet.get(w);
            final MantisWorkerMetadata wi = workerByIndexMetadataSet.get(mwmd.getWorkerIndex());
            if (wi == null || wi.getWorkerNumber() != mwmd.getWorkerNumber()) {
                workerByNumberMetadataSet.remove(w);
                removedWorkers.add((MantisWorkerMetadataWritable) mwmd);
            }
        }
        return removedWorkers;
    }

    /**
     * Use the given new worker to add or replace the target index.
     * If the stage worker index already exists, replace it only when the given worker has higher worker number.
     * @param newWorker new worker metadata instance.
     * @return true if the new worker is used in this stage.
     */
    public boolean replaceWorkerIndex(MantisWorkerMetadata newWorker) {
        int index = newWorker.getWorkerIndex();
        boolean result = true;

        if (workerByIndexMetadataSet.containsKey(index)) {
            MantisWorkerMetadata existingWorker = workerByIndexMetadataSet.get(index);
            int existingWorkerNum = existingWorker.getWorkerNumber();
            if (existingWorkerNum >= newWorker.getWorkerNumber()) {
                logger.warn("Encounter stale worker: {} when newer worker exist: {}, ignore.", newWorker,
                    existingWorker);
                result = false;
            }
            else {
                logger.warn("Replace stale worker {} with {}.", existingWorker, newWorker);
                addNewWorker(newWorker);
            }
        } else {
            addNewWorker(newWorker);
        }

        return result;
    }

    private void addNewWorker(MantisWorkerMetadata newWorker) {
        workerByIndexMetadataSet.put(newWorker.getWorkerIndex(), newWorker);
        workerByNumberMetadataSet.put(newWorker.getWorkerNumber(), newWorker);
    }
}
