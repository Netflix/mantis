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

package io.mantisrx.master.jobcluster.job;

import static io.mantisrx.master.StringConstants.MANTIS_STAGE_CONTAINER_SIZE_NAME_KEY;
import static java.util.Optional.of;

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerTerminate;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.master.WorkerRequest;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.persistence.exceptions.InvalidWorkerStateChangeException;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements the {@link IMantisStageMetadata} interface. Represents information related to a Job stage.
 */
public class MantisStageMetadataImpl implements IMantisStageMetadata {

    private final JobId jobId;
    private final int stageNum;
    private final int numStages;
    private final MachineDefinition machineDefinition;
    private int numWorkers;
    @JsonIgnore
    private boolean isSubscribed = false;
    private final List<JobConstraints> hardConstraints;
    private final List<JobConstraints> softConstraints;
    // scaling policy be null
    private final StageScalingPolicy scalingPolicy;
    private final boolean scalable;
    private final String sizeAttribute;
    @JsonIgnore
    private final ConcurrentMap<Integer, JobWorker> workerByIndexMetadataSet;
    @JsonIgnore
    private final ConcurrentMap<Integer, JobWorker> workerByNumberMetadataSet;
    private static final Logger LOGGER = LoggerFactory.getLogger(MantisStageMetadataImpl.class);

    /**
     * Default constructor.
     * @param jobId
     * @param stageNum
     * @param numStages
     * @param machineDefinition
     * @param numWorkers
     * @param hardConstraints
     * @param softConstraints
     * @param scalingPolicy
     * @param scalable
     * @param sizeAttribute
     */
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MantisStageMetadataImpl(@JsonProperty("jobId") JobId jobId,
                                   @JsonProperty("stageNum") int stageNum,
                                   @JsonProperty("numStages") int numStages,
                                   @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                                   @JsonProperty("numWorkers") int numWorkers,
                                   @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                                   @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                                   @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                                   @JsonProperty("scalable") boolean scalable,
                                   @JsonProperty("sizeAttribute") String sizeAttribute) {
        this.jobId = jobId;
        this.stageNum = stageNum;
        this.numStages = numStages;
        this.machineDefinition = machineDefinition;
        this.numWorkers = numWorkers;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.scalingPolicy = scalingPolicy;
        this.scalable = scalable;
        this.sizeAttribute = sizeAttribute;
        workerByIndexMetadataSet = new ConcurrentHashMap<>();
        workerByNumberMetadataSet = new ConcurrentHashMap<>();

    }

    @Override
    public JobId getJobId() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantisStageMetadataImpl that = (MantisStageMetadataImpl) o;
        return stageNum == that.stageNum && numStages == that.numStages && numWorkers == that.numWorkers
                && scalable == that.scalable && Objects.equals(jobId, that.jobId)
                && Objects.equals(machineDefinition, that.machineDefinition)
                && Objects.equals(hardConstraints, that.hardConstraints)
                && Objects.equals(softConstraints, that.softConstraints)
                && Objects.equals(scalingPolicy, that.scalingPolicy);
    }

    @Override
    public int hashCode() {

        return Objects.hash(jobId, stageNum, numStages, machineDefinition, numWorkers, hardConstraints,
                softConstraints, scalingPolicy, scalable);
    }


    /**
     * Builder to create an instance of {@link MantisStageMetadataImpl}.
     */
    public static class Builder {

        private JobId jobId;
        private int stageNum = -1;
        private int numStages = 0;
        private MachineDefinition machineDefinition;
        private int numWorkers = 0;
        private List<JobConstraints> hardConstraints = Collections.emptyList();
        private List<JobConstraints> softConstraints = Collections.emptyList();
        private StageScalingPolicy scalingPolicy;
        private boolean scalable;
        private String sizeAttribute;

        /**
         * Ctor.
         */
        public Builder() {

        }

        /**
         * Sets the {@link JobId}.
         * @param jId
         * @return
         */
        public Builder withJobId(JobId jId) {
            this.jobId = jId;
            return this;
        }

        /**
         * Sets the stage number.
         * @param stageNum
         * @return
         */
        public Builder withStageNum(int stageNum) {
            this.stageNum = stageNum;
            return this;
        }

        /**
         * Sets the total number of stages.
         * @param numStages
         * @return
         */
        public Builder withNumStages(int numStages) {
            this.numStages = numStages;
            return this;
        }

        /**
         * Sets the {@link MachineDefinition} to be used by the workers of this stage.
         * @param md
         * @return
         */
        public Builder withMachineDefinition(MachineDefinition md) {
            this.machineDefinition = md;
            return this;
        }

        /**
         * The total number of workers in this stage.
         * @param numWorkers
         * @return
         */
        public Builder withNumWorkers(int numWorkers) {
            this.numWorkers = numWorkers;
            return this;
        }

        /**
         * Sets the mandatory scheduling constraints associated with this stage.
         * @param hardC
         * @return
         */
        public Builder withHardConstraints(List<JobConstraints> hardC) {
            if (hardC != null) {
                this.hardConstraints = hardC;
            }
            return this;
        }

        /**
         * Sets the best effort scheduling constraints associated with this stage.
         * @param softC
         * @return
         */
        public Builder withSoftConstraints(List<JobConstraints> softC) {
            if (softC != null) {
                this.softConstraints = softC;
            }
            return this;
        }

        /**
         * The scaling policy associated with this stage.
         * @param pol
         * @return
         */
        public Builder withScalingPolicy(StageScalingPolicy pol) {
            this.scalingPolicy = pol;
            return this;
        }

        /**
         * Sets the whether this stage is scalable.
         * @param s
         * @return
         */
        public Builder isScalable(boolean s) {
            scalable = s;
            return this;
        }

        public Builder withSizeAttribute(String s) {
            sizeAttribute = s;
            return this;
        }

        /**
         * Convenience method to clone data from an old worker of this stage.
         * @param workerRequest
         * @return
         */
        public Builder from(WorkerRequest workerRequest) {
            Objects.requireNonNull(workerRequest);
            this.jobId = (JobId.fromId(workerRequest.getJobId()).orElse(null));
            this.stageNum = (workerRequest.getWorkerStage());
            this.numStages = (workerRequest.getTotalStages());
            this.machineDefinition = (workerRequest.getDefinition());
            this.numWorkers = (workerRequest.getNumInstancesAtStage());
            this.hardConstraints = (workerRequest.getHardConstraints() != null ? workerRequest.getHardConstraints()
                    : new ArrayList<>());
            this.softConstraints = (workerRequest.getSoftConstraints() != null ? workerRequest.getSoftConstraints()
                    : new ArrayList<>());
            this.scalingPolicy = (workerRequest.getSchedulingInfo().forStage(
                    workerRequest.getWorkerStage()).getScalingPolicy());
            this.scalable = (workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalable());
            this.sizeAttribute = Optional.ofNullable(workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getContainerAttributes()).map(attrs -> attrs.get(MANTIS_STAGE_CONTAINER_SIZE_NAME_KEY)).orElse(null);
            return this;
        }

        /**
         * Builds and returns an instance of {@link MantisStageMetadataImpl}.
         * @return
         */
        public IMantisStageMetadata build() {
            Objects.requireNonNull(jobId, "JobId cannot be null");
            //Objects.requireNonNull(scalingPolicy, "Scaling policy cannot be null");
            if (stageNum <= -1) {
                throw new IllegalArgumentException(String.format("Invalid stage number %d", stageNum));
            }

            if (numStages <= 0) {
                throw new IllegalArgumentException(String.format("Invalid no of stages %d", numStages));
            }


            return new MantisStageMetadataImpl(jobId, stageNum, numStages, machineDefinition, numWorkers,
                    hardConstraints, softConstraints, scalingPolicy, scalable, sizeAttribute);
        }
    }

    /**
     * Updates the total number of workers in this stage.
     * @param numWorkers
     * @param store
     * @throws Exception
     */
    public void unsafeSetNumWorkers(int numWorkers, MantisJobStore store) throws Exception {
        this.numWorkers = numWorkers;

        store.updateStage(this);
    }

    /**
     * Removes the referenced worker from this stage.
     * @param index
     * @param number
     * @param store
     * @return
     */
    public boolean unsafeRemoveWorker(int index, int number, MantisJobStore store) {
        final JobWorker removedIdx = workerByIndexMetadataSet.remove(index);
        final JobWorker removedNum = workerByNumberMetadataSet.remove(number);
        if (removedIdx != null && removedNum != null && removedIdx.getMetadata().getWorkerNumber() == number
                && removedNum.getMetadata().getWorkerIndex() == index) {
            LOGGER.info("Worker index {} - number {} marked for deletion", index, number);
            //        pendingDeleteWorkerMap.put(number, removedNum);
            try {
                archiveWorker(removedIdx.getMetadata(), store);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    @Override
    public List<JobConstraints> getHardConstraints() {
        return Collections.unmodifiableList(hardConstraints);
    }

    @Override
    public List<JobConstraints> getSoftConstraints() {
        return Collections.unmodifiableList(softConstraints);
    }

    @Override
    public StageScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }

    @Override
    public boolean getScalable() {
        return scalable;
    }

    @Override
    public MachineDefinition getMachineDefinition() {
        return machineDefinition;
    }

    @Deprecated
    @JsonIgnore
    @Override
    public Collection<JobWorker> getWorkerByIndexMetadataSet() {
        return Collections.unmodifiableCollection(workerByIndexMetadataSet.values());
    }

    @JsonIgnore
    @Override
    public Collection<JobWorker> getAllWorkers() {
        return Collections.unmodifiableCollection(workerByNumberMetadataSet.values());
    }

    @JsonIgnore
    @Override
    public JobWorker getWorkerByIndex(int workerId) throws InvalidJobException {
        JobWorker worker = workerByIndexMetadataSet.get(workerId);
        if (worker == null)
            throw new InvalidJobException(jobId, -1, workerId);
        return worker;
    }

    @JsonIgnore
    @Override
    public JobWorker getWorkerByWorkerNumber(int workerNumber) throws InvalidJobException {
        JobWorker worker = workerByNumberMetadataSet.get(workerNumber);
        if (worker == null)
            throw new InvalidJobException(jobId, -1, workerNumber);
        return worker;
    }

    @Override
    public Optional<String> getSizeAttribute() {
        return Optional.ofNullable(sizeAttribute);
    }

    /**
     * Remove the given worker from the stage if it is in a terminal state.
     * @param workerNumber
     * @return
     */
    JobWorker removeWorkerInFinalState(int workerNumber) {
        JobWorker worker = workerByNumberMetadataSet.get(workerNumber);
        if (worker != null && WorkerState.isTerminalState(worker.getMetadata().getState())) {
            workerByNumberMetadataSet.remove(workerNumber);
            return worker;
        }
        return null;
    }

    /**
     * Removes any workers from the state that are in a terminal state.
     * @return
     */
    public Collection<JobWorker> removeArchiveableWorkers() {
        Collection<JobWorker> removedWorkers = new LinkedList<>();
        Set<Integer> workerNumbers = new HashSet<>(workerByNumberMetadataSet.keySet());
        for (Integer w : workerNumbers) {
            JobWorker worker = workerByNumberMetadataSet.get(w);
            final JobWorker wi = workerByIndexMetadataSet.get(worker.getMetadata().getWorkerIndex());
            if (wi == null || wi.getMetadata().getWorkerNumber() != worker.getMetadata().getWorkerNumber()) {
                workerByNumberMetadataSet.remove(w);
                removedWorkers.add(worker);
            }
        }
        return removedWorkers;
    }

    /**
     * Replace the old worker with the new worker. New worker has not been scheduled yet so it is
     * just an in memory representation.
     * Invalid conditions:
     * 1. New worker is in error state
     * 2. Old worker Index != new worker Index
     * 3. Given Old worker is in fact the one associated with this index
     * <p>
     * Does the following:
     * 1. Marks old worker as terminated
     * 2. Associates new worker to this index
     * 3. Removes old worker from number -> worker set
     * 4. Saves the data to the store
     * 5. Associates new worker number -> new worker
     * 6. archives the worker
     *
     * @param newWorker
     * @param oldWorker
     * @param jobStore
     */
    public void replaceWorkerIndex(JobWorker newWorker, JobWorker oldWorker, MantisJobStore jobStore)
            throws Exception {
        Preconditions.checkNotNull(newWorker, "Replacement worker cannot be null");
        Preconditions.checkNotNull(oldWorker, "old worker cannot be null");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("In MantisStageMetadataImpl:replaceWorkerIndex oldWorker {} new Worker {} for Job {}",
                    oldWorker, newWorker, this.getJobId());
        }
        IMantisWorkerMetadata newWorkerMetadata = newWorker.getMetadata();
        IMantisWorkerMetadata oldWorkerMetadata = oldWorker.getMetadata();
        int index = newWorkerMetadata.getWorkerIndex();

        boolean result = true;
        // check if new worker is in error state
        if (WorkerState.isErrorState(newWorkerMetadata.getState())) {
            // should not get here
            String errMsg = String.format("New worker cannot be in error state %s", newWorkerMetadata.getState());
            LOGGER.error(errMsg);
            throw new IllegalStateException(errMsg);
        }
        // if old worker is null, ensure no other worker is associated with this index
        if (!workerByIndexMetadataSet.containsKey(index)) {
            // This index is associated with some worker but given oldWorker is null abort.
            String errMsg = String.format("Index %d does not exist in workerByIndexMetadataSet %s for job %s", index,
                    workerByIndexMetadataSet, this.jobId);
            throw new IllegalArgumentException(errMsg);
        } else {
            if (oldWorkerMetadata.getWorkerIndex() != index) {
                String errMsg = String.format("While replacing worker in Job %s, Old worker index %d does not match new %d",
                        this.jobId, oldWorkerMetadata.getWorkerIndex(), index);
                LOGGER.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
            LOGGER.debug("workerByIndexMetadatSet {}", workerByIndexMetadataSet);

            // confirm old worker is present in the workerByIndexSet with the given worker number
            JobWorker worker = workerByIndexMetadataSet.get(index);
            if (worker.getMetadata().getWorkerNumber() != oldWorkerMetadata.getWorkerNumber()) {

                String errMsg = String.format("Did not replace worker %d with %d for index %d of job %s, different worker %d exists already",
                    oldWorkerMetadata.getWorkerNumber(), newWorkerMetadata.getWorkerNumber(), newWorkerMetadata.getWorkerIndex(),
                    jobId, worker.getMetadata().getWorkerNumber());
                throw new IllegalArgumentException(errMsg);
            } else {
                // mark old worker as terminated
                processWorkerEvent(new WorkerTerminate(oldWorkerMetadata.getWorkerId(), WorkerState.Failed,
                        JobCompletedReason.Relaunched, System.currentTimeMillis()), jobStore);

                // insert new worker
                workerByIndexMetadataSet.put(index, newWorker);

                // remove old worker from workerNumberSet
                removeWorkerInFinalState(oldWorkerMetadata.getWorkerNumber());
                // persist changes
                jobStore.replaceTerminatedWorker(oldWorkerMetadata, newWorkerMetadata);

                workerByNumberMetadataSet.put(newWorkerMetadata.getWorkerNumber(), newWorker);
                // archive worker
                try {
                    archiveWorker(oldWorkerMetadata, jobStore);
                } catch (Exception e) {
                    LOGGER.error("Exception archiving worker", e);
                }
                LOGGER.info("Replaced worker {} with {} for index {} of job {}", oldWorkerMetadata.getWorkerNumber(),
                    newWorkerMetadata.getWorkerNumber(), newWorkerMetadata.getWorkerIndex(), jobId);
            }

        }


    }

    private void archiveWorker(IMantisWorkerMetadata worker, MantisJobStore jobStore) throws IOException {
        jobStore.archiveWorker(worker);
    }

    /**
     * Adds the given {@link JobWorker} to this stage.
     * @param newWorker
     * @return
     */
    public boolean addWorkerIndex(JobWorker newWorker) {

        IMantisWorkerMetadata newWorkerMetadata = newWorker.getMetadata();
        if (workerByIndexMetadataSet.putIfAbsent(newWorkerMetadata.getWorkerIndex(), newWorker) != null) {
            LOGGER.warn("WorkerIndex {} already exists. Existing worker={} ",
                    newWorkerMetadata.getWorkerIndex(), workerByIndexMetadataSet.get(
                            newWorkerMetadata.getWorkerIndex()));
            return false;
        }

        workerByNumberMetadataSet.put(newWorkerMetadata.getWorkerNumber(), newWorker);
        return true;

    }

    /**
     * Updates the the state of a worker based on the worker event.
     * @param event
     * @param jobStore
     * @return
     */
    public Optional<JobWorker> processWorkerEvent(WorkerEvent event, MantisJobStore jobStore) {
        try {
            JobWorker worker = getWorkerByIndex(event.getWorkerId().getWorkerIndex());
            try {
                worker.processEvent(event, jobStore);
            } catch (InvalidWorkerStateChangeException wex) {
                LOGGER.warn("InvalidWorkerStateChangeException from: ", wex);
            }

            return of(worker);
        } catch (Exception e) {
            LOGGER.warn("Exception saving worker update", e);
        }
        return Optional.empty();
    }

    /**
     * Iterates through all workers of this stage and returns true if all workers are in started state.
     * @return
     */
    @JsonIgnore
    public boolean isAllWorkerStarted() {

        for (JobWorker w : workerByIndexMetadataSet.values()) {
            if (!w.getMetadata().getState().equals(WorkerState.Started))
                return false;
        }
        return true;
    }

    /**
     * Iterates through all workers of this stage and returns true if all workers are in terminal state.
     * @return
     */
    @JsonIgnore
    public boolean isAllWorkerCompleted() {
        for (JobWorker w : workerByIndexMetadataSet.values()) {
            if (!WorkerState.isTerminalState(w.getMetadata().getState())) {
                LOGGER.debug("isAllWorkerCompleted returns false");
                return false;
            }
        }
        LOGGER.info("isAllWorkerCompleted returns true");
        return true;
    }

    /**
     * Returns the number of workers that are in started state.
     * @return
     */
    @JsonIgnore
    public int getNumStartedWorkers() {
        int startedCount = 0;
        for (JobWorker w : workerByIndexMetadataSet.values()) {
            if (w.getMetadata().getState().equals(WorkerState.Started))
                startedCount++;
        }
        return startedCount;
    }

    @Override
    public String toString() {
        return "MantisStageMetadataImpl [jobId=" + jobId + ", stageNum=" + stageNum + ", numStages=" + numStages
                + ", machineDefinition=" + machineDefinition + ", numWorkers=" + numWorkers + ", hardConstraints="
                + hardConstraints + ", softConstraints=" + softConstraints + ", scalingPolicy=" + scalingPolicy
                + ", scalable=" + scalable + ", workerByIndexMetadataSet=" + workerByIndexMetadataSet
                + ", workerByNumberMetadataSet=" + workerByNumberMetadataSet + "]";
    }


}
