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

package io.mantisrx.master.jobcluster.job.worker;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.exceptions.InvalidWorkerStateChangeException;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Holds metadata related to a Mantis Worker.
 */
@JsonFilter("topLevelFilter")
public class MantisWorkerMetadataImpl implements IMantisWorkerMetadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantisWorkerMetadataImpl.class);
    /**
     * metrics, debug, console and custom port.
     */
    @JsonIgnore
    public static final int MANTIS_SYSTEM_ALLOCATED_NUM_PORTS = 4;
    private final int workerIndex;
    private int workerNumber;
    private String jobId;
    @JsonIgnore
    private JobId jobIdObj;
    private final int stageNum;
    private final int numberOfPorts;
    @JsonIgnore
    private final WorkerId workerId;
    private WorkerPorts workerPorts;


    private volatile WorkerState state;
    private volatile String slave;
    private volatile String slaveID;
    private volatile long acceptedAt = 0;
    private volatile long launchedAt = 0;
    private volatile long startingAt = 0;
    private volatile long startedAt = 0;
    private volatile long completedAt = 0;

    private volatile JobCompletedReason reason;
    private volatile int resubmitOf = -1;
    private volatile int totalResubmitCount = 0;
    @JsonIgnore
    private volatile Optional<Instant> lastHeartbeatAt = Optional.empty();

    private volatile boolean subscribed;

    private volatile Optional<String> preferredCluster;

    private volatile Optional<ClusterID> resourceCluster;

    /**
     * Creates an instance of this class.
     * @param workerIndex
     * @param workerNumber
     * @param jobId
     * @param stageNum
     * @param numberOfPorts
     * @param workerPorts
     * @param state
     * @param slave
     * @param slaveID
     * @param acceptedAt
     * @param launchedAt
     * @param startingAt
     * @param startedAt
     * @param completedAt
     * @param reason
     * @param resubmitOf
     * @param totalResubmitCount
     * @param preferredCluster
     */
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MantisWorkerMetadataImpl(@JsonProperty("workerIndex") int workerIndex,
            @JsonProperty("workerNumber") int workerNumber,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("stageNum") int stageNum,
            @JsonProperty("numberOfPorts") int numberOfPorts,
            @JsonProperty("workerPorts") WorkerPorts workerPorts,
            @JsonProperty("state") WorkerState state,
            @JsonProperty("slave") String slave,
            @JsonProperty("slaveID") String slaveID,
            @JsonProperty("acceptedAt") long acceptedAt,
            @JsonProperty("launchedAt") long launchedAt,
            @JsonProperty("startingAt") long startingAt,
            @JsonProperty("startedAt") long startedAt,
            @JsonProperty("completedAt") long completedAt,
            @JsonProperty("reason") JobCompletedReason reason,
            @JsonProperty("resubmitOf") int resubmitOf,
            @JsonProperty("totalResubmitCount") int totalResubmitCount,
            @JsonProperty("preferredCluster") Optional<String> preferredCluster,
            @JsonProperty("resourceCluster") Optional<ClusterID> resourceCluster
            ) {
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobId = jobId;
        this.jobIdObj = JobId.fromId(jobId).orElseThrow(() -> new IllegalArgumentException(
                "jobId format is invalid" + jobId));
        this.workerId = new WorkerId(jobId, workerIndex, workerNumber);
        this.stageNum = stageNum;
        this.numberOfPorts = numberOfPorts;
        this.workerPorts = workerPorts;
        this.state = state;
        this.slave = slave;
        this.slaveID = slaveID;

        this.state = state;
        this.acceptedAt = acceptedAt;
        this.launchedAt = launchedAt;
        this.completedAt = completedAt;
        this.startedAt = startedAt;
        this.startingAt = startingAt;

        this.reason = reason;
        this.resubmitOf = resubmitOf;
        this.totalResubmitCount = totalResubmitCount;
        this.preferredCluster = preferredCluster;
        this.resourceCluster = resourceCluster;


        this.totalResubmitCount = totalResubmitCount;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }


    public int getWorkerNumber() {
        return workerNumber;
    }


    public String getJobId() {
        return jobId;
    }

    public JobId getJobIdObject() {
        return jobIdObj;
    }

    public WorkerId getWorkerId() {
        return workerId;
    }

    public int getStageNum() {
        return stageNum;
    }

    public int getNumberOfPorts() {
        return numberOfPorts;
    }

    public Optional<WorkerPorts> getPorts() {
        return Optional.ofNullable(workerPorts);
    }

    public WorkerPorts getWorkerPorts() {
        return this.workerPorts;
    }

    void addPorts(final WorkerPorts ports) {
        this.workerPorts = (ports);
    }

    public int getTotalResubmitCount() {
        return totalResubmitCount;
    }

    public int getMetricsPort() {
        return workerPorts == null ? -1 : workerPorts.getMetricsPort();
    }

    public int getDebugPort() {
        return workerPorts == null ? -1 : workerPorts.getDebugPort();
    }

    public int getConsolePort() {
        return workerPorts == null ? -1 : workerPorts.getConsolePort();
    }
    public int getCustomPort() {
        return workerPorts == null ? -1 : workerPorts.getCustomPort();
    }

    public int getSinkPort() {
        return workerPorts == null ? -1 : workerPorts.getSinkPort();
    }

    public int getResubmitOf() {
        return resubmitOf;
    }

    @JsonIgnore
    private void setResubmitInfo(int resubmitOf, int totalCount) {
        this.resubmitOf = resubmitOf;
        this.totalResubmitCount = totalCount;
    }

    @JsonIgnore
    public Optional<Instant> getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    @JsonIgnore
    void setLastHeartbeatAt(long lastHeartbeatAt) {
        this.lastHeartbeatAt = Optional.of(Instant.ofEpochMilli(lastHeartbeatAt));
    }

    private void validateStateChange(WorkerState newState) throws InvalidWorkerStateChangeException {
        if (!WorkerState.isValidStateChgTo(state, newState))
            throw new InvalidWorkerStateChangeException(jobId, workerId, state, newState);
    }

    /**
     * Update the state of the worker.
     * @param newState
     * @param when
     * @param reason
     * @throws InvalidWorkerStateChangeException
     */
    void setState(WorkerState newState, long when, JobCompletedReason reason) throws InvalidWorkerStateChangeException {
        WorkerState previousState = this.state;
        validateStateChange(newState);
        this.state = newState;
        LOGGER.info("Worker {} State changed from {} to {}", this.workerId, previousState, state);
        switch (state) {
        case Accepted:
            this.acceptedAt = when;
            break;
        case Launched:
            this.launchedAt = when;
            break;
        case StartInitiated:
            this.startingAt = when;
            break;
        case Started:
            this.startedAt = when;
            break;
        case Failed:
            this.completedAt = when;
            LOGGER.info("Worker {} failedAt  {}", this.workerId, when);
            this.reason = reason == null ? JobCompletedReason.Lost : reason;
            break;
        case Completed:
            this.completedAt = when;
            LOGGER.info("Worker {} completedAt  {}", this.workerId, when);
            this.reason = reason == null ? JobCompletedReason.Normal : reason;
            break;
        default:
            assert false : "Unexpected job state to set";
        }
    }


    public WorkerState getState() {
        return state;
    }

    void setSlave(String slave) {
        this.slave = slave;
    }


    public String getSlave() {
        return slave;
    }

    void setSlaveID(String slaveID) {
        this.slaveID = slaveID;
    }

    void setCluster(Optional<String> cluster) {
        this.preferredCluster = cluster;
    }

    void setResourceCluster(ClusterID resourceCluster) {
        this.resourceCluster = Optional.of(resourceCluster);
    }


    public String getSlaveID() {
        return slaveID;
    }


    public long getAcceptedAt() {
        return acceptedAt;
    }


    public long getLaunchedAt() {
        return launchedAt;
    }

    public boolean hasLaunched() {
        return launchedAt > 0;
    }


    public long getStartingAt() {
        return startingAt;
    }


    public long getStartedAt() {
        return startedAt;
    }


    public long getCompletedAt() {
        return completedAt;
    }


    void setIsSubscribed(boolean isSub) {
        this.subscribed = isSub;

    }

    public boolean getIsSubscribed() {
        return this.subscribed;
    }


    public JobCompletedReason getReason() {
        return reason;
    }


    @Override
    public Optional<String> getPreferredClusterOptional() {

        return this.preferredCluster;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantisWorkerMetadataImpl that = (MantisWorkerMetadataImpl) o;
        return workerIndex == that.workerIndex
                && workerNumber == that.workerNumber
                && stageNum == that.stageNum
                && numberOfPorts == that.numberOfPorts
                && acceptedAt == that.acceptedAt
                && launchedAt == that.launchedAt
                && startingAt == that.startingAt
                && startedAt == that.startedAt
                && completedAt == that.completedAt
                && resubmitOf == that.resubmitOf
                && totalResubmitCount == that.totalResubmitCount
                && Objects.equals(jobId, that.jobId)
                && Objects.equals(workerId, that.workerId)
                && Objects.equals(workerPorts, that.workerPorts)
                && state == that.state
                && Objects.equals(slave, that.slave)
                && Objects.equals(slaveID, that.slaveID)
                && reason == that.reason
                && Objects.equals(preferredCluster, that.preferredCluster)
                && Objects.equals(resourceCluster, that.resourceCluster);
    }

    @Override
    public int hashCode() {

        return Objects.hash(workerIndex, workerNumber, jobId, stageNum, numberOfPorts,
                workerId, workerPorts, state, slave, slaveID, acceptedAt, launchedAt,
                startingAt, startedAt, completedAt, reason, resubmitOf, totalResubmitCount, preferredCluster);
    }

    @Override
    public String toString() {
        return "MantisWorkerMetadataImpl{"
                + "workerIndex=" + workerIndex
                + ", workerNumber=" + workerNumber
                + ", jobId=" + jobId
                + ", stageNum=" + stageNum
                + ", numberOfPorts=" + numberOfPorts
                + ", workerId=" + workerId
                + ", workerPorts=" + workerPorts
                + ", state=" + state
                + ", slave='" + slave + '\''
                + ", slaveID='" + slaveID + '\''
                + ", acceptedAt=" + acceptedAt
                + ", launchedAt=" + launchedAt
                + ", startingAt=" + startingAt
                + ", startedAt=" + startedAt
                + ", completedAt=" + completedAt
                + ", reason=" + reason
                + ", resubmitOf=" + resubmitOf
                + ", totalResubmitCount=" + totalResubmitCount
                + ", lastHeartbeatAt=" + lastHeartbeatAt
                + ", subscribed=" + subscribed
                + ", preferredCluster=" + preferredCluster
                + ", resourceCluster=" + resourceCluster
                + '}';
    }

    @Override
    public Optional<String> getCluster() {
        return this.preferredCluster;
    }

    @Override
    public Optional<ClusterID> getResourceCluster() {
        return this.resourceCluster;
    }
}
