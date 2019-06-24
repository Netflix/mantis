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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.WorkerId;


public class MantisWorkerMetadataWritable implements MantisWorkerMetadata {

    @JsonIgnore
    private final WorkerId workerId;
    @JsonIgnore
    private final ReentrantLock lock = new ReentrantLock();
    private int workerIndex;
    private int workerNumber;
    private String jobId;
    private int stageNum;
    private int numberOfPorts;
    private int metricsPort;
    private int consolePort;
    private int debugPort = -1;
    private int customPort;
    private List<Integer> ports;
    private volatile MantisJobState state;
    private String slave;
    private String slaveID;
    private Optional<String> cluster = Optional.empty();
    private long acceptedAt = 0;
    private long launchedAt = 0;
    private long startingAt = 0;
    private long startedAt = 0;
    private long completedAt = 0;
    private JobCompletedReason reason;
    private int resubmitOf = -1;
    private int totalResubmitCount = 0;
    @JsonIgnore
    private volatile long lastHeartbeatAt = 0;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MantisWorkerMetadataWritable(@JsonProperty("workerIndex") int workerIndex,
                                        @JsonProperty("workerNumber") int workerNumber,
                                        @JsonProperty("jobId") String jobId,
                                        @JsonProperty("stageNum") int stageNum,
                                        @JsonProperty("numberOfPorts") int numberOfPorts) {
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobId = jobId;
        this.workerId = new WorkerId(jobId, workerIndex, workerNumber);
        this.stageNum = stageNum;
        this.numberOfPorts = numberOfPorts;
        this.state = MantisJobState.Accepted;
        this.acceptedAt = System.currentTimeMillis();
        this.ports = new ArrayList<>();
    }

    @Override
    public int getWorkerIndex() {
        return workerIndex;
    }

    @Override
    public int getWorkerNumber() {
        return workerNumber;
    }

    @Override
    public WorkerId getWorkerId() {
        return workerId;
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
    public int getNumberOfPorts() {
        return numberOfPorts;
    }

    @Override
    public List<Integer> getPorts() {
        return ports;
    }

    @Override
    public void addPorts(List<Integer> ports) {
        this.ports.addAll(ports);
    }

    @Override
    public int getTotalResubmitCount() {
        return totalResubmitCount;
    }

    @Override
    public int getMetricsPort() {
        return metricsPort;
    }

    public void setMetricsPort(int metricsPort) {
        this.metricsPort = metricsPort;
    }

    @Override
    public int getDebugPort() {
        return debugPort;
    }

    public void setDebugPort(int debugPort) {
        this.debugPort = debugPort;
    }

    @Override
    public int getConsolePort() {
        return consolePort;
    }

    public void setConsolePort(int port) {
        this.consolePort = port;
    }

    @Override
    public int getCustomPort() {
        return customPort;
    }

    public void setCustomPort(int port) {
        this.customPort = port;
    }

    @Override
    public int getResubmitOf() {
        return resubmitOf;
    }

    @JsonIgnore
    public void setResubmitInfo(int resubmitOf, int totalCount) {
        this.resubmitOf = resubmitOf;
        this.totalResubmitCount = totalCount;
    }

    @JsonIgnore
    public long getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    @JsonIgnore
    public void setLastHeartbeatAt(long lastHeartbeatAt) {
        this.lastHeartbeatAt = lastHeartbeatAt;
    }

    private void validateStateChange(MantisJobState newState) throws InvalidJobStateChangeException {
        if (!state.isValidStateChgTo(newState))
            throw new InvalidJobStateChangeException(jobId, state, newState);
    }

    /**
     * Added for use by new Mantis Master to reuse old DAOs
     * Does not do state transition validation
     *
     * @param state
     * @param when
     * @param reason
     */
    public void setStateNoValidation(MantisJobState state, long when, JobCompletedReason reason) {

        this.state = state;
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
            this.reason = reason == null ? JobCompletedReason.Lost : reason;
            break;
        case Completed:
            this.completedAt = when;
            this.reason = reason == null ? JobCompletedReason.Normal : reason;
            break;
        default:
            assert false : "Unexpected job state to set";
        }
    }

    public void setState(MantisJobState state, long when, JobCompletedReason reason) throws InvalidJobStateChangeException {
        validateStateChange(state);
        this.state = state;
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
            this.reason = reason == null ? JobCompletedReason.Lost : reason;
            break;
        case Completed:
            this.completedAt = when;
            this.reason = reason == null ? JobCompletedReason.Normal : reason;
            break;
        default:
            assert false : "Unexpected job state to set";
        }
    }

    @Override
    public MantisJobState getState() {
        return state;
    }

    @Override
    public String getSlave() {
        return slave;
    }

    public void setSlave(String slave) {
        this.slave = slave;
    }

    public Optional<String> getCluster() {
        return cluster;
    }

    public void setCluster(final Optional<String> cluster) {
        this.cluster = cluster;
    }

    @Override
    public String getSlaveID() {
        return slaveID;
    }

    public void setSlaveID(String slaveID) {
        this.slaveID = slaveID;
    }

    @Override
    public long getAcceptedAt() {
        return acceptedAt;
    }

    public void setAcceptedAt(long when) {
        this.acceptedAt = when;
    }

    @Override
    public long getLaunchedAt() {
        return launchedAt;
    }

    public void setLaunchedAt(long when) {
        this.launchedAt = when;
    }

    @Override
    public long getStartingAt() {
        return startingAt;
    }

    public void setStartingAt(long when) {
        this.startingAt = when;
    }

    @Override
    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long when) {
        this.startedAt = when;
    }

    @Override
    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long when) {
        this.completedAt = when;
    }

    @Override
    public JobCompletedReason getReason() {
        return reason;
    }

    public void setReason(JobCompletedReason reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "Worker " + workerNumber + " state=" + state + ", acceptedAt=" + acceptedAt +
                ((launchedAt == 0) ? "" : ", launchedAt=" + launchedAt) +
                ((startingAt == 0) ? "" : ", startingAt=" + startingAt) +
                ((startedAt == 0) ? "" : ", startedAt=" + startedAt) +
                ((completedAt == 0) ? "" : ", completedAt=" + completedAt) +
                ", #ports=" + ports.size() + ", ports=" + ports;
    }

}
