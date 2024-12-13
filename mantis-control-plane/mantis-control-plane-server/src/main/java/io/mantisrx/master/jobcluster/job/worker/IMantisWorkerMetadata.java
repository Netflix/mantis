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
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonSubTypes;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Instant;
import java.util.Optional;

/**
 * Metadata object for a Mantis worker. Modification operations do not perform locking. Instead, a lock can be
 * obtained via the <code>obtainLock()</code> method which is an instance of {@link java.lang.AutoCloseable}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MantisWorkerMetadataImpl.class)
})

public interface IMantisWorkerMetadata {

    /**
     * Index assigned to this worker.
     * @return
     */
    int getWorkerIndex();

    /**
     * Number assigned to this worker.
     * @return
     */
    int getWorkerNumber();

    /**
     * JobId of the Job this worker belongs to.
     * @return
     */
    String getJobId();

    /**
     * Returns the {@link JobId} for this worker.
     * @return
     */
    @JsonIgnore
    JobId getJobIdObject();

    /**
     * Returns the {@link WorkerId} associated with this worker.
     * @return
     */
    WorkerId getWorkerId();

    /**
     * The stage number this worker belongs to.
     * @return
     */
    int getStageNum();

    /**
     * @return the {@link WorkerPorts} for this worker.
     */
    WorkerPorts getWorkerPorts();

    /**
     * The port on which Metrics stream is served.
     * @return
     */
    int getMetricsPort();

    /**
     * The port which can be used to connect jconsole to.
     * @return
     */
    int getDebugPort();

    /**
     * A custom port associated with Netflix Admin console if enabled.
     * @return
     */
    int getConsolePort();

    /**
     * A free form port to be used by the job for any purpose.
     * @return
     */
    int getCustomPort();

    /**
     * The port which can be used to connect to other workers.
     *
     * @return
     */
    int getSinkPort();


    /**
     * The AWS cluster on which the worker was launched.
     * Used to maintain affinity during deploys.
     * @return
     */
    Optional<String> getCluster();

    Optional<ClusterID> getResourceCluster();

    /**
     * Get number of ports for this worker, including the metrics port.
     * @return The number of ports
     */
    int getNumberOfPorts();

    /**
     * Returns an optional of {@link WorkerPorts} associated with this worker.
     * @return
     */
    Optional<WorkerPorts> getPorts();

    /**
     * A count of the number of times this worker has been resubmitted.
     * @return
     */
    int getTotalResubmitCount();

    /**
     * Get the worker number (not index) of which this is a resubmission of.
     * @return
     */
    int getResubmitOf();

    /**
     * Returns the current {@link WorkerState} of this worker.
     * @return
     */
    WorkerState getState();

    /**
     * Returns the mesos slave on which this worker is executing.
     * @return
     */
    String getSlave();

    /**
     * Returns the mesos slaveId on which this worker is executing.
     * @return
     */
    String getSlaveID();


    /**
     * Returns whether a listener exists that is streaming the results computed by this worker.
     * @return
     */
    boolean getIsSubscribed();

    /**
     * The timestamp at which this worker went into Accepted state.
     * @return
     */
    long getAcceptedAt();

    /**
     * The timestamp at which this worker landed on a mesos slave.
     * @return
     */

    long getLaunchedAt();

    /**
     * Whether this worker has entered launched state.
     * @return true if launched.
     */
    boolean hasLaunched();

    /**
     * The timestamp at which this worker started initialization.
     * @return
     */

    long getStartingAt();

    /**
     * The timestamp the worker reported as running.
     * @return
     */
    long getStartedAt();

    /**
     * The timestamp the worker was marked for termination.
     * @return
     */
    long getCompletedAt();

    /**
     * If in terminal state returns the reason for completion.
     * @return
     */
    JobCompletedReason getReason();

    /**
     * The preferred AWS cluster on which to schedule this worker.
     * @return
     */
    Optional<String> getPreferredClusterOptional();

    /**
     * The last time a heartbeat was received from this worker.
     * @return
     */
    Optional<Instant> getLastHeartbeatAt();

}
