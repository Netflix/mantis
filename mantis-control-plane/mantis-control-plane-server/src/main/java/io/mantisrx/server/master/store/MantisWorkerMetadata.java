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

import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.WorkerId;
import java.util.List;
import java.util.Optional;


/**
 * Metadata object for a Mantis worker. Modification operations do not perform locking. Instead, a lock can be
 * obtained via the <code>obtainLock()</code> method which is an instance of {@link AutoCloseable}.
 */
public interface MantisWorkerMetadata {

    int getWorkerIndex();

    int getWorkerNumber();

    WorkerId getWorkerId();

    String getJobId();

    int getStageNum();

    int getMetricsPort();

    int getDebugPort();

    int getConsolePort();

    int getCustomPort();

    // cluster on which the worker was launched
    Optional<String> getCluster();

    /**
     * Get number of ports for this worker, including the metrics port
     *
     * @return The number of ports
     */
    int getNumberOfPorts();

    List<Integer> getPorts();

    void addPorts(List<Integer> ports);

    int getTotalResubmitCount();

    /**
     * Get the worker number (not index) of which this is a resubmission of.
     *
     * @return
     */
    int getResubmitOf();

    MantisJobState getState();

    String getSlave();

    String getSlaveID();

    long getAcceptedAt();

    long getLaunchedAt();

    long getStartingAt();

    long getStartedAt();

    long getCompletedAt();

    JobCompletedReason getReason();

}
