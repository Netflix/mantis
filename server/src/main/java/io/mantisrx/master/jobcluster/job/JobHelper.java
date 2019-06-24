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

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.master.jobcluster.job.worker.WorkerTerminate;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * General Job utility methods.
 */
public final class JobHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobHelper.class);

    private JobHelper() {

    }
    /**
     * Give scheduling info and whether job has Job Master return the list of user stages.
     * Job Master stage is considered a system stage so is excluded if present.
     * @param schedulingInfo
     * @param hasJobMaster
     * @return
     */
    public static List<Integer> getUserStageNumbers(SchedulingInfo schedulingInfo, boolean hasJobMaster) {
        List<Integer> stageNumbers = new ArrayList<>();

        int totalStages = schedulingInfo.getStages().size();
        if (hasJobMaster) {
            totalStages = totalStages - 1;
        }
        for (int i = 1; i <= totalStages; i++) {
            stageNumbers.add(i);
        }

        return stageNumbers;

    }

    /**
     * Determines whether a workerevent is terminal.
     * @param workerEvent
     * @return
     */
    public static boolean isTerminalWorkerEvent(WorkerEvent workerEvent) {
        if (workerEvent instanceof WorkerTerminate) {
            return true;
        } else if (workerEvent instanceof WorkerStatus) {
            WorkerStatus status = (WorkerStatus) workerEvent;
            if (WorkerState.isTerminalState(status.getState())) {
                return true;
            }
        } else if (workerEvent instanceof WorkerResourceStatus) {
            WorkerResourceStatus.VMResourceState state = ((WorkerResourceStatus) workerEvent).getState();
            if (WorkerResourceStatus.VMResourceState.FAILED.equals(state)
                    || WorkerResourceStatus.VMResourceState.COMPLETED.equals(state)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extract hostname from worker event if present.
     * @param event
     * @return
     */
    public static Optional<String> getWorkerHostFromWorkerEvent(WorkerEvent event) {
        Optional<String> host = empty();
        if (event instanceof WorkerLaunched) {
            host = ofNullable(((WorkerLaunched) event).getHostname());
        } else if (event instanceof WorkerHeartbeat) {
            host = ofNullable(((WorkerHeartbeat) event).getStatus().getHostname());
        } else {
            LOGGER.warn("Host name unknown for workerId {}", event.getWorkerId());
        }
        return host;
    }
}
