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

package io.mantisrx.server.master.domain;

import java.util.Optional;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkerId {

    private static final Logger logger = LoggerFactory.getLogger(WorkerId.class);
    private static final String DELIMITER = "-";
    private static final String WORKER_DELIMITER = "-worker-";
    private final String jobCluster;
    private final String jobId;
    private final int wIndex;
    private final int wNum;
    private final String id;

    public WorkerId(final String jobId,
                    final int wIndex,
                    final int wNum) {
        this(WorkerId.getJobClusterFromId(jobId), jobId, wIndex, wNum);
    }

    public WorkerId(final String jobCluster,
                    final String jobId,
                    final int wIndex,
                    final int wNum) {
        Preconditions.checkNotNull(jobCluster, "jobCluster");
        Preconditions.checkNotNull(jobId, "jobId");
        Preconditions.checkArgument(wIndex >= 0);
        Preconditions.checkArgument(wNum >= 0);
        this.jobCluster = jobCluster;
        this.jobId = jobId;
        this.wIndex = wIndex;
        this.wNum = wNum;
        this.id = new StringBuilder()
                .append(jobId)
                .append(WORKER_DELIMITER)
                .append(wIndex)
                .append('-')
                .append(wNum)
                .toString();
    }

    private static String getJobClusterFromId(final String jobId) {
        final int jobClusterIdx = jobId.lastIndexOf(DELIMITER);
        if (jobClusterIdx > 0) {
            return jobId.substring(0, jobClusterIdx);
        } else {
            logger.error("Failed to get JobCluster name from Job ID {}", jobId);
            throw new IllegalArgumentException("Job ID is invalid " + jobId);
        }
    }

    /*
    Returns a valid WorkerId only if the passed 'id' string is well-formed.
    There are some instances in Master currently where we could get back index = -1 which would fail to get a valid WorkerId from String.
     */
    public static Optional<WorkerId> fromId(final String id) {
        final int workerDelimIndex = id.indexOf(WORKER_DELIMITER);
        if (workerDelimIndex > 0) {
            final String jobId = id.substring(0, workerDelimIndex);
            final int jobClusterIdx = jobId.lastIndexOf(DELIMITER);
            if (jobClusterIdx > 0) {
                final String jobCluster = jobId.substring(0, jobClusterIdx);
                final String workerInfo = id.substring(workerDelimIndex + WORKER_DELIMITER.length());

                final int delimiterIndex = workerInfo.indexOf(DELIMITER);
                if (delimiterIndex > 0) {
                    try {
                        final int wIndex = Integer.parseInt(workerInfo.substring(0, delimiterIndex));
                        final int wNum = Integer.parseInt(workerInfo.substring(delimiterIndex + 1));
                        return Optional.of(new WorkerId(jobCluster, jobId, wIndex, wNum));
                    } catch (NumberFormatException nfe) {
                        logger.warn("failed to parse workerId from {}", id, nfe);
                    }
                }
            }
        }
        logger.warn("failed to parse workerId from {}", id);
        return Optional.empty();
    }

    public String getJobCluster() {
        return jobCluster;
    }

    public String getJobId() {
        return jobId;
    }

    public int getWorkerIndex() {
        return wIndex;
    }

    public int getWorkerNum() {
        return wNum;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WorkerId workerId = (WorkerId) o;

        if (wIndex != workerId.wIndex) return false;
        if (wNum != workerId.wNum) return false;
        if (!jobCluster.equals(workerId.jobCluster)) return false;
        if (!jobId.equals(workerId.jobId)) return false;
        return id.equals(workerId.id);
    }

    @Override
    public int hashCode() {
        int result = jobCluster.hashCode();
        result = 31 * result + jobId.hashCode();
        result = 31 * result + wIndex;
        result = 31 * result + wNum;
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return id;
    }
}
