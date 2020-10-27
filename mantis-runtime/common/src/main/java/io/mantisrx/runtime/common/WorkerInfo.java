/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.runtime.common;

import io.mantisrx.common.WorkerPorts;


public class WorkerInfo {

    private final String jobClusterName;
    private final String jobId;
    private final int stageNumber;
    private final int workerIndex;
    private final int workerNumber;
    private final String host;

    private final WorkerPorts workerPorts;
    private final MantisJobDurationType durationType;

    public WorkerInfo(String jobClusterName, String jobId, int stageNumber, int workerIndex, int workerNumber,
                      MantisJobDurationType durationType, String host) {
        this(jobClusterName, jobId, stageNumber, workerIndex, workerNumber, durationType, host, new WorkerPorts(-1,
            -1, -1, -1, -1));
    }

    public WorkerInfo(String jobClusterName, String jobId, int stageNumber, int workerIndex, int workerNumber,
                      MantisJobDurationType durationType, String host, WorkerPorts workerPorts) {
        this.jobClusterName = jobClusterName;
        this.jobId = jobId;
        this.stageNumber = stageNumber;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.durationType = durationType;
        this.host = host;
        this.workerPorts = workerPorts;

    }

    public String getJobClusterName() {
        return jobClusterName;
    }

    public String getJobId() {
        return jobId;
    }

    public int getStageNumber() {
        return stageNumber;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public MantisJobDurationType getDurationType() {
        return durationType;
    }

    public String getHost() {
        return host;
    }

    public WorkerPorts getWorkerPorts() {

        return workerPorts;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkerInfo{");
        sb.append("jobName='").append(jobClusterName).append('\'');
        sb.append(", jobId='").append(jobId).append('\'');
        sb.append(", stageNumber=").append(stageNumber);
        sb.append(", workerIndex=").append(workerIndex);
        sb.append(", workerNumber=").append(workerNumber);
        sb.append(", host='").append(host).append('\'');
        sb.append(", workerPorts=").append(workerPorts);
        sb.append(", durationType=").append(durationType);
        sb.append('}');
        return sb.toString();
    }


}


