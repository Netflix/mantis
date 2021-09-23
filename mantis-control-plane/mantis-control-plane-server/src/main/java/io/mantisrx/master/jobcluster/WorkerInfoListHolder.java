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

package io.mantisrx.master.jobcluster;

import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.server.master.domain.JobId;
import java.util.List;

public class WorkerInfoListHolder {

    private final JobId jobId;
    private final List<IMantisWorkerMetadata> workerMetadataList;

    public WorkerInfoListHolder(JobId jobId, List<IMantisWorkerMetadata> workerMetadataList) {
        this.jobId = jobId;
        this.workerMetadataList = workerMetadataList;
    }

    public JobId getJobId() {
        return jobId;
    }

    public List<IMantisWorkerMetadata> getWorkerMetadataList() {
        return workerMetadataList;
    }

    @Override
    public String toString() {
        return "WorkerInfoListHolder{"
                + " jobId=" + jobId
                + ", workerMetadataList=" + workerMetadataList
                + '}';
    }
}
