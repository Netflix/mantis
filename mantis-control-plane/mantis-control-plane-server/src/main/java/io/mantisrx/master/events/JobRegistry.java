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

package io.mantisrx.master.events;

import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobId;
import java.util.List;

public interface JobRegistry {

    public void addClusters(List<IJobClusterMetadata> jobClusters);

    public void updateCluster(IJobClusterMetadata clusterMetadata);

    public void deleteJobCluster(String clusterName);



    void addJobs(String clusterName, List<IMantisJobMetadata> jobList);

    public void addCompletedJobs(List<JobClusterDefinitionImpl.CompletedJob> completedJobList);

    public void updateJob(IMantisJobMetadata jobMetadata);

    public void removeJob(JobId jobId);



}
