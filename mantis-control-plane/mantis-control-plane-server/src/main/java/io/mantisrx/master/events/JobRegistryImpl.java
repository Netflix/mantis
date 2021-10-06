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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JobRegistryImpl implements JobRegistry {

    ConcurrentMap<String, IJobClusterMetadata> jobClusterMap = new ConcurrentHashMap<>() ;

    ConcurrentMap<JobId, IMantisJobMetadata> jobMap = new ConcurrentHashMap<>();

    ConcurrentMap<String, Set<IMantisJobMetadata>> clusterToJobsMap = new ConcurrentHashMap<>();


    @Override
    public void addClusters(List<IJobClusterMetadata> jobClusters) {
        jobClusters.forEach((jc) -> {
            jobClusterMap.put(jc.getJobClusterDefinition().getName(), jc);
        });

    }

    @Override
    public void updateCluster(IJobClusterMetadata clusterMetadata) {
        jobClusterMap.put(clusterMetadata.getJobClusterDefinition().getName(), clusterMetadata);
    }

    @Override
    public void deleteJobCluster(String clusterName) {
        jobClusterMap.remove(clusterName);

    }

    @Override
    public void addJobs(String clusterName, List<IMantisJobMetadata> jobList) {
        jobList.forEach((jb) -> {
            jobMap.put(jb.getJobId(),jb);

        });

        clusterToJobsMap.computeIfAbsent(clusterName,(x -> new HashSet<>())).addAll(jobList);

    }

    @Override
    public void addCompletedJobs(List<JobClusterDefinitionImpl.CompletedJob> completedJobList) {

    }

    @Override
    public void updateJob(IMantisJobMetadata jobMetadata) {

    }


    @Override
    public void removeJob(JobId jobId) {

    }
}
