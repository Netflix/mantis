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

package io.mantisrx.server.master.jobmgmt;


import java.util.*;

public class JobRegistry {
//    final ConcurrentHashMap<String, MantisJobMgr> jobManagers = new ConcurrentHashMap<>();
//    final ConcurrentMap<String, NamedJob> jobClusters = new ConcurrentHashMap<>();
//
//
//    public Optional<MantisJobMgr> getJobManager(final String jobId) {
//        return Optional.ofNullable(jobManagers.get(jobId));
//    }
//
//    public Optional<NamedJob> getJobCluster(final String jobCluster) {
//        return Optional.ofNullable(jobClusters.get(jobCluster));
//    }
//
//    public MantisJobMgr addJobId(final String jobId, final MantisJobMgr mantisJobMgr) {
//        return jobManagers.put(jobId, mantisJobMgr);
//    }
//
//    public MantisJobMgr removeJobId(final String jobId) {
//        return jobManagers.remove(jobId);
//    }
//
//    public Set<String> getAllJobIds() {
//        return new HashSet<>(jobManagers.keySet());
//    }
//
//    public Set<String> getAllActiveJobIds() {
//        Set<String> retSet = new HashSet<>();
//        for(Map.Entry<String, MantisJobMgr> entry: jobManagers.entrySet()) {
//            if(!entry.getValue().getAllRunningWorkers().isEmpty())
//                retSet.add(entry.getKey());
//        }
//        return retSet;
//    }
//
//    public Collection<MantisJobMgr> getAllJobManagers() {
//        return jobManagers.values();
//    }
//
//    public Collection<NamedJob> getAllJobClusters() {
//        return jobClusters.values();
//    }
//
//    public NamedJob addJobClusterIfAbsent(final NamedJob jobCluster) {
//        return jobClusters.putIfAbsent(jobCluster.getName(), jobCluster);
//    }
//
//    public NamedJob removeJobCluster(final String jobCluster) {
//        return jobClusters.remove(jobCluster);
//    }
}
