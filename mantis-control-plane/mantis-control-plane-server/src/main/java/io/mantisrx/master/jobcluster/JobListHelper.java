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

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mantisrx.shaded.com.google.common.collect.Lists;

import io.mantisrx.master.jobcluster.JobClusterActor.JobInfo;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobCriteria;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import rx.Observable;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

public class JobListHelper {
    private static final Logger logger = LoggerFactory.getLogger(JobListHelper.class);


//    /**
//     * Note: rawResults are already filtered by jobstate and limit has been already applied to reduce unnecessary work
//     * @param rawResultList
//     * @param request
//     * @return
//     */
//    public static List<JobInfo> getPreFilteredNonTerminalJobList(final List<JobInfo> rawResultList,
//            ListJobCriteria request) {
//        if(logger.isDebugEnabled()) { logger.debug("Entering getPreFilteredNonTerminalJobList with raw list size {} and criteria {}", rawResultList.size(), request); }
//
//        Collections.sort(rawResultList,Comparator.comparingLong(jinfo -> jinfo.submittedAt));
//
//        if(request.getLimit().isPresent()) {
//            return rawResultList.subList(0, Math.min(rawResultList.size(), request.getLimit().get()));
//        }
//        if(logger.isDebugEnabled()) { logger.debug("Returning {} jobs in nonterminalstate ", rawResultList.size()); }
//        return rawResultList;
//    }
//
//    public static List<CompletedJob> getPreFilteredTerminalJobList(final List<CompletedJob> rawResultList, ListJobCriteria request) {
//        List<CompletedJob> resultList = Lists.newArrayList();
//        Observable.from(rawResultList)
//        .filter((completedJob) -> {
//            if(request.getActiveOnly().isPresent())  {
//                return false;
//            }
//            return true;
//        })
//        .toSortedList((c1,  c2) -> Long.compare(c1.getSubmittedAt(), c2.getSubmittedAt()) )
//        .subscribe((cList) -> {
//            resultList.addAll(cList);
//        });
//        if(request.getLimit().isPresent()) {
//            return resultList.subList(0, Math.min(resultList.size(), request.getLimit().get()));
//        }
//        return resultList;
//    }

    public static Optional<JobId> getLastSubmittedJobId(final List<JobInfo> existingJobsList,
                                                                  final List<CompletedJob> completedJobs) {
        if(logger.isTraceEnabled()) { logger.trace("Entering getLastSubmittedJobDefinition existing jobs {} completedJobs {}",existingJobsList.size(),completedJobs.size() ); }

        long highestJobNumber = -1;
        JobInfo jInfoWithHighestJobNumber = null;
        CompletedJob completedJobWithHighestJobNumber = null;
        if(logger.isDebugEnabled()) { logger.debug("No of active jobs: {}", existingJobsList.size()); }
        for (JobInfo jInfo : existingJobsList) {
            if (jInfo.jobId.getJobNum() > highestJobNumber) {
                highestJobNumber = jInfo.jobId.getJobNum();
                jInfoWithHighestJobNumber = jInfo;
            }
        }
        if(logger.isDebugEnabled()) { logger.debug("Highest Active job number: {}", highestJobNumber); }
        if(highestJobNumber != -1) {

            return ofNullable(jInfoWithHighestJobNumber.jobId);
        } else {
            // search in completed Jobs
            for (CompletedJob cJob : completedJobs) {
                Optional<JobId> completedJobId = JobId.fromId(cJob.getJobId());
                if (completedJobId.isPresent() && completedJobId.get().getJobNum() > highestJobNumber) {
                    highestJobNumber = completedJobId.get().getJobNum();
                    completedJobWithHighestJobNumber = cJob;
                }
            }

            if(highestJobNumber != -1) {
                if(logger.isDebugEnabled()) { logger.debug("Highest completed job number: {}", highestJobNumber); }
                return (JobId.fromId(completedJobWithHighestJobNumber.getJobId()));
            }

        }
        return empty();
    }





}
