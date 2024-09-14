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

import io.mantisrx.master.jobcluster.JobClusterActor.JobInfo;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLAEnforcer {
	private static final Logger logger = LoggerFactory.getLogger(SLAEnforcer.class);
	private final Optional<SLA> sla;

    // Comparator sorts JobInfo by job number in ascending order.
	private final Comparator<JobInfo> comparator = (o1, o2) -> {
        if (o2 == null)
            return -1;
        if (o1 == null)
            return 1;
        return Long.compare(o1.jobId.getJobNum(), o2.jobId.getJobNum());
    };

	public SLAEnforcer(SLA sla) {

		this.sla = Optional.ofNullable(sla);
	}

	/**
	 *
	 * @param activeJobsCount
	 * @param acceptedJobsCount
	 * @return
	 */
	public int enforceSLAMin(int activeJobsCount, int acceptedJobsCount) {
		Preconditions.checkArgument(activeJobsCount >=0, "Invalid activeJobsCount " + activeJobsCount);
		Preconditions.checkArgument(acceptedJobsCount >=0, "Invalid acceptedJobsCount " + activeJobsCount);
		// if no min sla defined
		if(!sla.isPresent() || sla.get().getMin() == 0) {
			logger.debug("SLA min not set nothing to enforce");
			return 0;
		}
		int jobsInActiveOrSubmittedState = activeJobsCount + acceptedJobsCount;
		if(jobsInActiveOrSubmittedState < sla.get().getMin()) {
			int jobsToLaunch = sla.get().getMin()-jobsInActiveOrSubmittedState;
			logger.info("Submit {} jobs per sla min of {}", jobsToLaunch, sla.get().getMin());
			return jobsToLaunch;
		}
		logger.debug("SLA min already satisfied");
		return 0;
	}

    /**
     * Walk the set of jobs in descending order (newest jobs first) track no. of  running jobs. Once this
     * count equals slamax mark the rest of them for deletion.
     *
     * @param list A sorted (by job number) set of jobs in either running or accepted state
     * @return A list of jobs ids that need to be terminated.
     */
    public List<JobId> enforceSLAMax(List<JobInfo> list) {
        return enforceSLAMax(list, 0);
    }

	/**
	 * Walk the set of jobs in descending order (newest jobs first) track no. of  running jobs. Once this
	 * count equals slamax mark the rest of them for deletion.
	 *
	 * @param list A sorted (by job number) set of jobs in either running or accepted state
     * @param slaMaxAcceptedJobAllowance The number of accepted jobs above the SLA max allowed. 0 for unlimited.
	 * @return A list of jobs ids that need to be terminated.
	 */
	public List<JobId> enforceSLAMax(List<JobInfo> list, int slaMaxAcceptedJobAllowance) {
		Preconditions.checkNotNull(list, "runningOrAcceptedJobSet is null");

		List<JobId> jobsToDelete = Lists.newArrayList();
		// if no max sla defined;
		if(!sla.isPresent() || sla.get().getMax() ==0 ) {
			return jobsToDelete;
		}

		SortedSet<JobInfo> sortedJobSet = new TreeSet<>(comparator);
		sortedJobSet.addAll(list);

		JobInfo [] jobIdArray = sortedJobSet.toArray(new JobInfo[list.size()]);

		int activeJobCount = 0;
		int slaMax = sla.get().getMax();
        boolean addToDeleteList = false;
		for(int i=jobIdArray.length-1; i>=0; i--) {
		    JobInfo jInfo = jobIdArray[i];
		    if(addToDeleteList) {
		        jobsToDelete.add(jInfo.jobId);
            } else {
                if (jInfo.state.equals(JobState.Launched)) {
                    activeJobCount++;
                    if (activeJobCount == slaMax) {
                        addToDeleteList = true;
                    }
                }
            }
        }

        int headroom = slaMax - activeJobCount; // If we're under SLA max we account for this in our allowance.
        if (slaMaxAcceptedJobAllowance > 0) {
            list.stream()
                .filter(job -> job.state == JobState.Accepted)
                .sorted(comparator.reversed())
                .skip(headroom + slaMaxAcceptedJobAllowance)
                .forEach(job -> jobsToDelete.add(job.jobId));
        }

		return jobsToDelete;

	}

	public boolean hasSLA() {
		if(!sla.isPresent() || sla == null || (sla.get().getMin() == 0 && sla.get().getMax() == 0)) {
			// No SLA == NO OP
			return false;
		}
		return true;
	}

    /**
     * For Testing
     * @param list
     * @return
     */
	Set<JobInfo> sortJobsByIdDesc(List<JobInfo> list) {
        SortedSet<JobInfo> sortedJobSet = new TreeSet<>(comparator);
        sortedJobSet.addAll(list);
        return sortedJobSet;
    }

}
