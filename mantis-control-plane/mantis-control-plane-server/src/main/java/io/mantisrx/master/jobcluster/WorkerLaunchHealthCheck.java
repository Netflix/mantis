/*
 * Copyright 2026 Netflix, Inc.
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

import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.jobcluster.job.FilterableMantisWorkerMetadataWritable;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.proto.HealthCheckResponse;
import io.mantisrx.master.jobcluster.proto.HealthCheckResponse.FailedWorker;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobCriteria;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsResponse;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default health check that verifies all workers in a job cluster are in the {@link
 * MantisJobState#Started} state.
 */
public class WorkerLaunchHealthCheck implements HealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(WorkerLaunchHealthCheck.class);

    private final JobRouteHandler jobRouteHandler;

    public WorkerLaunchHealthCheck(JobRouteHandler jobRouteHandler) {
        this.jobRouteHandler = jobRouteHandler;
    }

    @Override
    public HealthCheckResponse check(
            String clusterName, List<String> jobIds, Map<String, Object> context) {
        List<MantisJobMetadataView> jobs = fetchJobs(clusterName, jobIds);
        List<FailedWorker> failedWorkers = findUnhealthyWorkers(jobs);

        if (!failedWorkers.isEmpty()) {
            logger.info(
                    "Health check failed for cluster {}: {} unhealthy workers",
                    clusterName,
                    failedWorkers.size());
            return HealthCheckResponse.unhealthyWorkers(0L, failedWorkers);
        }

        return HealthCheckResponse.healthy(0L);
    }

    private List<MantisJobMetadataView> fetchJobs(String clusterName, List<String> jobIds) {
        if (jobIds != null && !jobIds.isEmpty()) {
            List<MantisJobMetadataView> jobs = new ArrayList<>();
            for (String jobId : jobIds) {
                try {
                    GetJobDetailsResponse response =
                            jobRouteHandler
                                    .getJobDetails(new GetJobDetailsRequest("healthcheck", jobId))
                                    .toCompletableFuture()
                                    .join();
                    response.getJobMetadata()
                            .ifPresent(meta -> jobs.add(new MantisJobMetadataView(
                                    meta, -1,
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    false)));
                } catch (Exception e) {
                    logger.warn("Failed to fetch job details for {}", jobId, e);
                }
            }
            return jobs;
        }

        try {
            ListJobCriteria criteria = new ListJobCriteria(
                    Optional.of(1),
                    Optional.empty(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Optional.of(true),
                    Optional.of(clusterName),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            ListJobsResponse response =
                    jobRouteHandler
                            .listJobs(new ListJobsRequest(criteria))
                            .toCompletableFuture()
                            .join();
            return response.getJobList();
        } catch (Exception e) {
            logger.warn("Failed to list jobs for cluster {}", clusterName, e);
            return List.of();
        }
    }

    private List<FailedWorker> findUnhealthyWorkers(List<MantisJobMetadataView> jobs) {
        List<FailedWorker> failedWorkers = new ArrayList<>();
        for (MantisJobMetadataView job : jobs) {
            if (job.getWorkerMetadataList() == null) {
                continue;
            }
            for (FilterableMantisWorkerMetadataWritable worker : job.getWorkerMetadataList()) {
                if (worker.getState() != MantisJobState.Started) {
                    failedWorkers.add(
                            new FailedWorker(
                                    worker.getWorkerIndex(),
                                    worker.getWorkerNumber(),
                                    worker.getState().name()));
                }
            }
        }
        return failedWorkers;
    }
}
