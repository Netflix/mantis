/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.master;

import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import java.util.Optional;

import io.mantisrx.shaded.com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ExecuteStageRequestFactory {
  private final MasterConfiguration masterConfiguration;

  public ExecuteStageRequest of(
      ScheduleRequest scheduleRequest,
      TaskExecutorRegistration matchedTaskExecutorInfo) {
    return new ExecuteStageRequest(
        scheduleRequest.getWorkerId().getJobCluster(),
        scheduleRequest.getWorkerId().getJobId(),
        scheduleRequest.getWorkerId().getWorkerIndex(),
        scheduleRequest.getWorkerId().getWorkerNum(),
        scheduleRequest.getJobMetadata().getJobJarUrl(),
        scheduleRequest.getStageNum(),
        scheduleRequest.getJobMetadata().getTotalStages(),
        matchedTaskExecutorInfo.getWorkerPorts().getPorts(),
        masterConfiguration.getTimeoutSecondsToReportStart(),
        matchedTaskExecutorInfo.getWorkerPorts().getMetricsPort(),
        scheduleRequest.getJobMetadata().getParameters(),
        scheduleRequest.getJobMetadata().getSchedulingInfo(),
        scheduleRequest.getDurationType(),
        scheduleRequest.getJobMetadata().getHeartbeatIntervalSecs(),
        scheduleRequest.getJobMetadata().getSubscriptionTimeoutSecs(),
        scheduleRequest.getJobMetadata().getMinRuntimeSecs() - (System.currentTimeMillis() - scheduleRequest.getJobMetadata().getMinRuntimeSecs()),
        matchedTaskExecutorInfo.getWorkerPorts(),
        Optional.empty(),
        scheduleRequest.getJobMetadata().getUser(),
        scheduleRequest.getJobMetadata().getJobVersion());
  }

  public ExecuteStageRequest of(
      TaskExecutorRegistration matchedTaskExecutorInfo,
      TaskExecutorAllocationRequest allocationRequest) {
      Preconditions.checkNotNull(allocationRequest);
      Preconditions.checkNotNull(allocationRequest.getJobMetadata());
    return new ExecuteStageRequest(
        allocationRequest.getWorkerId().getJobCluster(),
        allocationRequest.getWorkerId().getJobId(),
        allocationRequest.getWorkerId().getWorkerIndex(),
        allocationRequest.getWorkerId().getWorkerNum(),
        allocationRequest.getJobMetadata().getJobJarUrl(),
        allocationRequest.getStageNum(),
        allocationRequest.getJobMetadata().getTotalStages(),
        matchedTaskExecutorInfo.getWorkerPorts().getPorts(),
        masterConfiguration.getTimeoutSecondsToReportStart(),
        matchedTaskExecutorInfo.getWorkerPorts().getMetricsPort(),
        allocationRequest.getJobMetadata().getParameters(),
        allocationRequest.getJobMetadata().getSchedulingInfo(),
        allocationRequest.getDurationType(),
        allocationRequest.getJobMetadata().getHeartbeatIntervalSecs(),
        allocationRequest.getJobMetadata().getSubscriptionTimeoutSecs(),
        allocationRequest.getJobMetadata().getMinRuntimeSecs() - (System.currentTimeMillis() - allocationRequest.getJobMetadata().getMinRuntimeSecs()),
        matchedTaskExecutorInfo.getWorkerPorts(),
        Optional.empty(),
        allocationRequest.getJobMetadata().getUser(),
        allocationRequest.getJobMetadata().getJobVersion());
  }
}
