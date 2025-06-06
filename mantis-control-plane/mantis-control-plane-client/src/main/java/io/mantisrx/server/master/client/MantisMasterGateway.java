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
package io.mantisrx.server.master.client;

import io.mantisrx.common.Ack;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import rx.Observable;

public interface MantisMasterGateway {
  Observable<JobSchedulingInfo> schedulingChanges(final String jobId);

  Observable<Boolean> scaleJobStage(
      final String jobId,
      final int stageNum,
      final int numWorkers,
      final String reason);

  Observable<Boolean> resubmitJobWorker(final String jobId, final String user, final int workerNum,
      final String reason);

  Observable<NamedJobInfo> namedJobInfo(final String jobName);

  Observable<Boolean> namedJobExists(final String jobName);

  Observable<Integer> getSinkStageNum(final String jobId);

  Observable<JobSubmitResponse> submitJob(final String name, final String version,
      final List<Parameter> parameters,
      final JobSla jobSla,
      final SchedulingInfo schedulingInfo);

  Observable<JobSubmitResponse> submitJob(final String name, final String version,
      final List<Parameter> parameters,
      final JobSla jobSla,
      final long subscriptionTimeoutSecs,
      final SchedulingInfo schedulingInfo);

  Observable<JobSubmitResponse> submitJob(final String name, final String version,
      final List<Parameter> parameters,
      final JobSla jobSla,
      final long subscriptionTimeoutSecs,
      final SchedulingInfo schedulingInfo,
      final boolean readyForJobMaster);

  Observable<JobSubmitResponse> submitJob(final String name, final String version,
      final List<Parameter> parameters,
      final JobSla jobSla,
      final long subscriptionTimeoutSecs,
      final SchedulingInfo schedulingInfo,
      final boolean readyForJobMaster,
      final WorkerMigrationConfig migrationConfig);

  Observable<JobSubmitResponse> submitJob(final String name, final String version,
      final List<Parameter> parameters,
      final JobSla jobSla,
      final long subscriptionTimeoutSecs,
      final SchedulingInfo schedulingInfo,
      final boolean readyForJobMaster,
      final WorkerMigrationConfig migrationConfig,
      final List<Label> labels);

  Observable<Void> killJob(final String jobId);

  Observable<Void> killJob(final String jobId, final String user, final String reason);

  Observable<String> getJobsOfNamedJob(final String jobName, final MantisJobState.MetaState state);

  Observable<String> getJobStatusObservable(final String jobId);

  Observable<JobAssignmentResult> assignmentResults(String jobId);

    /**
     * Subscribe to job scaler rule stream API.
     * @param jobId
     * @return Observable of JobScalerRuleInfo for the given jobId.
     */
    Observable<JobScalerRuleInfo> jobScalerRulesStream(final String jobId);

    /**
   * Update the status of the worker to the mantis-master.
   *
   * @param status status that contains all the information about the worker such as the WorkerId,
   *               State of the worker, etc...
   * @return Acknowledgement if the update was received by the mantis-master.
   */
  CompletableFuture<Ack> updateStatus(Status status);
}
