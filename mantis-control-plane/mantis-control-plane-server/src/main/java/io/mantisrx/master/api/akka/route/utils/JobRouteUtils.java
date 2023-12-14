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

package io.mantisrx.master.api.akka.route.utils;

import static io.mantisrx.master.api.akka.route.utils.QueryParamUtils.paramValue;
import static io.mantisrx.master.api.akka.route.utils.QueryParamUtils.paramValueAsBool;
import static io.mantisrx.master.api.akka.route.utils.QueryParamUtils.paramValueAsInt;
import static io.mantisrx.master.api.akka.route.utils.QueryParamUtils.paramValuesAsInt;
import static io.mantisrx.master.api.akka.route.utils.QueryParamUtils.paramValuesAsMetaState;

import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.core.PostJobStatusRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobRouteUtils {
    private static final Logger logger = LoggerFactory.getLogger(JobRouteUtils.class);

    public static final String QUERY_PARAM_LIMIT = "limit";
    public static final String QUERY_PARAM_JOB_STATE = "jobState";
    public static final String QUERY_PARAM_STAGE_NUM = "stageNumber";
    public static final String QUERY_PARAM_WORKER_INDEX = "workerIndex";
    public static final String QUERY_PARAM_WORKER_NUM = "workerNumber";
    public static final String QUERY_PARAM_WORKER_STATE = "workerState";
    public static final String QUERY_PARAM_ACTIVE_ONLY = "activeOnly";
    public static final String QUERY_PARAM_LABELS_QUERY = "labels";
    public static final String QUERY_PARAM_LABELS_OPERAND = "labels.op";
    public static final String QUERY_PARAM_END_JOB_ID = "startJobIdExclusive";

    public static WorkerEvent createWorkerStatusRequest(final PostJobStatusRequest req) {
        final Status status = req.getStatus();
        if (status.getType() != Status.TYPE.HEARTBEAT) {
            final WorkerId workerId = new WorkerId(req.getJobId(), status.getWorkerIndex(), status.getWorkerNumber());
            if (logger.isTraceEnabled()) {
                logger.trace("forwarding worker status type {} from worker {}", status.getType().name(), workerId);
            }
            return new WorkerStatus(status);
        } else {
            return new WorkerHeartbeat(status);
        }
    }

    public static JobClusterManagerProto.ListJobsRequest createListJobsRequest(final Map<String, List<String>> params,
                                                                               final Optional<String> regex,
                                                                               final boolean activeOnlyDefault) {
        if(params == null) {
            if (regex.isPresent()) {
                return new JobClusterManagerProto.ListJobsRequest(regex.get());
            } else {
                return new JobClusterManagerProto.ListJobsRequest();
            }
        }

        final Optional<Integer> limit = paramValueAsInt(params, QUERY_PARAM_LIMIT);
        final Optional<JobState.MetaState> jobState = paramValue(params, QUERY_PARAM_JOB_STATE).map(p -> JobState.MetaState.valueOf(p));
        final List<Integer> stageNumber = paramValuesAsInt(params, QUERY_PARAM_STAGE_NUM);
        final List<Integer> workerIndex = paramValuesAsInt(params, QUERY_PARAM_WORKER_INDEX);
        final List<Integer> workerNumber = paramValuesAsInt(params, QUERY_PARAM_WORKER_NUM);
        final List<WorkerState.MetaState> workerState = paramValuesAsMetaState(params, QUERY_PARAM_WORKER_STATE);

        final Optional<Boolean> activeOnly = Optional.of(paramValueAsBool(params, QUERY_PARAM_ACTIVE_ONLY).orElse(activeOnlyDefault));
        final Optional<String> labelsQuery = paramValue(params, QUERY_PARAM_LABELS_QUERY);
        final Optional<String> labelsOperand = paramValue(params, QUERY_PARAM_LABELS_OPERAND);
        final Optional<JobId> jobId = paramValue(params, QUERY_PARAM_END_JOB_ID).flatMap(JobId::fromId);

        return new JobClusterManagerProto.ListJobsRequest(new JobClusterManagerProto.ListJobCriteria(limit,
            jobState,
            stageNumber,
            workerIndex,
            workerNumber,
            workerState,
            activeOnly,
            regex,
            labelsQuery,
            labelsOperand,
            jobId));
    }

    public static JobClusterManagerProto.ListJobIdsRequest createListJobIdsRequest(final Map<String, List<String>> params, final Optional<String> regex,
                                                                                   final boolean activeOnlyDefault) {
        if(params == null) {
            return new JobClusterManagerProto.ListJobIdsRequest();
        }

        final Optional<Integer> limit = paramValueAsInt(params, QUERY_PARAM_LIMIT);
        final Optional<JobState.MetaState> jobState = paramValue(params, QUERY_PARAM_JOB_STATE).map(p -> JobState.MetaState.valueOf(p));
        // list job ids is used on job cluster detail page, the UI does not set this flag explicitly but expects to see completed jobs as well
        final Optional<Boolean> activeOnly = Optional.of(paramValueAsBool(params, QUERY_PARAM_ACTIVE_ONLY).orElse(activeOnlyDefault));
        final Optional<String> labelsQuery = paramValue(params, QUERY_PARAM_LABELS_QUERY);
        final Optional<String> labelsOperand = paramValue(params, QUERY_PARAM_LABELS_OPERAND);
        final Optional<JobId> jobId = paramValue(params, QUERY_PARAM_END_JOB_ID).flatMap(JobId::fromId);

        return new JobClusterManagerProto.ListJobIdsRequest(limit, jobState, activeOnly, regex, labelsQuery, labelsOperand, jobId);
    }

}
