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

import akka.japi.Pair;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.PostJobStatusRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.config.ConfigurationProvider;
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

    /**
     * @return true to indicate valid, false otherwise. The String holds the error message when the request is invalid
     */
    public static Pair<Boolean, String> validateSubmitJobRequest(MantisJobDefinition mjd, Optional<String> clusterNameInResource) {
        if (null == mjd) {
            logger.error("rejecting job submit request, job definition is malformed {}", mjd);
            return Pair.apply(false, "Malformed job definition.");
        }

        // must include job cluster name
        if (mjd.getName() == null || mjd.getName().length() == 0) {
            logger.info("rejecting job submit request, must include name {}", mjd);
            return Pair.apply(false, "Job definition must include name");
        }

        // validate specified job cluster name matches with what specified in REST resource endpoint
        if (clusterNameInResource.isPresent()) {
            if (!clusterNameInResource.get().equals(mjd.getName())) {
                String msg = String.format("Cluster name specified in request payload [%s] " +
                        "does not match with what specified in resource endpoint [%s]",
                    mjd.getName(), clusterNameInResource.get());
                logger.info("rejecting job submit request, {} {}", msg, mjd);
                return Pair.apply(false, msg);
            }
        }

        // validate scheduling info
        SchedulingInfo schedulingInfo = mjd.getSchedulingInfo();
        if (schedulingInfo != null) {
            Map<Integer, StageSchedulingInfo> stages = schedulingInfo.getStages();
            if (stages != null) {
                for (StageSchedulingInfo stageSchedInfo : stages.values()) {
                    double cpuCores = stageSchedInfo.getMachineDefinition().getCpuCores();
                    int maxCpuCores = ConfigurationProvider.getConfig()
                        .getWorkerMachineDefinitionMaxCpuCores();
                    if (cpuCores > maxCpuCores) {
                        logger.info(
                            "rejecting job submit request, requested CPU {} > max for {} (user: {}) (stage: {})",
                            cpuCores,
                            mjd.getName(),
                            mjd.getUser(),
                            stages);
                        return Pair.apply(
                            false,
                            "requested CPU cannot be more than max CPU per worker " +
                                maxCpuCores);
                    }
                    double memoryMB = stageSchedInfo.getMachineDefinition().getMemoryMB();
                    int maxMemoryMB = ConfigurationProvider.getConfig()
                        .getWorkerMachineDefinitionMaxMemoryMB();
                    if (memoryMB > maxMemoryMB) {
                        logger.info(
                            "rejecting job submit request, requested memory {} > max for {} (user: {}) (stage: {})",
                            memoryMB,
                            mjd.getName(),
                            mjd.getUser(),
                            stages);
                        return Pair.apply(
                            false,
                            "requested memory cannot be more than max memoryMB per worker " +
                                maxMemoryMB);
                    }
                    double networkMbps = stageSchedInfo.getMachineDefinition().getNetworkMbps();
                    int maxNetworkMbps = ConfigurationProvider.getConfig()
                        .getWorkerMachineDefinitionMaxNetworkMbps();
                    if (networkMbps > maxNetworkMbps) {
                        logger.info(
                            "rejecting job submit request, requested network {} > max for {} (user: {}) (stage: {})",
                            networkMbps,
                            mjd.getName(),
                            mjd.getUser(),
                            stages);
                        return Pair.apply(
                            false,
                            "requested network cannot be more than max networkMbps per worker " +
                                maxNetworkMbps);
                    }
                    int numberOfInstances = stageSchedInfo.getNumberOfInstances();
                    int maxWorkersPerStage = ConfigurationProvider.getConfig()
                        .getMaxWorkersPerStage();
                    if (numberOfInstances > maxWorkersPerStage) {
                        logger.info(
                            "rejecting job submit request, requested num instances {} > max for {} (user: {}) (stage: {})",
                            numberOfInstances,
                            mjd.getName(),
                            mjd.getUser(),
                            stages);
                        return Pair.apply(
                            false,
                            "requested number of instances per stage cannot be more than " +
                                maxWorkersPerStage);
                    }

                    StageScalingPolicy scalingPolicy = stageSchedInfo.getScalingPolicy();
                    if (scalingPolicy != null) {
                        if (scalingPolicy.getMax() > maxWorkersPerStage) {
                            logger.info(
                                "rejecting job submit request, requested num instances in scaling policy {} > max for {} (user: {}) (stage: {})",
                                numberOfInstances,
                                mjd.getName(),
                                mjd.getUser(),
                                stages);
                            return Pair.apply(
                                false,
                                "requested number of instances per stage in scaling policy cannot be more than " +
                                    maxWorkersPerStage);
                        }
                    }
                }
            }
        }
        return Pair.apply(true, "");
    }
}
