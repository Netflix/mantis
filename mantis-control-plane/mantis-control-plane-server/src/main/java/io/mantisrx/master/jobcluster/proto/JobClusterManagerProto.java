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

package io.mantisrx.master.jobcluster.proto;

import akka.actor.ActorRef;
import akka.http.javadsl.model.Uri;
import com.mantisrx.common.utils.LabelUtils;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.Label;
import io.mantisrx.master.api.akka.route.pagination.ListObject;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter.JobIdInfo;
import io.mantisrx.master.jobcluster.MantisJobClusterMetadataView;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.JobState.MetaState;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import rx.subjects.BehaviorSubject;

public class JobClusterManagerProto {


    public static final class CreateJobClusterRequest extends BaseRequest {
        private final JobClusterDefinitionImpl jobClusterDefinition;
        private final String user;

        public CreateJobClusterRequest(
                final JobClusterDefinitionImpl jobClusterDefinition,
                String user) {
            super();
            Preconditions.checkNotNull(jobClusterDefinition, "JobClusterDefn cannot be null");
            this.jobClusterDefinition = jobClusterDefinition;
            this.user = user;
        }

        public JobClusterDefinitionImpl getJobClusterDefinition() {
            return jobClusterDefinition;
        }

        public String getUser() {
            return user;
        }
    }

    public static final class ReconcileJobCluster {
        public final Instant timeOfEnforcement;

        public ReconcileJobCluster(Instant now) {
            timeOfEnforcement = now;
        }

        public ReconcileJobCluster() {
            timeOfEnforcement = Instant.now();
        }
    }

    public static final class CreateJobClusterResponse extends BaseResponse {

        private final String jobClusterName;

        public CreateJobClusterResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final String jobClusterName
        ) {
            super(requestId, responseCode, message);
            this.jobClusterName = jobClusterName;
        }

        public String getJobClusterName() {
            return jobClusterName;
        }

        @Override
        public String toString() {
            return "CreateJobClusterResponse{" +
                   "jobClusterName='" + jobClusterName + '\'' +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    public static final class DeleteJobClusterRequest extends BaseRequest {
        private final String name;
        private final String user;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public DeleteJobClusterRequest(
                @JsonProperty("user") final String user,
                @JsonProperty("name") final String name) {
            super();
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    name != null & !name.isEmpty(),
                    "Must provide job cluster name in request");
            this.user = user;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getUser() {
            return user;
        }
    }

    public static final class DeleteJobClusterResponse extends BaseResponse {
        public DeleteJobClusterResponse(long requestId, ResponseCode responseCode, String message) {
            super(requestId, responseCode, message);
        }
    }

    public static final class JobClustersManagerInitialize extends BaseRequest {
        private final MantisSchedulerFactory schedulerFactory;
        private final boolean loadJobsFromStore;

        public JobClustersManagerInitialize(
                final MantisSchedulerFactory schedulerFactory,
                final boolean loadJobsFromStore) {
            Preconditions.checkNotNull(schedulerFactory, "MantisScheduler cannot be null");
            this.schedulerFactory = schedulerFactory;
            this.loadJobsFromStore = loadJobsFromStore;
        }

        public MantisSchedulerFactory getScheduler() {
            return schedulerFactory;
        }

        public boolean isLoadJobsFromStore() {
            return loadJobsFromStore;
        }
    }

    public static final class JobClustersManagerInitializeResponse extends BaseResponse {

        public JobClustersManagerInitializeResponse(
                long requestId,
                ResponseCode responseCode,
                String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "JobClustersManagerInitializeResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    /**
     * Get a list of all job clusters in the system
     *
     * @author njoshi
     */

    public static final class ListJobClustersRequest extends BaseRequest {
        public ListJobClustersRequest() {
            super();
        }
    }

    public static final class ListJobClustersResponse extends BaseResponse {
        private final List<MantisJobClusterMetadataView> jobClusters;

        public ListJobClustersResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                List<MantisJobClusterMetadataView> jobClusters) {
            super(requestId, responseCode, message);
            this.jobClusters = jobClusters;
        }

        public List<MantisJobClusterMetadataView> getJobClusters() {
            return jobClusters;
        }

        public ListObject<MantisJobClusterMetadataView> getJobClusters(
                String regexMatcher,
                Integer limit,
                Integer offset,
                String sortField,
                Boolean sortAscending,
                Uri uri) {


            List<MantisJobClusterMetadataView> targetJobClusters = jobClusters;
            if (!Strings.isNullOrEmpty(regexMatcher)) {
                Pattern matcher = Pattern.compile(regexMatcher, Pattern.CASE_INSENSITIVE);

                targetJobClusters = targetJobClusters.stream()
                        .filter(jobCluster -> matcher.matcher(jobCluster.getName()).find())
                        .collect(Collectors.toList());
            }

            ListObject.Builder<MantisJobClusterMetadataView> builder =
                    new ListObject.Builder<MantisJobClusterMetadataView>()
                            .withObjects(targetJobClusters, MantisJobClusterMetadataView.class);
            if (limit != null) {
                builder = builder.withLimit(limit);
            }
            if (offset != null) {
                builder = builder.withOffset(offset);
            }
            if (sortField != null) {
                builder = builder.withSortField(sortField);
            }
            if (sortAscending != null) {
                builder = builder.withSortAscending(sortAscending);
            }
            if (uri != null) {
                builder = builder.withUri(uri);
            }

            return builder.build();
        }

        @Override
        public String toString() {
            return "ListJobClustersResponse{" +
                   "jobClusters=" + jobClusters +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    /**
     * Invoked by user to update a job cluster
     *
     * @author njoshi
     */

    public static final class UpdateJobClusterRequest extends BaseRequest {
        private final JobClusterDefinitionImpl jobClusterDefinition;
        private final String user;

        public UpdateJobClusterRequest(
                final JobClusterDefinitionImpl jobClusterDefinition,
                String user) {
            Preconditions.checkNotNull(jobClusterDefinition, "JobClusterDefinition cannot be null");
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            this.jobClusterDefinition = jobClusterDefinition;
            this.user = user;

        }

        public JobClusterDefinitionImpl getJobClusterDefinition() {
            return jobClusterDefinition;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "UpdateJobClusterRequest{" +
                   "jobClusterDefinition=" + jobClusterDefinition +
                   ", user='" + user + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    /**
     * Indicates whether an update was successful
     *
     * @author njoshi
     */

    public static final class UpdateJobClusterResponse extends BaseResponse {
        public UpdateJobClusterResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "UpdateJobClusterResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    /**
     * Updates the SLA for the job cluster with an optional force enable option if cluster is disabled
     *
     * @author njoshi
     */
    public static final class UpdateJobClusterSLARequest extends BaseRequest {
        private final String clusterName;
        private final int min;
        private final int max;
        private final String cronSpec;
        private final IJobClusterDefinition.CronPolicy cronPolicy;
        private final boolean forceEnable;
        private final String user;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public UpdateJobClusterSLARequest(
                @JsonProperty("name") final String name,
                @JsonProperty("min") final Integer min,
                @JsonProperty("max") final Integer max,
                @JsonProperty("cronspec") final String cronSpec,
                @JsonProperty("cronpolicy") final IJobClusterDefinition.CronPolicy cronPolicy,
                @JsonProperty(value = "forceenable", defaultValue = "false") boolean forceEnable,
                @JsonProperty("user") final String user) {
            Preconditions.checkNotNull(min, "min");
            Preconditions.checkNotNull(max, "max");
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    name != null & !name.isEmpty(),
                    "Must provide job cluster name in request");
            this.clusterName = name;
            this.max = max;
            this.min = min;
            this.cronSpec = cronSpec;
            this.cronPolicy = cronPolicy;
            this.forceEnable = forceEnable;
            this.user = user;
        }

        public UpdateJobClusterSLARequest(
                final String name,
                final int min,
                final int max,
                final String user) {
            this(name, min, max, null, null, false, user);
        }

        public String getClusterName() {
            return clusterName;
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public String getCronSpec() {
            return cronSpec;
        }

        public IJobClusterDefinition.CronPolicy getCronPolicy() {
            return cronPolicy;
        }

        public boolean isForceEnable() {
            return forceEnable;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "UpdateJobClusterSLARequest{" +
                   "clusterName='" + clusterName + '\'' +
                   ", min=" + min +
                   ", max=" + max +
                   ", cronSpec='" + cronSpec + '\'' +
                   ", cronPolicy=" + cronPolicy +
                   ", forceEnable=" + forceEnable +
                   ", user='" + user + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class UpdateJobClusterSLAResponse extends BaseResponse {
        public UpdateJobClusterSLAResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "UpdateJobClusterSLAResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    public static final class UpdateJobClusterLabelsRequest extends BaseRequest {
        private final List<Label> labels;
        private final String user;
        private final String clusterName;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public UpdateJobClusterLabelsRequest(
                @JsonProperty("name") final String clusterName,
                @JsonProperty("labels") final List<Label> labels,
                @JsonProperty("user") final String user) {
            Preconditions.checkNotNull(labels, "labels");
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    clusterName != null & !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            this.labels = labels;
            this.user = user;
            this.clusterName = clusterName;
        }

        public List<Label> getLabels() {
            return labels;
        }

        public String getUser() {
            return user;
        }

        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String toString() {
            return "UpdateJobClusterLabelsRequest{" +
                   "labels=" + labels +
                   ", user='" + user + '\'' +
                   ", clusterName='" + clusterName + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class UpdateJobClusterLabelsResponse extends BaseResponse {

        public UpdateJobClusterLabelsResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "UpdateJobClusterLabelsResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    @ToString
    @EqualsAndHashCode
    @Getter
    public static final class UpdateSchedulingInfoRequest extends BaseRequest {
        private final SchedulingInfo schedulingInfo;
        private final String version;

        public UpdateSchedulingInfoRequest(
            @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo,
            @JsonProperty("version") final String version) {
            this.schedulingInfo = schedulingInfo;
            this.version = version;
        }
    }

    public static final class UpdateJobClusterArtifactRequest extends BaseRequest {
        private final String artifactName;
        private final String jobJarUrl;
        private final String version;
        private final boolean skipSubmit;
        private final String user;
        private final String clusterName;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public UpdateJobClusterArtifactRequest(
                @JsonProperty("name") final String clusterName,
                @JsonProperty("url") final String artifact,
                @JsonProperty("jobJarUrl") final String jobJarUrl,
                @JsonProperty("version") final String version,
                @JsonProperty("skipsubmit") final boolean skipSubmit,
                @JsonProperty("user") final String user) {
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    clusterName != null & !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            Preconditions.checkArg(
                    artifact != null && !artifact.isEmpty(),
                    "Artifact cannot be null or empty");
            Preconditions.checkArg(
                    version != null && !version.isEmpty(),
                    "version cannot be null or empty");

            this.clusterName = clusterName;
            this.artifactName = artifact;
            // [Note] in the legacy setup this artifact field is used to host the job jar url field (it maps to the
            // json property "url".
            this.jobJarUrl = jobJarUrl != null ?
                jobJarUrl :
                (artifact.startsWith("http://") || artifact.startsWith("https://") ? artifact : "http://" + artifact);
            this.version = version;
            this.skipSubmit = skipSubmit;
            this.user = user;
        }

        public String getArtifactName() {
            return artifactName;
        }

        public String getjobJarUrl() {
            return jobJarUrl;
        }

        public String getVersion() {
            return version;
        }

        public boolean isSkipSubmit() {
            return skipSubmit;
        }

        public String getUser() {
            return user;
        }

        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String toString() {
            return "UpdateJobClusterArtifactRequest{" +
                   "artifactName='" + artifactName + '\'' +
                   "jobJarUrl='" + jobJarUrl + '\'' +
                   ", version='" + version + '\'' +
                   ", skipSubmit=" + skipSubmit +
                   ", user='" + user + '\'' +
                   ", clusterName='" + clusterName + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    @EqualsAndHashCode
    @ToString
    public static final class UpdateSchedulingInfoResponse extends BaseResponse {
        public UpdateSchedulingInfoResponse(
            final long requestId,
            final ResponseCode responseCode,
            final String message) {
            super(requestId, responseCode, message);
        }
    }

    public static final class UpdateJobClusterArtifactResponse extends BaseResponse {
        public UpdateJobClusterArtifactResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "UpdateJobClusterArtifactResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    public static final class UpdateJobClusterWorkerMigrationStrategyRequest extends BaseRequest {
        private final WorkerMigrationConfig migrationConfig;
        private final String clusterName;
        private final String user;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public UpdateJobClusterWorkerMigrationStrategyRequest(
                @JsonProperty("name") final String clusterName,
                @JsonProperty("migrationConfig") final WorkerMigrationConfig config,
                @JsonProperty("user") final String user) {
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    clusterName != null & !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            Preconditions.checkNotNull(config, "migrationConfig");
            this.migrationConfig = config;
            this.clusterName = clusterName;
            this.user = user;
        }

        public WorkerMigrationConfig getMigrationConfig() {
            return migrationConfig;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "UpdateJobClusterWorkerMigrationStrategyRequest{" +
                   "migrationConfig=" + migrationConfig +
                   ", clusterName='" + clusterName + '\'' +
                   ", user='" + user + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class UpdateJobClusterWorkerMigrationStrategyResponse extends BaseResponse {
        public UpdateJobClusterWorkerMigrationStrategyResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "UpdateJobClusterWorkerMigrationStrategyResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    /**
     * Invoked by user.
     * Kills all currently running jobs and puts itself in disabled state (also updates store)
     * Any SLA enforcement is disabled
     *
     * @author njoshi
     */
    public static final class DisableJobClusterRequest extends BaseRequest {
        private final String user;
        private final String clusterName;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public DisableJobClusterRequest(
                @JsonProperty("name") String clusterName,
                @JsonProperty("user") String user) {
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    clusterName != null & !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            this.user = user;
            this.clusterName = clusterName;
        }

        public String getUser() {
            return user;
        }

        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String toString() {
            return "DisableJobClusterRequest{" +
                   "user='" + user + '\'' +
                   ", clusterName='" + clusterName + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    /**
     * Whether a disable request was successful
     *
     * @author njoshi
     */
    public static final class DisableJobClusterResponse extends BaseResponse {
        public DisableJobClusterResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "DisableJobClusterResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    /**
     * Enables the job cluster. Restarts SLA enforcement logic and updates store.
     *
     * @author njoshi
     */
    public static final class EnableJobClusterRequest extends BaseRequest {
        private final String user;
        private final String clusterName;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public EnableJobClusterRequest(
                @JsonProperty("name") final String clusterName,
                @JsonProperty("user") final String user) {
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    clusterName != null & !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            this.user = user;
            this.clusterName = clusterName;
        }

        public String getUser() {
            return user;
        }

        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String toString() {
            return "EnableJobClusterRequest{" +
                   "user='" + user + '\'' +
                   ", clusterName='" + clusterName + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    /**
     * Whether enable was successfull
     *
     * @author njoshi
     */
    public static final class EnableJobClusterResponse extends BaseResponse {
        public EnableJobClusterResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "EnableJobClusterResponse{" +
                   "requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }


    /**
     * Request the job cluster definition
     *
     * @author njoshi
     */
    public static final class GetJobClusterRequest extends BaseRequest {
        private final String jobClusterName;

        public GetJobClusterRequest(final String name) {
            super();
            Preconditions.checkArg(
                    name != null && !name.isEmpty(),
                    "Jobcluster name cannot be null or empty");
            this.jobClusterName = name;
        }

        public String getJobClusterName() {
            return jobClusterName;
        }

        @Override
        public String toString() {
            return "GetJobClusterRequest{" +
                   "jobClusterName='" + jobClusterName + '\'' +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    /**
     * Response to the getJobClusterRequest with the actual job cluster definition.
     *
     * @author njoshi
     */
    public static final class GetJobClusterResponse extends BaseResponse {
        private final Optional<MantisJobClusterMetadataView> jobClusterOp;

        public GetJobClusterResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                Optional<MantisJobClusterMetadataView> jobClusterOp) {
            super(requestId, responseCode, message);
            Preconditions.checkNotNull(jobClusterOp, "Job cluster cannot be null");
            this.jobClusterOp = jobClusterOp;
        }

        public Optional<MantisJobClusterMetadataView> getJobCluster() {
            return jobClusterOp;
        }

        @Override
        public String toString() {
            return "GetJobClusterResponse{" +
                   "jobClusterOp=" + jobClusterOp +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    public static final class ListJobCriteria {
        private final Optional<Integer> limit;
        private final Optional<JobState.MetaState> jobState;
        private final List<Integer> stageNumberList;
        private final List<Integer> workerIndexList;
        private final List<Integer> workerNumberList;
        private final List<WorkerState.MetaState> workerStateList;
        private final Optional<Boolean> activeOnly;
        private final Optional<String> matchingRegex;
        private final List<Label> matchingLabels;
        private final Optional<String> labelsOperand;

        private final Optional<JobId> startJobIdExclusive;

        public ListJobCriteria(
                final Optional<Integer> limit,
                final Optional<MetaState> jobState,
                final List<Integer> stageNumber,
                final List<Integer> workerIndex,
                final List<Integer> workerNumber,
                final List<WorkerState.MetaState> workerState,
                final Optional<Boolean> activeOnly,
                final Optional<String> matchingRegex,
                final Optional<String> matchingLabels,
                final Optional<String> labelsOperand,
                final Optional<JobId> startJobIdExclusive) {
            this.limit = limit;
            this.jobState = jobState;
            this.stageNumberList = stageNumber;
            this.workerIndexList = workerIndex;
            this.workerNumberList = workerNumber;
            this.workerStateList = workerState;
            this.activeOnly = activeOnly;
            this.matchingRegex = matchingRegex;
            this.matchingLabels = matchingLabels.map(query -> LabelUtils.generatePairs(query))
                                                .orElse(Collections.emptyList());
            this.labelsOperand = labelsOperand;
            this.startJobIdExclusive = startJobIdExclusive;
        }

        public ListJobCriteria() {
            this(
                    Optional.empty(),
                    Optional.empty(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        public Optional<Integer> getLimit() {
            return limit;
        }

        public Optional<JobState.MetaState> getJobState() {
            return jobState;
        }

        public List<Integer> getStageNumberList() {
            return stageNumberList;
        }

        public List<Integer> getWorkerIndexList() {
            return workerIndexList;
        }

        public List<Integer> getWorkerNumberList() {
            return workerNumberList;
        }

        public List<WorkerState.MetaState> getWorkerStateList() {
            return workerStateList;
        }

        public Optional<Boolean> getActiveOnly() {
            return activeOnly;
        }

        public Optional<String> getMatchingRegex() {
            return matchingRegex;
        }

        public List<Label> getMatchingLabels() {
            return matchingLabels;
        }

        public Optional<String> getLabelsOperand() {
            return labelsOperand;
        }

        public Optional<JobId> getStartJobIdExclusive() {
            return startJobIdExclusive;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ListJobCriteria that = (ListJobCriteria) o;
            return Objects.equals(limit, that.limit) &&
                   Objects.equals(jobState, that.jobState) &&
                   Objects.equals(stageNumberList, that.stageNumberList) &&
                   Objects.equals(workerIndexList, that.workerIndexList) &&
                   Objects.equals(workerNumberList, that.workerNumberList) &&
                   Objects.equals(workerStateList, that.workerStateList) &&
                   Objects.equals(activeOnly, that.activeOnly) &&
                   Objects.equals(matchingRegex, that.matchingRegex) &&
                   Objects.equals(matchingLabels, that.matchingLabels) &&
                   Objects.equals(labelsOperand, that.labelsOperand);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    limit,
                    jobState,
                    stageNumberList,
                    workerIndexList,
                    workerNumberList,
                    workerStateList,
                    activeOnly,
                    matchingRegex,
                    matchingLabels,
                    labelsOperand);
        }

        @Override
        public String toString() {
            return "ListJobCriteria{" +
                   "limit=" + limit +
                   ", jobState=" + jobState +
                   ", stageNumberList=" + stageNumberList +
                   ", workerIndexList=" + workerIndexList +
                   ", workerNumberList=" + workerNumberList +
                   ", workerStateList=" + workerStateList +
                   ", activeOnly=" + activeOnly +
                   ", matchingRegex=" + matchingRegex +
                   ", matchingLabels=" + matchingLabels +
                   ", labelsOperand=" + labelsOperand +
                   '}';
        }
    }

    /**
     * Request a list of job metadata based on different criteria
     */
    public static class ListJobsRequest extends BaseRequest {
        private final ListJobCriteria filters;

        public ListJobsRequest(final ListJobCriteria filters) {
            this.filters = filters;
        }

        public ListJobsRequest() {
            this(new ListJobCriteria());
        }

        public ListJobsRequest(final String clusterName) {
            this(new ListJobCriteria(
                    Optional.empty(),
                    Optional.empty(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Optional.empty(),
                    Optional.ofNullable(clusterName),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        public ListJobCriteria getCriteria() {
            return filters;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ListJobsRequest that = (ListJobsRequest) o;
            return Objects.equals(filters, that.filters);
        }

        @Override
        public int hashCode() {

            return Objects.hash(filters);
        }

        @Override
        public String toString() {
            return "ListJobsRequest{" +
                   "filters=" + filters +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class ListJobsResponse extends BaseResponse {

        private final List<MantisJobMetadataView> jobs;

        public ListJobsResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                List<MantisJobMetadataView> list) {
            super(requestId, responseCode, message);
            Preconditions.checkNotNull(list, "job ids list cannot be null");
            this.jobs = list;
        }

        public List<MantisJobMetadataView> getJobList() {
            return jobs;
        }
        public <R> ListObject<R> getJobList(Function<MantisJobMetadataView, R> func,
                                            Class<R> classType,
                                            Integer pageSize,
                                            Integer offset,
                                            String sortField,
                                            Boolean sortAscending,
                                            Uri uri) {
             List<R> mappedList = jobs.stream().map(func).collect(Collectors.toList());

            return getTransformedJobList(mappedList,
                                         classType,
                                         pageSize,
                                         offset,
                                         sortField,
                                         sortAscending,
                                         uri);
        }

        public ListObject<MantisJobMetadataView> getJobList(
                Integer pageSize,
                Integer offset,
                String sortField,
                Boolean sortAscending,
                Uri uri) {

            return getTransformedJobList(jobs,
                                         MantisJobMetadataView.class,
                                         pageSize,
                                         offset,
                                         sortField,
                                         sortAscending,
                                         uri);
        }

        private <T> ListObject<T> getTransformedJobList(
                List<T> list,
                Class<T> classType,
                Integer pageSize,
                Integer offset,
                String sortField,
                Boolean sortAscending,
                Uri uri) {

            ListObject.Builder<T> builder = new ListObject.Builder<T>().withObjects(list, classType);
            if (uri != null) {
                builder = builder.withUri(uri);
            }
            if (pageSize != null) {
                builder = builder.withLimit(pageSize);
            }
            if (offset != null) {
                builder = builder.withOffset(offset);
            }
            if (sortAscending != null) {
                builder = builder.withSortAscending(sortAscending);
            }
            if (sortField != null) {
                builder = builder.withSortField(sortField);
            }

            return builder.build();
        }

        @Override
        public String toString() {
            return "ListJobsResponse{" +
                   "jobs=" + jobs +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }


    /**
     * Request a list of job IDs based on different criteria
     */
    public static final class ListJobIdsRequest extends BaseRequest {
        public final ListJobCriteria filters;

        public ListJobIdsRequest(
                final Optional<Integer> limit,
                final Optional<JobState.MetaState> jobState,
                final Optional<Boolean> activeOnly,
                final Optional<String> matchingRegex,
                final Optional<String> matchingLabels,
                final Optional<String> labelsOperand,
                final Optional<JobId> startJobIdExclusive) {
            super();
            filters = new ListJobCriteria(
                    limit,
                    jobState,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    activeOnly,
                    matchingRegex,
                    matchingLabels,
                    labelsOperand,
                    startJobIdExclusive);


        }

        public ListJobIdsRequest() {
            this(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        public ListJobCriteria getCriteria() {
            return this.filters;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ListJobIdsRequest that = (ListJobIdsRequest) o;
            return Objects.equals(filters, that.filters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filters);
        }

        @Override
        public String toString() {
            return "ListJobIdsRequest{" +
                   "filters=" + filters +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class ListJobIdsResponse extends BaseResponse {
        private final List<JobIdInfo> jobIds;

        public ListJobIdsResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                List<JobIdInfo> list) {
            super(requestId, responseCode, message);
            Preconditions.checkNotNull(list, "job ids list cannot be null");
            this.jobIds = list;
        }

        public List<JobIdInfo> getJobIds() {
            return jobIds;
        }

        @Override
        public String toString() {
            return "ListJobIdsResponse{" +
                   "jobIds=" + jobIds +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    /**
     * Request a list of archived workers for the given job ID
     */
    public static final class ListArchivedWorkersRequest extends BaseRequest {
        public static final int DEFAULT_LIST_ARCHIVED_WORKERS_LIMIT = 100;
        private final JobId jobId;
        private final int limit;

        public ListArchivedWorkersRequest(final JobId jobId) {
            this(jobId, DEFAULT_LIST_ARCHIVED_WORKERS_LIMIT);
        }

        public ListArchivedWorkersRequest(final JobId jobId, int limit) {
            Preconditions.checkNotNull(jobId, "JobId");
            this.jobId = jobId;
            this.limit = limit;
        }

        public JobId getJobId() {
            return jobId;
        }

        public int getLimit() {
            return limit;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ListArchivedWorkersRequest that = (ListArchivedWorkersRequest) o;
            return limit == that.limit &&
                   Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, limit);
        }

        @Override
        public String toString() {
            return "ListArchivedWorkersRequest{" +
                   "jobId=" + jobId +
                   ", limit=" + limit +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class ListArchivedWorkersResponse extends BaseResponse {

        private final List<IMantisWorkerMetadata> mantisWorkerMetadata;

        public ListArchivedWorkersResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                List<IMantisWorkerMetadata> list) {
            super(requestId, responseCode, message);
            Preconditions.checkNotNull(list, "worker metadata list cannot be null");
            this.mantisWorkerMetadata = list;
        }

        public List<IMantisWorkerMetadata> getWorkerMetadata() {
            return mantisWorkerMetadata;
        }


        public ListObject<IMantisWorkerMetadata> getWorkerMetadata(
                Integer pageSize,
                Integer offset,
                String sortField,
                Boolean sortAscending,
                Uri uri) {
            return getTransformedWorkerMetadata(mantisWorkerMetadata,
                                                IMantisWorkerMetadata.class,
                                                pageSize,
                                                offset,
                                                sortField,
                                                sortAscending,
                                                uri);
        }

        public <R> ListObject<R> getWorkerMetadata(
                Function<IMantisWorkerMetadata, R> func,
                Class<R> classType,
                Integer pageSize,
                Integer offset,
                String sortField,
                Boolean sortAscending,
                Uri uri) {

            List<R> mappedList = mantisWorkerMetadata.stream().map(func).collect(Collectors.toList());
            return getTransformedWorkerMetadata(mappedList,
                                                classType,
                                                pageSize,
                                                offset,
                                                sortField,
                                                sortAscending,
                                                uri);
        }

        private <T> ListObject<T> getTransformedWorkerMetadata(
                List<T> list,
                Class<T> classType,
                Integer pageSize,
                Integer offset,
                String sortField,
                Boolean sortAscending,
                Uri uri) {

            ListObject.Builder<T> builder = new ListObject.Builder<T>()
                            .withObjects(list, classType);
            if (pageSize != null) {
                builder = builder.withLimit(pageSize);
            }
            if (offset != null) {
                builder = builder.withOffset(offset);
            }
            if (sortField != null) {
                builder = builder.withSortField(sortField);
            }
            if (sortAscending != null) {
                builder = builder.withSortAscending(sortAscending);
            }

            if (uri != null) {
                builder = builder.withUri(uri);
            }

            return builder.build();
        }

        @Override
        public String toString() {
            return "ListArchivedWorkersResponse{" +
                   "mantisWorkerMetadata=" + mantisWorkerMetadata +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }


    public static final class ListWorkersRequest extends BaseRequest {
        public static final int DEFAULT_LIST_WORKERS_LIMIT = 100;
        private final JobId jobId;
        private final int limit;

        public ListWorkersRequest(final JobId jobId) {
            this(jobId, DEFAULT_LIST_WORKERS_LIMIT);
        }

        public ListWorkersRequest(final JobId jobId, int limit) {
            this.jobId = jobId;
            this.limit = limit;
        }

        public JobId getJobId() {
            return jobId;
        }

        public int getLimit() {
            return limit;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ListWorkersRequest that = (ListWorkersRequest) o;
            return limit == that.limit &&
                   Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {

            return Objects.hash(jobId, limit);
        }

        @Override
        public String toString() {
            return "ListWorkersRequest{" +
                   "jobId=" + jobId +
                   ", limit=" + limit +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class ListWorkersResponse extends BaseResponse {

        private final List<IMantisWorkerMetadata> mantisWorkerMetadata;

        public ListWorkersResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                List<IMantisWorkerMetadata> list) {
            super(requestId, responseCode, message);
            Preconditions.checkNotNull(list, "worker metadata list cannot be null");
            this.mantisWorkerMetadata = list;
        }

        public List<IMantisWorkerMetadata> getWorkerMetadata() {
            return mantisWorkerMetadata;
        }

        @Override
        public String toString() {
            return "ListWorkersResponse{" +
                   "mantisWorkerMetadata=" + mantisWorkerMetadata +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }


    /**
     * Request a list of completed job Ids in this cluster
     *
     * @author njoshi
     */
    public static final class ListCompletedJobsInClusterRequest extends BaseRequest {
        private final String clusterName;
        private final int limit;

        public ListCompletedJobsInClusterRequest(final String name) {
            this(name, 100);
        }

        public ListCompletedJobsInClusterRequest(final String name, final int limit) {
            super();
            Preconditions.checkArg(
                    name != null && !name.isEmpty(),
                    "Jobcluster name cannot be null or empty");
            this.clusterName = name;
            this.limit = limit;
        }

        public int getLimit() {
            return this.limit;
        }

        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String toString() {
            return "ListCompletedJobsInClusterRequest{" +
                   "clusterName='" + clusterName + '\'' +
                   ", limit=" + limit +
                   ", requestId=" + requestId +
                   '}';
        }
    }

    public static final class ListCompletedJobsInClusterResponse extends BaseResponse {
        private final List<CompletedJob> completedJobs;

        public ListCompletedJobsInClusterResponse(
                long requestId,
                ResponseCode responseCode,
                String message,
                List<CompletedJob> completedJobs) {
            super(requestId, responseCode, message);
            this.completedJobs = completedJobs;
        }

        public List<CompletedJob> getCompletedJobs() {
            return completedJobs;
        }

        @Override
        public String toString() {
            return "ListCompletedJobsInClusterResponse [completedJobs=" + completedJobs + "]";
        }
    }

    @ToString
    public static final class SubmitJobRequest extends BaseRequest {
        private final Optional<JobDefinition> jobDefinition;
        private final String submitter;
        private final String clusterName;
        private final boolean isAutoResubmit;
        private final boolean submitLatest;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public SubmitJobRequest(
                @JsonProperty("name") final String clusterName,
                @JsonProperty("user") final String user,
                @JsonProperty(value = "jobDefinition") final Optional<JobDefinition> jobDefinition,
                @JsonProperty("submitLatestJobCluster") final boolean submitLatest) {
            super();
            Preconditions.checkArg(user != null && !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    clusterName != null && !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            Preconditions.checkNotNull(jobDefinition, "jobDefinition");

            this.jobDefinition = jobDefinition;
            this.submitter = user;
            this.clusterName = clusterName;
            this.isAutoResubmit = false;
            this.submitLatest = submitLatest;
        }

        public SubmitJobRequest(String clusterName, String submitter, JobDefinition jobDefinition) {
            this(clusterName, submitter, false, Optional.of(jobDefinition));
        }

        //quick submit
        public SubmitJobRequest(final String clusterName, final String user) {
            this(clusterName, user, false, Optional.empty());
        }

        // used to during sla enforcement
        public SubmitJobRequest(
                final String clusterName,
                final String user,
                boolean isAutoResubmit,
                final Optional<JobDefinition> jobDefinition) {
            super();
            Preconditions.checkArg(user != null && !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(clusterName != null && !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            this.jobDefinition = jobDefinition;
            this.submitter = user;
            this.clusterName = clusterName;
            this.isAutoResubmit = isAutoResubmit;
            this.submitLatest = false;
        }

        public Optional<JobDefinition> getJobDefinition() {
            return jobDefinition;
        }

        public String getSubmitter() {
            return submitter;
        }

        public String getClusterName() {
            return clusterName;
        }

        public boolean isAutoResubmit() {
            return isAutoResubmit;
        }

        public boolean isSubmitLatest() {
            return submitLatest;
        }
    }

    public static final class SubmitJobResponse extends BaseResponse {
        private final Optional<JobId> jobId;

        public SubmitJobResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final Optional<JobId> jobId) {
            super(requestId, responseCode, message);
            this.jobId = jobId;
        }

        public Optional<JobId> getJobId() {
            return jobId;
        }

        @Override
        public String toString() {
            return "SubmitJobResponse{" +
                   "jobId=" + jobId +
                   ", requestId=" + requestId +
                   ", responseCode=" + responseCode +
                   ", message='" + message + '\'' +
                   '}';
        }
    }

    public static final class GetJobDetailsRequest extends BaseRequest {
        private final String user;
        private final JobId jobId;

        public GetJobDetailsRequest(final String user, final JobId jobId) {
            super();
            this.jobId = jobId;
            this.user = user;

        }

        public GetJobDetailsRequest(final String user, final String jobId) {
            super();
            Preconditions.checkNotNull(user, "user");
            Preconditions.checkArg(
                    jobId != null && !jobId.isEmpty(),
                    "Must provide job ID in request");
            Optional<JobId> jOp = JobId.fromId(jobId);
            if (jOp.isPresent()) {
                this.jobId = jOp.get();
            } else {
                throw new IllegalArgumentException(
                        String.format("Invalid jobId %s. JobId must be in the format [JobCLusterName-NumericID]", jobId));
            }
            this.user = user;
        }

        public JobId getJobId() {
            return jobId;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "GetJobDetailsRequest [jobId=" + jobId + ", user=" + user + "]";
        }
    }

    /**
     * This request is sent to a job actor asking the job actor to use its current state to merge live metadata e.g. stage
     * instance count to the given job definition object for job submission process to inherit from target job actor.
     */
    @Getter
    @ToString
    public static final class GetJobDefinitionUpdatedFromJobActorRequest extends BaseRequest {
        private final String user;
        private final JobId jobId;
        private final JobDefinition jobDefinition;
        private final boolean isQuickSubmit;
        private final boolean isAutoResubmit;
        private final ActorRef originalSender;

        public GetJobDefinitionUpdatedFromJobActorRequest(final String user,
                                                          final JobId jobId,
                                                          final JobDefinition jobDefinition,
                                                          final boolean isQuickSubmit,
                                                          final boolean isAutoResubmit,
                                                          final ActorRef originalSender) {
            super();
            Preconditions.checkNotNull(user, "user");
            Preconditions.checkNotNull(jobId, "jobId");
            Preconditions.checkNotNull(originalSender, "originalSender");
            Preconditions.checkNotNull(jobDefinition, "jobDefinition");
            this.jobId = jobId;
            this.user = user;
            this.jobDefinition = jobDefinition;
            this.isQuickSubmit = isQuickSubmit;
            this.isAutoResubmit = isAutoResubmit;
            this.originalSender = originalSender;
        }
    }


    public static final class GetJobDetailsResponse extends BaseResponse {
        private final Optional<IMantisJobMetadata> jobMetadata;

        public GetJobDetailsResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final Optional<IMantisJobMetadata> jobMetadata) {
            super(requestId, responseCode, message);
            this.jobMetadata = jobMetadata;
        }

        public Optional<IMantisJobMetadata> getJobMetadata() {
            return jobMetadata;
        }

        @Override
        public String toString() {
            return "GetJobDetailsResponse [jobMetadata=" + jobMetadata + "]";
        }
    }

    @ToString
    @Getter
    public static final class GetJobDefinitionUpdatedFromJobActorResponse extends BaseResponse {
        private final String user;
        private final JobDefinition jobDefinition;
        private final boolean isAutoResubmit;
        private final boolean isQuickSubmitMode;
        private final ActorRef originalSender;

        public GetJobDefinitionUpdatedFromJobActorResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final String user,
                final JobDefinition jobDefn,
                final boolean isAutoResubmit,
                final boolean isQuickSubmitMode,
                final ActorRef originalSender) {
            super(requestId, responseCode, message);
            Preconditions.checkNotNull(user, "user");
            Preconditions.checkNotNull(originalSender, "originalSender");
            this.user = user;
            this.jobDefinition = jobDefn;
            this.isAutoResubmit = isAutoResubmit;
            this.isQuickSubmitMode = isQuickSubmitMode;
            this.originalSender = originalSender;
        }
    }

    public static final class GetLatestJobDiscoveryInfoRequest extends BaseRequest {

        private final String jobCluster;

        public GetLatestJobDiscoveryInfoRequest(final String jobCluster) {
            Preconditions.checkNotNull(jobCluster, "jobCluster");
            this.jobCluster = jobCluster;
        }

        public String getJobCluster() {
            return jobCluster;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetLatestJobDiscoveryInfoRequest that = (GetLatestJobDiscoveryInfoRequest) o;
            return Objects.equals(jobCluster, that.jobCluster);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobCluster);
        }

        @Override
        public String toString() {
            return "GetLatestJobDiscoveryInfoRequest{" +
                "jobCluster='" + jobCluster + '\'' +
                '}';
        }
    }

    public static final class GetLatestJobDiscoveryInfoResponse extends BaseResponse {
        private final Optional<JobSchedulingInfo> jobSchedulingInfo;

        public GetLatestJobDiscoveryInfoResponse(
            final long requestId,
            final ResponseCode code,
            final String msg,
            final Optional<JobSchedulingInfo> jobSchedulingInfo) {
            super(requestId, code, msg);
            this.jobSchedulingInfo = jobSchedulingInfo;
        }

        public Optional<JobSchedulingInfo> getDiscoveryInfo() {
            return jobSchedulingInfo;
        }
    }

    public static final class GetJobSchedInfoRequest extends BaseRequest {

        private final JobId jobId;

        public GetJobSchedInfoRequest(final JobId jobId) {
            this.jobId = jobId;
        }

        public JobId getJobId() {
            return jobId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final GetJobSchedInfoRequest that = (GetJobSchedInfoRequest) o;
            return Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId);
        }

        @Override
        public String toString() {
            return "GetJobStatusSubjectRequest{" +
                   "jobId=" + jobId +
                   '}';
        }
    }

    public static final class GetJobSchedInfoResponse extends BaseResponse {
        private final Optional<BehaviorSubject<JobSchedulingInfo>> jobStatusSubject;

        public GetJobSchedInfoResponse(
                final long requestId,
                final ResponseCode code,
                final String msg,
                final Optional<BehaviorSubject<JobSchedulingInfo>> statusSubject) {
            super(requestId, code, msg);
            this.jobStatusSubject = statusSubject;
        }

        public Optional<BehaviorSubject<JobSchedulingInfo>> getJobSchedInfoSubject() {
            return jobStatusSubject;
        }
    }

    /**
     * Stream of JobId submissions for a cluster
     */
    public static final class GetLastSubmittedJobIdStreamRequest extends BaseRequest {
        private final String clusterName;

        public GetLastSubmittedJobIdStreamRequest(final String clusterName) {
            Preconditions.checkArg(
                    clusterName != null & !clusterName.isEmpty(),
                    "Must provide job cluster name in request");
            this.clusterName = clusterName;
        }

        public String getClusterName() {
            return clusterName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final GetLastSubmittedJobIdStreamRequest that = (GetLastSubmittedJobIdStreamRequest) o;
            return Objects.equals(clusterName, that.clusterName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName);
        }

        @Override
        public String toString() {
            return "GetLastSubmittedJobIdStreamRequest{" +
                   "clusterName='" + clusterName + '\'' +
                   '}';
        }
    }

    public static final class GetLastSubmittedJobIdStreamResponse extends BaseResponse {
        private final Optional<BehaviorSubject<JobId>> jobIdBehaviorSubject;

        public GetLastSubmittedJobIdStreamResponse(
                final long requestId,
                final ResponseCode code,
                final String msg,
                final Optional<BehaviorSubject<JobId>> jobIdBehaviorSubject) {
            super(requestId, code, msg);
            this.jobIdBehaviorSubject = jobIdBehaviorSubject;
        }

        public Optional<BehaviorSubject<JobId>> getjobIdBehaviorSubject() {
            return this.jobIdBehaviorSubject;
        }

    }

    public static final class KillJobRequest extends BaseRequest {
        private final JobId jobId;
        private final String reason;
        private final String user;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public KillJobRequest(
                @JsonProperty("JobId") final String jobId,
                @JsonProperty("reason") final String reason,
                @JsonProperty("user") final String user) {
            super();
            Preconditions.checkArg(user != null & !user.isEmpty(), "Must provide user in request");
            Preconditions.checkArg(
                    jobId != null & !jobId.isEmpty(),
                    "Must provide job ID in request");

            this.jobId = JobId.fromId(jobId).get();
            this.reason = Optional.ofNullable(reason).orElse("");
            this.user = user;

        }

        public JobId getJobId() {
            return jobId;
        }

        public String getReason() {
            return reason;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "KillJobRequest [jobId=" + jobId + ", reason=" + reason + ", user=" + user + "]";
        }
    }

    public static final class KillJobResponse extends BaseResponse {
        private final JobId jobId;

        private final JobState state;
        private final String user;

        public KillJobResponse(
                long requestId,
                ResponseCode responseCode,
                JobState state,
                String message,
                JobId jobId,
                String user) {
            super(requestId, responseCode, message);
            this.jobId = jobId;

            this.state = state;
            this.user = user;
        }

        public JobId getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "KillJobResponse [jobId=" + jobId + ", state=" + state + ", user="
                   + user + "]";
        }
    }

    public static final class ScaleStageRequest extends BaseRequest {
        private final int stageNum;
        private final int numWorkers;
        private final String user;
        private final String reason;
        private final JobId jobId;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public ScaleStageRequest(
                @JsonProperty("JobId") final String jobId,
                @JsonProperty("StageNumber") final Integer stageNo,
                @JsonProperty("NumWorkers") final Integer numWorkers,
                @JsonProperty("User") final String user,
                @JsonProperty("Reason") final String reason) {
            super();
            Preconditions.checkArg(
                    jobId != null & !jobId.isEmpty(),
                    "Must provide job ID in request");
            Preconditions.checkArg(stageNo > 0, "Invalid stage Number " + stageNo);
            Preconditions.checkArg(
                    numWorkers != null && numWorkers > 0,
                    "NumWorkers must be greater than 0");
            this.stageNum = stageNo;
            this.numWorkers = numWorkers;
            this.user = Optional.ofNullable(user).orElse("UserNotKnown");
            this.reason = Optional.ofNullable(reason).orElse("");
            this.jobId = JobId.fromId(jobId).get();
        }

        public int getStageNum() {
            return stageNum;
        }

        public int getNumWorkers() {
            return numWorkers;
        }

        public String getUser() {
            return user;
        }

        public String getReason() {
            return reason;
        }

        public JobId getJobId() {
            return jobId;
        }

        @Override
        public String toString() {
            return "ScaleStageRequest{" +
                   "stageNum=" + stageNum +
                   ", numWorkers=" + numWorkers +
                   ", user='" + user + '\'' +
                   ", reason='" + reason + '\'' +
                   ", jobId=" + jobId +
                   '}';
        }
    }

    public static final class ScaleStageResponse extends BaseResponse {
        private final int actualNumWorkers;

        public ScaleStageResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message,
                final int actualNumWorkers) {
            super(requestId, responseCode, message);
            this.actualNumWorkers = actualNumWorkers;
        }

        public int getActualNumWorkers() {
            return actualNumWorkers;
        }

        @Override
        public String toString() {
            return "ScaleStageResponse{" +
                   "actualNumWorkers=" + actualNumWorkers +
                   '}';
        }
    }

    public static final class ResubmitWorkerRequest extends BaseRequest {
        private final String user;
        private final JobId jobId;
        private final int workerNum;
        private final Optional<String> reason;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public ResubmitWorkerRequest(
                @JsonProperty("JobId") final String jobIdStr,
                @JsonProperty("workerNumber") final Integer workerNum,
                @JsonProperty("user") final String user,
                @JsonProperty("reason") final Optional<String> reason) {
            super();
            Preconditions.checkArg(
                    jobIdStr != null & !jobIdStr.isEmpty(),
                    "Must provide job ID in request");
            Preconditions.checkNotNull(workerNum, "workerNumber");
            Preconditions.checkArg(workerNum > 0, "Worker number must be greater than 0");
            this.jobId = JobId.fromId(jobIdStr)
                              .orElseThrow(() -> new IllegalArgumentException(
                                      "invalid JobID in resubmit worker request " + jobIdStr));
            this.workerNum = workerNum;
            this.user = user;
            this.reason = reason;
        }

        public JobId getJobId() {
            return jobId;
        }

        public int getWorkerNum() {
            return workerNum;
        }

        public String getUser() {
            return user;
        }

        public Optional<String> getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return "ResubmitWorkerRequest{" +
                   "user='" + user + '\'' +
                   ", jobId=" + jobId +
                   ", workerNum=" + workerNum +
                   ", reason=" + reason +
                   '}';
        }
    }

    public static final class V1ResubmitWorkerRequest extends BaseRequest {
        private final String user;
        private final int workerNum;
        private final Optional<String> reason;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public V1ResubmitWorkerRequest(
                @JsonProperty("workerNumber") final Integer workerNum,
                @JsonProperty("user") final String user,
                @JsonProperty("reason") final Optional<String> reason) {
            super();
            Preconditions.checkNotNull(workerNum, "workerNumber");
            Preconditions.checkArg(workerNum > 0, "Worker number must be greater than 0");
            this.workerNum = workerNum;
            this.user = user;
            this.reason = reason;
        }

        public int getWorkerNum() {
            return workerNum;
        }

        public String getUser() {
            return user;
        }

        public Optional<String> getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return "ResubmitWorkerRequest{" +
                    "user='" + user + '\'' +
                    ", workerNum=" + workerNum +
                    ", reason=" + reason +
                    '}';
        }
    }

    public static final class ResubmitWorkerResponse extends BaseResponse {

        public ResubmitWorkerResponse(
                final long requestId,
                final ResponseCode responseCode,
                final String message) {
            super(requestId, responseCode, message);
        }

        @Override
        public String toString() {
            return "ResubmitWorkerResponse [requestId=" + requestId + ", respCode=" + responseCode +
                   ", message="
                   + message + "]";
        }
    }


}
