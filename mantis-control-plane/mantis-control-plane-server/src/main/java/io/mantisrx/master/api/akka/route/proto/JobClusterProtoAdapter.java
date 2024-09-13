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

package io.mantisrx.master.api.akka.route.proto;

import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.LabelManager.SystemLabels;
import io.mantisrx.master.jobcluster.MantisJobClusterMetadataView;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.server.master.http.api.CompactJobInfo;
import io.mantisrx.server.master.http.api.JobClusterInfo;
import io.mantisrx.server.master.store.MantisJobMetadata;
import io.mantisrx.server.master.store.MantisStageMetadata;
import io.mantisrx.server.master.store.MantisWorkerMetadata;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobClusterProtoAdapter {
    // explicit private constructor to prohibit instantiation
    private JobClusterProtoAdapter() {}

    public static final CreateJobClusterRequest toCreateJobClusterRequest(final NamedJobDefinition njd) {
        MantisJobDefinition jd = njd.getJobDefinition();

        final CreateJobClusterRequest request = new CreateJobClusterRequest(new JobClusterDefinitionImpl(
                jd.getName(),
                Arrays.asList(new JobClusterConfig(
                        jd.getJobJarFileLocation().toString(),
                        jd.getJobJarFileLocation().toString(),
                        System.currentTimeMillis(),
                        jd.getVersion(),
                        jd.getSchedulingInfo()

                        )),
                njd.getOwner(),

                jd.getUser(),
                new SLA(jd.getSlaMin(),
                        jd.getSlaMax(),
                        jd.getCronSpec(),
                        jd.getCronPolicy() == NamedJobDefinition.CronPolicy.KEEP_EXISTING ?
                                IJobClusterDefinition.CronPolicy.KEEP_EXISTING : IJobClusterDefinition.CronPolicy.KEEP_NEW),
                jd.getMigrationConfig(),
                jd.getIsReadyForJobMaster(),
                jd.getParameters(),
                processLabels(jd)
        )

        , "user"
                );

        return request;
    }

//    public static final JobSla toJobSla(final io.mantisrx.master.core.proto.JobSla protoSla) {
//        return new JobSla(protoSla.getRuntimeLimitSecs(),
//            protoSla.getMinRuntimeSecs(),
//            JobSla.StreamSLAType.valueOf(protoSla.getSlaType().name()),
//            MantisJobDurationType.valueOf(protoSla.getDurationType().name()),
//            protoSla.getUserProvidedType());
//    }
//
//    public static final MachineDefinition toMachineDefinition(final io.mantisrx.master.core.proto.MachineDefinition md) {
//        return new MachineDefinition(md.getCpuCores(),
//            md.getMemoryMB(), md.getNetworkMbps(), md.getDiskMB(), md.getNumPorts());
//    }


//    public static final StageScalingPolicy.Strategy toStageScalingStrategy(final io.mantisrx.master.core.proto.StageScalingPolicy.Strategy s) {
//        return new StageScalingPolicy.Strategy(
//            StageScalingPolicy.ScalingReason.valueOf(s.getReason().name()),
//            s.getScaleDownBelowPct(),
//            s.getScaleUpAbovePct(),
//            s.hasRollingCount() ?
//                new StageScalingPolicy.RollingCount(
//                    s.getRollingCount().getCount(),
//                    s.getRollingCount().getOf()) :
//                null
//        );
//    }
//    public static final StageScalingPolicy toStageScalingPolicy(final io.mantisrx.master.core.proto.StageScalingPolicy p) {
//        return new StageScalingPolicy(
//            p.getStage(),
//            p.getMin(),
//            p.getMax(),
//            p.getIncrement(),
//            p.getDecrement(),
//            p.getCoolDownSecs(),
//            p.getStrategiesMap().entrySet().stream().collect(
//                Collectors.toMap(
//                    e -> StageScalingPolicy.ScalingReason.valueOf(e.getKey()),
//                    e -> toStageScalingStrategy(e.getValue())
//                )
//            )
//        );
//    }
//
//    private static final StageSchedulingInfo toStageSchedulingInfo(final io.mantisrx.master.core.proto.SchedulingInfo.StageSchedulingInfo s) {
//        return new StageSchedulingInfo(
//            s.getNumberOfInstances(),
//            toMachineDefinition(s.getMachineDefinition()),
//            s.getHardConstraintsList().stream().map(c -> JobConstraints.valueOf(c.name())).collect(Collectors.toList()),
//            s.getSoftConstraintsList().stream().map(c -> JobConstraints.valueOf(c.name())).collect(Collectors.toList()),
//            s.hasScalingPolicy() ? toStageScalingPolicy(s.getScalingPolicy()) : null,
//            s.getScalable()
//        );
//    }
//    private static final SchedulingInfo toSchedulingInfo(final io.mantisrx.master.core.proto.SchedulingInfo s) {
//
//        return new SchedulingInfo(
//            s.getStagesMap().entrySet().stream()
//            .collect(Collectors.toMap(e -> e.getKey(),
//                e -> toStageSchedulingInfo(e.getValue())))
//        );
//    }

//    private static final WorkerMigrationConfig toMigrationConfig(final io.mantisrx.master.core.proto.WorkerMigrationConfig cfg) {
//        return new WorkerMigrationConfig(
//            WorkerMigrationConfig.MigrationStrategyEnum.valueOf(cfg.getStrategy().name()),
//            cfg.getConfigString()
//        );
//    }

//    public static final MantisJobDefinition toMantisJobDefinition(final JobSubmitRequest jsr) throws MalformedURLException {
//
//        return new MantisJobDefinition(jsr.getName(),
//            jsr.getUser(),
//            new URL(jsr.getUrl()),
//            jsr.getVersion(),
//            jsr.getParametersList().stream().map(p -> new Parameter(p.getName(), p.getValue())).collect(Collectors.toList()),
//            jsr.hasJobSla() ? toJobSla(jsr.getJobSla()) : null,
//            jsr.getSubscriptionTimeoutSecs(),
//            jsr.hasSchedulingInfo() ? toSchedulingInfo(jsr.getSchedulingInfo()) : null,
//            jsr.getSlaMin(),
//            jsr.getSlaMax(),
//            jsr.getCronSpec(),
//            NamedJobDefinition.CronPolicy.valueOf(jsr.getCronPolicy().name()),
//            true,
//            jsr.hasMigrationConfig() ? toMigrationConfig(jsr.getMigrationConfig()) : WorkerMigrationConfig.DEFAULT,
//            jsr.getLabelsList().stream().map(l -> new Label(l.getName(), l.getValue())).collect(Collectors.toList())
//        );
//    }

    public static final UpdateJobClusterRequest toUpdateJobClusterRequest(final NamedJobDefinition njd) {
        MantisJobDefinition jd = njd.getJobDefinition();

        final UpdateJobClusterRequest request = new UpdateJobClusterRequest(new JobClusterDefinitionImpl(
                jd.getName(),
                Arrays.asList(new JobClusterConfig(
                    jd.getJobJarFileLocation().toString(),
                    jd.getJobJarFileLocation().toString(),
                    System.currentTimeMillis(),
                    jd.getVersion(),
                    jd.getSchedulingInfo()
                    )),
                njd.getOwner(),
                jd.getUser(),

                new SLA(jd.getSlaMin(),
                        jd.getSlaMax(),
                        jd.getCronSpec(),
                        jd.getCronPolicy() == NamedJobDefinition.CronPolicy.KEEP_EXISTING ?
                                IJobClusterDefinition.CronPolicy.KEEP_EXISTING : IJobClusterDefinition.CronPolicy.KEEP_NEW),
                jd.getMigrationConfig(),
                jd.getIsReadyForJobMaster(),
                jd.getParameters(),
                processLabels(jd)
        ),

        "user");

        return request;
    }

//    public static final JobClusterManagerProto.SubmitJobRequest toSubmitJobClusterRequest(final SubmitJobRequest jd)
//        throws InvalidJobException {
//
//        final JobClusterManagerProto.SubmitJobRequest request = new JobClusterManagerProto.SubmitJobRequest(
//            jd.getName(),
//            jd.getUser(),
//            Optional.of(
//                new JobDefinition(
//                    jd.getName(),
//                    jd.getUser(),
//                    (DataFormatAdapter.extractArtifactName(jd.getJobJarFileLocation())).orElse(""),
//                    jd.getVersion(),
//                    jd.getParametersList().stream().map(p -> new Parameter(p.getName(), p.getValue())).collect(Collectors.toList()),
//                    jd.hasJobSla() ? toJobSla(jd.getJobSla()) : null,
//                    jd.getSubscriptionTimeoutSecs(),
//                    jd.hasSchedulingInfo() ? toSchedulingInfo(jd.getSchedulingInfo()) : null,
//                    jd.getSchedulingInfo() == null ? -1 : jd.getSchedulingInfo().getStagesMap().size(),
//                    jd.getLabelsList().stream().map(l -> new Label(l.getName(), l.getValue())).collect(Collectors.toList()))
//            ));
//
//        return request;
//    }

    public static final JobClusterManagerProto.SubmitJobRequest toSubmitJobClusterRequest(final MantisJobDefinition jd)
        throws InvalidJobException {
        final JobClusterManagerProto.SubmitJobRequest request = new JobClusterManagerProto.SubmitJobRequest(
            jd.getName(),
            jd.getUser(),
            new JobDefinition(
                jd.getName(),
                jd.getUser(),
                (jd.getJobJarFileLocation() == null) ? "" : jd.getJobJarFileLocation().toString(),
                (DataFormatAdapter.extractArtifactName(jd.getJobJarFileLocation())).orElse(""),
                jd.getVersion(),
                jd.getParameters(),
                jd.getJobSla(),
                jd.getSubscriptionTimeoutSecs(),
                jd.getSchedulingInfo(),
                jd.getSchedulingInfo() == null ? -1 : jd.getSchedulingInfo().getStages().size(),
                processLabels(jd),
                jd.getDeploymentStrategy()));

        return request;
    }

    public static JobClusterInfo toJobClusterInfo(MantisJobClusterMetadataView jobClusterMetadataView) {
        List<JobClusterInfo.JarInfo> jarInfoList = DataFormatAdapter.convertNamedJobJarListToJarInfoList(jobClusterMetadataView.getJars());
        JobClusterInfo jobClusterInfo =  new JobClusterInfo(jobClusterMetadataView.getName(),
                jobClusterMetadataView.getSla(),
                jobClusterMetadataView.getOwner(),
                jobClusterMetadataView.isDisabled(),
                jobClusterMetadataView.isCronActive(),
                jarInfoList,
                jobClusterMetadataView.getParameters(),
                jobClusterMetadataView.getLabels());

        return jobClusterInfo;
    }

    protected static List<Label> processLabels(MantisJobDefinition jd) {
        Map<String, Label> labelMap = new HashMap<>();
        jd.getLabels().forEach(l -> labelMap.put(l.getName(), l));
        if (jd.getDeploymentStrategy() != null &&
            !Strings.isNullOrEmpty(jd.getDeploymentStrategy().getResourceClusterId())) {
            Label rcLabel = new Label(
                SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label,
                jd.getDeploymentStrategy().getResourceClusterId());
            labelMap.put(rcLabel.getName(), rcLabel);
        }

        return ImmutableList.copyOf(labelMap.values());
    }

    public static class JobIdInfo {
        private final String jobId;
        private final String version;
        private final MantisJobState state;
        private final String submittedAt;
        private final String terminatedAt;
        private final String user;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public JobIdInfo(@JsonProperty("jobId") String jobId,
                         @JsonProperty("version") String version,
                         @JsonProperty("state") MantisJobState state,
                         @JsonProperty("submittedAt") String submittedAt,
                         @JsonProperty("terminatedAt") String terminatedAt,
                         @JsonProperty("user") String user) {
            this.jobId = jobId;
            this.version = version;
            this.state = state;
            this.submittedAt = submittedAt;
            this.terminatedAt = terminatedAt;
            this.user = user;
        }
        public String getJobId() {
            return jobId;
        }
        public String getVersion() {
            return version;
        }
        public MantisJobState getState() {
            return state;
        }
        public String getSubmittedAt() {
            return submittedAt;
        }
        public String getTerminatedAt() {
            return terminatedAt;
        }
        public String getUser() {
            return user;
        }

        @Override
        public String toString() {
            return "JobIdInfo{" +
                "jobId='" + jobId + '\'' +
                ", version='" + version + '\'' +
                ", state=" + state +
                ", submittedAt='" + submittedAt + '\'' +
                ", terminatedAt='" + terminatedAt + '\'' +
                ", user='" + user + '\'' +
                '}';
        }

        public static class Builder {
            private  String jobId;
            private  String version;
            private  MantisJobState state;
            private  String submittedAt = "";
            private  String terminatedAt = "";
            private  String user = "";
            public Builder() {

            }

            public Builder withJobIdStr(String jobId) {
                this.jobId = jobId;
                return this;
            }
            public Builder withJobId(JobId jId) {
                jobId = jId.getId();
                return this;
            }

            public Builder withJobState(JobState st) {
                state = toJobState(st);
                return this;
            }

            public Builder withVersion(String version) {
                this.version = version;
                return this;
            }

            public Builder withSubmittedAt(long time) {
                submittedAt = Long.toString(time);
                return this;
            }

            public Builder withTerminatedAt(long time) {
                if(time != -1) {
                   terminatedAt = Long.toString(time);
                }
                return this;
            }

            public Builder withUser(String user) {
                this.user = user;
                return this;
            }

            public JobIdInfo build() {
                return new JobIdInfo(jobId,version,state,submittedAt,terminatedAt,user);
            }

        }
    }

    private static MantisJobState toJobState(final JobState state) {
        switch (state) {
            case Accepted:
                return MantisJobState.Accepted;
            case Launched:
                return MantisJobState.Launched;
            case Terminating_normal:
            case Completed:
                return MantisJobState.Completed;
            case Terminating_abnormal:
            case Failed:
                return MantisJobState.Failed;
            case Noop:
                return MantisJobState.Noop;
            default:
                    throw new IllegalArgumentException("cannot translate JobState to MantisJobState " + state);
        }
    }

    public static final CompactJobInfo toCompactJobInfo(final MantisJobMetadataView view) {
        MantisJobMetadata jm = view.getJobMetadata();

        int workers=0;
        double totCPUs = 0.0;
        double totMem = 0.0;
        Map<String, Integer> stSmry = new HashMap<>();

        for (MantisStageMetadata s: view.getStageMetadataList()) {
            workers += s.getNumWorkers();
            totCPUs += s.getNumWorkers() * s.getMachineDefinition().getCpuCores();
            totMem += s.getNumWorkers() * s.getMachineDefinition().getMemoryMB();
        }
        for (MantisWorkerMetadata w: view.getWorkerMetadataList()) {
            final Integer prevVal = stSmry.get(w.getState() + "");
            if (prevVal == null) {
                stSmry.put(w.getState() + "", 1);
            } else {
                stSmry.put(w.getState() + "", prevVal + 1);
            }
        }


        return new CompactJobInfo(
            jm.getJobId(),
            (jm.getJarUrl() != null) ? jm.getJarUrl().toString() : "",
            jm.getSubmittedAt(),
            view.getTerminatedAt(),
            jm.getUser(),
            jm.getState(),
            jm.getSla() != null ? jm.getSla().getDurationType() : MantisJobDurationType.Transient,
            jm.getNumStages(),
            workers,
            totCPUs,
            totMem,
            stSmry,
            jm.getLabels()
        );
    }
}
