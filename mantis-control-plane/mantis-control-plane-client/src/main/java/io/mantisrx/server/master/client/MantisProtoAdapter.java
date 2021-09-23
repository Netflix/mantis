/*
 * Copyright 2021 Netflix, Inc.
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
//package io.mantisrx.server.master.client;
//
//import io.mantisrx.common.Label;
//import io.mantisrx.master.api.proto.CreateJobClusterRequest;
//import io.mantisrx.master.api.proto.UpdateJobClusterRequest;
//import io.mantisrx.master.core.proto.JobDefinition;
//import io.mantisrx.runtime.JobConstraints;
//import io.mantisrx.runtime.JobOwner;
//import io.mantisrx.runtime.JobSla;
//import io.mantisrx.runtime.MachineDefinition;
//import io.mantisrx.runtime.MantisJobDefinition;
//import io.mantisrx.runtime.MantisJobDurationType;
//import io.mantisrx.runtime.NamedJobDefinition;
//import io.mantisrx.runtime.WorkerMigrationConfig;
//import io.mantisrx.runtime.descriptor.SchedulingInfo;
//import io.mantisrx.runtime.descriptor.StageScalingPolicy;
//import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
//import io.mantisrx.runtime.parameter.Parameter;
//
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.util.stream.Collectors;
//
//public class MantisProtoAdapter {
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
//    public static final MachineDefinition toMachineDefinition(final io.mantisrx.master.core.proto.MachineDefinition md) {
//        return new MachineDefinition(md.getCpuCores(),
//            md.getMemoryMB(), md.getNetworkMbps(), md.getDiskMB(), md.getNumPorts());
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
//                .collect(Collectors.toMap(e -> e.getKey(),
//                    e -> toStageSchedulingInfo(e.getValue())))
//        );
//    }
//
//    public static final JobSla toJobSla(final io.mantisrx.master.core.proto.JobSla protoSla) {
//        return new JobSla(protoSla.getRuntimeLimitSecs(),
//            protoSla.getMinRuntimeSecs(),
//            JobSla.StreamSLAType.valueOf(protoSla.getSlaType().name()),
//            MantisJobDurationType.valueOf(protoSla.getDurationType().name()),
//            protoSla.getUserProvidedType());
//    }
//
//    private static final WorkerMigrationConfig toMigrationConfig(final io.mantisrx.master.core.proto.WorkerMigrationConfig cfg) {
//        return new WorkerMigrationConfig(
//            WorkerMigrationConfig.MigrationStrategyEnum.valueOf(cfg.getStrategy().name()),
//            cfg.getConfigString()
//        );
//    }
//
//    private static final JobOwner toJobOwner(final io.mantisrx.master.core.proto.JobOwner owner) {
//        return new JobOwner(
//            owner.getName(),
//            owner.getTeamName(),
//            owner.getDescription(),
//            owner.getContactEmail(),
//            owner.getRepo()
//        );
//    }
//
//    public static NamedJobDefinition toNamedJobDefinition(final CreateJobClusterRequest request) throws MalformedURLException {
//        JobDefinition jd = request.getJobDefinition();
//        io.mantisrx.master.core.proto.JobOwner owner = request.getOwner();
//        MantisJobDefinition jobDefinition = new MantisJobDefinition(
//            jd.getName(),
//            jd.getUser(),
//            jd.getUrl() == null ? null : new URL(jd.getUrl()),
//            jd.getVersion(),
//            jd.getParametersList().stream().map(p -> new Parameter(p.getName(), p.getValue())).collect(Collectors.toList()),
//            jd.hasJobSla() ? toJobSla(jd.getJobSla()) : null,
//            jd.getSubscriptionTimeoutSecs(),
//            jd.hasSchedulingInfo() ? toSchedulingInfo(jd.getSchedulingInfo()) : null,
//            jd.getSlaMin(),
//            jd.getSlaMax(),
//            jd.getCronSpec(),
//            NamedJobDefinition.CronPolicy.valueOf(jd.getCronPolicy().name()),
//            jd.getIsReadyForJobMaster(),
//            jd.hasMigrationConfig() ? toMigrationConfig(jd.getMigrationConfig()) : WorkerMigrationConfig.DEFAULT,
//            jd.getLabelsList().stream().map(l -> new Label(l.getName(), l.getValue())).collect(Collectors.toList()));
//        return new NamedJobDefinition(
//            jobDefinition,
//            request.hasOwner() ? toJobOwner(owner) : null
//        );
//    }
//
//    public static NamedJobDefinition toNamedJobDefinition(final UpdateJobClusterRequest request) throws MalformedURLException {
//        JobDefinition jd = request.getJobDefinition();
//        io.mantisrx.master.core.proto.JobOwner owner = request.getOwner();
//        MantisJobDefinition jobDefinition = new MantisJobDefinition(
//            jd.getName(),
//            jd.getUser(),
//            jd.getUrl() == null ? null : new URL(jd.getUrl()),
//            jd.getVersion(),
//            jd.getParametersList().stream().map(p -> new Parameter(p.getName(), p.getValue())).collect(Collectors.toList()),
//            jd.hasJobSla() ? toJobSla(jd.getJobSla()) : null,
//            jd.getSubscriptionTimeoutSecs(),
//            jd.hasSchedulingInfo() ? toSchedulingInfo(jd.getSchedulingInfo()) : null,
//            jd.getSlaMin(),
//            jd.getSlaMax(),
//            jd.getCronSpec(),
//            NamedJobDefinition.CronPolicy.valueOf(jd.getCronPolicy().name()),
//            jd.getIsReadyForJobMaster(),
//            jd.hasMigrationConfig() ? toMigrationConfig(jd.getMigrationConfig()) : WorkerMigrationConfig.DEFAULT,
//            jd.getLabelsList().stream().map(l -> new Label(l.getName(), l.getValue())).collect(Collectors.toList()));
//        return new NamedJobDefinition(
//            jobDefinition,
//            request.hasOwner() ? toJobOwner(owner) : null
//        );
//    }
//
//}
