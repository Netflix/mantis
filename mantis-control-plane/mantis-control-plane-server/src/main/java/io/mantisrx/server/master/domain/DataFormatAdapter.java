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

package io.mantisrx.server.master.domain;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import io.mantisrx.common.Label;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.JobClusterMetadataImpl;
import io.mantisrx.master.jobcluster.job.FilterableMantisJobMetadataWritable;
import io.mantisrx.master.jobcluster.job.FilterableMantisStageMetadataWritable;
import io.mantisrx.master.jobcluster.job.FilterableMantisWorkerMetadataWritable;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.master.jobcluster.job.MantisStageMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.master.MantisJobOperations;
import io.mantisrx.server.master.MantisJobStatus;
import io.mantisrx.server.master.http.api.JobClusterInfo;
import io.mantisrx.server.master.store.InvalidNamedJobException;
import io.mantisrx.server.master.store.MantisJobMetadata;
import io.mantisrx.server.master.store.MantisJobMetadataWritable;
import io.mantisrx.server.master.store.MantisStageMetadata;
import io.mantisrx.server.master.store.MantisStageMetadataWritable;
import io.mantisrx.server.master.store.MantisWorkerMetadata;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.server.master.store.NamedJob;
import io.mantisrx.server.master.store.NamedJobDeleteException;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;


public class DataFormatAdapter {

    private static final Logger logger = LoggerFactory.getLogger(DataFormatAdapter.class);
    public static NamedJob convertJobClusterMetadataToNamedJob(IJobClusterMetadata jobCluster) {

        return new NamedJob(new NoOpMantisJobOperations(),
                jobCluster.getJobClusterDefinition().getName(),
                convertJobClusterConfigsToJars(jobCluster.getJobClusterDefinition().getJobClusterConfigs()),
                convertSLAToNamedJobSLA(jobCluster.getJobClusterDefinition().getSLA()),
                jobCluster.getJobClusterDefinition().getParameters(),
                jobCluster.getJobClusterDefinition().getOwner(),
                jobCluster.getLastJobCount(),
                jobCluster.isDisabled(),
                jobCluster.getJobClusterDefinition().getIsReadyForJobMaster(),
                jobCluster.getJobClusterDefinition().getWorkerMigrationConfig(),
                jobCluster.getJobClusterDefinition().getLabels());
    }

    public static NamedJob.CompletedJob convertCompletedJobToNamedJobCompletedJob(JobClusterDefinitionImpl.CompletedJob cJob) {
        return new NamedJob.CompletedJob(cJob.getName(), cJob.getJobId(), cJob.getVersion(), DataFormatAdapter.convertToMantisJobState(cJob.getState()), cJob.getSubmittedAt(), cJob.getTerminatedAt(), cJob.getUser(), cJob.getLabelList());
    }

    public static JobClusterDefinitionImpl.CompletedJob convertNamedJobCompletedJobToCompletedJob(NamedJob.CompletedJob completedJob) {
        return new JobClusterDefinitionImpl.CompletedJob(completedJob.getName(),completedJob.getJobId(),completedJob.getVersion(),DataFormatAdapter.convertMantisJobStateToJobState(completedJob.getState()),completedJob.getSubmittedAt(),completedJob.getTerminatedAt(),completedJob.getUser(), completedJob.getLabels());
    }


    public static IJobClusterMetadata convertNamedJobToJobClusterMetadata(NamedJob nJob) {


        return new JobClusterMetadataImpl.Builder()
                .withIsDisabled(nJob.getDisabled())
                .withLastJobCount(nJob.getLastJobCount())
                .withJobClusterDefinition(new JobClusterDefinitionImpl.Builder()
                        .withIsReadyForJobMaster(nJob.getIsReadyForJobMaster())
                        .withMigrationConfig(nJob.getMigrationConfig())
                        .withName(nJob.getName())
                        .withOwner(ofNullable(nJob.getOwner()).orElse(new JobOwner("unknown","unknown","","email@netflix.com","norepo")))
                        .withSla(DataFormatAdapter.convertToSLA(nJob.getSla()))
                        .withLabels(nJob.getLabels())
                        .withParameters(nJob.getParameters())
                        .withJobClusterConfigs(DataFormatAdapter.convertJarsToJobClusterConfigs(nJob.getJars()))
                        .withIsDisabled(nJob.getDisabled())
                        .build())
                .build();

    }


    public static List<NamedJob.Jar> convertJobClusterConfigsToJars(List<JobClusterConfig> jobClusterConfigs) {
        Preconditions.checkNotNull(jobClusterConfigs);
        List<NamedJob.Jar> jarList = new ArrayList<>(jobClusterConfigs.size());
        jobClusterConfigs.stream().forEach((jConfig) -> {
            try {
                jarList.add(convertJobClusterConfigToJar(jConfig));
            } catch (MalformedURLException e) {
                logger.warn("Exception {} transforming {}", e.getMessage(), jConfig);
            }

        });
        return jarList;
    }

    public static List<JobClusterConfig> convertJarsToJobClusterConfigs(List<NamedJob.Jar> jars) {
        Preconditions.checkNotNull(jars);
        List<JobClusterConfig> configs = new ArrayList<>(jars.size());
        jars.stream().forEach((jar) -> {
            try {
                configs.add(convertJarToJobClusterConfig(jar));
            } catch(Exception e) {
                logger.warn("Exception loading config {}. Skipping...", jar);
            }
        });
        return configs;
    }

    public static NamedJob.Jar convertJobClusterConfigToJar(JobClusterConfig jConfig) throws MalformedURLException {
        SchedulingInfo sInfo = jConfig.getSchedulingInfo();
        long uploadedAt = jConfig.getUploadedAt();
        String version = jConfig.getVersion();

        return new NamedJob.Jar(new URL(jConfig.getJobJarUrl()), uploadedAt, version, sInfo);
    }

    public static JobClusterConfig convertJarToJobClusterConfig(NamedJob.Jar jar ) {
        Preconditions.checkNotNull(jar);

        Optional<String> artifactName = extractArtifactName(jar.getUrl());
        String version = jar.getVersion();
        return new JobClusterConfig.Builder()
                .withJobJarUrl(jar.getUrl().toString())
                .withArtifactName(artifactName.orElse(""))
                .withVersion(version)
                .withSchedulingInfo(jar.getSchedulingInfo())
                .withUploadedAt(jar.getUploadedAt())
                .build();
    }


    public static URL generateURL(String artifactName) throws MalformedURLException {
        Preconditions.checkNotNull(artifactName, "Artifact Name cannot be null");
        if(!artifactName.startsWith("http") ) {
            return new URL("http://" + artifactName);
        }
        return new URL(artifactName);
    }

    public static Optional<String> extractArtifactName(String jarStr) {
        // http://somehose/my-artifact-name-0.0.1.zip
        //http://mantisui.eu-west-1.dyntest.netflix.net/mantis-artifacts/nfmantis-sources-genericqueryable-source-6.0.8.zip
        if(jarStr != null && !jarStr.isEmpty()) {
            int lastIndexOfForwardSlash = jarStr.lastIndexOf('/');
            if (lastIndexOfForwardSlash != -1) {
                String artifactName = jarStr.substring(lastIndexOfForwardSlash + 1, jarStr.length());
                return of(artifactName);
            }
        }
        logger.warn("Could not extract artifactName from " + jarStr);
        return empty();


    }

    public static Optional<String> extractArtifactName(URL jar) {

        if(jar != null) {
            String jarStr = jar.toString();

            return extractArtifactName(jarStr);
        }
        return empty();
    }

    /**
     * Extracts the base part of an artifact from a URL, excluding the .zip extension if present.
     * @param jar The URL of the artifact.
     * @return An optional that, if possible, contains the extracted artifact base name.
     */
    public static Optional<String> extractArtifactBaseName(URL jar) {
        if(jar != null) {
            String jarStr = jar.toString();
            return extractArtifactBaseName(jarStr);
        }
        return Optional.empty();
    }

    private static Optional<String> extractArtifactBaseName(String jarStr) {
        Optional<String> artifactNameOpt = extractArtifactName(jarStr);
        if (artifactNameOpt.isPresent()) {
            String artifactName = artifactNameOpt.get();
            if (artifactName.endsWith(".zip")) {
                // If the name ends with .zip, remove it
                return Optional.of(artifactName.substring(0, artifactName.length() - 4));
            } else {
                // If there's no .zip extension, return the entire string
                return Optional.of(artifactName);
            }
        }
        logger.warn("Could not extract artifactBaseName from " + jarStr);
        return Optional.empty();
    }

    public static NamedJob.SLA convertSLAToNamedJobSLA(io.mantisrx.server.master.domain.SLA sla) {

        return new NamedJob.SLA(sla.getMin(), sla.getMax(), sla.getCronSpec(), convertToNamedJobDefinitionCronPolicy(sla.getCronPolicy()));
    }

    public static NamedJobDefinition.CronPolicy convertToNamedJobDefinitionCronPolicy(IJobClusterDefinition.CronPolicy cPolicy) {
        if(cPolicy != null) {
            switch (cPolicy) {
                case KEEP_EXISTING:
                    return NamedJobDefinition.CronPolicy.KEEP_EXISTING;
                case KEEP_NEW:
                    return NamedJobDefinition.CronPolicy.KEEP_NEW;

                default:
                    return NamedJobDefinition.CronPolicy.KEEP_EXISTING;
            }
        }
        return NamedJobDefinition.CronPolicy.KEEP_NEW;
    }

    public static MantisWorkerMetadataWritable convertMantisWorkerMetadataToMantisWorkerMetadataWritable(IMantisWorkerMetadata workerMeta) {
        MantisWorkerMetadataWritable writable =  new MantisWorkerMetadataWritable(workerMeta.getWorkerIndex(),
                                                            workerMeta.getWorkerNumber(),
                                                            workerMeta.getJobId(),
                                                            workerMeta.getStageNum(),
                                                            workerMeta.getNumberOfPorts());

       setWorkerMetadataWritable(writable, workerMeta);

        return writable;
    }

    public static FilterableMantisWorkerMetadataWritable convertMantisWorkerMetadataToFilterableMantisWorkerMetadataWritable(IMantisWorkerMetadata workerMeta) {
        FilterableMantisWorkerMetadataWritable writable =  new FilterableMantisWorkerMetadataWritable(workerMeta.getWorkerIndex(),
                workerMeta.getWorkerNumber(),
                workerMeta.getJobId(),
                workerMeta.getStageNum(),
                workerMeta.getNumberOfPorts());
        setWorkerMetadataWritable(writable, workerMeta);
        return writable;

    }

    public static void setWorkerMetadataWritable(MantisWorkerMetadataWritable writable, IMantisWorkerMetadata workerMeta) {
        writable.setAcceptedAt(workerMeta.getAcceptedAt());
        writable.setLaunchedAt(workerMeta.getLaunchedAt());
        writable.setCompletedAt(workerMeta.getCompletedAt());
        writable.setStartingAt(workerMeta.getStartingAt());
        writable.setStartedAt(workerMeta.getStartedAt());

        writable.setCluster(workerMeta.getCluster());
        writable.setResourceCluster(workerMeta.getResourceCluster());
        writable.setSlave(workerMeta.getSlave());
        writable.setSlaveID(workerMeta.getSlaveID());

        Optional<WorkerPorts> wPorts = workerMeta.getPorts();
        if(wPorts.isPresent()) {
            WorkerPorts wP = wPorts.get();


            writable.addPorts(wP.getPorts());
        }
        writable.setConsolePort(workerMeta.getConsolePort());
        writable.setDebugPort(workerMeta.getDebugPort());
        writable.setMetricsPort(workerMeta.getMetricsPort());
        writable.setCustomPort(workerMeta.getCustomPort());

        MantisJobState state = convertWorkerStateToMantisJobState(workerMeta.getState());
        try {
            switch (state) {

            case Accepted:
                writable.setStateNoValidation(state, workerMeta.getAcceptedAt(), workerMeta.getReason());
                break;
            case Launched:
                writable.setStateNoValidation(state, workerMeta.getLaunchedAt(), workerMeta.getReason());
                break;
            case StartInitiated:
                writable.setStateNoValidation(state, workerMeta.getStartingAt(), workerMeta.getReason());
                break;
            case Started:
                writable.setStateNoValidation(state, workerMeta.getStartedAt(), workerMeta.getReason());
                break;
            case Failed:
                writable.setStateNoValidation(state, workerMeta.getCompletedAt(), workerMeta.getReason());

                break;
            case Completed:
                writable.setStateNoValidation(state, workerMeta.getCompletedAt(), workerMeta.getReason());

                break;
            default:
                assert false : "Unexpected job state to set";

            }
        } catch (Exception e) {
            throw new RuntimeException("Error converting to MantisWorkerWriteable " + e.getMessage());
        }


        writable.setResubmitInfo(workerMeta.getResubmitOf(),workerMeta.getTotalResubmitCount());

        writable.setReason(workerMeta.getReason());
    }

    /**
     * Convert/Deserialize metadata into a {@link JobWorker}.
     *
     * The converted object could have no worker ports which returns Null.
     *
     * Legit Cases:
     *
     * 1. Loaded worker was in Accepted state (hasn't been assigned ports yet).
     * 2. Loaded worker was in Archived state but previously archived from Accepted state.
     *
     * Error Cases:
     *
     * 1. Loaded worker was in Non-Accepted state (data corruption).
     * 2. Loaded worker was in Archived state but previously was running or completed (data corruption, but same
     *    semantic as Legit Case 2 above.
     *
     * @return a valid converted job worker.
     */
    public static JobWorker convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(MantisWorkerMetadata writeable, LifecycleEventPublisher eventPublisher) {
        if(logger.isDebugEnabled()) { logger.debug("DataFormatAdatper:converting worker {}", writeable); }
        String jobId = writeable.getJobId();
        List<Integer> ports = new ArrayList<>(writeable.getNumberOfPorts());
        ports.add(writeable.getMetricsPort());
        ports.add(writeable.getDebugPort());
        ports.add(writeable.getConsolePort());
        ports.add(writeable.getCustomPort());
        if(writeable.getPorts().size() > 0) {
            ports.add(writeable.getPorts().get(0));
        }

        WorkerPorts workerPorts = null;
        try {
            workerPorts = new WorkerPorts(ports);
        } catch (IllegalArgumentException | IllegalStateException e) {
            logger.warn("problem loading worker {} for Job ID {}", writeable.getWorkerId(), jobId, e);
        }

        JobWorker.Builder builder = new JobWorker.Builder()
                .withJobId(jobId)

                .withAcceptedAt(writeable.getAcceptedAt())
                .withLaunchedAt(writeable.getLaunchedAt())
                .withStartingAt(writeable.getStartingAt())
                .withStartedAt(writeable.getStartedAt())
                .withCompletedAt(writeable.getCompletedAt())

                .withNumberOfPorts(ports.size())
                .withWorkerPorts(workerPorts)

                .withResubmitCount(writeable.getTotalResubmitCount())
                .withResubmitOf(writeable.getResubmitOf())

                .withSlave(writeable.getSlave())
                .withSlaveID(writeable.getSlaveID())
                .withStageNum(writeable.getStageNum())

                .withState(convertMantisJobStateToWorkerState(writeable.getState()))
                .withWorkerIndex(writeable.getWorkerIndex())
                .withWorkerNumber(writeable.getWorkerNumber())

                .withJobCompletedReason(writeable.getReason())
                .withPreferredCluster(writeable.getCluster())
                .withLifecycleEventsPublisher(eventPublisher);

        writeable.getResourceCluster().ifPresent(builder::withResourceCluster);
        JobWorker converted = builder.build();

        if( logger.isDebugEnabled()) { logger.debug("DataFormatAdatper:converted worker {}", converted); }
        return converted;
    }

    public static MantisStageMetadataWritable convertMantisStageMetadataToMantisStageMetadataWriteable(IMantisStageMetadata stageMeta) {
        return new MantisStageMetadataWritable(stageMeta.getJobId().getId(),
                stageMeta.getStageNum(),
                stageMeta.getNumStages(),
                stageMeta.getMachineDefinition(),
                stageMeta.getNumWorkers(),
                stageMeta.getHardConstraints(),
                stageMeta.getSoftConstraints(),
                stageMeta.getScalingPolicy(),
                stageMeta.getScalable()
        );
    }

    public static FilterableMantisStageMetadataWritable convertFilterableMantisStageMetadataToMantisStageMetadataWriteable(IMantisStageMetadata stageMeta) {
        return new FilterableMantisStageMetadataWritable(stageMeta.getJobId().getId(),
                stageMeta.getStageNum(),
                stageMeta.getNumStages(),
                stageMeta.getMachineDefinition(),
                stageMeta.getNumWorkers(),
                stageMeta.getHardConstraints(),
                stageMeta.getSoftConstraints(),
                stageMeta.getScalingPolicy(),
                stageMeta.getScalable()
        );
    }


    public static io.mantisrx.server.master.domain.SLA convertToSLA(NamedJob.SLA sla) {
        return new io.mantisrx.server.master.domain.SLA(sla.getMin(),sla.getMax(),sla.getCronSpec(),convertToCronPolicy(sla.getCronPolicy()));
    }

    public static IJobClusterDefinition.CronPolicy convertToCronPolicy(NamedJobDefinition.CronPolicy cronPolicy) {
        if(cronPolicy != null) {
            switch (cronPolicy) {
                case KEEP_EXISTING:
                    return IJobClusterDefinition.CronPolicy.KEEP_EXISTING;
                case KEEP_NEW:
                    return IJobClusterDefinition.CronPolicy.KEEP_NEW;
                default:
                    return IJobClusterDefinition.CronPolicy.KEEP_NEW;
            }
        }
        return null;
    }

    public static IMantisJobMetadata convertMantisJobWriteableToMantisJobMetadata(MantisJobMetadata archJob, LifecycleEventPublisher eventPublisher) throws Exception {
        return convertMantisJobWriteableToMantisJobMetadata(archJob, eventPublisher, false);
    }

    // TODO job specific migration config is not supported, migration config will be at cluster level
    public static IMantisJobMetadata convertMantisJobWriteableToMantisJobMetadata(MantisJobMetadata archJob, LifecycleEventPublisher eventPublisher, boolean isArchived) throws Exception {
        if(logger.isTraceEnabled()) { logger.trace("DataFormatAdapter:Converting {}", archJob); }


        // convert stages to new format
        List<IMantisStageMetadata> convertedStageList = new ArrayList<>();
        for (MantisStageMetadata stageMeta : archJob.getStageMetadata()) {
            // if this is an archived job then add workerIndex may fail as there maybe
            // multiple workers related to a given index so skip adding workers to stage
            boolean skipAddingWorkers = isArchived;

            convertedStageList.add(convertMantisStageMetadataWriteableToMantisStageMetadata(stageMeta, eventPublisher, skipAddingWorkers));
        }

        // generate SchedulingInfo
        SchedulingInfo schedulingInfo = generateSchedulingInfo(convertedStageList);

        URL jarUrl = archJob.getJarUrl();
        Optional<String> artifactName = extractArtifactName(jarUrl);

        // generate job defn
        JobDefinition jobDefn = new JobDefinition(archJob.getName(), archJob.getUser(),
                jarUrl == null ? "" : jarUrl.toString(), artifactName.orElse(""), null, archJob.getParameters(), archJob.getSla(),
                archJob.getSubscriptionTimeoutSecs(),schedulingInfo, archJob.getNumStages(),archJob.getLabels(), null);
        Optional<JobId> jIdOp = JobId.fromId(archJob.getJobId());
        if(!jIdOp.isPresent()) {
            throw new IllegalArgumentException("Invalid JobId " + archJob.getJobId());
        }

        // generate job meta
        MantisJobMetadataImpl mantisJobMetadata = new MantisJobMetadataImpl(jIdOp.get(), archJob.getSubmittedAt(),
                archJob.getStartedAt(), jobDefn, convertMantisJobStateToJobState(archJob.getState()),
                archJob.getNextWorkerNumberToUse(), archJob.getHeartbeatIntervalSecs(), archJob.getWorkerTimeoutSecs());


        // add the stages
        for(IMantisStageMetadata stageMetadata : convertedStageList) {
            mantisJobMetadata.addJobStageIfAbsent(stageMetadata);
        }


        if(logger.isTraceEnabled()) { logger.trace("DataFormatAdapter:Completed conversion to IMantisJobMetadata {}",
                mantisJobMetadata); }
        return mantisJobMetadata;
    }

    private static StageSchedulingInfo generateStageSchedulingInfo(IMantisStageMetadata mantisStageMetadata) {
        return StageSchedulingInfo.builder()
                .numberOfInstances(mantisStageMetadata.getNumWorkers())
                .machineDefinition(mantisStageMetadata.getMachineDefinition())
                .hardConstraints(mantisStageMetadata.getHardConstraints())
                .softConstraints(mantisStageMetadata.getSoftConstraints())
                .scalingPolicy(mantisStageMetadata.getScalingPolicy())
                .scalable(mantisStageMetadata.getScalable())
                .build();
    }

    private static SchedulingInfo generateSchedulingInfo(List<IMantisStageMetadata> convertedStageList) {
        Map<Integer, StageSchedulingInfo> stageSchedulingInfoMap = new HashMap<>();

        Iterator<IMantisStageMetadata> it = convertedStageList.iterator();
        while(it.hasNext()) {
            IMantisStageMetadata stageMeta = it.next();

            StageSchedulingInfo stageSchedulingInfo = generateStageSchedulingInfo(stageMeta);
            stageSchedulingInfoMap.put(stageMeta.getStageNum(), stageSchedulingInfo);

        }

        SchedulingInfo schedulingInfo = new SchedulingInfo(stageSchedulingInfoMap);
        return schedulingInfo;
    }

    public static IMantisStageMetadata convertMantisStageMetadataWriteableToMantisStageMetadata(
            MantisStageMetadata stageMeta,
            LifecycleEventPublisher eventPublisher) {
        return convertMantisStageMetadataWriteableToMantisStageMetadata(stageMeta,eventPublisher,
                false);
    }

    public static IMantisStageMetadata convertMantisStageMetadataWriteableToMantisStageMetadata(MantisStageMetadata stageMeta,
                                                                                                LifecycleEventPublisher eventPublisher,
                                                                                                boolean skipAddingWorkerMetaData) {
        if(logger.isTraceEnabled()) { logger.trace("DataFormatAdapter:converting stage {}, skipadding workers {}", stageMeta, skipAddingWorkerMetaData); }
        Optional<JobId> jIdOp = JobId.fromId(stageMeta.getJobId());
        if(!jIdOp.isPresent()) {
            throw new IllegalArgumentException("Invalid jobid " + stageMeta.getJobId());
        }

        IMantisStageMetadata newStageMeta = new MantisStageMetadataImpl.Builder()
                .withHardConstraints(stageMeta.getHardConstraints())
                .withSoftConstraints(stageMeta.getSoftConstraints())
                .withJobId(jIdOp.get())
                .withMachineDefinition(stageMeta.getMachineDefinition())
                .withNumStages(stageMeta.getNumStages())
                .withNumWorkers(stageMeta.getNumWorkers())
                .withScalingPolicy(stageMeta.getScalingPolicy())
                .withStageNum(stageMeta.getStageNum())
                .isScalable(stageMeta.getScalable())
                .build();

        if(!skipAddingWorkerMetaData) {
            if(logger.isDebugEnabled()) {logger.debug("Skip adding workers to stage meta");}
            stageMeta.getAllWorkers()
                    .forEach((mantisWorkerMetadata) -> {
                        ((MantisStageMetadataImpl) newStageMeta).addWorkerIndex(convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(mantisWorkerMetadata, eventPublisher));
                    });
        }
        if(logger.isDebugEnabled()) { logger.debug("DataFormatAdapter:converted stage {}", newStageMeta); }
        return newStageMeta;
    }


    public static MantisJobMetadataWritable convertMantisJobMetadataToMantisJobMetadataWriteable(IMantisJobMetadata jobMetadata) {

        Instant startedAtInstant = jobMetadata.getStartedAtInstant().orElse(Instant.ofEpochMilli(0));
        return new MantisJobMetadataWritable(jobMetadata.getJobId().getId(),
                jobMetadata.getJobId().getCluster(),
                jobMetadata.getUser(),
                jobMetadata.getSubmittedAtInstant().toEpochMilli(),
                startedAtInstant.toEpochMilli(),
                jobMetadata.getJobJarUrl(),
                jobMetadata.getTotalStages(),
                jobMetadata.getSla().orElse(null),
                convertToMantisJobState(jobMetadata.getState()),
                jobMetadata.getWorkerTimeoutSecs(),
                jobMetadata.getHeartbeatIntervalSecs(),
                jobMetadata.getSubscriptionTimeoutSecs(),
                jobMetadata.getParameters(),
                jobMetadata.getNextWorkerNumberToUse(),
                // TODO need to wire migration config here so it can get persisted
                null,
                jobMetadata.getLabels());
    }

    public static FilterableMantisJobMetadataWritable convertMantisJobMetadataToFilterableMantisJobMetadataWriteable(IMantisJobMetadata jobMetadata) {
        Instant startedAtInstant = jobMetadata.getStartedAtInstant().orElse(Instant.ofEpochMilli(0));
        return new FilterableMantisJobMetadataWritable(jobMetadata.getJobId().getId(),
                jobMetadata.getJobId().getCluster(),
                jobMetadata.getUser(),
                jobMetadata.getSubmittedAtInstant().toEpochMilli(),
                startedAtInstant.toEpochMilli(),
                jobMetadata.getJobJarUrl(),
                jobMetadata.getTotalStages(),
                jobMetadata.getSla().orElse(null),
                convertToMantisJobState(jobMetadata.getState()),
                jobMetadata.getWorkerTimeoutSecs(),
                jobMetadata.getHeartbeatIntervalSecs(),
                jobMetadata.getSubscriptionTimeoutSecs(),
                jobMetadata.getParameters(),
                jobMetadata.getNextWorkerNumberToUse(),
                // TODO need to wire migration config here so it can get persisted
                null,
                jobMetadata.getLabels(),
                jobMetadata.getJobCosts());
    }



    public static JobState convertMantisJobStateToJobState(MantisJobState state) {
        JobState oState;
        switch(state) {
            case Accepted:
                oState = JobState.Accepted;
                break;
            case Launched:
                oState = JobState.Launched;
                break;
            case Started:
                oState = JobState.Launched;
                break;
            case StartInitiated:
                oState = JobState.Launched;
                break;
            case Completed:
                oState = JobState.Completed;
                break;
            case Failed:
                oState = JobState.Failed;
                break;
            default:
                oState = JobState.Noop;
                break;
        }
        return oState;
    }

    public static MantisJobState convertToMantisJobState(JobState state) {
        MantisJobState oldState;
        switch(state) {
            case Accepted:
                oldState = MantisJobState.Accepted;
                break;
            case Launched:
                oldState = MantisJobState.Launched;
                break;
            case Terminating_abnormal:
                oldState = MantisJobState.Failed;
                break;
            case Terminating_normal:
                oldState = MantisJobState.Completed;
                break;
            case Failed:
                oldState = MantisJobState.Failed;
                break;
            case Completed:
                oldState = MantisJobState.Completed;
                break;
            case Noop:
                oldState = MantisJobState.Noop;
                break;
            default:
                oldState = MantisJobState.Noop;
        }

        return oldState;

    }

    public static MantisJobState convertWorkerStateToMantisJobState(WorkerState state) {
        MantisJobState wState;
        switch(state) {
            case Accepted:
                wState = MantisJobState.Accepted;
                break;
            case Failed:
                wState = MantisJobState.Failed;
                break;
            case Completed:
                wState = MantisJobState.Completed;
                break;
            case Noop:
                wState = MantisJobState.Noop;
                break;
            case StartInitiated:
                wState = MantisJobState.StartInitiated;
                break;
            case Started:
                wState = MantisJobState.Started;
                break;
            case Launched:
                wState = MantisJobState.Launched;
                break;
            default:
                wState = MantisJobState.Noop;
                break;

        }
        return wState;
    }


    public static WorkerState convertMantisJobStateToWorkerState(MantisJobState state) {
        WorkerState wState;
        switch(state) {
            case Accepted:
                wState = WorkerState.Accepted;
                break;
            case Failed:
                wState = WorkerState.Failed;
                break;
            case Completed:
                wState = WorkerState.Completed;
                break;
            case Noop:
                wState = WorkerState.Noop;
                break;
            case StartInitiated:
                wState = WorkerState.StartInitiated;
                break;
            case Started:
                wState = WorkerState.Started;
                break;
            case Launched:
                wState = WorkerState.Launched;
                break;
            default:
                wState = WorkerState.Unknown;
                break;
        }
        return wState;
    }

    public static List<JobClusterInfo.JarInfo> convertNamedJobJarListToJarInfoList(List<NamedJob.Jar> jars) {

        return jars.stream().map((jar) -> new JobClusterInfo.JarInfo(jar.getVersion(),jar.getUploadedAt(),jar.getUrl().toString())).collect(Collectors.toList());

    }
}



class NoOpMantisJobOperations implements MantisJobOperations {

    @Override
    public NamedJob createNamedJob(NamedJobDefinition namedJobDefinition) throws InvalidNamedJobException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NamedJob updateNamedJar(NamedJobDefinition namedJobDefinition, boolean createIfNeeded)
            throws InvalidNamedJobException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NamedJob quickUpdateNamedJob(String user, String name, URL jobJar, String version)
            throws InvalidNamedJobException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void updateSla(String user, String name, NamedJob.SLA sla, boolean forceEnable) throws InvalidNamedJobException {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateLabels(String user, String name, List<Label> labels) throws InvalidNamedJobException {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateMigrateStrategy(String user, String name, WorkerMigrationConfig migrationConfig)
            throws InvalidNamedJobException {
        // TODO Auto-generated method stub

    }

    @Override
    public String quickSubmit(String jobName, String user)
            throws InvalidNamedJobException, io.mantisrx.server.master.store.InvalidJobException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<NamedJob> getNamedJob(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteNamedJob(String name, String user) throws NamedJobDeleteException {
        // TODO Auto-generated method stub

    }

    @Override
    public void disableNamedJob(String name, String user) throws InvalidNamedJobException {
        // TODO Auto-generated method stub

    }

    @Override
    public void enableNamedJob(String name, String user) throws InvalidNamedJobException {
        // TODO Auto-generated method stub

    }

    @Override
    public MantisJobStatus submit(MantisJobDefinition jobDefinition) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean deleteJob(String jobId) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void killJob(String user, String jobId, String reason) {
        // TODO Auto-generated method stub

    }

    @Override
    public void terminateJob(String jobId) {
        // TODO Auto-generated method stub

    }

    @Override
    public Observable<MantisJobStatus> jobs() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MantisJobStatus status(String jobId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Action1<String> getSlaveDisabler() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Action1<String> getSlaveEnabler() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReady() {
        // TODO Auto-generated method stub

    }

}
