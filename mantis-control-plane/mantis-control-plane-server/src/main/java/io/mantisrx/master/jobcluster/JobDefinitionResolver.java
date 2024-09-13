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

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for 'filling the blanks' in the provided JobDefinition during a Job Submit.
 */
public class JobDefinitionResolver {

    private final Logger logger = LoggerFactory.getLogger(JobDefinitionResolver.class);

    /**
     *
     * Encodes the logic of how to resolve the relevant fields of the submitted JobDefinition.
     * Artifact |  Version | SchedulingInfo | Resolution
     * -------------------------------------------------
     *    Y     |      Y   |   Y            |  Use given scheduling info
     * ------------------------------------------------------------
     *    Y     |     Y    |   N            |  INVALID (new artifact with no sched info)
     * -------------------------------------------------------------
     *    Y     |    N     |   Y            |  Generate version and use given sched info
     * --------------------------------------------------------------
     *    Y     |    N     |   N            |  INVALID (new artifact with no sched info)
     * ---------------------------------------------------------------
     *    N     |    Y     |   Y            |  Lookup Cluster Config for given Version, get the SchedInfo from it and ensure given SchedInfo is compatible
     * ----------------------------------------------------------------
     *    N     |    Y     \   N            |  Lookup Cluster config for given version and use it
     * -----------------------------------------------------------------
     *    N     |    N     |   Y            |  Get latest cluster config, get the SchedInfo from it and ensure given SchedInfo is compatible
     * -----------------------------------------------------------------
     *    N     |   N      |   N            | Get latest cluster config, get the SchedInfo from it
     * -------------------------------------------------------------------
     * @param user
     * @param givenJobDefnOp
     * @param jobClusterMetadata
     * @return
     * @throws Exception
     */
    JobDefinition getResolvedJobDefinition(final String user, final JobDefinition givenJobDefnOp, final IJobClusterMetadata jobClusterMetadata) throws Exception {
        Preconditions.checkNotNull(givenJobDefnOp, "JobDefinition cannot be null");
        Preconditions.checkNotNull(jobClusterMetadata, "JobClusterMetadata cannot be null");

        JobDefinition resolvedJobDefn = givenJobDefnOp;

        logger.info("Given JobDefn {}", resolvedJobDefn);

        // inherit params from cluster if not specified
        List<Parameter> parameters = (resolvedJobDefn.getParameters() != null && !resolvedJobDefn.getParameters().isEmpty()) ? resolvedJobDefn.getParameters() : jobClusterMetadata.getJobClusterDefinition().getParameters();

        // Inherit labels from cluster, if resolvedJobDefn has labels, override the existing ones
        Map<String, Label> labelMap = jobClusterMetadata.getJobClusterDefinition().getLabels().stream()
            .collect(Collectors.toMap(Label::getName, label -> label));
        if (resolvedJobDefn.getLabels() != null && !resolvedJobDefn.getLabels().isEmpty()) {
            resolvedJobDefn.getLabels()
                .forEach(label -> labelMap.put(label.getName(), label));
        }
        List<Label> labels = Collections.unmodifiableList(new ArrayList<>(labelMap.values()));

        String artifactName = resolvedJobDefn.getArtifactName();
        String jobJarUrl = resolvedJobDefn.getJobJarUrl();
        SchedulingInfo schedulingInfo = resolvedJobDefn.getSchedulingInfo();
        String version = resolvedJobDefn.getVersion();
        JobClusterConfig jobClusterConfig = null;

        if(!isNull(artifactName) && !isNull(jobJarUrl) && !isNull(version) && !schedulingInfoNotValid(schedulingInfo)) {
            // update cluster ?

        } else if(!isNull(artifactName) && !isNull(jobJarUrl) && !isNull(version) && schedulingInfoNotValid(schedulingInfo)) { // scheduling Info is not given while new artifact is specified

            // exception
            String msg = String.format("Scheduling info is not specified during Job Submit for cluster %s while new artifact is specified %s. Job Submit fails", jobClusterMetadata.getJobClusterDefinition().getName(), artifactName);
            logger.warn(msg);
            throw new Exception(msg);

        } else if(!isNull(artifactName) && !isNull(jobJarUrl)&& isNull(version) && !schedulingInfoNotValid(schedulingInfo)) { // artifact & schedulingInfo are given

            // generate new version and update cluster
            version = String.valueOf(System.currentTimeMillis());
            // update cluster ?

        } else if(!isNull(artifactName) && !isNull(jobJarUrl) && isNull(version) && schedulingInfoNotValid(schedulingInfo)) { // scheduling info not given while new artifact is specified

            // exception
            String msg = String.format("Scheduling info is not specified during Job Submit for cluster %s while new artifact %s is specified. Job Submit fails", jobClusterMetadata.getJobClusterDefinition().getName(), artifactName);
            logger.warn(msg);
            throw new Exception(msg);

        } else if(isNull(artifactName) && isNull(jobJarUrl) && !isNull(version) && !schedulingInfoNotValid(schedulingInfo)) { // version is given & scheduling info is given

            // fetch JobCluster config for version and validate the given schedulingInfo is compatible
            Optional<JobClusterConfig> clusterConfigForVersion = getJobClusterConfigForVersion(jobClusterMetadata, version);
            if(!clusterConfigForVersion.isPresent()) {
                String msg = String.format("No Job Cluster config could be found for version %s in JobCluster %s. Job Submit fails", version, jobClusterMetadata.getJobClusterDefinition().getName());
                logger.warn(msg);
                throw new Exception(msg);
            }

            jobClusterConfig = clusterConfigForVersion.get();
            if(!validateSchedulingInfo(schedulingInfo, jobClusterConfig.getSchedulingInfo(), jobClusterMetadata)) {

                String msg = String.format("Given SchedulingInfo %s is incompatible with that associated with the given version %s in JobCluster %s. Job Submit fails", schedulingInfo, version, jobClusterMetadata.getJobClusterDefinition().getName());
                logger.warn(msg);
                throw new Exception(msg);
            }

            artifactName = jobClusterConfig.getArtifactName();
            jobJarUrl = jobClusterConfig.getJobJarUrl();

        } else if(isNull(artifactName) && isNull(jobJarUrl) && !isNull(version) && schedulingInfoNotValid(schedulingInfo)) { // Only version is given

            // fetch JobCluster config for version
            Optional<JobClusterConfig> clusterConfigForVersion = getJobClusterConfigForVersion(jobClusterMetadata, version);
            if(!clusterConfigForVersion.isPresent()) {
                String msg = String.format("No Job Cluster config could be found for version %s in JobCluster %s. Job Submit fails", version, jobClusterMetadata.getJobClusterDefinition().getName());
                logger.warn(msg);
                throw new Exception(msg);
            }

            jobClusterConfig = clusterConfigForVersion.get();
            schedulingInfo = jobClusterConfig.getSchedulingInfo();
            artifactName = jobClusterConfig.getArtifactName();
            jobJarUrl = jobClusterConfig.getJobJarUrl();


        } else if(isNull(artifactName) && isNull(jobJarUrl) && isNull(version) && !schedulingInfoNotValid(schedulingInfo)) { // only scheduling info is given

            // fetch latest Job Cluster config
            jobClusterConfig = jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig();
            version = jobClusterConfig.getVersion();
            artifactName = jobClusterConfig.getArtifactName();
            jobJarUrl = jobClusterConfig.getJobJarUrl();
            // set version to it
            // validate given scheduling info is compatible
            if(!validateSchedulingInfo(schedulingInfo, jobClusterConfig.getSchedulingInfo(), jobClusterMetadata)) {
                String msg = String.format("Given SchedulingInfo %s is incompatible with that associated with the given version %s in JobCluster %s which is %s. Job Submit fails", schedulingInfo, version, jobClusterMetadata.getJobClusterDefinition().getName(), jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig().getSchedulingInfo());
                logger.warn(msg);
                throw new Exception(msg);
            }


        } else if(isNull(artifactName) && isNull(jobJarUrl) && isNull(version) && schedulingInfoNotValid(schedulingInfo)){ // Nothing is given. Use the latest on the cluster

            // fetch latest job cluster config
            jobClusterConfig = jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig();
            // set version to it
            version = jobClusterConfig.getVersion();
            // use scheduling info from that.
            schedulingInfo = jobClusterConfig.getSchedulingInfo();
            artifactName = jobClusterConfig.getArtifactName();
            jobJarUrl = jobClusterConfig.getJobJarUrl();

        } else {
            // exception should never get here.
            throw new Exception(String.format("Invalid case for resolveJobDefinition artifactName %s jobJarUrl %s version %s schedulingInfo %s", jobJarUrl, artifactName, version, schedulingInfo));
        }

        logger.info("Resolved version {}, schedulingInfo {}, artifactName {}, jobJarUrl {}", version, schedulingInfo, artifactName, jobJarUrl);

        if(isNull(artifactName)  || isNull(jobJarUrl)  || isNull(version) || schedulingInfoNotValid(schedulingInfo)) {
            String msg = String.format(" SchedulingInfo %s or artifact %s or jobJarUrl %s or version %s could not be resolved in JobCluster %s. Job Submit fails", schedulingInfo, artifactName, jobJarUrl, version, jobClusterMetadata.getJobClusterDefinition().getName());
            logger.warn(msg);
            throw new Exception(msg);
        }
        return new JobDefinition.Builder()
                .from(resolvedJobDefn)
                .withParameters(parameters)
                .withLabels(labels)
                .withSchedulingInfo(schedulingInfo)
                .withUser(user)
                .withVersion(version)
                .withArtifactName(artifactName)
                .withJobJarUrl(jobJarUrl)
                .build();

    }

    private static boolean schedulingInfoNotValid(SchedulingInfo schedulingInfo) {
        if(schedulingInfo == null || schedulingInfo.getStages().isEmpty()) {
            return true;
        }
        return false;
    }

    private static boolean isNull(String key) {

        return (key == null || key.equals("null") || key.isEmpty()) ? true : false;

    }

    /**
     * Lookup the job cluster config for the given version in the list of job cluster configs
     * @param jobClusterMetadata
     * @param version
     * @return
     */
     Optional<JobClusterConfig> getJobClusterConfigForVersion(final IJobClusterMetadata jobClusterMetadata, final String version) {
        Preconditions.checkNotNull(jobClusterMetadata, "JobClusterMetadata cannot be null");
        Preconditions.checkNotNull(version, "Version cannot be null");
        final String versionToFind = version;
        List<JobClusterConfig> configList = jobClusterMetadata.getJobClusterDefinition()
                .getJobClusterConfigs()
                .stream()
                .filter((cfg) -> cfg.getVersion().equals(versionToFind))
                .collect(Collectors.toList());
        if(!configList.isEmpty()) {
            return of(configList.get(0));

        } else {
            // unknown version
            String msg = String.format("No config with version %s found for Job Cluster %s. Job Submit fails", versionToFind, jobClusterMetadata.getJobClusterDefinition().getName());
            logger.warn(msg);
            return empty();
        }
    }

    /**
     * Compare given scheduling info with that configured for this artifact to make sure it is compatible
     * - Ensure number of stages match
     * @param givenSchedulingInfo
     * @param configuredSchedulingInfo
     * @param jobClusterMetadata
     * @return
     * @throws Exception
     */

    private boolean validateSchedulingInfo(final SchedulingInfo givenSchedulingInfo, final SchedulingInfo configuredSchedulingInfo, final IJobClusterMetadata jobClusterMetadata) throws Exception {
        int givenNumStages = givenSchedulingInfo.getStages().size();
        int existingNumStages = configuredSchedulingInfo.getStages().size();
        // isReadyForJobMaster is not reliable, just check if stage 0 is defined and decrement overall count
        //if (jobClusterMetadata.getJobClusterDefinition().getIsReadyForJobMaster()) {
            if (givenSchedulingInfo.forStage(0) != null)
                givenNumStages--; // decrement to get net numStages without job master
            if (configuredSchedulingInfo.forStage(0) != null)
                existingNumStages--;
        //}
        if(givenNumStages != existingNumStages) {
            logger.warn("Mismatched scheduling info: expecting #stages=" +
                    existingNumStages + " for given jar version [" + " " +
                    "], where as, given scheduling info has #stages=" + givenNumStages);
            return false;
        }
        return true;
    }

//    private SchedulingInfo getSchedulingInfoForArtifact(String artifactName, final IJobClusterMetadata jobClusterMetadata) throws Exception{
//        logger.info("Entering getSchedulingInfoForArtifact {}", artifactName);
//        SchedulingInfo resolvedSchedulingInfo = null;
//        List<JobClusterConfig> configList = jobClusterMetadata.getJobClusterDefinition().getJobClusterConfigs().stream().filter((cfg) -> cfg.getArtifactName().equals(artifactName)).collect(Collectors.toList());
//        if (configList.isEmpty()) { // new artifact
//            throw new Exception("Scheduling info must be provided along with new artifact");
//        } else {
//
//            JobClusterConfig preconfiguredConfigForArtifact = null;
//            for(JobClusterConfig config : configList) {
//                if(artifactName.equals(config.getArtifactName())) {
//                    preconfiguredConfigForArtifact = config;
//                    break;
//                }
//            }
//            if(preconfiguredConfigForArtifact != null) {
//
//                logger.info("Found schedulingInfo {} for artifact {}", preconfiguredConfigForArtifact.getSchedulingInfo(), artifactName);
//                resolvedSchedulingInfo = preconfiguredConfigForArtifact.getSchedulingInfo();
//            } else {
//
//                logger.warn("No Config found for artifact {} using default", artifactName);
//                JobClusterConfig config = configList.get(0);
//                resolvedSchedulingInfo = config.getSchedulingInfo();
//            }
//        }
//        logger.info("Exiting getSchedulingInfoForArtifact {} -> with resolved config {}", artifactName, resolvedSchedulingInfo);
//        return resolvedSchedulingInfo;
//
//    }



}
