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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.LabelManager.SystemLabels;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class JobDefinitionResolverTest {


    public static final SLA NO_OP_SLA = new SLA(0, 0, null, null);

    public static final MachineDefinition DEFAULT_MACHINE_DEFINITION = new MachineDefinition(1, 10, 10, 10, 2);
    public static final SchedulingInfo SINGLE_WORKER_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(DEFAULT_MACHINE_DEFINITION, Lists.newArrayList(), Lists.newArrayList()).build();
    public static final SchedulingInfo TWO_WORKER_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1).multiWorkerStage(2, DEFAULT_MACHINE_DEFINITION).build();
    public static final JobOwner DEFAULT_JOB_OWNER = new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo");
    public static final String DEFAULT_ARTIFACT_NAME = "myart";
    public static final String DEFAULT_VERSION = "0.0.1";

    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName, List<Label> labels, List<Parameter> parameters) {
        return createFakeJobClusterDefn(clusterName, labels, parameters, NO_OP_SLA, SINGLE_WORKER_SCHED_INFO);
    }

    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName, List<Label> labels, List<Parameter> parameters, SLA sla, SchedulingInfo schedulingInfo)  {
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + DEFAULT_ARTIFACT_NAME)
                .withArtifactName(DEFAULT_ARTIFACT_NAME)
                .withSchedulingInfo(schedulingInfo)
                .withVersion(DEFAULT_VERSION)
                .build();

        return new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(clusterName)
                .withParameters(parameters)
                .withLabels(labels)
                .withUser("user")
                .withIsReadyForJobMaster(true)
                .withOwner(DEFAULT_JOB_OWNER)
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .withSla(sla)
                .build();
    }
    @Test
    public void artifactSchedPresentTest() {
        String clusterName = "artifactVersionSchedPresentTest";
        List<Label> labels = new ArrayList<>();
        Label label = new Label("l1", "lv1");
        labels.add(label);
        labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));

        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter("paramName", "paramValue");
        parameters.add(parameter);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels, parameters);
        IJobClusterMetadata jobClusterMetadata = new JobClusterMetadataImpl(fakeJobCluster,1,false);


        String version = "0.0.2";
        String artifactName = "myArt2";
        String artifactUrl = "http://foo/bar/" + artifactName;
        SchedulingInfo schedulingInfo = TWO_WORKER_SCHED_INFO;

        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withJobJarUrl(artifactUrl).withArtifactName(artifactName).withName(clusterName).withSchedulingInfo(schedulingInfo).withVersion(version).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // assert the specified values are being used
            assertEquals(artifactName, resolvedJobDefinition.getArtifactName());
            assertEquals(artifactUrl, resolvedJobDefinition.getJobJarUrl().toString());
            assertEquals(schedulingInfo, resolvedJobDefinition.getSchedulingInfo());
            assertEquals(version, resolvedJobDefinition.getVersion());

            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // Only ArtifactName and schedInfo is specified
        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withJobJarUrl(artifactUrl).withArtifactName(artifactName).withName(clusterName).withSchedulingInfo(schedulingInfo).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // assert the specified values are being used
            assertEquals(artifactName, resolvedJobDefinition.getArtifactName());
            assertEquals(artifactUrl, resolvedJobDefinition.getJobJarUrl().toString());
            assertEquals(schedulingInfo, resolvedJobDefinition.getSchedulingInfo());
            // assert a version no was generated
            assertTrue(resolvedJobDefinition.getVersion()!= null && !resolvedJobDefinition.getVersion().isEmpty());


            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }


    }

    @Test
    public void artifactPresentButSchedAbsentFailsTest() {
        String clusterName = "artifactPresentButSchedAbsentFailsTest";
        List<Label> labels = new ArrayList<>();
        Label label = new Label("l1", "lv1");
        labels.add(label);
        labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));

        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter("paramName", "paramValue");
        parameters.add(parameter);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels, parameters);
        IJobClusterMetadata jobClusterMetadata = new JobClusterMetadataImpl(fakeJobCluster,1,false);


        String version = "0.0.2";
        String artifactName = "myArt2";
        String artifactUrl = "http://foo/bar/" + artifactName;

        // Only new artifact and version is specified
        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withJobJarUrl(artifactUrl).withArtifactName(artifactName).withName(clusterName).withVersion(version).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);
            fail();

        }  catch (Exception e) {
            e.printStackTrace();

        }

        // Only new artifact is specified
        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withJobJarUrl("http://" + artifactName).withArtifactName(artifactName).withName(clusterName).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);
            fail();

        }  catch (Exception e) {
            e.printStackTrace();

        }

    }

    @Test
    public void versionSchedPresentTest() {
        String clusterName = "versionSchedPresentTest";
        List<Label> labels = new ArrayList<>();
        Label label = new Label("l1", "lv1");
        labels.add(label);
        labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));

        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter("paramName", "paramValue");
        parameters.add(parameter);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels, parameters);
        IJobClusterMetadata jobClusterMetadata = new JobClusterMetadataImpl(fakeJobCluster,1,false);


        String version = "0.0.1";

        JobConstraints softConstraints = JobConstraints.ExclusiveHost;
        List<JobConstraints> constraintsList = new ArrayList<>();
        constraintsList.add(softConstraints);
        SchedulingInfo schedulingInfo = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(DEFAULT_MACHINE_DEFINITION, Lists.newArrayList(), constraintsList).build();

        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withName(clusterName).withSchedulingInfo(schedulingInfo).withVersion(version).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // artifact will get populated using the given version.
            assertEquals(DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getArtifactName());
            assertEquals("http://" + DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getJobJarUrl().toString());
            // scheduling info will be the one specified by us
            assertEquals(schedulingInfo, resolvedJobDefinition.getSchedulingInfo());
            // version should match what we set.
            assertEquals(version, resolvedJobDefinition.getVersion());

            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // Only version is specified
        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withName(clusterName).withVersion(version).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // assert the artifact is inherited
            assertEquals(DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getArtifactName());
            assertEquals("http://" + DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getJobJarUrl().toString());
            // assert the scheduling info is inherited
            assertEquals(SINGLE_WORKER_SCHED_INFO, resolvedJobDefinition.getSchedulingInfo());
            // assert a version is the one we gave
            assertEquals(version, resolvedJobDefinition.getVersion());


            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }


    }


    @Test
    public void SchedPresentTest() {
        String clusterName = "SchedPresentTest";
        List<Label> labels = new ArrayList<>();
        Label label = new Label("l1", "lv1");
        labels.add(label);
        labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));

        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter("paramName", "paramValue");
        parameters.add(parameter);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels, parameters);
        IJobClusterMetadata jobClusterMetadata = new JobClusterMetadataImpl(fakeJobCluster,1,false);


        JobConstraints softConstraints = JobConstraints.ExclusiveHost;
        List<JobConstraints> constraintsList = new ArrayList<>();
        constraintsList.add(softConstraints);
        SchedulingInfo schedulingInfo = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(DEFAULT_MACHINE_DEFINITION, Lists.newArrayList(), constraintsList).build();

        try {
            // only sched info set.
            JobDefinition givenJobDefn = new JobDefinition.Builder().withName(clusterName).withSchedulingInfo(schedulingInfo).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // artifact will get populated using the given version.
            assertEquals(DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getArtifactName());
            assertEquals("http://" + DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getJobJarUrl().toString());
            // scheduling info will be the one specified by us
            assertEquals(schedulingInfo, resolvedJobDefinition.getSchedulingInfo());
            // version should match the latest on the cluster
            assertEquals(DEFAULT_VERSION, resolvedJobDefinition.getVersion());

            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // NOTHING is specified
        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withName(clusterName).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // assert the artifact is inherited
            assertEquals(DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getArtifactName());
            assertEquals("http://" + DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getJobJarUrl().toString());
            // assert the scheduling info is inherited
            assertEquals(SINGLE_WORKER_SCHED_INFO, resolvedJobDefinition.getSchedulingInfo());
            // assert a version is the dfeault one.
            assertEquals(DEFAULT_VERSION, resolvedJobDefinition.getVersion());


            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // NOTHING is specified2
        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withName(clusterName).withVersion("null").build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);

            // assert the artifact is inherited
            assertEquals(DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getArtifactName());
            assertEquals("http://" + DEFAULT_ARTIFACT_NAME, resolvedJobDefinition.getJobJarUrl().toString());
            // assert the scheduling info is inherited
            assertEquals(SINGLE_WORKER_SCHED_INFO, resolvedJobDefinition.getSchedulingInfo());
            // assert a version is the dfeault one.
            assertEquals(DEFAULT_VERSION, resolvedJobDefinition.getVersion());


            // assert the parameters and labels are inherited since they were not specified

            assertEquals(2, resolvedJobDefinition.getLabels().size());
            assertEquals(label, resolvedJobDefinition.getLabels().get(0));

            assertEquals(1, resolvedJobDefinition.getParameters().size());
            assertEquals(parameter, resolvedJobDefinition.getParameters().get(0));

        } catch (InvalidJobException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }



    }

    @Test
    public void versionNotFoundTest() {
        String clusterName = "versionNotFoundTest";
        List<Label> labels = new ArrayList<>();
        Label label = new Label("l1", "lv1");
        labels.add(label);
        labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));

        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter("paramName", "paramValue");
        parameters.add(parameter);

        final JobClusterDefinitionImpl fakeJobCluster = createFakeJobClusterDefn(clusterName, labels, parameters);
        IJobClusterMetadata jobClusterMetadata = new JobClusterMetadataImpl(fakeJobCluster,1,false);

        String version = "0.0.2";

        try {
            JobDefinition givenJobDefn = new JobDefinition.Builder().withName(clusterName).withVersion(version).build();
            JobDefinitionResolver resolver = new JobDefinitionResolver();
            JobDefinition resolvedJobDefinition = resolver.getResolvedJobDefinition("user", givenJobDefn, jobClusterMetadata);
            fail();
        } catch (Exception e) {
            e.printStackTrace();

        }

    }
    @Test
    public void lookupJobClusterConfigTest() {

        String clusterName = "lookupJobClusterConfigTest";
        JobClusterConfig clusterConfig1 = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + DEFAULT_ARTIFACT_NAME)
                .withArtifactName(DEFAULT_ARTIFACT_NAME)
                .withSchedulingInfo(SINGLE_WORKER_SCHED_INFO)
                .withVersion(DEFAULT_VERSION)
                .build();

        String artifactName = "artifact2";
        JobClusterConfig clusterConfig2 = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withSchedulingInfo(TWO_WORKER_SCHED_INFO)
                .withVersion("0.0.2")
                .build();

        List<JobClusterConfig> configList = new ArrayList<>();
        configList.add(clusterConfig1);
        configList.add(clusterConfig2);

        JobClusterDefinitionImpl jobClusterDefinition =  new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfigs(configList)
                .withName(clusterName)
                .withParameters(Lists.newArrayList())
                .withLabel(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"))
                .withUser("user")
                .withIsReadyForJobMaster(true)
                .withOwner(DEFAULT_JOB_OWNER)
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .withSla(NO_OP_SLA)
                .build();

        IJobClusterMetadata jobClusterMetadata = new JobClusterMetadataImpl.Builder().withJobClusterDefinition(jobClusterDefinition).withLastJobCount(1).withIsDisabled(false).build();


        JobDefinitionResolver resolver = new JobDefinitionResolver();
        Optional<JobClusterConfig> config = resolver.getJobClusterConfigForVersion(jobClusterMetadata, DEFAULT_VERSION);
        assertTrue(config.isPresent());
        assertEquals(DEFAULT_ARTIFACT_NAME, config.get().getArtifactName());
        assertEquals("http://" + DEFAULT_ARTIFACT_NAME, config.get().getJobJarUrl());
        assertEquals(DEFAULT_VERSION, config.get().getVersion());
        assertEquals(SINGLE_WORKER_SCHED_INFO, config.get().getSchedulingInfo());


        Optional<JobClusterConfig> config2 = resolver.getJobClusterConfigForVersion(jobClusterMetadata, "0.0.2");
        assertTrue(config2.isPresent());
        assertEquals("artifact2", config2.get().getArtifactName());
        assertEquals("http://artifact2", config2.get().getJobJarUrl());
        assertEquals("0.0.2", config2.get().getVersion());
        assertEquals(TWO_WORKER_SCHED_INFO, config2.get().getSchedulingInfo());

        try {
            Optional<JobClusterConfig> config3 = resolver.getJobClusterConfigForVersion(jobClusterMetadata, "0.0.3");
            assertTrue(!config3.isPresent());
        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }



    }

}
