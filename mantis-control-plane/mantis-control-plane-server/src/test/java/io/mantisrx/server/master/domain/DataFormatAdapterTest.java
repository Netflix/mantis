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

import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.mantisrx.common.Label;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.JobClusterMetadataImpl;
import io.mantisrx.master.jobcluster.LabelManager.SystemLabels;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.master.jobcluster.job.MantisStageMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.store.MantisJobMetadata;
import io.mantisrx.server.master.store.MantisStageMetadataWritable;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.server.master.store.NamedJob;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class DataFormatAdapterTest {
    public static final MachineDefinition DEFAULT_MACHINE_DEFINITION = new MachineDefinition(1, 10, 10, 10, 2);
    private static final SchedulingInfo DEFAULT_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(DEFAULT_MACHINE_DEFINITION, Lists.newArrayList(), Lists.newArrayList()).build();
    private final LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());
    @Test
    public void jobClusterConfigToJarTest() {

        long uploadedAt = 1234l;
        String artifactName = "artifact1";
        String version = "0.0.1";
        JobClusterConfig config = new JobClusterConfig("http://" + artifactName, artifactName, uploadedAt, version, DEFAULT_SCHED_INFO);

        try {
            NamedJob.Jar convertedJar = DataFormatAdapter.convertJobClusterConfigToJar(config);

            assertEquals(uploadedAt, convertedJar.getUploadedAt());
            assertEquals("http://" + artifactName, convertedJar.getUrl().toString());
            assertEquals(version, convertedJar.getVersion());
            assertEquals(DEFAULT_SCHED_INFO,convertedJar.getSchedulingInfo());

            JobClusterConfig regeneratedConfig = DataFormatAdapter.convertJarToJobClusterConfig(convertedJar);

            assertEquals(uploadedAt, regeneratedConfig.getUploadedAt());
            assertEquals(artifactName, regeneratedConfig.getArtifactName());
            assertEquals(version, regeneratedConfig.getVersion());
            assertEquals(DEFAULT_SCHED_INFO, regeneratedConfig.getSchedulingInfo());

        } catch (MalformedURLException e) {
            fail();
            e.printStackTrace();
        }

    }
    @Test
    public void artifactNameTest() {
        String artifactName = "myartifact-0.0.1.zip";
        String version = "0.0.1";

        try {
            URL jar = DataFormatAdapter.generateURL(artifactName);
            assertEquals("http://myartifact-0.0.1.zip", jar.toString());

            assertEquals(artifactName, DataFormatAdapter.extractArtifactName(jar).orElse(""));


        } catch (MalformedURLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void artifactNameTest2() {
        String artifactName = "https://myartifact-0.0.1.zip";
        String version = "0.0.1";

        try {
            URL jar = DataFormatAdapter.generateURL(artifactName);
            assertEquals("https://myartifact-0.0.1.zip", jar.toString());

            assertEquals("myartifact-0.0.1.zip", DataFormatAdapter.extractArtifactName(jar).orElse(""));


        } catch (MalformedURLException e) {
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void extractArtifactNameTest1() throws MalformedURLException {

        URL url = new URL("http://mantisui.eu-west-1.dyntest.netflix.net/mantis-artifacts/nfmantis-sources-genericqueryable-source-6.0.8.zip");

        assertEquals("nfmantis-sources-genericqueryable-source-6.0.8.zip",DataFormatAdapter.extractArtifactName(url).orElse(""));
    }

    @Test
    public void extractArtifactNameTest2() throws MalformedURLException {

        URL url = new URL("http://nfmantis-sources-genericqueryable-source-6.0.8.zip");

        assertEquals("nfmantis-sources-genericqueryable-source-6.0.8.zip",DataFormatAdapter.extractArtifactName(url).orElse(""));
    }


    @Test
    public void slaConversionTestWithCronSpec() {
        int min = 1;
        int max = 10;
        String cronSpec = "0 0 0-23 * * ?";
        io.mantisrx.server.master.domain.SLA sla = new SLA(min, max, cronSpec,IJobClusterDefinition.CronPolicy.KEEP_EXISTING);

        NamedJob.SLA oldSlaFormat = DataFormatAdapter.convertSLAToNamedJobSLA(sla);

//        assertEquals(min, oldSlaFormat.getMin());
//        assertEquals(max, oldSlaFormat.getMax());
        assertEquals(cronSpec, oldSlaFormat.getCronSpec());
        assertEquals(NamedJobDefinition.CronPolicy.KEEP_EXISTING,oldSlaFormat.getCronPolicy());

        SLA reconvertedSLA = DataFormatAdapter.convertToSLA(oldSlaFormat);

        assertEquals(sla, reconvertedSLA);

    }

    @Test
    public void slaConversionTestNoCronSpec() {
        int min = 1;
        int max = 10;
        String cronSpec = "0 0 0-23 * * ?";
        io.mantisrx.server.master.domain.SLA sla = new SLA(min, max, null,null);

        NamedJob.SLA oldSlaFormat = DataFormatAdapter.convertSLAToNamedJobSLA(sla);

        assertEquals(min, oldSlaFormat.getMin());
        assertEquals(max, oldSlaFormat.getMax());
//        assertEquals(cronSpec, oldSlaFormat.getCronSpec());
//        assertEquals(NamedJobDefinition.CronPolicy.KEEP_EXISTING,oldSlaFormat.getCronPolicy());

        SLA reconvertedSLA = DataFormatAdapter.convertToSLA(oldSlaFormat);

        assertEquals(sla, reconvertedSLA);

    }
    @Test
    public void jobClusterMetadataConversionTest() {
        String artifactName = "artifact1";
        String version = "0.0.1";
        List<Parameter> parameterList = new ArrayList<>();
        Parameter parameter = new Parameter("param1", "value1");
        parameterList.add(parameter);


        List<Label> labels = new ArrayList<>();
        Label label = new Label("label1", "labelvalue1");
        labels.add(label);
        labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));

        long uAt = 1234l;
        JobClusterConfig jobClusterConfig =  new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withSchedulingInfo(DEFAULT_SCHED_INFO)
                .withVersion(version)
                .withUploadedAt(uAt)
                .build();

        String clusterName = "clusterName1";
        JobOwner owner = new JobOwner("Neeraj", "Mantis", "desc", "nma@netflix.com", "repo");
        boolean isReadyForMaster = true;
        SLA sla = new SLA(1, 10, null, null);
        JobClusterDefinitionImpl clusterDefn = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(jobClusterConfig)
                .withName(clusterName)
                .withUser("user1")
                .withIsReadyForJobMaster(isReadyForMaster)
                .withOwner(owner)
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .withSla(sla)
                .withParameters(parameterList)
                .withLabels(labels)
                .build();

        int lastJobCnt = 10;
        boolean disabled = false;
        IJobClusterMetadata clusterMeta = new JobClusterMetadataImpl.Builder()
                                                                        .withJobClusterDefinition(clusterDefn)
                                                                        .withLastJobCount(lastJobCnt)
                                                                        .withIsDisabled(disabled)
                                                                        .build();

        NamedJob namedJob = DataFormatAdapter.convertJobClusterMetadataToNamedJob(clusterMeta);

        assertEquals(disabled,namedJob.getDisabled());
        assertEquals(clusterName, namedJob.getName());
        assertEquals(lastJobCnt,namedJob.getLastJobCount());
        assertEquals(2, namedJob.getLabels().size());
        assertEquals(label, namedJob.getLabels().get(0));
        assertEquals(owner, namedJob.getOwner());
        assertEquals(isReadyForMaster, namedJob.getIsReadyForJobMaster());
        assertEquals(WorkerMigrationConfig.DEFAULT, namedJob.getMigrationConfig());

        // assert parameters
        assertEquals(parameterList.size(), namedJob.getParameters().size());
        assertEquals(parameter, namedJob.getParameters().get(0));

        // assert sla
        assertEquals(sla.getMin(), namedJob.getSla().getMin());
        assertEquals(sla.getMax(), namedJob.getSla().getMax());

        // assert jar info
        assertEquals(1, namedJob.getJars().size());

        // jar info
        NamedJob.Jar jar = namedJob.getJars().get(0);
        assertEquals(uAt, jar.getUploadedAt());
        assertEquals(DEFAULT_SCHED_INFO,jar.getSchedulingInfo());
        assertEquals(version, jar.getVersion());
        assertEquals(artifactName, DataFormatAdapter.extractArtifactName(jar.getUrl()).orElse(""));

        IJobClusterMetadata reconvertedJobCluster = DataFormatAdapter.convertNamedJobToJobClusterMetadata(namedJob);

        assertEquals(disabled,reconvertedJobCluster.isDisabled());
        assertEquals(clusterName,reconvertedJobCluster.getJobClusterDefinition().getName());
        assertEquals(lastJobCnt,reconvertedJobCluster.getLastJobCount());
        assertEquals(2, reconvertedJobCluster.getJobClusterDefinition().getLabels().size());
        assertEquals(label, reconvertedJobCluster.getJobClusterDefinition().getLabels().get(0));
        assertEquals(owner, reconvertedJobCluster.getJobClusterDefinition().getOwner());
        assertEquals(isReadyForMaster,reconvertedJobCluster.getJobClusterDefinition().getIsReadyForJobMaster());
        assertEquals(WorkerMigrationConfig.DEFAULT,reconvertedJobCluster.getJobClusterDefinition().getWorkerMigrationConfig());

        assertEquals(parameterList.size(), reconvertedJobCluster.getJobClusterDefinition().getParameters().size());
        assertEquals(parameter, reconvertedJobCluster.getJobClusterDefinition().getParameters().get(0));

        assertEquals(sla.getMin(), reconvertedJobCluster.getJobClusterDefinition().getSLA().getMin());
        assertEquals(sla.getMax(), reconvertedJobCluster.getJobClusterDefinition().getSLA().getMax());

        JobClusterConfig clusterConfig1 = reconvertedJobCluster.getJobClusterDefinition().getJobClusterConfig();
        assertEquals(uAt,clusterConfig1.getUploadedAt());
        assertEquals(DEFAULT_SCHED_INFO,clusterConfig1.getSchedulingInfo());
        assertEquals(version,clusterConfig1.getVersion());
        assertEquals(artifactName, clusterConfig1.getArtifactName());
        assertEquals("http://" + artifactName, clusterConfig1.getJobJarUrl());

    }

    @Test
    public void jobClusterMetadataConversionTestDisabledInvalidJob() {
        String artifactName = "artifact1";
        String version = "0.0.1";
        List<Parameter> parameterList = new ArrayList<>();
        Parameter parameter = new Parameter("param1", "value1");
        parameterList.add(parameter);


        List<Label> labels = new ArrayList<>();
        Label label = new Label("label1", "labelvalue1");
        labels.add(label);

        long uAt = 1234l;
        JobClusterConfig jobClusterConfig =  new JobClusterConfig.Builder()
            .withJobJarUrl("http://" + artifactName)
            .withArtifactName(artifactName)
            .withSchedulingInfo(DEFAULT_SCHED_INFO)
            .withVersion(version)
            .withUploadedAt(uAt)
            .build();

        String clusterName = "clusterName1";
        JobOwner owner = new JobOwner("Neeraj", "Mantis", "desc", "nma@netflix.com", "repo");
        boolean isReadyForMaster = true;
        SLA sla = new SLA(1, 10, null, null);
        JobClusterDefinitionImpl clusterDefn = new JobClusterDefinitionImpl.Builder()
            .withJobClusterConfig(jobClusterConfig)
            .withName(clusterName)
            .withUser("user1")
            .withIsReadyForJobMaster(isReadyForMaster)
            .withOwner(owner)
            .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
            .withSla(sla)
            .withParameters(parameterList)
            .withLabels(labels)
            .withIsDisabled(true)
            .build();

        int lastJobCnt = 10;
        boolean disabled = true;
        IJobClusterMetadata clusterMeta = new JobClusterMetadataImpl.Builder()
            .withJobClusterDefinition(clusterDefn)
            .withLastJobCount(lastJobCnt)
            .withIsDisabled(disabled)
            .build();

        NamedJob namedJob = DataFormatAdapter.convertJobClusterMetadataToNamedJob(clusterMeta);

        assertEquals(disabled,namedJob.getDisabled());
        assertEquals(clusterName, namedJob.getName());
        assertEquals(lastJobCnt,namedJob.getLastJobCount());
        assertEquals(1, namedJob.getLabels().size());
        assertEquals(label, namedJob.getLabels().get(0));
        assertEquals(owner, namedJob.getOwner());
        assertEquals(isReadyForMaster, namedJob.getIsReadyForJobMaster());
        assertEquals(WorkerMigrationConfig.DEFAULT, namedJob.getMigrationConfig());

        // assert parameters
        assertEquals(parameterList.size(), namedJob.getParameters().size());
        assertEquals(parameter, namedJob.getParameters().get(0));

        // assert sla
        assertEquals(sla.getMin(), namedJob.getSla().getMin());
        assertEquals(sla.getMax(), namedJob.getSla().getMax());

        // assert jar info
        assertEquals(1, namedJob.getJars().size());

        // jar info
        NamedJob.Jar jar = namedJob.getJars().get(0);
        assertEquals(uAt, jar.getUploadedAt());
        assertEquals(DEFAULT_SCHED_INFO,jar.getSchedulingInfo());
        assertEquals(version, jar.getVersion());
        assertEquals(artifactName, DataFormatAdapter.extractArtifactName(jar.getUrl()).orElse(""));

        IJobClusterMetadata reconvertedJobCluster = DataFormatAdapter.convertNamedJobToJobClusterMetadata(namedJob);

        assertEquals(disabled,reconvertedJobCluster.isDisabled());
        assertEquals(clusterName,reconvertedJobCluster.getJobClusterDefinition().getName());
        assertEquals(lastJobCnt,reconvertedJobCluster.getLastJobCount());
        assertEquals(1, reconvertedJobCluster.getJobClusterDefinition().getLabels().size());
        assertEquals(label, reconvertedJobCluster.getJobClusterDefinition().getLabels().get(0));
        assertEquals(owner, reconvertedJobCluster.getJobClusterDefinition().getOwner());
        assertEquals(isReadyForMaster,reconvertedJobCluster.getJobClusterDefinition().getIsReadyForJobMaster());
        assertEquals(WorkerMigrationConfig.DEFAULT,reconvertedJobCluster.getJobClusterDefinition().getWorkerMigrationConfig());

        assertEquals(parameterList.size(), reconvertedJobCluster.getJobClusterDefinition().getParameters().size());
        assertEquals(parameter, reconvertedJobCluster.getJobClusterDefinition().getParameters().get(0));

        assertEquals(sla.getMin(), reconvertedJobCluster.getJobClusterDefinition().getSLA().getMin());
        assertEquals(sla.getMax(), reconvertedJobCluster.getJobClusterDefinition().getSLA().getMax());

        JobClusterConfig clusterConfig1 = reconvertedJobCluster.getJobClusterDefinition().getJobClusterConfig();
        assertEquals(uAt,clusterConfig1.getUploadedAt());
        assertEquals(DEFAULT_SCHED_INFO,clusterConfig1.getSchedulingInfo());
        assertEquals(version,clusterConfig1.getVersion());
        assertEquals(artifactName, clusterConfig1.getArtifactName());
        assertEquals("http://" + artifactName, clusterConfig1.getJobJarUrl());

    }

    @Test
    public void completedJobToNamedJobCompletedJobTest() {
        String name = "name";
        String jobId = "name-1";
        String version = "0.0.1";
        JobState jobState = JobState.Completed;
        long submittedAt = 1234l;
        long terminatedAt = 2234l;
        String me = "me";
        List<Label> labels = new ArrayList<>();
        labels.add(new Label("l1","v1"));
        JobClusterDefinitionImpl.CompletedJob cJob = new JobClusterDefinitionImpl.CompletedJob(
                name, jobId, version, jobState, submittedAt, terminatedAt, me, labels);

        NamedJob.CompletedJob njobCJob = DataFormatAdapter.convertCompletedJobToNamedJobCompletedJob(cJob);
        assertEquals(name,njobCJob.getName());
        assertEquals(jobId,njobCJob.getJobId());
        assertEquals(version,njobCJob.getVersion());
        assertEquals(MantisJobState.Completed,njobCJob.getState());
        assertEquals(submittedAt,njobCJob.getSubmittedAt());
        assertEquals(terminatedAt,njobCJob.getTerminatedAt());

        JobClusterDefinitionImpl.CompletedJob reconverted = DataFormatAdapter.convertNamedJobCompletedJobToCompletedJob(njobCJob);

        assertEquals(cJob,reconverted);

    }

    @Test
    public void oldMantisWorkerMetadataReadTest() throws IOException {

        ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

        final String oldWorkerMetadataWriteableStr = "{\n" +
            "  \"workerIndex\": 0,\n" +
            "  \"workerNumber\": 1,\n" +
            "  \"jobId\": \"cname-1\",\n" +
            "  \"stageNum\": 1,\n" +
            "  \"numberOfPorts\": 3,\n" +
            "  \"metricsPort\": 1,\n" +
            "  \"consolePort\": 3,\n" +
            "  \"debugPort\": 2,\n" +
            "  \"customPort\": 5,\n" +
            "  \"ports\": [4],\n" +
            "  \"state\": \"Completed\",\n" +
            "  \"slave\": \"slave1\",\n" +
            "  \"slaveID\": \"slaveId1\",\n" +
            "  \"cluster\": \"prefCluster\",\n" +
            "  \"acceptedAt\": 999,\n" +
            "  \"launchedAt\": 1000,\n" +
            "  \"startingAt\": 1234,\n" +
            "  \"startedAt\": 1001,\n" +
            "  \"completedAt\": 2000,\n" +
            "  \"reason\": \"Normal\",\n" +
            "  \"resubmitOf\": 42,\n" +
            "  \"totalResubmitCount\": 1\n" +
            "}";

        MantisWorkerMetadataWritable oldMetadataWritable = mapper.readValue(oldWorkerMetadataWriteableStr, MantisWorkerMetadataWritable.class);

        Optional<String> prefCluster = of("prefCluster");
        int metricsPort = 1;
        int debugPort = 2;
        int consolePort = 3;
        int customPort = 5;
        int ssePort = 4;
        List<Integer> ports = Lists.newArrayList();

        ports.add(metricsPort);
        ports.add(debugPort);
        ports.add(consolePort);
        ports.add(customPort);
        ports.add(ssePort);

        WorkerPorts workerPorts = new WorkerPorts(ports);
        int workerNum = 1;
        int workerIndex = 0;
        long startingAt = 1234l;
        int stageNum = 1;
        String slaveid = "slaveId1";
        String slave = "slave1";
        int resubmitCnt = 1;
        int portNums = ports.size();
        long launchedAt = 1000l;
        JobId jobId = new JobId("cname", 1);
        long acceptedAt = 999l;
        long completedAt = 2000l;
        long startedAt = 1001l;
        int resubOf = 42;
        JobWorker worker = new JobWorker.Builder()
            .withPreferredCluster(prefCluster)
            .withJobCompletedReason(JobCompletedReason.Normal)
            .withWorkerPorts(workerPorts)
            .withWorkerNumber(workerNum)
            .withWorkerIndex(workerIndex)
            .withState(WorkerState.Completed)
            .withStartingAt(startingAt)
            .withStartedAt(startedAt)
            .withCompletedAt(completedAt)
            .withStageNum(stageNum)
            .withSlaveID(slaveid)
            .withSlave(slave)
            .withResubmitCount(resubmitCnt)
            .withResubmitOf(resubOf)
            .withNumberOfPorts(portNums)

            .withLaunchedAt(launchedAt)
            .withJobId(jobId)

            .withAcceptedAt(acceptedAt)
            .withLifecycleEventsPublisher(eventPublisher)
            .build();
        IMantisWorkerMetadata expectedWorkerMeta = worker.getMetadata();

        assertEquals(prefCluster,oldMetadataWritable.getCluster());
        assertEquals(workerIndex, oldMetadataWritable.getWorkerIndex());
        assertEquals(workerNum, oldMetadataWritable.getWorkerNumber());
        assertEquals(jobId.getId(),oldMetadataWritable.getJobId());

        assertEquals(acceptedAt,oldMetadataWritable.getAcceptedAt());
        assertEquals(startingAt,oldMetadataWritable.getStartingAt());
        assertEquals(startedAt, oldMetadataWritable.getStartedAt());
        assertEquals(launchedAt, oldMetadataWritable.getLaunchedAt());
        assertEquals(completedAt, oldMetadataWritable.getCompletedAt());

        assertEquals(stageNum, oldMetadataWritable.getStageNum());
        assertEquals(slave, oldMetadataWritable.getSlave());
        assertEquals(slaveid, oldMetadataWritable.getSlaveID());

        assertEquals(metricsPort, oldMetadataWritable.getMetricsPort());
        assertEquals(consolePort, oldMetadataWritable.getConsolePort());
        assertEquals(debugPort, oldMetadataWritable.getDebugPort());
        assertEquals(5, oldMetadataWritable.getCustomPort());

        assertEquals(MantisJobState.Completed, oldMetadataWritable.getState());

        assertEquals(resubmitCnt, oldMetadataWritable.getTotalResubmitCount());
        assertEquals(resubOf, oldMetadataWritable.getResubmitOf());

        assertEquals(3, oldMetadataWritable.getNumberOfPorts());
        assertEquals(1, oldMetadataWritable.getPorts().size());
        assertEquals(ssePort, (long)oldMetadataWritable.getPorts().get(0));
        assertEquals(JobCompletedReason.Normal, oldMetadataWritable.getReason());

        JobWorker convertedMetadata = DataFormatAdapter.convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(oldMetadataWritable, eventPublisher);

        assertEquals(expectedWorkerMeta, convertedMetadata.getMetadata());

    }

    @Test
    public void mantisWorkerMetadataReadTest() throws IOException {

        ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

        final String oldWorkerMetadataWriteableStr = "{\n" +
            "  \"workerIndex\": 0,\n" +
            "  \"workerNumber\": 1,\n" +
            "  \"jobId\": \"cname-1\",\n" +
            "  \"stageNum\": 1,\n" +
            "  \"numberOfPorts\": 3,\n" +
            "  \"metricsPort\": 1,\n" +
            "  \"consolePort\": 3,\n" +
            "  \"debugPort\": 2,\n" +
            "  \"customPort\": 5,\n" +
            "  \"ports\": [4],\n" +
            "  \"state\": \"Completed\",\n" +
            "  \"slave\": \"slave1\",\n" +
            "  \"slaveID\": \"slaveId1\",\n" +
            "  \"cluster\": \"prefCluster\",\n" +
            "  \"acceptedAt\": 999,\n" +
            "  \"launchedAt\": 1000,\n" +
            "  \"startingAt\": 1234,\n" +
            "  \"startedAt\": 1001,\n" +
            "  \"completedAt\": 2000,\n" +
            "  \"reason\": \"Normal\",\n" +
            "  \"resubmitOf\": 42,\n" +
            "  \"totalResubmitCount\": 1,\n" +
            "  \"resourceCluster\": {\"resourceID\": \"resourceCluster\"}\n" +
            "}";

        MantisWorkerMetadataWritable metadataWritable = mapper.readValue(oldWorkerMetadataWriteableStr, MantisWorkerMetadataWritable.class);

        Optional<String> prefCluster = of("prefCluster");
        ClusterID resourceCluster = ClusterID.of("resourceCluster");
        int metricsPort = 1;
        int debugPort = 2;
        int consolePort = 3;
        int customPort = 5;
        int ssePort = 4;
        List<Integer> ports = Lists.newArrayList();

        ports.add(metricsPort);
        ports.add(debugPort);
        ports.add(consolePort);
        ports.add(customPort);
        ports.add(ssePort);

        WorkerPorts workerPorts = new WorkerPorts(ports);
        int workerNum = 1;
        int workerIndex = 0;
        long startingAt = 1234l;
        int stageNum = 1;
        String slaveid = "slaveId1";
        String slave = "slave1";
        int resubmitCnt = 1;
        int portNums = ports.size();
        long launchedAt = 1000l;
        JobId jobId = new JobId("cname", 1);
        long acceptedAt = 999l;
        long completedAt = 2000l;
        long startedAt = 1001l;
        int resubOf = 42;
        JobWorker worker = new JobWorker.Builder()
            .withPreferredCluster(prefCluster)
            .withResourceCluster(resourceCluster)
            .withJobCompletedReason(JobCompletedReason.Normal)
            .withWorkerPorts(workerPorts)
            .withWorkerNumber(workerNum)
            .withWorkerIndex(workerIndex)
            .withState(WorkerState.Completed)
            .withStartingAt(startingAt)
            .withStartedAt(startedAt)
            .withCompletedAt(completedAt)
            .withStageNum(stageNum)
            .withSlaveID(slaveid)
            .withSlave(slave)
            .withResubmitCount(resubmitCnt)
            .withResubmitOf(resubOf)
            .withNumberOfPorts(portNums)

            .withLaunchedAt(launchedAt)
            .withJobId(jobId)

            .withAcceptedAt(acceptedAt)
            .withLifecycleEventsPublisher(eventPublisher)
            .build();
        IMantisWorkerMetadata expectedWorkerMeta = worker.getMetadata();

        assertEquals(prefCluster,metadataWritable.getCluster());
        assertEquals(resourceCluster,metadataWritable.getResourceCluster().get());
        assertEquals(workerIndex, metadataWritable.getWorkerIndex());
        assertEquals(workerNum, metadataWritable.getWorkerNumber());
        assertEquals(jobId.getId(),metadataWritable.getJobId());

        assertEquals(acceptedAt,metadataWritable.getAcceptedAt());
        assertEquals(startingAt,metadataWritable.getStartingAt());
        assertEquals(startedAt, metadataWritable.getStartedAt());
        assertEquals(launchedAt, metadataWritable.getLaunchedAt());
        assertEquals(completedAt, metadataWritable.getCompletedAt());

        assertEquals(stageNum, metadataWritable.getStageNum());
        assertEquals(slave, metadataWritable.getSlave());
        assertEquals(slaveid, metadataWritable.getSlaveID());

        assertEquals(metricsPort, metadataWritable.getMetricsPort());
        assertEquals(consolePort, metadataWritable.getConsolePort());
        assertEquals(debugPort, metadataWritable.getDebugPort());
        assertEquals(5, metadataWritable.getCustomPort());

        assertEquals(MantisJobState.Completed, metadataWritable.getState());

        assertEquals(resubmitCnt, metadataWritable.getTotalResubmitCount());
        assertEquals(resubOf, metadataWritable.getResubmitOf());

        assertEquals(3, metadataWritable.getNumberOfPorts());
        assertEquals(1, metadataWritable.getPorts().size());
        assertEquals(ssePort, (long)metadataWritable.getPorts().get(0));
        assertEquals(JobCompletedReason.Normal, metadataWritable.getReason());

        JobWorker convertedMetadata = DataFormatAdapter.convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(metadataWritable, eventPublisher);

        assertEquals(expectedWorkerMeta, convertedMetadata.getMetadata());

    }

    @Test
    public void mantisWorkerMetadataToMetadataWritebleTest() {

        Optional<String> prefCluster = of("prefCluster");
        ClusterID resourceCluster = ClusterID.of("resourceCluster");
        int metricsPort = 1;
        int debugPort = 2;
        int consolePort = 3;
        int customPort = 4;
        int ssePort = 5;
        List<Integer> ports = Lists.newArrayList();

        ports.add(metricsPort);
        ports.add(debugPort);
        ports.add(consolePort);
        ports.add(customPort);
        ports.add(ssePort);

        WorkerPorts workerPorts = new WorkerPorts(ports);
        int workerNum = 1;
        int workerIndex = 0;
        long startingAt = 1234l;
        int stageNum = 1;
        String slaveid = "slaveId1";
        String slave = "slave1";
        int resubmitCnt = 1;
        int portNums = ports.size();
        long launchedAt = 1000l;
        JobId jobId = new JobId("cname", 1);
        long acceptedAt = 999l;
        long completedAt = 2000l;
        long startedAt = 1001l;
        int resubOf = 42;
        JobWorker worker = new JobWorker.Builder()
            .withPreferredCluster(prefCluster)
            .withResourceCluster(resourceCluster)
            .withJobCompletedReason(JobCompletedReason.Normal)
            .withWorkerPorts(workerPorts)
            .withWorkerNumber(workerNum)
            .withWorkerIndex(workerIndex)
            .withState(WorkerState.Completed)
            .withStartingAt(startingAt)
            .withStartedAt(startedAt)
            .withCompletedAt(completedAt)
            .withStageNum(stageNum)
            .withSlaveID(slaveid)
            .withSlave(slave)
            .withResubmitCount(resubmitCnt)
            .withResubmitOf(resubOf)
            .withNumberOfPorts(portNums)

            .withLaunchedAt(launchedAt)
            .withJobId(jobId)

            .withAcceptedAt(acceptedAt)
            .withLifecycleEventsPublisher(eventPublisher)
            .build();
        IMantisWorkerMetadata workerMeta = worker.getMetadata();
        MantisWorkerMetadataWritable metadataWritable = DataFormatAdapter.convertMantisWorkerMetadataToMantisWorkerMetadataWritable(workerMeta);

        assertEquals(prefCluster,metadataWritable.getCluster());
        assertEquals(resourceCluster, metadataWritable.getResourceCluster().get());
        assertEquals(workerIndex, metadataWritable.getWorkerIndex());
        assertEquals(workerNum, metadataWritable.getWorkerNumber());
        assertEquals(jobId.getId(),metadataWritable.getJobId());

        assertEquals(acceptedAt,metadataWritable.getAcceptedAt());
        assertEquals(startingAt,metadataWritable.getStartingAt());
        assertEquals(startedAt, metadataWritable.getStartedAt());
        assertEquals(launchedAt, metadataWritable.getLaunchedAt());
        assertEquals(completedAt, metadataWritable.getCompletedAt());

        assertEquals(stageNum, metadataWritable.getStageNum());
        assertEquals(slave, metadataWritable.getSlave());
        assertEquals(slaveid, metadataWritable.getSlaveID());

        assertEquals(metricsPort, metadataWritable.getMetricsPort());
        assertEquals(consolePort, metadataWritable.getConsolePort());
        assertEquals(debugPort, metadataWritable.getDebugPort());
        assertEquals(customPort, metadataWritable.getCustomPort());

        assertEquals(MantisJobState.Completed, metadataWritable.getState());

        assertEquals(resubmitCnt, metadataWritable.getTotalResubmitCount());
        assertEquals(resubOf, metadataWritable.getResubmitOf());

        assertEquals(portNums, metadataWritable.getNumberOfPorts());
        assertEquals(1, metadataWritable.getPorts().size());
        assertEquals(ssePort, (long)metadataWritable.getPorts().get(0));
        assertEquals(JobCompletedReason.Normal, metadataWritable.getReason());

        JobWorker reconverted = DataFormatAdapter.convertMantisWorkerMetadataWriteableToMantisWorkerMetadata(metadataWritable, eventPublisher);

        assertEquals(workerMeta, reconverted.getMetadata());

    }

    @Test
    public void convertMantisStageMetaTest() {
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));
        int stageNo = 1;
        int min = 3;
        int max = 10;
        int increment = 1;
        int decrement = 1;
        int coolDownSecs = 300;
        StageScalingPolicy stageScalingPolicy = new StageScalingPolicy(stageNo, min, max, increment, decrement, coolDownSecs, smap, true);
        List<JobConstraints> softConstraintsList = new ArrayList<>();
        softConstraintsList.add(JobConstraints.ExclusiveHost);
        List<JobConstraints> hardConstraintsList = new ArrayList<>();
        hardConstraintsList.add(JobConstraints.M3Cluster);

        JobId jobId = new JobId("cName",1);
        int numWorkers = 1;
        int numStages = 2;
        boolean isScalable = true;
        IMantisStageMetadata stageMeta = new MantisStageMetadataImpl.Builder()
                                                                        .withStageNum(stageNo)
                                                                        .withScalingPolicy(stageScalingPolicy)
                                                                        .withNumWorkers(numWorkers)
                                                                        .withMachineDefinition(DEFAULT_MACHINE_DEFINITION)
                                                                        .withNumStages(numStages)
                                                                        .withSoftConstraints(softConstraintsList)
                                                                        .withHardConstraints(hardConstraintsList)
                                                                        .withJobId(jobId)
                                                                        .isScalable(isScalable)
                                                                        .build();
        MantisStageMetadataWritable stageMetadataWritable = DataFormatAdapter.convertMantisStageMetadataToMantisStageMetadataWriteable(stageMeta);

        assertEquals(jobId.getId(),stageMetadataWritable.getJobId());
        assertEquals(JobConstraints.M3Cluster,stageMetadataWritable.getHardConstraints().get(0));
        assertEquals(JobConstraints.ExclusiveHost, stageMetadataWritable.getSoftConstraints().get(0));
        assertEquals(stageScalingPolicy, stageMetadataWritable.getScalingPolicy());
        assertTrue(stageMetadataWritable.getScalable());
        assertEquals(DEFAULT_MACHINE_DEFINITION, stageMetadataWritable.getMachineDefinition());
        assertEquals(numWorkers, stageMetadataWritable.getNumWorkers());
        assertEquals(numStages, stageMetadataWritable.getNumStages());
        assertEquals(stageNo,stageMetadataWritable.getStageNum());

        IMantisStageMetadata reconverted = DataFormatAdapter.convertMantisStageMetadataWriteableToMantisStageMetadata(stageMetadataWritable, eventPublisher);
        assertEquals(stageMeta,reconverted);


    }
    @Test
    public void convertMantisJobWriteableTest() throws Exception {
        String artifactName = "artifact";
        String version = "1.0.0";
        String clusterName = "myCluster";
        List<Label> labels = new ArrayList<>();
        Label label = new Label("myLable","myVal");
        labels.add(label);

        List<Parameter> params = new ArrayList<>();
        Parameter param = new Parameter("myparam", "myval");
        params.add(param);

        long subTimeout = 1000;

        JobSla jobSla = new JobSla(100,10,JobSla.StreamSLAType.Lossy,MantisJobDurationType.Perpetual,"userType");

        JobDefinition jobDefn = new JobDefinition.Builder()
                                                    .withJobJarUrl("http://" + artifactName)
                                                    .withArtifactName(artifactName)
                                                    .withName(clusterName)
                                                    .withLabels(labels)
                                                    .withParameters(params)
                                                    .withSchedulingInfo(DEFAULT_SCHED_INFO)
                                                    .withUser("user")
                                                    .withJobSla(jobSla)
                                                    .withSubscriptionTimeoutSecs(subTimeout)
                                                    .withNumberOfStages(DEFAULT_SCHED_INFO.getStages().size())
                                                    .build();
        JobId jobId = new JobId(clusterName,1);
        long currTime = System.currentTimeMillis();
        Instant startedAt = Instant.ofEpochMilli(currTime);
        Instant endedAt = startedAt.plusSeconds(5);
        Instant submittedAt = startedAt.minusSeconds(5);
        IMantisJobMetadata jobmeta = new MantisJobMetadataImpl.Builder()
                                                                        .withJobDefinition(jobDefn)
                                                                        .withJobId(jobId)
                                                                        .withNextWorkerNumToUse(2)
                                                                        .withSubmittedAt(submittedAt)
                                                                        .withJobState(JobState.Launched)
                                                                        .build();
        IMantisWorkerMetadata workerMetadata = new MantisWorkerMetadataImpl(0,
                1, jobId.getId(),
                1,3, new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)), WorkerState.Started,
                "slave","slaveId",startedAt.toEpochMilli(),startedAt.toEpochMilli(),
                startedAt.toEpochMilli(),startedAt.toEpochMilli(),-1,JobCompletedReason.Normal,
                0,0,of("cluster"), Optional.empty());

        ((MantisJobMetadataImpl) jobmeta).addJobStageIfAbsent(new MantisStageMetadataImpl.Builder()
                .withNumStages(1)
                .withStageNum(1)
                .withNumWorkers(1)
                .withJobId(jobId)
                .withHardConstraints(Lists.newArrayList())
                .withSoftConstraints(Lists.newArrayList())
                .withMachineDefinition(DEFAULT_MACHINE_DEFINITION)

                .build());

        ((MantisJobMetadataImpl) jobmeta).addWorkerMetadata(1, new JobWorker(workerMetadata,eventPublisher));

        MantisJobMetadata oldFormat = DataFormatAdapter.convertMantisJobMetadataToMantisJobMetadataWriteable(jobmeta);
        System.out.println("oldForamt -> " + oldFormat);
        assertEquals(jobId.getId(), oldFormat.getJobId());
        assertEquals(label,oldFormat.getLabels().get(0));
        assertEquals(param,oldFormat.getParameters().get(0));
        assertEquals(clusterName,oldFormat.getName());
        assertEquals(jobSla,oldFormat.getSla());
        assertEquals(1,oldFormat.getNumStages());
        assertEquals(subTimeout,oldFormat.getSubscriptionTimeoutSecs());
        assertEquals(2,oldFormat.getNextWorkerNumberToUse());
        assertEquals("http://" + artifactName,oldFormat.getJarUrl().toString());
        assertEquals(MantisJobState.Launched, oldFormat.getState());
        assertEquals(submittedAt.toEpochMilli(),oldFormat.getSubmittedAt());
        assertEquals("user",oldFormat.getUser());

        IMantisJobMetadata reconverted = DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(oldFormat, eventPublisher);
        System.out.println("newForamt -> " + reconverted);
        //assertEquals(jobmeta, reconverted);
     //   assertTrue(jobmeta.equals(reconverted));
        assertEquals(jobmeta.getArtifactName(),reconverted.getArtifactName());
        assertEquals(jobmeta.getClusterName(),reconverted.getClusterName());
        System.out.println("expected Jobdef " + jobmeta.getJobDefinition());
        System.out.println("actual   Jobdef " + reconverted.getJobDefinition());
        assertEquals(jobmeta.getJobDefinition(),reconverted.getJobDefinition());
        assertEquals(jobmeta.getJobId(),reconverted.getJobId());

        assertEquals(jobmeta.getJobJarUrl(),reconverted.getJobJarUrl());
        assertEquals(jobmeta.getLabels().get(0),reconverted.getLabels().get(0));
        assertEquals(jobmeta.getParameters().get(0),reconverted.getParameters().get(0));
        assertEquals(jobmeta.getMinRuntimeSecs(),reconverted.getMinRuntimeSecs());
        assertEquals(jobmeta.getNextWorkerNumberToUse(),reconverted.getNextWorkerNumberToUse());
        assertEquals(jobmeta.getSla().get(),reconverted.getSla().get());

        assertEquals(jobmeta.getSubmittedAtInstant(),reconverted.getSubmittedAtInstant());
        assertEquals(jobmeta.getState(),reconverted.getState());
        assertEquals(jobmeta.getSubscriptionTimeoutSecs(),reconverted.getSubscriptionTimeoutSecs());
        assertEquals(jobmeta.getTotalStages(),reconverted.getTotalStages());
        assertEquals(jobmeta.getUser(),reconverted.getUser());
       // assertEquals(jobmeta.getSchedulingInfo(), reconverted.getSchedulingInfo());


    }

}
