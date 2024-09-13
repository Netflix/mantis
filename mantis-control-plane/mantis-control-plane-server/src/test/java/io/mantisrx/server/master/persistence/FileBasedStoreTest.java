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

package io.mantisrx.server.master.persistence;

import static io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl.MANTIS_SYSTEM_ALLOCATED_NUM_PORTS;
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
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.master.jobcluster.job.MantisStageMetadataImpl;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.mantisrx.shaded.com.google.common.collect.Maps;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;

public class FileBasedStoreTest {
    private final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

    private final FileBasedStore fileProvider = new FileBasedStore();

    @After
    public void tearDown() {
        fileProvider.reset();
    }

    private JobClusterDefinitionImpl createFakeJobClusterDefn(String clusterName, List<Label> labels)  {
        String artifactName = "myart";
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withJobJarUrl("http://" + artifactName)
                .withArtifactName(artifactName)
                .withSchedulingInfo(new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2), Lists.newArrayList(), Lists.newArrayList()).build())
                .withVersion("0.0.1")
                .build();

        return new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(clusterName)
                .withUser("user")
                .withLabels(labels)
                .withParameters(Lists.newArrayList())
                .withIsReadyForJobMaster(true)
                .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .build();


    }
   @Test
    public void testCreateJob() {
        String clusterName = "testCreateJob";
        FileBasedPersistenceProvider sProvider = new FileBasedPersistenceProvider(fileProvider, eventPublisher);
        IJobClusterDefinition jobClusterDefn = JobTestHelper.generateJobClusterDefinition(clusterName);
        JobDefinition jobDefinition;
        try {
            jobDefinition = JobTestHelper.generateJobDefinition(clusterName);
            JobId jobId = JobId.fromId(clusterName + "-1").get();
            IMantisJobMetadata mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(jobId)
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)

                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefinition)
                    .build();
            sProvider.storeNewJob(mantisJobMetaData);

            SchedulingInfo schedInfo = jobDefinition.getSchedulingInfo();
            int numStages = schedInfo.getStages().size();
            for(int s=1; s<=numStages; s++) {
                StageSchedulingInfo stage = schedInfo.getStages().get(s);
                IMantisStageMetadata msmd = new MantisStageMetadataImpl.Builder().
                        withJobId(jobId)
                        .withStageNum(s)
                        .withNumStages(1)
                        .withMachineDefinition(stage.getMachineDefinition())
                        .withNumWorkers(stage.getNumberOfInstances())
                        .withHardConstraints(stage.getHardConstraints())
                        .withSoftConstraints(stage.getSoftConstraints())
                        .withScalingPolicy(stage.getScalingPolicy())
                        .isScalable(stage.getScalable())
                        .build();
                ((MantisJobMetadataImpl)mantisJobMetaData).addJobStageIfAbsent(msmd);
                sProvider.updateMantisStage(msmd);
                for(int w=0; w<stage.getNumberOfInstances(); w++) {
                    JobWorker mwmd = new JobWorker.Builder()
                            .withJobId(jobId)
                            .withWorkerIndex(w)
                            .withWorkerNumber(1)
                            .withNumberOfPorts(1 + MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
                            .withWorkerPorts(new WorkerPorts(ImmutableList.of(9091, 9092, 9093, 9094, 9095)))
                            .withStageNum(w+1)
                            .withLifecycleEventsPublisher(eventPublisher)
                            .build();
                    ((MantisJobMetadataImpl)mantisJobMetaData).addWorkerMetadata(1, mwmd);
                    sProvider.storeWorker(mwmd.getMetadata());
                }
            }

            Optional<IMantisJobMetadata> loadedJobMetaOp = sProvider.loadActiveJob(jobId.getId());
            assertTrue(loadedJobMetaOp.isPresent());
            IMantisJobMetadata loadedJobMeta = loadedJobMetaOp.get();
            System.out.println("Original Job -> " + mantisJobMetaData);

            System.out.println("Loaded Job ->" + loadedJobMeta);

            isEqual(mantisJobMetaData, loadedJobMeta);



        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }

   private void isEqual(IMantisJobMetadata orig, IMantisJobMetadata loaded) {

       assertEquals(orig.getJobId(), loaded.getJobId());
       assertEquals(orig.getSubmittedAtInstant(), loaded.getSubmittedAtInstant());
       assertEquals(orig.getSubscriptionTimeoutSecs(), loaded.getSubscriptionTimeoutSecs());
       assertEquals(orig.getState(),loaded.getState());
       assertEquals(orig.getNextWorkerNumberToUse(), loaded.getNextWorkerNumberToUse());
       System.out.println("Orig JobDefn: " + orig.getJobDefinition());
       System.out.println("load JobDefn: " + loaded.getJobDefinition());
       assertEquals(orig.getJobDefinition().toString(),loaded.getJobDefinition().toString());
       assertEquals(((MantisJobMetadataImpl)orig).getStageMetadata().size(),((MantisJobMetadataImpl)loaded).getStageMetadata().size());
       assertEquals(((MantisJobMetadataImpl)orig).getTotalStages(),((MantisJobMetadataImpl)loaded).getTotalStages());

       for(int s = 1; s <= ((MantisJobMetadataImpl)orig).getTotalStages(); s++) {
           assertTrue(((MantisJobMetadataImpl)loaded).getStageMetadata(s).isPresent());
           System.out.println("orig stage: " + ((MantisJobMetadataImpl)orig).getStageMetadata(s).get());
           System.out.println("load stage: " + ((MantisJobMetadataImpl)loaded).getStageMetadata(s).get());
           assertEquals(((MantisJobMetadataImpl)orig).getStageMetadata(s).get().toString(),((MantisJobMetadataImpl)loaded).getStageMetadata(s).get().toString());
       }


   }

   // @Test
    public void serde() throws IOException {
        String clusterName = "testCreateClusterClueter";
        File tmpFile = new File("/tmp/MantisSpool/jobClusters" + "/" + clusterName);
        tmpFile.createNewFile();
        IJobClusterDefinition jobClusterDefn = createFakeJobClusterDefn(clusterName, Lists.newArrayList());
        PrintWriter pwrtr = new PrintWriter(tmpFile);
        mapper.writeValue(pwrtr, jobClusterDefn);

        try (FileInputStream fis = new FileInputStream(tmpFile)) {
            IJobClusterDefinition jobClustermeta = mapper.readValue(fis, JobClusterDefinitionImpl.class);
            System.out.println("read: " + jobClustermeta.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testCreateAndGetJobCluster() {
        FileBasedPersistenceProvider sProvider = new FileBasedPersistenceProvider(fileProvider, eventPublisher);
        String clusterName = "testCreateClusterClueter";

        JobClusterDefinitionImpl jobClusterDefn = createFakeJobClusterDefn(
            clusterName,
            ImmutableList.of(
                new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster")));

        IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder().withLastJobCount(0).withJobClusterDefinition(jobClusterDefn).build();
        try {
            sProvider.createJobCluster(jobCluster);
            Optional<IJobClusterMetadata> readDataOp = sProvider.loadAllJobClusters().stream()
                .filter(jc -> clusterName.equals(jc.getJobClusterDefinition().getName())).findFirst();
            if(readDataOp.isPresent()) {
                assertEquals(clusterName, readDataOp.get().getJobClusterDefinition().getName());
            } else {
                fail();
            }

        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }
    @Test
    public void testUpdateJobCluster() {
        FileBasedPersistenceProvider sProvider = new FileBasedPersistenceProvider(fileProvider, eventPublisher);
        String clusterName = "testUpdateJobCluster";

        JobClusterDefinitionImpl jobClusterDefn = createFakeJobClusterDefn(
            clusterName,
            ImmutableList.of(
                new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster")));

        IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder().withLastJobCount(0).withJobClusterDefinition(jobClusterDefn).build();
        try {
            sProvider.createJobCluster(jobCluster);
            Optional<IJobClusterMetadata> readDataOp = sProvider.loadAllJobClusters().stream()
                .filter(jc -> clusterName.equals(jc.getJobClusterDefinition().getName())).findFirst();
            if(readDataOp.isPresent()) {
                assertEquals(clusterName, readDataOp.get().getJobClusterDefinition().getName());
                assertEquals(1, readDataOp.get().getJobClusterDefinition().getLabels().size());
            } else {
                fail();
            }

            List<Label> labels = Lists.newArrayList();
            labels.add(new Label("label1", "label1value"));
            labels.add(new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster"));
            jobClusterDefn = createFakeJobClusterDefn(clusterName, labels);
            IJobClusterMetadata jobClusterUpdated = new JobClusterMetadataImpl.Builder().withLastJobCount(0).withJobClusterDefinition(jobClusterDefn).build();
            sProvider.updateJobCluster(jobClusterUpdated);

            readDataOp = sProvider.loadAllJobClusters().stream()
                .filter(jc -> clusterName.equals(jc.getJobClusterDefinition().getName())).findFirst();
            if(readDataOp.isPresent()) {
                assertEquals(clusterName, readDataOp.get().getJobClusterDefinition().getName());
                assertEquals(2, readDataOp.get().getJobClusterDefinition().getLabels().size());
            } else {
                fail();
            }

        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }

    }
    @Test
    public void testGetAllJobClusters() throws Exception {
        FileBasedPersistenceProvider sProvider = new FileBasedPersistenceProvider(fileProvider, eventPublisher);
        String clusterPrefix = "testGetAllJobClustersCluster";
        for(int i=0; i<5; i++) {
            JobClusterDefinitionImpl jobClusterDefn = createFakeJobClusterDefn(
                clusterPrefix + "_" + i,
                ImmutableList.of(
                    new Label(SystemLabels.MANTIS_RESOURCE_CLUSTER_NAME_LABEL.label, "testcluster")));
            IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder().withLastJobCount(0).withJobClusterDefinition(jobClusterDefn).build();
            sProvider.createJobCluster(jobCluster);
        }

        List<IJobClusterMetadata> jobClusterList = sProvider.loadAllJobClusters();
        assertTrue(jobClusterList.size() >= 5);
        Map<String, IJobClusterMetadata> clustersMap = Maps.newHashMap();
        for(IJobClusterMetadata cluster : jobClusterList) {
            clustersMap.put(cluster.getJobClusterDefinition().getName(), cluster);
        }
        for(int i=0; i<5; i++) {
            assertTrue(clustersMap.containsKey(clusterPrefix + "_" + i));
        }
    }





}
