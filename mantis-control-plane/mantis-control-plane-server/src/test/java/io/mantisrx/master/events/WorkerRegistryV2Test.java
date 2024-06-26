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

package io.mantisrx.master.events;

import static io.mantisrx.master.events.LifecycleEventsProto.StatusEvent.StatusEventType.INFO;
import static io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl.MANTIS_SYSTEM_ALLOCATED_NUM_PORTS;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.events.LifecycleEventsProto.WorkerStatusEvent;
import io.mantisrx.master.jobcluster.WorkerInfoListHolder;
import io.mantisrx.master.jobcluster.job.IMantisStageMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.util.*;
import java.util.concurrent.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkerRegistryV2Test {

    static ActorSystem system;
    private static TestKit probe;

    private static MantisJobStore jobStore;
    private static IMantisPersistenceProvider storageProvider;

    private static final String user = "mantis";

    @BeforeClass
    public static void setup() {

        system = ActorSystem.create();
        probe = new TestKit(system);

//        JobTestHelper.createDirsIfRequired();
        TestHelpers.setupMasterConfig();
//        storageProvider = new MantisStorageProviderAdapter(new io.mantisrx.server.master.store.SimpleCachedFileStorageProvider(), eventPublisher);
//        jobStore = new MantisJobStore(storageProvider);

    }

    @AfterClass
    public static void tearDown() {
        //((SimpleCachedFileStorageProvider)storageProvider).deleteAllFiles();
        //JobTestHelper.deleteAllFiles();
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testGetRunningCount() {
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        initRegistryWithWorkers(workerRegistryV2,"testGetRunningCount-1", 5);

        assertEquals(5, workerRegistryV2.getNumRunningWorkers(null));

    }

    @Test
    public void testIsWorkerValid() {
        JobId jId = new JobId("testIsWorkerValid",1);
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        initRegistryWithWorkers(workerRegistryV2,"testIsWorkerValid-1", 5);

        for(int i=0; i<5; i++) {
            assertTrue(workerRegistryV2.isWorkerValid(new WorkerId(jId.getId(),i,i+5)));
        }
    }

    @Test
    public void testGetAllRunningWorkers() {
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        initRegistryWithWorkers(workerRegistryV2,"testGetAllRunningWorkers-1", 5);

        Set<WorkerId> allRunningWorkers = workerRegistryV2.getAllRunningWorkers(null);

        assertEquals(5, allRunningWorkers.size());
    }

    @Test
    public void testGetSlaveIdMappings() {
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        initRegistryWithWorkers(workerRegistryV2,"testGetSlaveIdMappings-1", 5);

        Map<WorkerId, String> workerIdToSlaveIdMap = workerRegistryV2.getAllRunningWorkerSlaveIdMappings(null);
        assertEquals(5, workerIdToSlaveIdMap.size());

        for(int i=0; i<5; i++) {
            assertEquals("slaveId-"+i, workerIdToSlaveIdMap.get(new WorkerId("testGetSlaveIdMappings-1",i,i+5)));
        }
    }

    @Test
    public void testGetAcceptedAt() {
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        initRegistryWithWorkers(workerRegistryV2,"testGetAcceptedAt-1", 5);

        Optional<Long> acceptedAt = workerRegistryV2.getAcceptedAt(new WorkerId("testGetAcceptedAt-1", 0, 5));

        assertTrue(acceptedAt.isPresent());
        assertEquals(new Long(0), acceptedAt.get());

        // try an invalid worker
        acceptedAt = workerRegistryV2.getAcceptedAt(new WorkerId("testGetAcceptedAt-1",10,1));

        assertFalse(acceptedAt.isPresent());

    }

    @Test
    public void testJobCompleteCleanup() {
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        JobId jobId = new JobId("testJobCompleteCleanup", 1);
        initRegistryWithWorkers(workerRegistryV2, "testJobCompleteCleanup-1", 5);

        assertEquals(5, workerRegistryV2.getNumRunningWorkers(null));

        workerRegistryV2.process(new LifecycleEventsProto.JobStatusEvent(INFO, "job shutdown",
                jobId, JobState.Failed));

        assertEquals(0, workerRegistryV2.getNumRunningWorkers(null));

    }

    @Test
    public void testJobScaleUp() throws Exception, InvalidJobException, io.mantisrx.runtime.command.InvalidJobException {

        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new DummyWorkerEventSubscriberImpl(workerRegistryV2));
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerScalableStageWithConstraints(1,
                        new MachineDefinition(1.0,1.0,1.0,3),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
                .build();
        String clusterName = "testJobScaleUp";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);

        ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, eventPublisher);

        assertEquals(2, workerRegistryV2.getNumRunningWorkers(null));



        // send scale up request
        jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1", 1, 2, "", ""), probe.getRef());
        JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
        System.out.println("ScaleupResp " + scaleResp.message);
        assertEquals(SUCCESS, scaleResp.responseCode);
        assertEquals(2,scaleResp.getActualNumWorkers());
        JobTestHelper.sendLaunchedInitiatedStartedEventsToWorker(probe,jobActor,clusterName+"-1",0,new WorkerId(clusterName+"-1",1,3));

        jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("user", new JobId(clusterName,1)),probe.getRef());
        JobClusterManagerProto.GetJobDetailsResponse resp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
        Map<Integer, ? extends IMantisStageMetadata> stageMetadata = resp.getJobMetadata().get().getStageMetadata();
        assertEquals(2, stageMetadata.get(1).getAllWorkers().size());


        int cnt = 0;
        for(int i=0; i<50; i++) {
            cnt++;
            if(workerRegistryV2.getNumRunningWorkers(null) == 3) {
                break;
            }
        }
        assertTrue(cnt < 50);



    }

    @Test
    public void testJobScaleDown() throws Exception {
        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new DummyWorkerEventSubscriberImpl(workerRegistryV2));
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerScalableStageWithConstraints(2,
                        new MachineDefinition(1.0,1.0,1.0,3),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
                .build();
        String clusterName = "testJobScaleDown";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);



        ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, eventPublisher);


        assertEquals(3, workerRegistryV2.getNumRunningWorkers(null));


        // send scale down request
        jobActor.tell(new JobClusterManagerProto.ScaleStageRequest(clusterName+"-1",1, 1, "", ""), probe.getRef());
        JobClusterManagerProto.ScaleStageResponse scaleResp = probe.expectMsgClass(JobClusterManagerProto.ScaleStageResponse.class);
        System.out.println("ScaleDownResp " + scaleResp.message);
        assertEquals(SUCCESS, scaleResp.responseCode);
        assertEquals(1,scaleResp.getActualNumWorkers());


        jobActor.tell(new JobClusterManagerProto.GetJobDetailsRequest("user", new JobId(clusterName,1)),probe.getRef());
        JobClusterManagerProto.GetJobDetailsResponse resp = probe.expectMsgClass(JobClusterManagerProto.GetJobDetailsResponse.class);
        Map<Integer, ? extends IMantisStageMetadata> stageMetadata = resp.getJobMetadata().get().getStageMetadata();
        assertEquals(1, stageMetadata.get(1).getAllWorkers().size());

        int cnt = 0;
        for(int i=0; i<50; i++) {
            cnt++;
            if(workerRegistryV2.getNumRunningWorkers(null) == 2) {
                break;
            }
        }
        assertTrue(cnt < 50);

     //   assertEquals(2, WorkerRegistryV2.INSTANCE.getNumRunningWorkers());



    }

    @Test
    public void testJobShutdown() {

        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new DummyWorkerEventSubscriberImpl(workerRegistryV2));
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerScalableStageWithConstraints(1,
                        new MachineDefinition(1.0,1.0,1.0,3),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
                .build();
        String clusterName = "testJobShutdown";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);


        try {
            ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, eventPublisher);

            assertEquals(2, workerRegistryV2.getNumRunningWorkers(null));


            jobActor.tell(new JobClusterProto.KillJobRequest(
                    new JobId(clusterName,1), "test reason", JobCompletedReason.Normal, "nj", probe.getRef()), probe.getRef());
            probe.expectMsgClass(JobClusterProto.KillJobResponse.class);

            Thread.sleep(1000);
            int cnt = 0;
            for(int i=0; i<100; i++) {
                cnt++;
                if(workerRegistryV2.getNumRunningWorkers(null) == 0) {
                    break;
                }
            }
            assertTrue(cnt < 100);

           // assertEquals(0, WorkerRegistryV2.INSTANCE.getNumRunningWorkers());




        } catch (Exception e) {
            e.printStackTrace();
        }

    }

//    @Test
    public void multiThreadAccessTest() {

        WorkerRegistryV2 workerRegistryV2 = new WorkerRegistryV2();
        CountDownLatch latch = new CountDownLatch(1);
        List<Writer> writerList = generateWriters(workerRegistryV2,4, latch);

        TotalWorkerCountReader reader = new TotalWorkerCountReader(workerRegistryV2, latch);

        ExecutorService fixedThreadPoolExecutor = Executors.newFixedThreadPool(5);
        try {
            Future<Integer> maxCountSeen = fixedThreadPoolExecutor.submit(reader);
            fixedThreadPoolExecutor.invokeAll(writerList);
            int expectedCount = workerRegistryV2.getNumRunningWorkers(null);
            System.out.println("Actual no of workers " + workerRegistryV2.getNumRunningWorkers(null));
            int maxSeenCount = maxCountSeen.get();
            System.out.println("Max Count seen " + maxCountSeen.get());
            assertEquals(expectedCount, maxSeenCount);

        } catch (InterruptedException e) {
            fail();
            e.printStackTrace();
        } catch (ExecutionException
                e) {
            fail();
            e.printStackTrace();
        }


    }

    List<Writer> generateWriters(WorkerEventSubscriber subscriber, int count, CountDownLatch latch) {
        List<Writer> writerList = new ArrayList<>();
        for(int i=0; i<count ;i++) {
            JobId jId = new JobId("multiThreadAccessTest" + i, 1);
            Writer writer1 = new Writer(subscriber, jId, 10, latch);
            writerList.add(writer1);
        }

        return writerList;
    }


    private void initRegistryWithWorkers(WorkerRegistryV2 workerRegistryV2, String jobId, int noOfWorkers) {

        LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new NoOpWorkerEventSubscriberImpl());
        JobId jId = JobId.fromId(jobId).get();
        List<IMantisWorkerMetadata> workerMetadataList = new ArrayList<>();
        for(int i=0; i<noOfWorkers; i++) {

            JobWorker jb = new JobWorker.Builder()
                    .withAcceptedAt(i)
                    .withJobId(jId)
                    .withSlaveID("slaveId-" + i)
                    .withState(WorkerState.Launched)
                    .withWorkerIndex(i)
                    .withWorkerNumber(i+5)
                    .withStageNum(1)
                    .withNumberOfPorts(1 + MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
                    .withLifecycleEventsPublisher(eventPublisher)
                    .build();
            workerMetadataList.add(jb.getMetadata());

        }

        LifecycleEventsProto.WorkerListChangedEvent workerListChangedEvent = new LifecycleEventsProto.WorkerListChangedEvent(new WorkerInfoListHolder(jId, workerMetadataList));
        workerRegistryV2.process(workerListChangedEvent);

    }


    class DummyWorkerEventSubscriberImpl implements WorkerEventSubscriber {

        WorkerEventSubscriber workerRegistry;
        public DummyWorkerEventSubscriberImpl(WorkerEventSubscriber wr) {
            this.workerRegistry = wr;
        }

        @Override
        public void process(LifecycleEventsProto.WorkerListChangedEvent event) {
            workerRegistry.process(event);

        }

        @Override
        public void process(LifecycleEventsProto.JobStatusEvent statusEvent) {
            workerRegistry.process(statusEvent);
        }

        @Override
        public void process(WorkerStatusEvent workerStatusEvent) {
            workerRegistry.process(workerStatusEvent);
        }
    }

    class NoOpWorkerEventSubscriberImpl implements  WorkerEventSubscriber {

        @Override
        public void process(LifecycleEventsProto.WorkerListChangedEvent event) {

        }

        @Override
        public void process(LifecycleEventsProto.JobStatusEvent statusEvent) {

        }

        @Override
        public void process(WorkerStatusEvent workerStatusEvent) {

        }
    }

    class Writer implements Callable<Void> {

        private final int noOfWorkers;
        private final JobId jobId;
        WorkerEventSubscriber subscriber;
        CountDownLatch latch;
        public Writer(WorkerEventSubscriber subscriber, JobId jobId, int totalWorkerCount, CountDownLatch latch) {
            this.subscriber = subscriber;
            this.jobId = jobId;
            this.noOfWorkers = totalWorkerCount;
            this.latch = latch;
        }


        @Override
        public Void call() throws Exception {
            LifecycleEventPublisher eventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new NoOpWorkerEventSubscriberImpl());

            List<IMantisWorkerMetadata> workerMetadataList = new ArrayList<>();
            for(int i=0; i<noOfWorkers; i++) {

                JobWorker jb = new JobWorker.Builder()
                        .withAcceptedAt(i)
                        .withJobId(jobId)
                        .withSlaveID("slaveId-" + i)
                        .withState(WorkerState.Launched)
                        .withWorkerIndex(i)
                        .withWorkerNumber(i+5)
                        .withStageNum(1)
                        .withNumberOfPorts(1 + MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
                        .withLifecycleEventsPublisher(eventPublisher)
                        .build();
                workerMetadataList.add(jb.getMetadata());

            }
            latch.await();
            for(int j =1; j<=noOfWorkers; j++) {
                LifecycleEventsProto.WorkerListChangedEvent workerListChangedEvent = new LifecycleEventsProto.WorkerListChangedEvent(new WorkerInfoListHolder(jobId, workerMetadataList.subList(0, j)));
                subscriber.process(workerListChangedEvent);
            }

//            for(int j =noOfWorkers-1; j>0; j--) {
//                LifecycleEventsProto.WorkerListChangedEvent workerListChangedEvent = new LifecycleEventsProto.WorkerListChangedEvent(new WorkerInfoListHolder(jobId, workerMetadataList.subList(0, j)));
//                subscriber.process(workerListChangedEvent);
//            }

            return null;
        }
    }

    class TotalWorkerCountReader implements Callable<Integer> {

        private final WorkerRegistry registry;

        private final CountDownLatch latch;

        public TotalWorkerCountReader(WorkerRegistryV2 registry, CountDownLatch latch) {
            this.registry = registry;
            this.latch = latch;
        }

        @Override
        public Integer call() throws Exception {
            int max = 0;
            latch.countDown();
            for(int i=0; i<100; i++) {
                int cnt = registry.getNumRunningWorkers(null);
                System.out.println("Total Cnt " + cnt);
                if(cnt > max) {
                    max = cnt;
                }

            }
            return max;
        }
    }


}
