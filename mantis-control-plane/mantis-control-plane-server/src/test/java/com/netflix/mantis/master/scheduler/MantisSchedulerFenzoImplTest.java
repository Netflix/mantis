//package com.netflix.mantis.master.scheduler;
//
//import com.netflix.fenzo.VirtualMachineLease;
//import com.netflix.mantis.master.JobMessageRouter;
//import com.netflix.mantis.master.WorkerRegistry;
//import com.netflix.mantis.master.jobcluster.job.worker.events.WorkerLaunchFailed;
//import com.netflix.mantis.master.jobcluster.job.worker.events.WorkerLaunched;
//import com.netflix.mantis.master.jobcluster.job.worker.events.WorkerUnscheduleable;
//import com.netflix.mantis.master.resourcemgmt.VMResourceManager;
//
//
//import io.mantisrx.runtime.MachineDefinition;
//import io.mantisrx.server.master.AgentClustersAutoScaler;
//import io.mantisrx.server.master.LaunchTaskException;
//import io.mantisrx.server.master.config.ConfigurationProvider;
//import io.mantisrx.server.master.domain.JobId;
//import io.mantisrx.server.master.domain.WorkerId;
//import io.mantisrx.server.master.domain.WorkerPorts;
//import io.mantisrx.server.master.mesos.VirtualMachineLeaseMesosImpl;
//import org.junit.Test;
//
//import java.net.MalformedURLException;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.Optional;
//import java.util.function.Consumer;
//
//import static org.mockito.Mockito.*;
//
//public class MantisSchedulerFenzoImplTest {
//    final VMResourceManager vmResourceManagerMock = mock(VMResourceManager.class);
//    final JobMessageRouter jobMessageRouterMock = mock(JobMessageRouter.class);
//    final WorkerRegistry workerRegistryMock = mock(WorkerRegistry.class);
//    final AgentClustersAutoScaler agentClustersAutoScalerMock = mock(AgentClustersAutoScaler.class);
//
//    public void runTestCase(final MantisSchedulerFenzoImpl mantisSchedulerFenzo,
//                            final Consumer<MantisSchedulerFenzoImpl> consumer) {
//        consumer.accept(mantisSchedulerFenzo);
//        mantisSchedulerFenzo.shutdown();
//    }
//
//    @Test
//    public void testWorkerLaunchSuccess() throws MalformedURLException {
//        final JobId jobId = JobId.fromId("TestJobCluster-1").get();
//        final WorkerId workerId = new WorkerId(jobId, 1, 2);
//        final String fakeHostname = "127.0.0.1";
//        final String fakeVMId = "VM_ID";
//
//        VirtualMachineLeaseMesosImpl leaseMock = TestHelpers.createMockLease("lease_id", fakeHostname, fakeVMId, 4.0,
//                12000, 1024, 1024, new VirtualMachineLease.Range(15000, 15010));
//
////        jobLocatorMock.locateJob(jobId);
////        when(jobLocatorMock.locateJob(jobId)).thenReturn(jobManagerMock);
//
//        when(workerRegistryMock.getAcceptedAt(workerId)).thenReturn(Optional.empty());
//        final MantisSchedulerFenzoImpl schedulerFenzo = TestHelpers.createMantisScheduler(vmResourceManagerMock, jobMessageRouterMock, workerRegistryMock, agentClustersAutoScalerMock);
//
//        runTestCase(schedulerFenzo, mantisScheduler -> {
//
//            mantisScheduler.addOffers(Arrays.asList(leaseMock));
//            ScheduleRequest fakeScheduleRequest = TestHelpers.createFakeScheduleRequest(workerId, 0, 1, new MachineDefinition(2, 1024, 128, 1024, 4));
//            mantisScheduler.scheduleWorker(fakeScheduleRequest);
//            WorkerPorts expectedAssignedPorts = new WorkerPorts(Arrays.asList(15000, 15001, 15002));
//            WorkerLaunched expectedLaunchedEvent = new WorkerLaunched(workerId, fakeHostname, fakeVMId, expectedAssignedPorts);
//            verify(jobMessageRouterMock, timeout(1_000).times(1)).routeWorkerEvent(expectedLaunchedEvent);
//            verifyNoMoreInteractions(jobMessageRouterMock);
//        });
//    }
//
//
//
//    @Test
//    public void testWorkerLaunchFailed() throws MalformedURLException {
//        final JobId jobId = JobId.fromId("TestJobCluster-1").get();
//        final WorkerId workerId = new WorkerId(jobId, 1, 2);
//        final String fakeHostname = "127.0.0.1";
//        final String fakeVMId = "VM_ID";
//        WorkerPorts workerPorts = new WorkerPorts(Arrays.asList(15000, 15001, 15002));
//
//        VirtualMachineLeaseMesosImpl leaseMock = TestHelpers.createMockLease("lease_id", fakeHostname, fakeVMId, 4.0,
//                12000, 1024, 1024, new VirtualMachineLease.Range(15000, 15010));
//
//       // when(jobLocatorMock.locateJob(jobId)).thenReturn(jobManagerMock);
//        when(workerRegistryMock.getAcceptedAt(workerId)).thenReturn(Optional.empty());
//
//        ScheduleRequest fakeScheduleRequest = TestHelpers.createFakeScheduleRequest(workerId, 0, 1, new MachineDefinition(2, 1024, 128, 1024, 4));
//
//        // Simulate Mesos launch failure, should trigger a WorkerLaunched event followed by a WorkerLaunchFailed event
//        when(vmResourceManagerMock.launchTasks(Arrays.asList(new LaunchTaskRequest(fakeScheduleRequest, workerPorts)), Arrays.asList(leaseMock)))
//                .thenReturn(Collections.singletonMap(fakeScheduleRequest, new LaunchTaskException("fake exception", new IllegalStateException())));
//
//        final MantisSchedulerFenzoImpl schedulerFenzo = TestHelpers.createMantisScheduler(vmResourceManagerMock, jobMessageRouterMock, workerRegistryMock, agentClustersAutoScalerMock);
//
//        runTestCase(schedulerFenzo, mantisScheduler -> {
//
//            mantisScheduler.addOffers(Arrays.asList(leaseMock));
//            mantisScheduler.scheduleWorker(fakeScheduleRequest);
//            WorkerPorts expectedAssignedPorts = new WorkerPorts(Arrays.asList(15000, 15001, 15002));
//            WorkerLaunched expectedLaunchedEvent = new WorkerLaunched(workerId, fakeHostname, fakeVMId, expectedAssignedPorts);
//            WorkerLaunchFailed expectedLaunchFailedEvent = new WorkerLaunchFailed(workerId, String.format("%s failed due to fake exception", workerId.toString()));
//            verify(jobMessageRouterMock, timeout(1_000).times(1)).routeWorkerEvent(expectedLaunchedEvent);
//            verify(jobMessageRouterMock, timeout(1_000).times(1)).routeWorkerEvent(expectedLaunchFailedEvent);
//        });
//    }
//
//
//    @Test
//    public void testWorkerUnscheduleable() throws MalformedURLException {
//        final JobId jobId = JobId.fromId("TestJobCluster-1").get();
//        final WorkerId workerId = new WorkerId(jobId, 1, 2);
//        final String fakeHostname = "127.0.0.1";
//        final String fakeVMId = "VM_ID";
//        final int requestedMemoryMB = 1024;
//        final int memoryFromResourceOffer = requestedMemoryMB / 2;
//
//        VirtualMachineLeaseMesosImpl leaseMock = TestHelpers.createMockLease("lease_id", fakeHostname, fakeVMId, 4.0,
//                memoryFromResourceOffer, 1024, 1024, new VirtualMachineLease.Range(15000, 15010));
//       // JobManager jobManagerMock = mock(JobManager.class);
//        //when(jobLocatorMock.locateJob(jobId)).thenReturn(jobManagerMock);
//        when(workerRegistryMock.getAcceptedAt(workerId)).thenReturn(Optional.empty());
//
//        final MantisSchedulerFenzoImpl schedulerFenzo = TestHelpers.createMantisScheduler(vmResourceManagerMock, jobMessageRouterMock, workerRegistryMock, agentClustersAutoScalerMock);
//
//        runTestCase(schedulerFenzo, mantisScheduler -> {
//            mantisScheduler.addOffers(Arrays.asList(leaseMock));
//            ScheduleRequest fakeScheduleRequest = TestHelpers.createFakeScheduleRequest(workerId, 0, 1, new MachineDefinition(2, requestedMemoryMB, 128, 1024, 4));
//            mantisScheduler.scheduleWorker(fakeScheduleRequest);
//            WorkerUnscheduleable expectedWorkerEvent = new WorkerUnscheduleable(workerId);
//            verify(jobMessageRouterMock, timeout(1_000).times(1)).routeWorkerEvent(expectedWorkerEvent);
//            verifyNoMoreInteractions(jobMessageRouterMock);
//        });
//    }
//
//    @Test
//    public void testLeaseRejectedAfterOfferExpiry() {
//        final String fakeHostname = "127.0.0.1";
//        final String fakeVMId = "VM_ID";
//
//        VirtualMachineLeaseMesosImpl leaseMock = TestHelpers.createMockLease("lease_id", fakeHostname, fakeVMId, 4.0,
//                12000, 1024, 1024, new VirtualMachineLease.Range(15000, 15010));
//
//        final MantisSchedulerFenzoImpl schedulerFenzo = TestHelpers.createMantisScheduler(vmResourceManagerMock, jobMessageRouterMock, workerRegistryMock, agentClustersAutoScalerMock);
//
//        runTestCase(schedulerFenzo, mantisScheduler -> {
//
//            mantisScheduler.addOffers(Arrays.asList(leaseMock));
//            try {
//                Thread.sleep(ConfigurationProvider.getConfig().getMesosLeaseOfferExpirySecs()*1000 + 50);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            verifyZeroInteractions(jobMessageRouterMock);
//            verifyZeroInteractions(workerRegistryMock);
//            verify(vmResourceManagerMock, timeout(10_000).times(1)).rejectLease(leaseMock);
//            verifyNoMoreInteractions(vmResourceManagerMock);
//        });
//    }
//}
