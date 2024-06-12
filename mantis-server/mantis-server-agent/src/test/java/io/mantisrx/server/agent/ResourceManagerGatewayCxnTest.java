/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.server.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mantisrx.common.utils.Services;
import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.agent.utils.DurableBooleanState;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorTaskCancelledException;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.State;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Slf4j
public class ResourceManagerGatewayCxnTest {

    private TaskExecutorRegistration registration;
    private TaskExecutorDisconnection disconnection;
    private TaskExecutorHeartbeat heartbeat;
    private ResourceClusterGateway gateway;
    private ResourceManagerGatewayCxn cxn;
    private TaskExecutorReport report;
    private TaskExecutor taskExecutor;
    private WorkerId workerId;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        WorkerPorts workerPorts = new WorkerPorts(100, 101, 102, 103, 104);
        TaskExecutorID taskExecutorID = TaskExecutorID.of("taskExecutor");
        ClusterID clusterID = ClusterID.of("cluster");
        registration =
            TaskExecutorRegistration.builder()
                .taskExecutorID(taskExecutorID)
                .clusterID(clusterID)
                .hostname("host")
                .taskExecutorAddress("localhost")
                .workerPorts(workerPorts)
                .machineDefinition(new MachineDefinition(1, 1, 1, 1, 5))
                .taskExecutorAttributes(ImmutableMap.of())
                .build();

        disconnection = new TaskExecutorDisconnection(taskExecutorID, clusterID);
        gateway = mock(ResourceClusterGateway.class);
        report = TaskExecutorReport.available();
        heartbeat = new TaskExecutorHeartbeat(taskExecutorID, clusterID, report);
        workerId = new WorkerId("jobId-0", 0, 1);
        taskExecutor = mock(TaskExecutor.class);
        when(taskExecutor.getCurrentReport()).thenReturn(CompletableFuture.completedFuture(report));
        when(taskExecutor.cancelTask(workerId)).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        MantisPropertiesLoader loader = new DefaultMantisPropertiesLoader(System.getProperties());
        LongDynamicProperty intervalDp = new LongDynamicProperty(
            loader,
            "mantis.resourcemanager.connection.interval",
            100L);
        LongDynamicProperty timeoutDp = new LongDynamicProperty(
            loader,
            "mantis.resourcemanager.connection.timeout",
            100L);

        cxn = new ResourceManagerGatewayCxn(0, registration, gateway, intervalDp,
            timeoutDp, taskExecutor, 3, 200,
            1000, 50, 2, 0.5, 3, new DurableBooleanState(tempFolder.newFile().getAbsolutePath()));
    }


    @Test
    public void testIfTaskExecutorRegistersItselfWithResourceManagerAndSendsHeartbeatsPeriodically()
        throws Exception {
        when(gateway.registerTaskExecutor(Matchers.eq(registration))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.disconnectTaskExecutor(Matchers.eq(disconnection))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenReturn(CompletableFuture.completedFuture(null));
        cxn.startAsync().awaitRunning();

        Thread.sleep(1000);
        cxn.stopAsync().awaitTerminated();
        verify(gateway, times(1)).disconnectTaskExecutor(disconnection);
        verify(gateway, atLeastOnce()).heartBeatFromTaskExecutor(heartbeat);
    }

    @Test
    public void testWhenRegistrationFailsIntermittently() throws Throwable {
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(gateway.disconnectTaskExecutor(Matchers.eq(disconnection))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.registerTaskExecutor(Matchers.eq(registration)))
            .thenAnswer(new Answer<CompletableFuture<Void>>() {
                private int count = 0;

                @Override
                public CompletableFuture<Void> answer(InvocationOnMock invocation) {
                    count++;
                    if (count % 2 == 0) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return CompletableFutures.exceptionallyCompletedFuture(
                            new UnknownError("exception"));
                    }
                }
            });
        cxn.startAsync().awaitRunning();

        Thread.sleep(1000);
        assertEquals(cxn.state(), State.RUNNING);
        cxn.stopAsync().awaitTerminated();
    }

    @Test
    public void testWhenRegistrationFailsContinuously() throws Throwable {
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(gateway.disconnectTaskExecutor(Matchers.eq(disconnection))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.registerTaskExecutor(Matchers.eq(registration)))
            .thenAnswer(new Answer<CompletableFuture<Void>>() {
                private int count = 0;

                @Override
                public CompletableFuture<Void> answer(InvocationOnMock invocation) {
                    count++;
                    return CompletableFutures.exceptionallyCompletedFuture(
                        new UnknownError("exception"));
                }
            });
        cxn.startAsync();
        Services.stopAsync(cxn, Executors.newSingleThreadExecutor()).join();
        Assert.assertFalse(cxn.isRegistered());
    }

    @Test
    public void testWhenHeartbeatFailsIntermittently() throws Exception {
        when(gateway.registerTaskExecutor(Matchers.eq(registration))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenAnswer(new Answer<CompletableFuture<Void>>() {
                private int count = 0;

                @Override
                public CompletableFuture<Void> answer(InvocationOnMock invocation) {
                    count++;
                    if (count % 2 == 0) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return CompletableFutures.exceptionallyCompletedFuture(
                            new Exception("error"));
                    }
                }
            });
        cxn.startAsync();
        Thread.sleep(1000);
        assertEquals(cxn.state(), State.RUNNING);
        assertTrue(cxn.isRegistered());
    }

    @Test
    public void testWhenHeartbeatFailsWithTaskCancelled() throws Exception {
        when(gateway.registerTaskExecutor(Matchers.eq(registration))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocation ->
                CompletableFutures.exceptionallyCompletedFuture(
                    new TaskExecutorTaskCancelledException("mock error", workerId)));
        cxn.startAsync();
        Thread.sleep(1000);

        verify(taskExecutor, atLeastOnce()).cancelTask(workerId);
        assertEquals(cxn.state(), State.RUNNING);
        assertTrue(cxn.isRegistered());
    }

    @Test
    public void testWhenHeartbeatFailsContinuously() throws Exception {
        when(gateway.registerTaskExecutor(Matchers.eq(registration))).thenReturn(
            CompletableFuture.completedFuture(null));
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch isNotRegisteredSignal = new CountDownLatch(5);
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenAnswer(new Answer<CompletableFuture<Void>>() {
                private int count = 0;

                @Override
                public CompletableFuture<Void> answer(InvocationOnMock invocation) {
                    count++;
                    startSignal.countDown();
                    isNotRegisteredSignal.countDown();
                    log.info("isNotRegistered {}", isNotRegisteredSignal.getCount());
                    log.info("count: {}", count);

                    return CompletableFutures.exceptionallyCompletedFuture(
                        new UnknownError("error"));
                }
            });
        when(gateway.disconnectTaskExecutor(Matchers.eq(disconnection))).thenReturn(
            CompletableFuture.completedFuture(null));
        cxn.startAsync();
        // wait for the heart beat failure
        startSignal.await();
        assertTrue(cxn.isRegistered());
        // wait for the third heartbeat failure
        isNotRegisteredSignal.await();
        assertFalse(cxn.isRegistered());
        Services.stopAsync(cxn, Executors.newSingleThreadExecutor()).join();
        verify(gateway, times(1)).disconnectTaskExecutor(disconnection);
        Assert.assertFalse(cxn.isRegistered());
    }
}
