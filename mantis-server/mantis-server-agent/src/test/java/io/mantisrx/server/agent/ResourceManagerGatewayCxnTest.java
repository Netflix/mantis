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

import static org.apache.flink.util.ExceptionUtils.stripExecutionException;
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
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.agent.utils.DurableBooleanState;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.State;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.time.Time;
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
        TaskExecutor taskExecutor = mock(TaskExecutor.class);
        when(taskExecutor.getCurrentReport()).thenReturn(CompletableFuture.completedFuture(report));
        cxn = new ResourceManagerGatewayCxn(0, registration, gateway, Time.milliseconds(100),
            Time.milliseconds(100), taskExecutor, 3, 200,
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

    @Test(expected = RuntimeException.class)
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
                    if (count >= 5) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return CompletableFutures.exceptionallyCompletedFuture(
                            new UnknownError("exception"));
                    }
                }
            });
        cxn.startAsync();
        CompletableFuture<Void> result = Services.awaitAsync(cxn,
            Executors.newSingleThreadExecutor());
        try {
            result.get();
        } catch (Exception e) {
            throw stripExecutionException(e);
        }
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
