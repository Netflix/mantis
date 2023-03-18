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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mantisrx.common.utils.Services;
import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.agent.TaskExecutor.ResourceManagerGatewayCxn;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service.State;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ResourceManagerGatewayCxnTest {

    private TaskExecutorRegistration registration;
    private TaskExecutorDisconnection disconnection;
    private TaskExecutorHeartbeat heartbeat;
    private ResourceClusterGateway gateway;
    private ResourceManagerGatewayCxn cxn;
    private TaskExecutorReport report;

    @BeforeEach
    public void setup() {
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
        cxn = new ResourceManagerGatewayCxn(0, registration, gateway, Time.milliseconds(10),
            Time.milliseconds(100), dontCare -> CompletableFuture.completedFuture(report), 3);
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
    public void testWhenRegistrationFails() throws Throwable {
        assertThrows(UnknownError.class, () -> {
            when(gateway.registerTaskExecutor(Matchers.eq(registration))).thenReturn(
                CompletableFutures.exceptionallyCompletedFuture(new UnknownError("exception")));
            cxn.startAsync();
            CompletableFuture<Void> result = Services.stopAsync(cxn, Executors.newSingleThreadExecutor());
            try {
                result.get();
            } catch (Exception e) {
                throw stripExecutionException(e);
            }
        });
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
                        return CompletableFutures.exceptionallyCompletedFuture(new Exception("error"));
                    }
                }
            });
        cxn.startAsync();
        Thread.sleep(1000);
        assertEquals(cxn.state(), State.RUNNING);
    }

    @Test
    public void testWhenHeartbeatFailsContinuously() {
        when(gateway.registerTaskExecutor(Matchers.eq(registration))).thenReturn(
            CompletableFuture.completedFuture(null));
        when(gateway.heartBeatFromTaskExecutor(Matchers.eq(heartbeat)))
            .thenAnswer(new Answer<CompletableFuture<Void>>() {
                private int count = 0;

                @Override
                public CompletableFuture<Void> answer(InvocationOnMock invocation) {
                    count++;
                    if (count < 5) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error"));
                    }
                }
            });
        when(gateway.disconnectTaskExecutor(Matchers.eq(disconnection))).thenReturn(CompletableFuture.completedFuture(null));
        cxn.startAsync();
        CompletableFuture<Void> result = Services.awaitAsync(cxn, Executors.newSingleThreadExecutor());
        Throwable throwable = null;
        try {
            result.get();
        } catch (Exception e) {
            throwable = stripExecutionException(e);
        }

        assertEquals(UnknownError.class, throwable.getClass());
        verify(gateway, times(1)).disconnectTaskExecutor(disconnection);
    }
}
