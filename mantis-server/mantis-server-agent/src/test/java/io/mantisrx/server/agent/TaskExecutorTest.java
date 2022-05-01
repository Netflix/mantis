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


import com.google.common.collect.ImmutableList;
import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.source.http.HttpServerProvider;
import io.mantisrx.runtime.source.http.HttpSources;
import io.mantisrx.runtime.source.http.impl.HttpClientFactories;
import io.mantisrx.runtime.source.http.impl.HttpRequestFactories;
import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.client.ClassLoaderHandle;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.ResourceLeaderConnection;
import io.mantisrx.server.master.client.SinkSubscriptionStateHandler;
import io.mantisrx.server.master.client.config.WorkerConfiguration;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.RpcService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import rx.Observable;
import rx.Subscription;

@Slf4j
public class TaskExecutorTest {

    private WorkerConfiguration workerConfiguration;
    private RpcService rpcService;
    private MantisMasterGateway masterMonitor;
    private HighAvailabilityServices highAvailabilityServices;
    private ClassLoaderHandle classLoaderHandle;
    private TaskExecutor taskExecutor;
    private CountDownLatch startedSignal;
    private CountDownLatch doneSignal;
    private CountDownLatch terminatedSignal;
    private Status finalStatus;
    private ResourceClusterGateway resourceManagerGateway;
    private SimpleResourceLeaderConnection<ResourceClusterGateway> resourceManagerGatewayCxn;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() {
        final Properties props = new Properties();
        props.setProperty("mantis.zookeeper.root", "");

        props.setProperty("mantis.taskexecutor.cluster.storage-dir", "");
        props.setProperty("mantis.taskexecutor.local.storage-dir", "");
        props.setProperty("mantis.taskexecutor.cluster-id", "default");
        props.setProperty("mantis.taskexecutor.heartbeats.interval", "100");

        startedSignal = new CountDownLatch(1);
        doneSignal = new CountDownLatch(1);
        terminatedSignal = new CountDownLatch(1);

        workerConfiguration = Configurations.frmProperties(props, WorkerConfiguration.class);
        rpcService = new TestingRpcService();

        masterMonitor = Mockito.mock(MantisMasterGateway.class);
        classLoaderHandle = ClassLoaderHandle.fixed(getClass().getClassLoader());
        resourceManagerGateway = getHealthyGateway("gateway 1");
        resourceManagerGatewayCxn = new SimpleResourceLeaderConnection<>(resourceManagerGateway);
        highAvailabilityServices = Mockito.mock(HighAvailabilityServices.class);
        Mockito.when(highAvailabilityServices.getMasterClientApi()).thenReturn(masterMonitor);
        Mockito.when(highAvailabilityServices.connectWithResourceManager(Matchers.any())).thenReturn(resourceManagerGatewayCxn);
    }

    private void start() throws Exception {
        Consumer<Status> updateTaskExecutionStatusFunction = status -> {
            log.info("Task Status = {}", status.getState());
            if (status.getState() == MantisJobState.Started) {
                startedSignal.countDown();
            }

            if (status.getState().isTerminalState()) {
                finalStatus = status;
                terminatedSignal.countDown();
            }
        };

        taskExecutor =
            new TestingTaskExecutor(
                rpcService,
                workerConfiguration,
                highAvailabilityServices,
                classLoaderHandle,
                executeStageRequest -> SinkSubscriptionStateHandler.noop(),
                updateTaskExecutionStatusFunction);
        taskExecutor.start();
        taskExecutor.awaitRunning().get(2, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() throws Exception {
        taskExecutor.close();
    }

    @Ignore
    @Test
    public void testTaskExecutorEndToEndWithASingleStageJobByLoadingFromClassLoader()
        throws Exception {
        start();

        List<Integer> ports = ImmutableList.of(100);
        double threshold = 5000.0;
        WorkerHost host = new WorkerHost("host0", 0, ports, MantisJobState.Started, 1, 8080, 8081);
        Map<Integer, WorkerAssignments> stageAssignmentMap =
            ImmutableMap.<Integer, WorkerAssignments>builder()
                .put(1, new WorkerAssignments(1, 1,
                    ImmutableMap.<Integer, WorkerHost>builder().put(0, host).build()))
                .build();
        Mockito.when(masterMonitor.schedulingChanges("jobId-0")).thenReturn(
            Observable.just(new JobSchedulingInfo("jobId-0", stageAssignmentMap)));

        WorkerId workerId = new WorkerId("jobId-0", 0, 1);
        CompletableFuture<Ack> wait = taskExecutor.callInMainThread(() -> taskExecutor.submitTask(
            new ExecuteStageRequest("jobName", "jobId-0", 0, 1,
                new URL("https://www.google.com/"),
                1, 1,
                ports, 100L, 1, ImmutableList.of(),
                new SchedulingInfo.Builder().numberOfStages(1)
                    .singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2),
                        Lists.newArrayList(), Lists.newArrayList()).build(),
                MantisJobDurationType.Transient,
                1000L,
                1L,
                new WorkerPorts(2, 3, 4, 5, 6),
                Optional.of(SineFunctionJobProvider.class.getName()))), Time.seconds(1));
        wait.get();
        Assert.assertTrue(startedSignal.await(5, TimeUnit.SECONDS));
        Subscription subscription = HttpSources.source(HttpClientFactories.sseClientFactory(),
                HttpRequestFactories.createGetFactory("/"))
            .withServerProvider(new HttpServerProvider() {
                @Override
                public Observable<ServerInfo> getServersToAdd() {
                    return Observable.just(new ServerInfo("localhost", ports.get(0)));
                }

                @Override
                public Observable<ServerInfo> getServersToRemove() {
                    return Observable.empty();
                }
            })
            .build()
            .call(null, null)
            .flatMap(obs -> obs)
            .flatMap(sse -> {
                try {
                    return Observable.just(objectMapper.readValue(sse.contentAsString(), Point.class));
                } catch (Exception e) {
                    log.error("failed to deserialize", e);
                    return Observable.error(e);
                }
            })
            .takeUntil(point -> point.getX() > threshold)
            .subscribe(point -> log.info("point={}", point), error -> log.error("failed", error),
                () -> doneSignal.countDown());
        Assert.assertTrue(doneSignal.await(10, TimeUnit.SECONDS));
        subscription.unsubscribe();
        Mockito.verify(resourceManagerGateway, Mockito.times(1)).notifyTaskExecutorStatusChange(
            new TaskExecutorStatusChange(taskExecutor.getTaskExecutorID(), taskExecutor.getClusterID(),
                TaskExecutorReport.occupied(workerId)));

        CompletableFuture<Ack> cancelFuture =
            taskExecutor.callInMainThread(() -> taskExecutor.cancelTask(workerId), Time.seconds(1));
        cancelFuture.get();

        Thread.sleep(5000);
        Mockito.verify(resourceManagerGateway, Mockito.times(1)).notifyTaskExecutorStatusChange(
            new TaskExecutorStatusChange(taskExecutor.getTaskExecutorID(), taskExecutor.getClusterID(),
                TaskExecutorReport.available()));
    }

    @Test
    public void testWhenSuccessiveHeartbeatsFail() throws Exception {
        ResourceClusterGateway resourceManagerGateway = Mockito.mock(ResourceClusterGateway.class);
        Mockito.when(resourceManagerGateway.registerTaskExecutor(Matchers.any())).thenReturn(
            CompletableFuture.completedFuture(null));
        Mockito.when(resourceManagerGateway.heartBeatFromTaskExecutor(Matchers.any()))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error1")))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error2")))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error3")))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error4")))
            .thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(resourceManagerGateway.disconnectTaskExecutor(Matchers.any())).thenReturn(
            CompletableFuture.completedFuture(null));
        resourceManagerGatewayCxn.newLeaderIs(resourceManagerGateway);

        start();
        Thread.sleep(1000);
        Mockito.verify(resourceManagerGateway, Mockito.times(2)).registerTaskExecutor(Matchers.any());
        Assert.assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
    }

    @Test
    public void testWhenResourceManagerLeaderChanges() throws Exception {
        start();

        // wait for a second
        Thread.sleep(1000);

        // change the leader
        ResourceClusterGateway newResourceClusterGateway = getHealthyGateway("gateway 2");
        resourceManagerGatewayCxn.newLeaderIs(newResourceClusterGateway);

        // wait for a second for new connections
        Thread.sleep(1000);

        // check if the switch has been made
        Mockito.verify(resourceManagerGateway, Mockito.times(1)).registerTaskExecutor(Matchers.any());
        Mockito.verify(resourceManagerGateway, Mockito.times(1)).disconnectTaskExecutor(Matchers.any());
        Mockito.verify(resourceManagerGateway, Mockito.atLeastOnce()).heartBeatFromTaskExecutor(Matchers.any());

        Mockito.verify(newResourceClusterGateway, Mockito.times(1)).registerTaskExecutor(Matchers.any());
        Mockito.verify(newResourceClusterGateway, Mockito.atLeastOnce()).heartBeatFromTaskExecutor(Matchers.any());

        // check if the task executor is registered
        Assert.assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
    }

    @Test
    public void testWhenReregistrationFails() throws Exception {
        start();

        // wait for a second
        Thread.sleep(1000);

        // change the leader
        ResourceClusterGateway newResourceManagerGateway1 = getUnhealthyGateway("gateway 2");
        resourceManagerGatewayCxn.newLeaderIs(newResourceManagerGateway1);

        // wait for a second for new connections
        Thread.sleep(1000);

        // check if the switch has been made
        Mockito.verify(resourceManagerGateway, Mockito.times(1)).registerTaskExecutor(Matchers.any());
        Mockito.verify(resourceManagerGateway, Mockito.times(1)).disconnectTaskExecutor(Matchers.any());
        Mockito.verify(resourceManagerGateway, Mockito.atLeastOnce()).heartBeatFromTaskExecutor(Matchers.any());

        Mockito.verify(newResourceManagerGateway1, Mockito.atLeastOnce()).registerTaskExecutor(Matchers.any());
        Mockito.verify(newResourceManagerGateway1, Mockito.atLeastOnce()).disconnectTaskExecutor(Matchers.any());
        Mockito.verify(newResourceManagerGateway1, Mockito.never()).heartBeatFromTaskExecutor(Matchers.any());

        ResourceClusterGateway newResourceManagerGateway2 = getHealthyGateway("gateway 3");
        resourceManagerGatewayCxn.newLeaderIs(newResourceManagerGateway2);
        Thread.sleep(1000);

        Mockito.verify(newResourceManagerGateway2, Mockito.times(1)).registerTaskExecutor(Matchers.any());
        Mockito.verify(newResourceManagerGateway2, Mockito.never()).disconnectTaskExecutor(Matchers.any());
        Mockito.verify(newResourceManagerGateway2, Mockito.atLeastOnce()).heartBeatFromTaskExecutor(Matchers.any());

        // check if the task executor is registered
        Assert.assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
    }

    private static ResourceClusterGateway getHealthyGateway(String name) {
        ResourceClusterGateway gateway = Mockito.mock(ResourceClusterGateway.class);
        Mockito.when(gateway.registerTaskExecutor(Matchers.any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        Mockito.when(gateway.heartBeatFromTaskExecutor(Matchers.any())).thenReturn(
            CompletableFuture.completedFuture(Ack.getInstance()));
        Mockito.when(gateway.notifyTaskExecutorStatusChange(Matchers.any()))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        Mockito.when(gateway.disconnectTaskExecutor(Matchers.any()))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        Mockito.when(gateway.toString()).thenReturn(name);
        return gateway;
    }

    private static ResourceClusterGateway getUnhealthyGateway(String name) {
        ResourceClusterGateway gateway = Mockito.mock(ResourceClusterGateway.class);
        Mockito.when(gateway.registerTaskExecutor(Matchers.any())).thenReturn(
            CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error")));
        Mockito.when(gateway.disconnectTaskExecutor(Matchers.any())).thenReturn(
            CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error")));
        Mockito.when(gateway.toString()).thenReturn(name);
        return gateway;
    }

    public static class SimpleResourceLeaderConnection<ResourceT> implements
        ResourceLeaderConnection<ResourceT> {

        private final AtomicReference<ResourceT> current;
        private final AtomicReference<ResourceLeaderChangeListener<ResourceT>> listener = new AtomicReference<>();

        public SimpleResourceLeaderConnection(ResourceT initial) {
            this.current = new AtomicReference<>(initial);
        }

        @Override
        public ResourceT getCurrent() {
            return current.get();
        }

        @Override
        public void register(ResourceLeaderChangeListener<ResourceT> changeListener) {
            Preconditions.checkArgument(listener.compareAndSet(null, changeListener),
                "changeListener already set");
        }

        public void newLeaderIs(ResourceT newLeader) {
            ResourceT old = current.getAndSet(newLeader);
            if (listener.get() != null) {
                listener.get().onResourceLeaderChanged(old, newLeader);
            }
        }
    }

    private static class TestingTaskExecutor extends TaskExecutor {

        private final Consumer<Status> consumer;

        public TestingTaskExecutor(RpcService rpcService,
                                   WorkerConfiguration workerConfiguration,
                                   HighAvailabilityServices highAvailabilityServices,
                                   ClassLoaderHandle classLoaderHandle,
                                   SinkSubscriptionStateHandler.Factory subscriptionStateHandlerFactory,
                                   Consumer<Status> consumer) {
            super(rpcService, workerConfiguration, highAvailabilityServices, classLoaderHandle,
                subscriptionStateHandlerFactory);
            this.consumer = consumer;
        }

        @Override
        protected void updateExecutionStatus(Status status) {
            consumer.accept(status);
        }
    }
}
