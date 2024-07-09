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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.futures.CompletableFutures;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.mantisrx.common.Ack;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.loader.ClassLoaderHandle;
import io.mantisrx.runtime.loader.RuntimeTask;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.runtime.source.http.HttpServerProvider;
import io.mantisrx.runtime.source.http.HttpSources;
import io.mantisrx.runtime.source.http.impl.HttpClientFactories;
import io.mantisrx.runtime.source.http.impl.HttpRequestFactories;
import io.mantisrx.server.agent.TaskExecutor.Listener;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.PostJobStatusRequest;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.ResourceLeaderConnection;
import io.mantisrx.server.master.resourcecluster.RequestThrottledException;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorStatusChange;
import io.mantisrx.server.worker.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.RpcService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import rx.Observable;
import rx.Subscription;

@Slf4j
public class RuntimeTaskImplExecutorTest {

    private WorkerConfiguration workerConfiguration;
    private RpcService rpcService;
    private MantisMasterGateway masterClientApi;
    private HighAvailabilityServices highAvailabilityServices;
    private HttpServer localApiServer;
    private ClassLoaderHandle classLoaderHandle;
    private TaskExecutor taskExecutor;
    private CountDownLatch startedSignal;
    private CountDownLatch doneSignal;
    private CountDownLatch terminatedSignal;
    private Status finalStatus;
    private ResourceClusterGateway resourceManagerGateway;
    private SimpleResourceLeaderConnection<ResourceClusterGateway> resourceManagerGatewayCxn;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private CollectingTaskLifecycleListener listener;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        final Properties props = new Properties();
        props.setProperty("mantis.zookeeper.root", "");

        props.setProperty("mantis.taskexecutor.cluster.storage-dir", "");
        props.setProperty("mantis.taskexecutor.registration.store", tempFolder.newFolder().getAbsolutePath());
        props.setProperty("mantis.taskexecutor.cluster-id", "default");
        props.setProperty("mantis.taskexecutor.heartbeats.interval", "100");
        props.setProperty("mantis.taskexecutor.metrics.collector", "io.mantisrx.server.agent.DummyMetricsCollector");
        props.setProperty("mantis.taskexecutor.registration.retry.initial-delay.ms", "10");
        props.setProperty("mantis.taskexecutor.registration.retry.mutliplier", "1");
        props.setProperty("mantis.taskexecutor.registration.retry.randomization-factor", "0.5");
        props.setProperty("mantis.taskexecutor.heartbeats.retry.initial-delay.ms", "100");
        props.setProperty("mantis.taskexecutor.heartbeats.retry.max-delay.ms", "500");

        props.setProperty("mantis.localmode", "true");
        props.setProperty("mantis.zookeeper.connectString", "localhost:8100");

        props.setProperty("mantis.taskexecutor.hardware.cpu-cores", "1.0");
        props.setProperty("mantis.taskexecutor.hardware.memory-in-mb", "4096.0");
        props.setProperty("mantis.taskexecutor.hardware.disk-in-mb", "10000.0");
        props.setProperty("mantis.taskexecutor.hardware.network-bandwidth-in-mb", "1000.0");

        startedSignal = new CountDownLatch(1);
        doneSignal = new CountDownLatch(1);
        terminatedSignal = new CountDownLatch(1);

        workerConfiguration = new StaticPropertiesConfigurationFactory(props).getConfig();
        rpcService = new TestingRpcService();

        masterClientApi = mock(MantisMasterGateway.class);
        classLoaderHandle = ClassLoaderHandle.fixed(getClass().getClassLoader());
        resourceManagerGateway = getHealthyGateway("gateway1");
        resourceManagerGatewayCxn = new SimpleResourceLeaderConnection<>(resourceManagerGateway);

        // worker and task executor do not share the same HA instance.
        highAvailabilityServices = mock(HighAvailabilityServices.class);
        when(highAvailabilityServices.getMasterClientApi()).thenReturn(masterClientApi);
        when(highAvailabilityServices.connectWithResourceManager(any())).thenReturn(resourceManagerGatewayCxn);
        when(highAvailabilityServices.startAsync()).thenReturn(highAvailabilityServices);

        setupLocalControl(8100);
    }

    private void start() throws Exception {

        listener = new CollectingTaskLifecycleListener();
        taskExecutor =
            new TestingTaskExecutor(
                rpcService,
                workerConfiguration,
                highAvailabilityServices,
                classLoaderHandle
            );
        taskExecutor.addListener(listener, MoreExecutors.directExecutor());
        taskExecutor.start();
        taskExecutor.awaitRunning().get(2, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() throws Exception {
        taskExecutor.close();
        this.localApiServer.stop(0);
    }

    @Ignore // todo: this test is failing on CI.
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
        when(masterClientApi.schedulingChanges("jobId-0")).thenReturn(
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
                    0,
                1000L,
                1L,
                new WorkerPorts(2, 3, 4, 5, 6),
                Optional.of(SineFunctionJobProvider.class.getName()),
                "user",
                "111")), Time.seconds(1));
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
        verify(resourceManagerGateway, times(1)).notifyTaskExecutorStatusChange(
            new TaskExecutorStatusChange(taskExecutor.getTaskExecutorID(), taskExecutor.getClusterID(),
                TaskExecutorReport.occupied(workerId)));

        CompletableFuture<Ack> cancelFuture =
            taskExecutor.callInMainThread(() -> taskExecutor.cancelTask(workerId), Time.seconds(1));
        cancelFuture.get();

        Thread.sleep(5000);
        verify(resourceManagerGateway, times(1)).notifyTaskExecutorStatusChange(
            new TaskExecutorStatusChange(taskExecutor.getTaskExecutorID(), taskExecutor.getClusterID(),
                TaskExecutorReport.available()));
        assertTrue(listener.isStartingCalled());
        assertTrue(listener.isCancellingCalled());
        assertTrue(listener.isCancelledCalled());
        assertFalse(listener.isFailedCalled());
    }

    private void setupLocalControl(int port) throws IOException {
        this.localApiServer = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        this.localApiServer.createContext(
            "/",
            new HttpHandler() {
                private JsonSerializer jsonSerializer = new JsonSerializer();

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        log.warn("unexpect get request: {}", exchange.getRequestURI());
                    } else if ("POST".equals(exchange.getRequestMethod())) {
                        InputStreamReader isr = new InputStreamReader(exchange.getRequestBody());
                        BufferedReader br = new BufferedReader(isr);
                        String value = br.readLine();
                        log.info("post body: {}", value);
                        PostJobStatusRequest statusReq =
                            jsonSerializer.fromJSON(value, PostJobStatusRequest.class);
                        log.info("Job signal: {}", statusReq.getStatus());
                        if (statusReq.getStatus().getState() == MantisJobState.Started) {
                            log.info("Job start signal received");
                            startedSignal.countDown();
                        }

                        if (statusReq.getStatus().getState().isTerminalState()) {
                            log.info("Job terminate signal received");
                            terminatedSignal.countDown();
                        }
                    }

                    OutputStream outputStream = exchange.getResponseBody();
                    String payload = "";
                    exchange.sendResponseHeaders(200, payload.length());
                    outputStream.write(payload.getBytes());
                    outputStream.flush();
                    outputStream.close();
                }
            });

        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
        this.localApiServer.setExecutor(threadPoolExecutor);
        this.localApiServer.start();
        log.info("Local API Server started on port: {}", port);
    }

    @Test
    public void testWhenSuccessiveHeartbeatsFail() throws Exception {
        ResourceClusterGateway resourceManagerGateway = mock(ResourceClusterGateway.class, "gateway2");
        when(resourceManagerGateway.registerTaskExecutor(any())).thenReturn(
            CompletableFuture.completedFuture(null));
        when(resourceManagerGateway.heartBeatFromTaskExecutor(any()))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error1")))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error2")))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error3")))
            .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error4")))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(resourceManagerGateway.disconnectTaskExecutor(any())).thenReturn(
            CompletableFuture.completedFuture(null));
        resourceManagerGatewayCxn.newLeaderIs(resourceManagerGateway);

        start();
        Thread.sleep(1000);
        verify(resourceManagerGateway, times(1)).registerTaskExecutor(any());
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
        verify(resourceManagerGateway, times(1)).registerTaskExecutor(any());
        verify(resourceManagerGateway, atLeastOnce()).heartBeatFromTaskExecutor(any());

        verify(newResourceClusterGateway, atLeastOnce()).heartBeatFromTaskExecutor(any());

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
        verify(resourceManagerGateway, times(1)).registerTaskExecutor(any());
        verify(resourceManagerGateway, atLeastOnce()).heartBeatFromTaskExecutor(any());
        verify(newResourceManagerGateway1, atLeastOnce()).heartBeatFromTaskExecutor(any());

        ResourceClusterGateway newResourceManagerGateway2 = getHealthyGateway("gateway 3");
        resourceManagerGatewayCxn.newLeaderIs(newResourceManagerGateway2);
        Thread.sleep(1000);

        verify(newResourceManagerGateway2, never()).disconnectTaskExecutor(any());
        verify(newResourceManagerGateway2, atLeastOnce()).heartBeatFromTaskExecutor(any());

        // check if the task executor is registered
        Assert.assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
    }

    private static ResourceClusterGateway getHealthyGateway(String name) {
        ResourceClusterGateway gateway = mock(ResourceClusterGateway.class, name);
        when(gateway.registerTaskExecutor(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(gateway.heartBeatFromTaskExecutor(any())).thenReturn(
            CompletableFuture.completedFuture(Ack.getInstance()));
        when(gateway.notifyTaskExecutorStatusChange(any()))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        when(gateway.disconnectTaskExecutor(any()))
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
        return gateway;
    }

    private static ResourceClusterGateway getUnhealthyGateway(String name) throws RequestThrottledException {
        ResourceClusterGateway gateway = mock(ResourceClusterGateway.class);
        when(gateway.registerTaskExecutor(any())).thenReturn(
            CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error")));
        when(gateway.disconnectTaskExecutor(any())).thenReturn(
            CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error")));
        when(gateway.toString()).thenReturn(name);
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


        public TestingTaskExecutor(RpcService rpcService,
                                   WorkerConfiguration workerConfiguration,
                                   HighAvailabilityServices highAvailabilityServices,
                                   ClassLoaderHandle classLoaderHandle) {
            super(rpcService, workerConfiguration, highAvailabilityServices, classLoaderHandle);
        }

    }

    @Getter
    private static class CollectingTaskLifecycleListener implements Listener {
        boolean startingCalled = false;
        boolean failedCalled = false;
        boolean cancellingCalled = false;
        boolean cancelledCalled = false;

        @Override
        public void onTaskStarting(RuntimeTask task) {
            startingCalled = true;
        }

        @Override
        public void onTaskFailed(RuntimeTask task, Throwable throwable) {
            failedCalled = true;
        }

        @Override
        public void onTaskCancelling(RuntimeTask task) {
            cancellingCalled = true;
        }

        @Override
        public void onTaskCancelled(RuntimeTask task, @Nullable Throwable throwable) {
            cancelledCalled = true;
        }
    }
}
