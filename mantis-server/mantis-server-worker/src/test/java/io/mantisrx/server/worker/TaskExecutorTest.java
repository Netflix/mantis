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
package io.mantisrx.server.worker;

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
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.ResourceLeaderChangeListener;
import io.mantisrx.server.master.client.ResourceLeaderConnection;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.mantisrx.shaded.com.google.common.io.Resources;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.RpcService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;

@Slf4j
public class TaskExecutorTest {

  private WorkerConfiguration workerConfiguration;
  private RpcService rpcService;
  private TaskTable taskTable;
  private MantisMasterGateway masterMonitor;
  private ClassLoaderHandle classLoaderHandle;
  private TaskExecutor taskExecutor;
  private CountDownLatch startedSignal;
  private CountDownLatch doneSignal;
  private Status finalStatus;
  private ResourceClusterGateway resourceManagerGateway;
  private SimpleResourceLeaderConnection<ResourceClusterGateway> resourceManagerGatewayCxn;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Before
  public void setUp() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mantis.cluster.storage-dir", "");
    props.setProperty("mantis.local.storage-dir", "");
    props.setProperty("mantis.zookeeper.root", "");
    props.setProperty("mantis.cluster.id", "default");
    props.setProperty("heartbeats.interval", "100");

    startedSignal = new CountDownLatch(1);
    doneSignal = new CountDownLatch(1);

    workerConfiguration = Configurations.frmProperties(props, WorkerConfiguration.class);
    rpcService = new TestingRpcService();

    taskTable = new TaskTableImpl();
    masterMonitor = mock(MantisMasterGateway.class);
    classLoaderHandle = new TestingClassLoaderHandle();
    resourceManagerGateway = getHealthyGateway("gateway 1");
    resourceManagerGatewayCxn = new SimpleResourceLeaderConnection<>(resourceManagerGateway);
  }

  private void start() {
    Function<Status, CompletableFuture<Ack>> updateTaskExecutionStatusFunction = status -> {
      log.info("Task Status = {}", status.getState());
      if (status.getState() == MantisJobState.Started) {
        startedSignal.countDown();
      }

      if (status.getState().isTerminalState()) {
        finalStatus = status;
        doneSignal.countDown();
      }

      return CompletableFuture.completedFuture(Ack.getInstance());
    };

    taskExecutor =
        new TaskExecutor(rpcService, workerConfiguration, masterMonitor,
            classLoaderHandle, updateTaskExecutionStatusFunction,
            executeStageRequest -> SinkSubscriptionStateHandler.noop(), resourceManagerGatewayCxn);
    taskExecutor.start();
    taskExecutor.awaitRunning();
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
    when(masterMonitor.schedulingChanges("jobId-0")).thenReturn(
        Observable.just(new JobSchedulingInfo("jobId-0", stageAssignmentMap)));
    CompletableFuture<Ack> wait = taskExecutor.submitTask(
        new ExecuteStageRequest("jobName", "jobId-0", 0, 1,
            Resources.getResource("example-job.jar"),
            1, 1,
            ports, 100L, 1, ImmutableList.of(),
            new SchedulingInfo.Builder().numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2),
                    Lists.newArrayList(), Lists.newArrayList()).build(),
            MantisJobDurationType.Perpetual,
            1000L,
            1L,
            new WorkerPorts(2, 3, 4, 5, 6),
            Optional.of(SineFunctionJobProvider.class.getName())));
    wait.get();
    assertTrue(startedSignal.await(5, TimeUnit.SECONDS));
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
        .subscribe(point -> log.info("point={}", point), error -> log.error("failed", error), () -> doneSignal.countDown());
    assertTrue(doneSignal.await(10, TimeUnit.SECONDS));
    subscription.unsubscribe();
  }

  @Test
  public void testWhenSuccessiveHeartbeatsFail() throws Exception {
    ResourceClusterGateway resourceManagerGateway = mock(ResourceClusterGateway.class);
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
    verify(resourceManagerGateway, times(2)).registerTaskExecutor(any());
    assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
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
    verify(resourceManagerGateway, times(1)).disconnectTaskExecutor(any());
    verify(resourceManagerGateway, atLeastOnce()).heartBeatFromTaskExecutor(any());

    verify(newResourceClusterGateway, times(1)).registerTaskExecutor(any());
    verify(newResourceClusterGateway, atLeastOnce()).heartBeatFromTaskExecutor(any());

    // check if the task executor is registered
    assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
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
    verify(resourceManagerGateway, times(1)).disconnectTaskExecutor(any());
    verify(resourceManagerGateway, atLeastOnce()).heartBeatFromTaskExecutor(any());

    verify(newResourceManagerGateway1, atLeastOnce()).registerTaskExecutor(any());
    verify(newResourceManagerGateway1, atLeastOnce()).disconnectTaskExecutor(any());
    verify(newResourceManagerGateway1, never()).heartBeatFromTaskExecutor(any());

    ResourceClusterGateway newResourceManagerGateway2 = getHealthyGateway("gateway 3");
    resourceManagerGatewayCxn.newLeaderIs(newResourceManagerGateway2);
    Thread.sleep(1000);

    verify(newResourceManagerGateway2, times(1)).registerTaskExecutor(any());
    verify(newResourceManagerGateway2, never()).disconnectTaskExecutor(any());
    verify(newResourceManagerGateway2, atLeastOnce()).heartBeatFromTaskExecutor(any());

    // check if the task executor is registered
    assertTrue(taskExecutor.isRegistered(Time.seconds(1)).get());
  }

  private static ResourceClusterGateway getHealthyGateway(String name) {
    ResourceClusterGateway gateway = mock(ResourceClusterGateway.class);
    when(gateway.registerTaskExecutor(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(gateway.heartBeatFromTaskExecutor(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(gateway.disconnectTaskExecutor(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(gateway.toString()).thenReturn(name);
    return gateway;
  }

  private static ResourceClusterGateway getUnhealthyGateway(String name) {
    ResourceClusterGateway gateway = mock(ResourceClusterGateway.class);
    when(gateway.registerTaskExecutor(any())).thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error")));
    when(gateway.disconnectTaskExecutor(any())).thenReturn(CompletableFutures.exceptionallyCompletedFuture(new UnknownError("error")));
    when(gateway.toString()).thenReturn(name);
    return gateway;
  }

  public static class SimpleResourceLeaderConnection<ResourceT> implements ResourceLeaderConnection<ResourceT> {
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
      Preconditions.checkArgument(listener.compareAndSet(null, changeListener), "changeListener already set");
    }

    public void newLeaderIs(ResourceT newLeader) {
      ResourceT old = current.getAndSet(newLeader);
      if (listener.get() != null) {
        listener.get().onResourceLeaderChanged(old, newLeader);
      }
    }
  }
}
