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

package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.TestingRpcService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusterTaskExecutorMapper;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;

public class ResourceClusterActorTest {
    private static final TaskExecutorID TASK_EXECUTOR_ID = TaskExecutorID.of("taskExecutorId");
    private static final String TASK_EXECUTOR_ADDRESS = "address";
    private static final ClusterID CLUSTER_ID = ClusterID.of("clusterId");
    private static final Duration heartbeatTimeout = Duration.ofSeconds(10);
    private static final Duration assignmentTimeout = Duration.ofSeconds(1);
    private static final String HOST_NAME = "hostname";
    private static final WorkerPorts WORKER_PORTS = new WorkerPorts(1, 2, 3, 4, 5);
    private static final MachineDefinition MACHINE_DEFINITION =
        new MachineDefinition(2f, 2014, 128.0, 1024, 1);
    private static final TaskExecutorRegistration TASK_EXECUTOR_REGISTRATION =
        new TaskExecutorRegistration(
            TASK_EXECUTOR_ID,
            CLUSTER_ID,
            TASK_EXECUTOR_ADDRESS,
            HOST_NAME,
            WORKER_PORTS,
            MACHINE_DEFINITION);
    private static final WorkerId WORKER_ID =
        WorkerId.fromIdUnsafe("late-sine-function-tutorial-1-worker-0-1");

    static ActorSystem actorSystem;

    private final TestingRpcService rpcService = new TestingRpcService();
    private final TaskExecutorGateway gateway = mock(TaskExecutorGateway.class);

    private MantisJobStore mantisJobStore;
    private ResourceClusterTaskExecutorMapper mapper;
    private ActorRef resourceClusterActor;
    private ResourceCluster resourceCluster;

    @BeforeClass
    public static void setup() {
        actorSystem = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }

    @Before
    public void setupRpcService() {
        rpcService.registerGateway(TASK_EXECUTOR_ADDRESS, gateway);
        mantisJobStore = mock(MantisJobStore.class);
        mapper = ResourceClusterTaskExecutorMapper.inMemory();
    }

    @Before
    public void setupActor() {
        final Props props =
            ResourceClusterActor.props(
                CLUSTER_ID,
                heartbeatTimeout,
                assignmentTimeout,
                Clock.systemDefaultZone(),
                rpcService,
                mantisJobStore);

        resourceClusterActor = actorSystem.actorOf(props);
        resourceCluster =
            new ResourceClusterAkkaImpl(
                resourceClusterActor,
                Duration.ofSeconds(1),
                CLUSTER_ID,
                mapper);
    }

    @Test
    public void testRegistration() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
    }

    @Test
    public void testInitializationAfterRestart() throws Exception {
        when(mantisJobStore.getTaskExecutor(Matchers.eq(TASK_EXECUTOR_ID))).thenReturn(TASK_EXECUTOR_REGISTRATION);
        assertEquals(
            Ack.getInstance(),
            resourceCluster.initializeTaskExecutor(TASK_EXECUTOR_ID, WORKER_ID).get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getBusyTaskExecutors().get());
    }

    @Test
    public void testGetFreeTaskExecutors() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
        assertEquals(TASK_EXECUTOR_ID, resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION, WORKER_ID).get());
        assertEquals(ImmutableList.of(), resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getRegisteredTaskExecutors().get());
    }

    @Test
    public void testAssignmentTimeout() throws Exception {
        assertEquals(Ack.getInstance(), resourceCluster.registerTaskExecutor(TASK_EXECUTOR_REGISTRATION).get());
        assertEquals(Ack.getInstance(),
            resourceCluster
                .heartBeatFromTaskExecutor(
                    new TaskExecutorHeartbeat(
                        TASK_EXECUTOR_ID,
                        CLUSTER_ID,
                        TaskExecutorReport.available())).get());
        assertEquals(TASK_EXECUTOR_ID, resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION, WORKER_ID).get());
        assertEquals(ImmutableList.of(), resourceCluster.getAvailableTaskExecutors().get());
        Thread.sleep(2000);
        assertEquals(ImmutableList.of(TASK_EXECUTOR_ID), resourceCluster.getAvailableTaskExecutors().get());
        assertEquals(TASK_EXECUTOR_ID, resourceCluster.getTaskExecutorFor(MACHINE_DEFINITION, WORKER_ID).get());
    }
}
