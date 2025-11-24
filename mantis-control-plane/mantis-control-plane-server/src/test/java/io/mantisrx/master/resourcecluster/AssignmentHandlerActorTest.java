/*
 * Copyright 2023 Netflix, Inc.
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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.Ack;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.resourcecluster.AssignmentHandlerActor.TaskExecutorAssignmentFailAndTerminate;
import io.mantisrx.master.resourcecluster.AssignmentHandlerActor.TaskExecutorAssignmentRequest;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.ExecuteStageRequestFactory;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.worker.TaskExecutorGateway;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AssignmentHandlerActorTest {
    private static ActorSystem actorSystem;

    private final ClusterID clusterID = ClusterID.of("test-cluster");
    private final TaskExecutorID taskExecutorID = TaskExecutorID.of("te-1");
    private final WorkerId workerId = WorkerId.fromIdUnsafe("job-1-worker-1-1");

    private JobMessageRouter jobMessageRouter;
    private ExecuteStageRequestFactory executeStageRequestFactory;
    private TaskExecutorGateway taskExecutorGateway;

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
    public void setupMocks() {
        jobMessageRouter = mock(JobMessageRouter.class);
        executeStageRequestFactory = mock(ExecuteStageRequestFactory.class);
        taskExecutorGateway = mock(TaskExecutorGateway.class);

        when(executeStageRequestFactory.of(any(TaskExecutorRegistration.class), any(TaskExecutorAllocationRequest.class))).thenReturn(mock(ExecuteStageRequest.class));
    }

    private TaskExecutorRegistration createRegistration() {
        return TaskExecutorRegistration.builder()
            .taskExecutorID(taskExecutorID)
            .clusterID(clusterID)
            .taskExecutorAddress("127.0.0.1")
            .hostname("localhost")
            .workerPorts(new WorkerPorts(1000, 1001, 1002, 1003, 1004))
            .machineDefinition(new MachineDefinition(1.0, 1024, 1024, 1024, 1))
            .taskExecutorAttributes(ImmutableMap.of())
            .build();
    }

    private TaskExecutorAllocationRequest createAllocationRequest() {
        return TaskExecutorAllocationRequest.of(
            workerId,
            SchedulingConstraints.of(new MachineDefinition(1.0, 1024, 1024, 1024, 1)),
            null,
            0
        );
    }

    @Test
    public void testAssignmentSuccess() {
        TestKit probe = new TestKit(actorSystem);
        Props props = AssignmentHandlerActor.props(
            clusterID,
            jobMessageRouter,
            Duration.ofSeconds(1),
            executeStageRequestFactory
        );
        ActorRef parent = actorSystem.actorOf(Props.create(ForwarderParent.class, props, probe.getRef()));
        ActorRef actor = probe.expectMsgClass(ActorRef.class);

        when(taskExecutorGateway.submitTask(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        TaskExecutorAssignmentRequest request = TaskExecutorAssignmentRequest.of(
            createAllocationRequest(),
            taskExecutorID,
            createRegistration(),
            CompletableFuture.completedFuture(taskExecutorGateway)
        );

        actor.tell(request, probe.getRef());

        // On success, no message is sent back to parent (probe), but logs are written.
        // We verify the gateway interaction.
        probe.expectNoMessage(Duration.ofMillis(200));
        verify(taskExecutorGateway, times(1)).submitTask(any());
    }

    @Test
    public void testAssignmentRetryAndSuccess() {
        TestKit probe = new TestKit(actorSystem);
        // short retry interval
        Props props = AssignmentHandlerActor.props(
            clusterID,
            jobMessageRouter,
            Duration.ofSeconds(10),
            executeStageRequestFactory,
            3,
            Duration.ofMillis(100)
        );
        ActorRef parent = actorSystem.actorOf(Props.create(ForwarderParent.class, props, probe.getRef()));
        ActorRef actor = probe.expectMsgClass(ActorRef.class);

        // First call fails, second succeeds
        CompletableFuture<Ack> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));

        when(taskExecutorGateway.submitTask(any()))
            .thenReturn(failedFuture)
            .thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));

        TaskExecutorAssignmentRequest request = TaskExecutorAssignmentRequest.of(
            createAllocationRequest(),
            taskExecutorID,
            createRegistration(),
            CompletableFuture.completedFuture(taskExecutorGateway)
        );

        actor.tell(request, probe.getRef());

        // Should eventually succeed and send no failure message
        probe.expectNoMessage(Duration.ofMillis(500));

        // Verify retry happened
        verify(taskExecutorGateway, times(2)).submitTask(any());
    }

    @Test
    public void testAssignmentMaxRetriesExceeded() {
        TestKit probe = new TestKit(actorSystem);
        Props props = AssignmentHandlerActor.props(
            clusterID,
            jobMessageRouter,
            Duration.ofSeconds(10),
            executeStageRequestFactory,
            2, // 2 retries (total 2 attempts)
            Duration.ofMillis(50)
        );
        ActorRef parent = actorSystem.actorOf(Props.create(ForwarderParent.class, props, probe.getRef()));
        ActorRef actor = probe.expectMsgClass(ActorRef.class);

        CompletableFuture<Ack> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));

        when(taskExecutorGateway.submitTask(any())).thenReturn(failedFuture);

        TaskExecutorAssignmentRequest request = TaskExecutorAssignmentRequest.of(
            createAllocationRequest(),
            taskExecutorID,
            createRegistration(),
            CompletableFuture.completedFuture(taskExecutorGateway)
        );

        actor.tell(request, probe.getRef());

        TaskExecutorAssignmentFailAndTerminate msg = probe.expectMsgClass(TaskExecutorAssignmentFailAndTerminate.class);
        assertEquals(taskExecutorID, msg.getTaskExecutorID());

        verify(taskExecutorGateway, times(2)).submitTask(any());
    }

    @Test
    public void testAssignmentTimeout() {
        TestKit probe = new TestKit(actorSystem);
        Duration timeout = Duration.ofMillis(100);
        Props props = AssignmentHandlerActor.props(
            clusterID,
            jobMessageRouter,
            timeout,
            executeStageRequestFactory,
            1, // fail immediately after first timeout
            Duration.ofMillis(50)
        );
        ActorRef parent = actorSystem.actorOf(Props.create(ForwarderParent.class, props, probe.getRef()));
        ActorRef actor = probe.expectMsgClass(ActorRef.class);

        // Future that never completes
        CompletableFuture<Ack> hangingFuture = new CompletableFuture<>();
        when(taskExecutorGateway.submitTask(any())).thenReturn(hangingFuture);

        TaskExecutorAssignmentRequest request = TaskExecutorAssignmentRequest.of(
            createAllocationRequest(),
            taskExecutorID,
            createRegistration(),
            CompletableFuture.completedFuture(taskExecutorGateway)
        );

        actor.tell(request, probe.getRef());

        TaskExecutorAssignmentFailAndTerminate msg = probe.expectMsgClass(TaskExecutorAssignmentFailAndTerminate.class);
        assertTrue(msg.getThrowable() instanceof java.util.concurrent.TimeoutException);
    }

    public static class ForwarderParent extends AbstractActor {
        private final ActorRef probe;
        private final Props childProps;

        public ForwarderParent(Props childProps, ActorRef probe) {
            this.childProps = childProps;
            this.probe = probe;
        }

        @Override
        public void preStart() {
            ActorRef child = getContext().actorOf(childProps, "child");
            probe.tell(child, self());
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .matchAny(msg -> probe.tell(msg, sender()))
                .build();
        }
    }
}

