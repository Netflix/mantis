/*
 * Copyright 2026 Netflix, Inc.
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

package io.mantisrx.master.api.akka.route.handlers;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.HealthCheckExtension;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.proto.HealthCheckResponse;
import io.mantisrx.master.jobcluster.proto.HealthCheckResponse.FailedWorker;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobClusterRouteHandlerAkkaImplTest {

    private static ActorSystem system;
    private static ActorRef jobClustersManagerActor;
    private static File stateDirectory;

    @BeforeClass
    public static void setup() throws Exception {
        TestHelpers.setupMasterConfig();
        system = ActorSystem.create("JobClusterRouteHandlerTest");
        stateDirectory = Files.createTempDirectory("test-handler").toFile();

        LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
                new AuditEventSubscriberLoggingImpl(),
                new StatusEventSubscriberLoggingImpl(),
                new WorkerEventSubscriberLoggingImpl());

        jobClustersManagerActor = system.actorOf(
                JobClustersManagerActor.props(
                        new MantisJobStore(new FileBasedPersistenceProvider(stateDirectory, true)),
                        lifecycleEventPublisher,
                        CostsCalculator.noop(),
                        0),
                "jobClustersManager");

        MantisSchedulerFactory mantisSchedulerFactory = mock(MantisSchedulerFactory.class);
        MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
        when(mantisSchedulerFactory.forJob(any())).thenReturn(fakeScheduler);
        jobClustersManagerActor.tell(
                new JobClusterManagerProto.JobClustersManagerInitialize(
                        mantisSchedulerFactory, false), ActorRef.noSender());

        Thread.sleep(1000);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        TestKit.shutdownActorSystem(system);
        system = null;
        FileUtils.deleteDirectory(stateDirectory);
    }

    @Test
    public void testHealthCheckNoExtensions() throws Exception {
        JobClusterRouteHandlerAkkaImpl handler = new JobClusterRouteHandlerAkkaImpl(jobClustersManagerActor);

        HealthCheckResponse response = handler.healthCheck("nonExistentCluster", null, Map.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertNotNull(response);
    }

    @Test
    public void testHealthCheckExtensionChainingStopsOnFailure() throws Exception {
        HealthCheckExtension passingExtension = new HealthCheckExtension() {
            @Override
            public String contextId() { return "passing"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                return HealthCheckResponse.healthy(0);
            }
        };

        HealthCheckExtension failingExtension = new HealthCheckExtension() {
            @Override
            public String contextId() { return "alertSystem"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                return HealthCheckResponse.unhealthyAlerts(0, List.of("alert1"));
            }
        };

        HealthCheckExtension neverCalledExtension = new HealthCheckExtension() {
            @Override
            public String contextId() { return "neverCalled"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                throw new AssertionError("This extension should not be called");
            }
        };

        ActorRef mockActor = system.actorOf(Props.create(HealthyActorStub.class));
        JobClusterRouteHandlerAkkaImpl handler = new JobClusterRouteHandlerAkkaImpl(
                mockActor, List.of(passingExtension, failingExtension, neverCalledExtension));

        HealthCheckResponse response = handler.healthCheck("testCluster", null, Map.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertFalse(response.isHealthy());
        assertTrue(response.getFailureReason() instanceof HealthCheckResponse.AlertFailure);
        HealthCheckResponse.AlertFailure alertFailure = (HealthCheckResponse.AlertFailure) response.getFailureReason();
        assertEquals(List.of("alert1"), alertFailure.alerts());
    }

    @Test
    public void testHealthCheckExtensionReceivesScopedContext() throws Exception {
        final Map<String, Object>[] capturedContext = new Map[1];

        HealthCheckExtension contextCapturingExtension = new HealthCheckExtension() {
            @Override
            public String contextId() { return "myCheck"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                capturedContext[0] = context;
                return HealthCheckResponse.healthy(0);
            }
        };

        ActorRef mockActor = system.actorOf(Props.create(HealthyActorStub.class));
        JobClusterRouteHandlerAkkaImpl handler = new JobClusterRouteHandlerAkkaImpl(
                mockActor, List.of(contextCapturingExtension));

        Map<String, Object> fullContext = Map.of(
                "myCheck.names", "foo,bar",
                "myCheck.threshold", "5",
                "otherCheck.something", "ignored",
                "job-ids", "job-1"
        );

        HealthCheckResponse response = handler.healthCheck("testCluster", null, fullContext)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertTrue(response.isHealthy());
        assertNotNull(capturedContext[0]);
        assertEquals("foo,bar", capturedContext[0].get("names"));
        assertEquals("5", capturedContext[0].get("threshold"));
        assertNull(capturedContext[0].get("otherCheck.something"));
        assertNull(capturedContext[0].get("job-ids"));
    }

    @Test
    public void testHealthCheckAllExtensionsPassReturnsHealthy() throws Exception {
        HealthCheckExtension ext1 = new HealthCheckExtension() {
            @Override
            public String contextId() { return "ext1"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                return HealthCheckResponse.healthy(0);
            }
        };

        HealthCheckExtension ext2 = new HealthCheckExtension() {
            @Override
            public String contextId() { return "ext2"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                return HealthCheckResponse.healthy(0);
            }
        };

        ActorRef mockActor = system.actorOf(Props.create(HealthyActorStub.class));
        JobClusterRouteHandlerAkkaImpl handler = new JobClusterRouteHandlerAkkaImpl(
                mockActor, List.of(ext1, ext2));

        HealthCheckResponse response = handler.healthCheck("testCluster", null, Map.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertTrue(response.isHealthy());
    }

    @Test
    public void testHealthCheckActorUnhealthySkipsExtensions() throws Exception {
        HealthCheckExtension shouldNotBeCalled = new HealthCheckExtension() {
            @Override
            public String contextId() { return "ext"; }

            @Override
            public HealthCheckResponse checkHealth(String clusterName, List<String> jobIds, Map<String, Object> context) {
                throw new AssertionError("Extension should not be called when actor reports unhealthy");
            }
        };

        ActorRef mockActor = system.actorOf(Props.create(UnhealthyActorStub.class));
        JobClusterRouteHandlerAkkaImpl handler = new JobClusterRouteHandlerAkkaImpl(
                mockActor, List.of(shouldNotBeCalled));

        HealthCheckResponse response = handler.healthCheck("testCluster", null, Map.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertFalse(response.isHealthy());
    }

    public static class HealthyActorStub extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(JobClusterManagerProto.HealthCheckRequest.class, req ->
                            getSender().tell(HealthCheckResponse.healthy(req.requestId), getSelf()))
                    .build();
        }
    }

    public static class UnhealthyActorStub extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(JobClusterManagerProto.HealthCheckRequest.class, req ->
                            getSender().tell(HealthCheckResponse.unhealthyWorkers(req.requestId,
                                    List.of(new FailedWorker(0, 1, "Accepted"))), getSelf()))
                    .build();
        }
    }
}
