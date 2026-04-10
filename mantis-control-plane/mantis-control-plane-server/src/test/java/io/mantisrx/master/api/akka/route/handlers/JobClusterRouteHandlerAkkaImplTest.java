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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.CostsCalculator;

import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import java.io.File;
import java.nio.file.Files;
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
    public void testHealthCheck() throws Exception {
        JobClusterRouteHandlerAkkaImpl handler = new JobClusterRouteHandlerAkkaImpl(jobClustersManagerActor);

        JobClusterManagerProto.HealthCheckResponse response = handler.healthCheck("nonExistentCluster", null)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertNotNull(response);
    }
}
