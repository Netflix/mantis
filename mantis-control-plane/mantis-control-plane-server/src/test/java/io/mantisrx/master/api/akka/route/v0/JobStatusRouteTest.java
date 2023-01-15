/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.master.api.akka.route.v0;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.netflix.mantis.master.scheduler.TestHelpers;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandlerAkkaImpl;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventBrokerActor;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.JobClusterSettings;
import io.mantisrx.master.jobcluster.job.JobSettings;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.AgentsErrorMonitorActor;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.StoreSettings;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class JobStatusRouteTest {
    private final static Logger logger = LoggerFactory.getLogger(JobStatusRouteTest.class);
    private final ActorMaterializer materializer = ActorMaterializer.create(system);
    private final Http http = Http.get(system);
    private static Thread t;
    private static final int serverPort = 8207;

    private static CompletionStage<ServerBinding> binding;
    private static ActorSystem system = ActorSystem.create("JobStatusRoute");
    private static ActorRef agentsErrorMonitorActor = system.actorOf(AgentsErrorMonitorActor.props());
    private static ActorRef statusEventBrokerActor = system.actorOf(StatusEventBrokerActor.props(agentsErrorMonitorActor));
    private static final JobSettings JOB_SETTINGS =
        JobSettings.fromConfig(
            ConfigFactory
                .load("job-definition-settings-sample.conf"));

    private static final JobClusterSettings JOB_CLUSTER_SETTINGS =
        JobClusterSettings.fromConfig(
            ConfigFactory
                .load("job-cluster-settings-sample.conf"));

    private static final StoreSettings STORE_SETTINGS =
        StoreSettings.fromConfig(
            ConfigFactory
                .load("store-settings-sample.conf"));

    @BeforeClass
    public static void setup() throws Exception {
        JobTestHelper.deleteAllFiles();
        JobTestHelper.createDirsIfRequired();
        final CountDownLatch latch = new CountDownLatch(1);

        t = new Thread(() -> {
            try {
                // boot up server using the route as defined below
                final Http http = Http.get(system);
                final ActorMaterializer materializer = ActorMaterializer.create(system);
                final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

                ActorRef jobClustersManagerActor = system.actorOf(
                    JobClustersManagerActor.props(
                        new MantisJobStore(new FileBasedPersistenceProvider(true), STORE_SETTINGS),
                        lifecycleEventPublisher,
                        JOB_SETTINGS,
                        JOB_CLUSTER_SETTINGS,
                        TestHelpers.CONSTRAINTS_EVALUATORS),
                    "jobClustersManager");

                MantisSchedulerFactory fakeSchedulerFactory = mock(MantisSchedulerFactory.class);
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                when(fakeSchedulerFactory.forJob(any())).thenReturn(fakeScheduler);
                jobClustersManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(fakeSchedulerFactory, false), ActorRef.noSender());

                agentsErrorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(fakeScheduler), ActorRef.noSender());

                final JobStatusRouteHandler jobStatusRouteHandler = new JobStatusRouteHandlerAkkaImpl(system, statusEventBrokerActor);

                final JobStatusRoute jobStatusRoute = new JobStatusRoute(jobStatusRouteHandler);
                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = jobStatusRoute.createRoute(Function.identity()).flow(system, materializer);
                logger.info("starting test server on port {}", serverPort);
                latch.countDown();
                binding = http.bindAndHandle(routeFlow,
                    ConnectHttp.toHost("localhost", serverPort), materializer);
            } catch (Exception e) {
                logger.info("caught exception", e);
                latch.countDown();
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();
        latch.await();
    }

    @AfterClass
    public static void teardown() {
        logger.info("JobStatusRouteTest teardown");
        binding
            .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
            .thenAccept(unbound -> system.terminate()); // and shutdown when done
        t.interrupt();
    }
}
