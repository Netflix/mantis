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

package io.mantisrx.master.api.akka.route.v1;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandlerAkkaImpl;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.master.persistence.FileBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class JobDiscoveryStreamRouteTest extends RouteTestBase {
    private final static Logger logger = LoggerFactory.getLogger(JobDiscoveryStreamRouteTest.class);
    private static Thread t;
    private static final int SERVER_PORT = 8201;

    private static volatile CompletionStage<ServerBinding> binding;
    private static ActorRef agentsErrorMonitorActor;
    private final TestMantisClient mantisClient = new TestMantisClient(SERVER_PORT);

    public JobDiscoveryStreamRouteTest(){
        super("JobDiscoveryRoute", SERVER_PORT);
    }

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

                TestHelpers.setupMasterConfig();
                ActorRef jobClustersManagerActor = system.actorOf(
                    JobClustersManagerActor.props(
                        new MantisJobStore(new FileBasedPersistenceProvider(true)),
                        lifecycleEventPublisher,
                        CostsCalculator.noop(),
                        0),
                    "jobClustersManager");

                MantisSchedulerFactory fakeSchedulerFactory = mock(MantisSchedulerFactory.class);
                MantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManagerActor);
                when(fakeSchedulerFactory.forJob(any())).thenReturn(fakeScheduler);
                jobClustersManagerActor.tell(new JobClusterManagerProto.JobClustersManagerInitialize(fakeSchedulerFactory, false), ActorRef.noSender());

                Duration idleTimeout = system.settings().config().getDuration("akka.http.server.idle-timeout");
                logger.info("idle timeout {} sec ", idleTimeout.getSeconds());
                final JobDiscoveryRouteHandler jobDiscoveryRouteHandler = new JobDiscoveryRouteHandlerAkkaImpl(jobClustersManagerActor, idleTimeout);

                final JobDiscoveryStreamRoute jobDiscoveryRoute = new JobDiscoveryStreamRoute(jobDiscoveryRouteHandler);
                final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = jobDiscoveryRoute.createRoute(Function.identity()).flow(system, materializer);
                logger.info("starting test server on port {}", SERVER_PORT);
                binding = http.bindAndHandle(routeFlow,
                    ConnectHttp.toHost("localhost", SERVER_PORT), materializer);
                latch.countDown();
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
        logger.info("JobDiscoveryRouteTest teardown");
        if (binding != null) {
            binding
                .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
                .thenAccept(unbound -> system.terminate()); // and shutdown when done
        }
        t.interrupt();
    }

    @Test
    public void testJobDiscoveryStreamForNonExistentJob() throws InterruptedException {
        // The current behavior of Mantis client is to retry non-200 responses
        // This test overrides the default retry/repeat behavior to test a Sched info observable would complete if the job id requested is non-existent
        final CountDownLatch latch = new CountDownLatch(1);
        Observable<JobSchedulingInfo> jobSchedulingInfoObservable = mantisClient
            .discoveryStream("testJobCluster-1",
                obs -> Observable.just(1),
                obs -> Observable.empty()
            );
        jobSchedulingInfoObservable
            .doOnNext(x -> logger.info("onNext {}", x))
            .doOnError(t -> logger.warn("onError", t))
            .doOnCompleted(() -> {
                logger.info("onCompleted");
                latch.countDown();
            })
            .subscribe();
        latch.await();
    }

    @Test
    public void testLastSubmittedJobIdStreamForNonExistentJob() throws InterruptedException {
        // The current behavior of Mantis client is to retry non-200 responses
        // This test overrides the default retry/repeat behavior to test a namedjob info observable would complete if the job cluster requested is non-existent
        final CountDownLatch latch = new CountDownLatch(1);
        Observable<NamedJobInfo> jobSchedulingInfoObservable = mantisClient
            .namedJobInfo("testJobCluster",
                obs -> Observable.just(1),
                obs -> Observable.empty()
            );
        jobSchedulingInfoObservable
            .doOnNext(x -> logger.info("onNext {}", x))
            .doOnError(t -> logger.warn("onError", t))
            .doOnCompleted(() -> {
                logger.info("onCompleted");
                latch.countDown();
            })
            .subscribe();
        latch.await();
    }
}
