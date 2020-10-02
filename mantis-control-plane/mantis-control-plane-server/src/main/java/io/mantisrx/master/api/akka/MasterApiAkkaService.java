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

package io.mantisrx.master.api.akka;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.settings.ServerSettings;
import akka.http.javadsl.settings.WebSocketSettings;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.v0.AgentClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobDiscoveryRoute;
import io.mantisrx.master.api.akka.route.v0.JobStatusRoute;
import io.mantisrx.master.api.akka.route.v0.JobRoute;

import io.mantisrx.master.api.akka.route.v1.AdminMasterRoute;
import io.mantisrx.master.api.akka.route.v1.AgentClustersRoute;
import io.mantisrx.master.api.akka.route.v1.JobClustersRoute;

import io.mantisrx.master.api.akka.route.MantisMasterRoute;
import io.mantisrx.master.api.akka.route.v0.MasterDescriptionRoute;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.v1.JobDiscoveryStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobStatusStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobsRoute;
import io.mantisrx.master.api.akka.route.v1.LastSubmittedJobIdStreamRoute;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.vm.AgentClusterOperations;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.master.ILeadershipManager;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.persistence.IMantisStorageProvider;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MasterApiAkkaService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(MasterApiAkkaService.class);
    private final MasterMonitor masterMonitor;
    private final MasterDescription masterDescription;
    private final ActorRef jobClustersManagerActor;
    private final ActorRef statusEventBrokerActor;
    private final int port;
    private final IMantisStorageProvider storageProvider;
    private final MantisScheduler scheduler;
    private final LifecycleEventPublisher lifecycleEventPublisher;
    private final MantisMasterRoute mantisMasterRoute;
    private final ILeadershipManager leadershipManager;
    private final ActorSystem system;
    private final ActorMaterializer materializer;
    private final ExecutorService executorService;
    private final CountDownLatch serviceLatch = new CountDownLatch(1);

    public MasterApiAkkaService(final MasterMonitor masterMonitor,
                                final MasterDescription masterDescription,
                                final ActorRef jobClustersManagerActor,
                                final ActorRef statusEventBrokerActor,
                                final int serverPort,
                                final IMantisStorageProvider mantisStorageProvider,
                                final MantisScheduler scheduler,
                                final LifecycleEventPublisher lifecycleEventPublisher,
                                final ILeadershipManager leadershipManager,
                                final AgentClusterOperations agentClusterOperations) {
        super(true);
        Preconditions.checkNotNull(masterMonitor, "MasterMonitor");
        Preconditions.checkNotNull(masterDescription, "masterDescription");
        Preconditions.checkNotNull(jobClustersManagerActor, "jobClustersManagerActor");
        Preconditions.checkNotNull(statusEventBrokerActor, "statusEventBrokerActor");
        Preconditions.checkNotNull(mantisStorageProvider, "mantisStorageProvider");
        Preconditions.checkNotNull(scheduler, "scheduler");
        Preconditions.checkNotNull(lifecycleEventPublisher, "lifecycleEventPublisher");
        Preconditions.checkNotNull(leadershipManager, "leadershipManager");
        Preconditions.checkNotNull(agentClusterOperations, "agentClusterOperations");
        this.masterMonitor = masterMonitor;
        this.masterDescription = masterDescription;
        this.jobClustersManagerActor = jobClustersManagerActor;
        this.statusEventBrokerActor = statusEventBrokerActor;
        this.port = serverPort;
        this.storageProvider = mantisStorageProvider;
        this.scheduler = scheduler;
        this.lifecycleEventPublisher = lifecycleEventPublisher;
        this.leadershipManager = leadershipManager;
        this.system = ActorSystem.create("MasterApiActorSystem");
        this.materializer = ActorMaterializer.create(system);
        this.mantisMasterRoute = configureApiRoutes(this.system, agentClusterOperations);
        this.executorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "MasterApiAkkaServiceThread");
            t.setDaemon(true);
            return t;
        });
        executorService.execute(() -> {
            try {
                startAPIServer();
            } catch (Exception e) {
                logger.warn("caught exception starting API server", e);
            }
        });
    }


    private MantisMasterRoute configureApiRoutes(final ActorSystem actorSystem, final AgentClusterOperations agentClusterOperations) {
        // Setup API routes
        final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(jobClustersManagerActor);
        final JobRouteHandler jobRouteHandler = new JobRouteHandlerAkkaImpl(jobClustersManagerActor);

        final MasterDescriptionRoute masterDescriptionRoute = new MasterDescriptionRoute(masterDescription);
        final JobRoute v0JobRoute = new JobRoute(jobRouteHandler, actorSystem);

        java.time.Duration idleTimeout = actorSystem.settings().config().getDuration("akka.http.server.idle-timeout");
        logger.info("idle timeout {} sec ", idleTimeout.getSeconds());
        final JobStatusRouteHandler jobStatusRouteHandler = new JobStatusRouteHandlerAkkaImpl(actorSystem, statusEventBrokerActor);
        final JobDiscoveryRouteHandler jobDiscoveryRouteHandler = new JobDiscoveryRouteHandlerAkkaImpl(jobClustersManagerActor, idleTimeout);

        final JobDiscoveryRoute v0JobDiscoveryRoute = new JobDiscoveryRoute(jobDiscoveryRouteHandler);
        final JobClusterRoute v0JobClusterRoute = new JobClusterRoute(jobClusterRouteHandler, jobRouteHandler, actorSystem);
        final AgentClusterRoute v0AgentClusterRoute = new AgentClusterRoute(agentClusterOperations, actorSystem);
        final JobStatusRoute v0JobStatusRoute = new JobStatusRoute(jobStatusRouteHandler);

        final JobClustersRoute v1JobClusterRoute = new JobClustersRoute(jobClusterRouteHandler, actorSystem);
        final JobsRoute v1JobsRoute = new JobsRoute(jobClusterRouteHandler, jobRouteHandler, actorSystem);
        final AdminMasterRoute v1AdminMasterRoute = new AdminMasterRoute(masterDescription);
        final AgentClustersRoute v1AgentClustersRoute = new AgentClustersRoute(agentClusterOperations);
        final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute = new JobDiscoveryStreamRoute(jobDiscoveryRouteHandler);
        final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute = new LastSubmittedJobIdStreamRoute(jobDiscoveryRouteHandler);
        final JobStatusStreamRoute v1JobStatusStreamRoute = new JobStatusStreamRoute(jobStatusRouteHandler);

        final LeaderRedirectionFilter leaderRedirectionFilter = new LeaderRedirectionFilter(masterMonitor, leadershipManager);
        return new MantisMasterRoute(leaderRedirectionFilter,
                                     masterDescriptionRoute,
                                     v0JobClusterRoute,
                                     v0JobRoute,
                                     v0JobDiscoveryRoute,
                                     v0JobStatusRoute,
                                     v0AgentClusterRoute,
                                     v1JobClusterRoute,
                                     v1JobsRoute,
                                     v1AdminMasterRoute,
                                     v1AgentClustersRoute,
                                     v1JobDiscoveryStreamRoute,
                                     v1LastSubmittedJobIdStreamRoute,
                                     v1JobStatusStreamRoute);
    }

    private void startAPIServer() {
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
            this.mantisMasterRoute.createRoute().flow(system, materializer);

        final Http http = Http.get(system);

        ServerSettings defaultSettings = ServerSettings.create(system);
        java.time.Duration idleTimeout = system.settings().config().getDuration("akka.http.server.idle-timeout");
        logger.info("idle timeout {} sec ", idleTimeout.getSeconds());
        WebSocketSettings customWebsocketSettings = defaultSettings.getWebsocketSettings()
            .withPeriodicKeepAliveMaxIdle(Duration.create(idleTimeout.getSeconds() - 1, TimeUnit.SECONDS))
            .withPeriodicKeepAliveMode("pong");

        ServerSettings customServerSettings = defaultSettings.withWebsocketSettings(customWebsocketSettings);

        final CompletionStage<ServerBinding> binding = http.bindAndHandle(routeFlow,
            ConnectHttp.toHost("0.0.0.0", port),
            customServerSettings,
            system.log(),
            materializer);
        binding.exceptionally(failure -> {
            System.err.println("API service exited, committing suicide !" + failure.getMessage());
            logger.info("Master API service exited in error, committing suicide !");
            system.terminate();
            System.exit(2);
            return null;
        });
        logger.info("Starting Mantis Master API on port {}", port);
        try {
            serviceLatch.await();
        } catch (InterruptedException e) {
            logger.error("Master API thread interrupted, committing suicide", e);
            System.exit(2);
        }
        binding
            .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
            .thenAccept(unbound -> {
                logger.error("Master API service unbind, committing suicide");
                system.terminate();
                System.exit(2);
            }); // and shutdown when done
    }

    @Override
    public void start() {
        super.awaitActiveModeAndStart(() -> {
            logger.info("marking leader READY");
            leadershipManager.setLeaderReady();
        });
    }

    @Override
    public void shutdown() {
        super.shutdown();
        logger.info("Shutting down Mantis Master API");
        serviceLatch.countDown();
        executorService.shutdownNow();
        system.terminate();
    }
}
