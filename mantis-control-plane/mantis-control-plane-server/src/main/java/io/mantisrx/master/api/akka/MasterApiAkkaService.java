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
import akka.http.javadsl.Http;
import akka.http.javadsl.HttpsConnectionContext;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.ServerBuilder;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.settings.ServerSettings;
import akka.http.javadsl.settings.WebSocketSettings;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.api.akka.route.MantisMasterRoute;
import io.mantisrx.master.api.akka.route.MasterApiMetrics;
import io.mantisrx.master.api.akka.route.handlers.JobArtifactRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobArtifactRouteHandlerImpl;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.JobStatusRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandlerAkkaImpl;
import io.mantisrx.master.api.akka.route.v0.JobClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobDiscoveryRoute;
import io.mantisrx.master.api.akka.route.v0.JobRoute;
import io.mantisrx.master.api.akka.route.v0.JobStatusRoute;
import io.mantisrx.master.api.akka.route.v0.MasterDescriptionRoute;
import io.mantisrx.master.api.akka.route.v1.AdminMasterRoute;
import io.mantisrx.master.api.akka.route.v1.JobArtifactsRoute;
import io.mantisrx.master.api.akka.route.v1.JobClustersRoute;
import io.mantisrx.master.api.akka.route.v1.JobDiscoveryStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobStatusStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobsRoute;
import io.mantisrx.master.api.akka.route.v1.LastSubmittedJobIdStreamRoute;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

public class MasterApiAkkaService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(MasterApiAkkaService.class);
    private final MasterMonitor masterMonitor;
    private final MasterDescription masterDescription;
    private final ActorRef jobClustersManagerActor;

    private final ActorRef resourceClustersHostManagerActor;
    private final ResourceClusters resourceClusters;
    private final ActorRef statusEventBrokerActor;
    private final int port;
    private final IMantisPersistenceProvider storageProvider;
    private final LifecycleEventPublisher lifecycleEventPublisher;
    private final MantisMasterRoute mantisMasterRoute;
    private final ILeadershipManager leadershipManager;
    private final ActorSystem system;
    private final Materializer materializer;
    private final ExecutorService executorService;
    private final CountDownLatch serviceLatch = new CountDownLatch(1);
    private final HttpsConnectionContext httpsConnectionContext;

    public MasterApiAkkaService(final MasterMonitor masterMonitor,
                                final MasterDescription masterDescription,
                                final ActorRef jobClustersManagerActor,
                                final ActorRef statusEventBrokerActor,
                                final ResourceClusters resourceClusters,
                                final ActorRef resourceClustersHostManagerActor,
                                final int serverPort,
                                final IMantisPersistenceProvider mantisStorageProvider,
                                final LifecycleEventPublisher lifecycleEventPublisher,
                                final ILeadershipManager leadershipManager
                                ) {
        this(
            masterMonitor,
            masterDescription,
            jobClustersManagerActor,
            statusEventBrokerActor,
            resourceClusters,
            resourceClustersHostManagerActor,
            serverPort,
            mantisStorageProvider,
            lifecycleEventPublisher,
            leadershipManager,
            null
        );
    }
    public MasterApiAkkaService(final MasterMonitor masterMonitor,
                                final MasterDescription masterDescription,
                                final ActorRef jobClustersManagerActor,
                                final ActorRef statusEventBrokerActor,
                                final ResourceClusters resourceClusters,
                                final ActorRef resourceClustersHostManagerActor,
                                final int serverPort,
                                final IMantisPersistenceProvider mantisStorageProvider,
                                final LifecycleEventPublisher lifecycleEventPublisher,
                                final ILeadershipManager leadershipManager,
                                final HttpsConnectionContext httpsConnectionContext) {
        super(true);
        Preconditions.checkNotNull(masterMonitor, "MasterMonitor");
        Preconditions.checkNotNull(masterDescription, "masterDescription");
        Preconditions.checkNotNull(jobClustersManagerActor, "jobClustersManagerActor");
        Preconditions.checkNotNull(statusEventBrokerActor, "statusEventBrokerActor");
        Preconditions.checkNotNull(mantisStorageProvider, "mantisStorageProvider");
        Preconditions.checkNotNull(lifecycleEventPublisher, "lifecycleEventPublisher");
        Preconditions.checkNotNull(leadershipManager, "leadershipManager");
        this.masterMonitor = masterMonitor;
        this.masterDescription = masterDescription;
        this.jobClustersManagerActor = jobClustersManagerActor;
        this.resourceClustersHostManagerActor = resourceClustersHostManagerActor;
        this.statusEventBrokerActor = statusEventBrokerActor;
        this.resourceClusters = resourceClusters;
        this.port = serverPort;
        this.storageProvider = mantisStorageProvider;
        this.lifecycleEventPublisher = lifecycleEventPublisher;
        this.leadershipManager = leadershipManager;
        this.system = ActorSystem.create("MasterApiActorSystem");
        this.materializer = Materializer.createMaterializer(system);
        this.mantisMasterRoute = configureApiRoutes(this.system);
        this.httpsConnectionContext = httpsConnectionContext;
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


    private MantisMasterRoute configureApiRoutes(final ActorSystem actorSystem) {
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
        final JobStatusRoute v0JobStatusRoute = new JobStatusRoute(jobStatusRouteHandler);

        final JobClustersRoute v1JobClusterRoute = new JobClustersRoute(jobClusterRouteHandler, actorSystem);
        final JobsRoute v1JobsRoute = new JobsRoute(jobClusterRouteHandler, jobRouteHandler, actorSystem);
        final AdminMasterRoute v1AdminMasterRoute = new AdminMasterRoute(masterDescription);
        final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute = new JobDiscoveryStreamRoute(jobDiscoveryRouteHandler);
        final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute = new LastSubmittedJobIdStreamRoute(jobDiscoveryRouteHandler);
        final JobStatusStreamRoute v1JobStatusStreamRoute = new JobStatusStreamRoute(jobStatusRouteHandler);

        final JobArtifactRouteHandler jobArtifactRouteHandler = new JobArtifactRouteHandlerImpl(storageProvider);
        final JobArtifactsRoute v1JobArtifactsRoute = new JobArtifactsRoute(jobArtifactRouteHandler);

        final LeaderRedirectionFilter leaderRedirectionFilter = new LeaderRedirectionFilter(masterMonitor, leadershipManager);
        final ResourceClusterRouteHandler resourceClusterRouteHandler = new ResourceClusterRouteHandlerAkkaImpl(
            resourceClustersHostManagerActor);
        return new MantisMasterRoute(
            actorSystem,
            leaderRedirectionFilter,
            masterDescriptionRoute,
            v0JobClusterRoute,
            v0JobRoute,
            v0JobDiscoveryRoute,
            v0JobStatusRoute,
            v1JobClusterRoute,
            v1JobsRoute,
            v1JobArtifactsRoute,
            v1AdminMasterRoute,
            v1JobDiscoveryStreamRoute,
            v1LastSubmittedJobIdStreamRoute,
            v1JobStatusStreamRoute,
            resourceClusters,
            resourceClusterRouteHandler);
    }

    private void startAPIServer() {
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
            this.mantisMasterRoute.createRoute().flow(system, materializer);

        ServerSettings defaultSettings = ServerSettings.create(system);
        java.time.Duration idleTimeout = system.settings().config().getDuration("akka.http.server.idle-timeout");
        logger.info("idle timeout {} sec ", idleTimeout.getSeconds());
        WebSocketSettings customWebsocketSettings = defaultSettings.getWebsocketSettings()
            .withPeriodicKeepAliveMaxIdle(Duration.create(idleTimeout.getSeconds() - 1, TimeUnit.SECONDS))
            .withPeriodicKeepAliveMode("pong");

        ServerSettings customServerSettings = defaultSettings.withWebsocketSettings(customWebsocketSettings);

        ServerBuilder httpServerBuilder = Http.get(system)
            .newServerAt("0.0.0.0", port);

        if(this.httpsConnectionContext != null) {
            httpServerBuilder = httpServerBuilder.enableHttps(this.httpsConnectionContext);
        }

        final CompletionStage<ServerBinding> binding = httpServerBuilder
                .withSettings(customServerSettings)
                .connectionSource()
                .to(Sink.foreach(connection -> {
                    MasterApiMetrics.getInstance().incrementIncomingRequestCount();
                    connection.handleWith(routeFlow, materializer);
                }))
                .run(materializer)
                .exceptionally(failure -> {
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
