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
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.netflix.fenzo.AutoScaleAction;
import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.DeadLetterActor;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.api.akka.route.handlers.JobDiscoveryRouteHandler;
import io.mantisrx.master.api.akka.route.v0.AgentClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobDiscoveryRoute;
import io.mantisrx.master.api.akka.route.v0.JobStatusRoute;
import io.mantisrx.master.api.akka.route.v0.JobRoute;
import io.mantisrx.master.api.akka.route.v0.MasterDescriptionRoute;
import io.mantisrx.master.api.akka.route.v1.AdminMasterRoute;
import io.mantisrx.master.api.akka.route.v1.AgentClustersRoute;
import io.mantisrx.master.api.akka.route.v1.JobClustersRoute;
import io.mantisrx.master.api.akka.route.MantisMasterRoute;
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
import io.mantisrx.master.events.AuditEventBrokerActor;
import io.mantisrx.master.events.AuditEventSubscriber;
import io.mantisrx.master.events.AuditEventSubscriberAkkaImpl;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventBrokerActor;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.scheduler.AgentsErrorMonitorActor;
import io.mantisrx.master.scheduler.FakeMantisScheduler;
import io.mantisrx.master.scheduler.JobMessageRouterImpl;
import io.mantisrx.master.vm.AgentClusterOperationsImpl;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.AgentClustersAutoScaler;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.LeadershipManagerLocalImpl;
import io.mantisrx.server.master.persistence.IMantisStorageProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.MantisStorageProviderAdapter;


import io.mantisrx.server.master.store.SimpleCachedFileStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;


public class MantisMasterAPI extends AllDirectives {
    public static final Logger logger = LoggerFactory.getLogger(MantisMasterAPI.class);

    private static void setupDummyAgentClusterAutoScaler() {
        final AutoScaleRule dummyAutoScaleRule = new AutoScaleRule() {
            @Override
            public String getRuleName() {
                return "test";
            }

            @Override
            public int getMinIdleHostsToKeep() {
                return 1;
            }

            @Override
            public int getMaxIdleHostsToKeep() {
                return 10;
            }

            @Override
            public long getCoolDownSecs() {
                return 300;
            }

            @Override
            public boolean idleMachineTooSmall(VirtualMachineLease lease) {
                return false;
            }
        };
        AgentClustersAutoScaler.initialize(() -> new HashSet<>(Collections.singletonList(
                dummyAutoScaleRule)), new Observer<AutoScaleAction>() {
            @Override
            public void onCompleted() {

            }

            @Override
            public void onError(Throwable e) {

            }

            @Override
            public void onNext(AutoScaleAction autoScaleAction) {

            }
        });
    }

    public static void main(String[] args) throws Exception {
        // boot up server using the route as defined below
        int port = 8182;
        TestHelpers.setupMasterConfig();
        ActorSystem system = ActorSystem.create("MantisMasterAPI");
        final ActorRef actor = system.actorOf(Props.create(DeadLetterActor.class));
        system.eventStream().subscribe(actor, DeadLetter.class);

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);


        final AuditEventSubscriber auditEventSubscriber = new AuditEventSubscriberLoggingImpl();
        ActorRef auditEventBrokerActor = system.actorOf(AuditEventBrokerActor.props(
                auditEventSubscriber), "AuditEventBroker");
        final AuditEventSubscriber auditEventSubscriberAkka = new AuditEventSubscriberAkkaImpl(
                auditEventBrokerActor);

        final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(
                auditEventSubscriberAkka,
                new StatusEventSubscriberLoggingImpl(),
                new WorkerEventSubscriberLoggingImpl());

        IMantisStorageProvider storageProvider = new MantisStorageProviderAdapter(
                new SimpleCachedFileStorageProvider(),
                lifecycleEventPublisher);
        ActorRef jobClustersManager = system.actorOf(
                JobClustersManagerActor.props(
                        new MantisJobStore(storageProvider), lifecycleEventPublisher),
                "JobClustersManager");
        final FakeMantisScheduler fakeScheduler = new FakeMantisScheduler(jobClustersManager);
        jobClustersManager.tell(new JobClusterManagerProto.JobClustersManagerInitialize(
                fakeScheduler,
                true), ActorRef.noSender());


//        Schedulers.newThread().createWorker().schedulePeriodically(() -> jobClustersManager.tell(new NullPointerException(), ActorRef.noSender()),0, 100, TimeUnit.SECONDS);

        setupDummyAgentClusterAutoScaler();
        final JobClusterRouteHandler jobClusterRouteHandler = new JobClusterRouteHandlerAkkaImpl(
                jobClustersManager);
        final JobRouteHandler jobRouteHandler = new JobRouteHandlerAkkaImpl(jobClustersManager);

        MasterDescription masterDescription = new MasterDescription(
                "localhost",
                "127.0.0.1",
                port,
                port + 2,
                port + 4,
                "api/postjobstatus",
                port + 6,
                System.currentTimeMillis());
        final MasterDescriptionRoute masterDescriptionRoute = new MasterDescriptionRoute(
                masterDescription);

        Duration idleTimeout = system.settings()
                                     .config()
                                     .getDuration("akka.http.server.idle-timeout");
        logger.info("idle timeout {} sec ", idleTimeout.getSeconds());

        ActorRef agentsErrorMonitorActor = system.actorOf(
                AgentsErrorMonitorActor.props(),
                "AgentsErrorMonitor");
        ActorRef statusEventBrokerActor = system.actorOf(StatusEventBrokerActor.props(
                agentsErrorMonitorActor), "StatusEventBroker");

        agentsErrorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(
                fakeScheduler), ActorRef.noSender());
        final JobStatusRouteHandler jobStatusRouteHandler = new JobStatusRouteHandlerAkkaImpl(
                system,
                statusEventBrokerActor);
        final AgentClusterOperationsImpl agentClusterOperations = new AgentClusterOperationsImpl(
                storageProvider,
                new JobMessageRouterImpl(jobClustersManager),
                fakeScheduler,
                lifecycleEventPublisher,
                "cluster");

        final JobDiscoveryRouteHandler jobDiscoveryRouteHandler = new JobDiscoveryRouteHandlerAkkaImpl(
                jobClustersManager,
                idleTimeout);
        final JobRoute v0JobRoute = new JobRoute(jobRouteHandler, system);
        final JobDiscoveryRoute v0JobDiscoveryRoute = new JobDiscoveryRoute(jobDiscoveryRouteHandler);
        final JobClusterRoute v0JobClusterRoute = new JobClusterRoute(
                jobClusterRouteHandler,
                jobRouteHandler,
                system);
        final JobStatusRoute v0JobStatusRoute = new JobStatusRoute(jobStatusRouteHandler);
        final AgentClusterRoute v0AgentClusterRoute = new AgentClusterRoute(
                agentClusterOperations,
                system);

        final JobClustersRoute v1JobClustersRoute = new JobClustersRoute(jobClusterRouteHandler, system);
        final JobsRoute v1JobsRoute = new JobsRoute(jobClusterRouteHandler, jobRouteHandler, system);
        final AdminMasterRoute v1AdminMasterRoute = new AdminMasterRoute(masterDescription);
        final AgentClustersRoute v1AgentClustersRoute = new AgentClustersRoute(agentClusterOperations);
        final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute = new JobDiscoveryStreamRoute(jobDiscoveryRouteHandler);
        final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute = new LastSubmittedJobIdStreamRoute(jobDiscoveryRouteHandler);
        final JobStatusStreamRoute v1JobStatusStreamRoute = new JobStatusStreamRoute(jobStatusRouteHandler);

        LocalMasterMonitor localMasterMonitor = new LocalMasterMonitor(masterDescription);
        LeadershipManagerLocalImpl leadershipMgr = new LeadershipManagerLocalImpl(masterDescription);
        leadershipMgr.setLeaderReady();
        LeaderRedirectionFilter leaderRedirectionFilter = new LeaderRedirectionFilter(
                localMasterMonitor,
                leadershipMgr);
        final MantisMasterRoute app = new MantisMasterRoute(
                leaderRedirectionFilter,
                masterDescriptionRoute,
                v0JobClusterRoute,
                v0JobRoute,
                v0JobDiscoveryRoute,
                v0JobStatusRoute,
                v0AgentClusterRoute,
                v1JobClustersRoute,
                v1JobsRoute,
                v1AdminMasterRoute,
                v1AgentClustersRoute,
                v1JobDiscoveryStreamRoute,
                v1LastSubmittedJobIdStreamRoute,
                v1JobStatusStreamRoute
                );
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute()
                                                                      .flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost(
                        "localhost",
                        port),
                materializer);
        binding.exceptionally(failure -> {
            System.err.println("Something very bad happened! " + failure.getMessage());
            system.terminate();
            return null;
        });
//        Schedulers.newThread().createWorker().schedule(() -> leadershipMgr.stopBeingLeader(), 10, TimeUnit.SECONDS);
        System.out.println(
                "Server online at http://localhost:" + port + "/\nPress RETURN to stop...");
        System.in.read(); // let it run until user presses return

        binding
                .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
                .thenAccept(unbound -> system.terminate()); // and shutdown when done
    }
}