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

package io.mantisrx.server.master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.fenzo.AutoScaleAction;
import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.VirtualMachineLease;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.DeadLetterActor;
import io.mantisrx.master.JobClustersManagerActor;
import io.mantisrx.master.JobClustersManagerService;
import io.mantisrx.master.api.akka.MasterApiAkkaService;
import io.mantisrx.master.events.AuditEventBrokerActor;
import io.mantisrx.master.events.AuditEventSubscriber;
import io.mantisrx.master.events.AuditEventSubscriberAkkaImpl;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventBrokerActor;
import io.mantisrx.master.events.StatusEventSubscriber;
import io.mantisrx.master.events.StatusEventSubscriberAkkaImpl;
import io.mantisrx.master.events.WorkerEventSubscriber;
import io.mantisrx.master.events.WorkerRegistryV2;
import io.mantisrx.master.scheduler.AgentsErrorMonitorActor;
import io.mantisrx.master.scheduler.JobMessageRouterImpl;
import io.mantisrx.master.vm.AgentClusterOperationsImpl;
import io.mantisrx.master.zk.LeaderElector;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.metrics.MetricsPublisherService;
import io.mantisrx.server.core.metrics.MetricsServerService;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.master.config.ConfigurationFactory;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.server.master.mesos.MesosDriverSupplier;
import io.mantisrx.server.master.mesos.VirtualMachineMasterServiceMesosImpl;
import io.mantisrx.server.master.persistence.IMantisStorageProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.MantisStorageProviderAdapter;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.subjects.PublishSubject;


public class MasterMain implements Service {

    private static final Logger logger = LoggerFactory.getLogger(MasterMain.class);
    @Argument(alias = "p", description = "Specify a configuration file", required = false)
    private static String propFile = "master.properties";
    private final ServiceLifecycle mantisServices = new ServiceLifecycle();
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private CountDownLatch blockUntilShutdown = new CountDownLatch(1);
    private volatile CuratorService curatorService = null;
    private volatile AgentClusterOperationsImpl agentClusterOps = null;
    private MasterConfiguration config;
    private SchedulingService schedulingService;
    private ILeadershipManager leadershipManager;

    public MasterMain(ConfigurationFactory configFactory, AuditEventSubscriber auditEventSubscriber) {

        String test = "{\"jobId\":\"sine-function-1\",\"status\":{\"jobId\":\"sine-function-1\",\"stageNum\":1,\"workerIndex\":0,\"workerNumber\":2,\"type\":\"HEARTBEAT\",\"message\":\"heartbeat\",\"state\":\"Noop\",\"hostname\":null,\"timestamp\":1525813363585,\"reason\":\"Normal\",\"payloads\":[{\"type\":\"SubscriptionState\",\"data\":\"false\"},{\"type\":\"IncomingDataDrop\",\"data\":\"{\\\"onNextCount\\\":0,\\\"droppedCount\\\":0}\"}]}}";

        Metrics metrics = new Metrics.Builder()
                .id("MasterMain")
                .addCounter("masterInitSuccess")
                .addCounter("masterInitError")
                .build();
        Metrics m = MetricsRegistry.getInstance().registerAndGet(metrics);
        try {
            ConfigurationProvider.initialize(configFactory);
            this.config = ConfigurationProvider.getConfig();
            leadershipManager = new LeadershipManagerZkImpl(config, mantisServices);

            Thread t = new Thread(() -> shutdown());
            t.setDaemon(true);
            // shutdown hook
            Runtime.getRuntime().addShutdownHook(t);

            // shared state
            PublishSubject<String> vmLeaseRescindedSubject = PublishSubject.create();

            final ActorSystem system = ActorSystem.create("MantisMaster");
            // log dead letter messages
            final ActorRef actor = system.actorOf(Props.create(DeadLetterActor.class), "MantisDeadLetter");
            system.eventStream().subscribe(actor, DeadLetter.class);
            //final IMantisStorageProvider mantisStorageProvider = new SimpleCachedFileStorageProvider(false);

            ActorRef agentsErrorMonitorActor = system.actorOf(AgentsErrorMonitorActor.props(), "AgentsErrorMonitor");
            ActorRef statusEventBrokerActor = system.actorOf(StatusEventBrokerActor.props(agentsErrorMonitorActor), "StatusEventBroker");
            ActorRef auditEventBrokerActor = system.actorOf(AuditEventBrokerActor.props(auditEventSubscriber), "AuditEventBroker");
            final StatusEventSubscriber statusEventSubscriber = new StatusEventSubscriberAkkaImpl(statusEventBrokerActor);
            final AuditEventSubscriber auditEventSubscriberAkka = new AuditEventSubscriberAkkaImpl(auditEventBrokerActor);
            final WorkerEventSubscriber workerEventSubscriber = WorkerRegistryV2.INSTANCE;

            // TODO who watches actors created at this level?
            final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(auditEventSubscriberAkka, statusEventSubscriber, workerEventSubscriber);


            IMantisStorageProvider storageProvider = new MantisStorageProviderAdapter(this.config.getStorageProvider(), lifecycleEventPublisher);
            final MantisJobStore mantisJobStore = new MantisJobStore(storageProvider);
            final ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(mantisJobStore, lifecycleEventPublisher), "JobClustersManager");

            final JobMessageRouter jobMessageRouter = new JobMessageRouterImpl(jobClusterManagerActor);
            final WorkerRegistry workerRegistry = WorkerRegistryV2.INSTANCE;

            final MesosDriverSupplier mesosDriverSupplier = new MesosDriverSupplier(this.config, vmLeaseRescindedSubject,
                    jobMessageRouter,
                    workerRegistry);
            final VirtualMachineMasterServiceMesosImpl vmService = new VirtualMachineMasterServiceMesosImpl(
                    this.config,
                    getDescriptionJson(),
                    mesosDriverSupplier);
            schedulingService = new SchedulingService(jobMessageRouter, workerRegistry, vmLeaseRescindedSubject, vmService);
            mesosDriverSupplier.setAddVMLeaseAction(schedulingService::addOffers);

            // initialize agents error monitor
            agentsErrorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(schedulingService), ActorRef.noSender());

            final boolean loadJobsFromStoreOnInit = true;
            final JobClustersManagerService jobClustersManagerService = new JobClustersManagerService(jobClusterManagerActor, schedulingService, loadJobsFromStoreOnInit);

            this.agentClusterOps = new AgentClusterOperationsImpl(storageProvider,
                    jobMessageRouter,
                    schedulingService,
                    lifecycleEventPublisher,
                    ConfigurationProvider.getConfig().getActiveSlaveAttributeName());

            // start serving metrics
            if (config.getMasterMetricsPort() > 0)
                new MetricsServerService(config.getMasterMetricsPort(), 1, Collections.emptyMap()).start();
            new MetricsPublisherService(config.getMetricsPublisher(), config.getMetricsPublisherFrequencyInSeconds(),
                    new HashMap<>()).start();

            // services
            mantisServices.addService(vmService);
            mantisServices.addService(schedulingService);
            mantisServices.addService(jobClustersManagerService);
            mantisServices.addService(agentClusterOps);
            if (this.config.isLocalMode()) {
                leadershipManager.becomeLeader();
                mantisServices.addService(new MasterApiAkkaService(new LocalMasterMonitor(leadershipManager.getDescription()), leadershipManager.getDescription(), jobClusterManagerActor, statusEventBrokerActor,
                        config.getApiPort(), storageProvider, schedulingService, lifecycleEventPublisher, leadershipManager, agentClusterOps));
            } else {
                curatorService = new CuratorService(this.config, leadershipManager.getDescription());
                curatorService.start();
                mantisServices.addService(createLeaderElector(curatorService, leadershipManager));
                mantisServices.addService(new MasterApiAkkaService(curatorService.getMasterMonitor(), leadershipManager.getDescription(), jobClusterManagerActor, statusEventBrokerActor,
                        config.getApiPort(), storageProvider, schedulingService, lifecycleEventPublisher, leadershipManager, agentClusterOps));
            }
            m.getCounter("masterInitSuccess").increment();
        } catch (Exception e) {
            logger.error("caught exception on Mantis Master initialization", e);
            m.getCounter("masterInitError").increment();
            shutdown();
            System.exit(1);
        }
    }

    private static Properties loadProperties(String propFile) {
        // config
        Properties props = new Properties();
        try (InputStream in = findResourceAsStream(propFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Can't load properties from the given property file %s: %s", propFile, e.getMessage()), e);
        }
        return props;
    }


    /**
     * Finds the given resource and returns its input stream. This method seeks the file first from the current working directory,
     * and then in the class path.
     *
     * @param resourceName the name of the resource. It can either be a file name, or a path.
     *
     * @return An {@link java.io.InputStream} instance that represents the found resource. Null otherwise.
     *
     * @throws FileNotFoundException
     */
    private static InputStream findResourceAsStream(String resourceName) throws FileNotFoundException {
        File resource = new File(resourceName);
        if (resource.exists()) {
            return new FileInputStream(resource);
        }

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (is == null) {
            throw new FileNotFoundException(String.format("Can't find property file %s. Make sure the property file is either in your path or in your classpath ", resourceName));
        }

        return is;
    }

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
        AgentClustersAutoScaler.initialize(() -> new HashSet<>(Collections.singletonList(dummyAutoScaleRule)), new Observer<AutoScaleAction>() {
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

    public static void main(String[] args) {
        try {
            Args.parse(MasterMain.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(MasterMain.class);
            System.exit(1);
        }

        try {
            StaticPropertiesConfigurationFactory factory = new StaticPropertiesConfigurationFactory(loadProperties(propFile));
            setupDummyAgentClusterAutoScaler();
            final AuditEventSubscriber auditEventSubscriber = new AuditEventSubscriberLoggingImpl();
            MasterMain master = new MasterMain(factory, auditEventSubscriber);
            master.start(); // blocks until shutdown hook (ctrl-c)
        } catch (Exception e) {
            // unexpected to get a RuntimeException, will exit
            logger.error("Unexpected error: " + e.getMessage(), e);
            System.exit(2);
        }
    }

    private LeaderElector createLeaderElector(CuratorService curatorService,
                                              ILeadershipManager leadershipManager) {
        return LeaderElector.builder(leadershipManager)
                .withCurator(curatorService.getCurator())
                .withJsonMapper(DefaultObjectMapper.getInstance())
                .withElectionPath(ZKPaths.makePath(config.getZkRoot(), config.getLeaderElectionPath()))
                .withAnnouncementPath(ZKPaths.makePath(config.getZkRoot(), config.getLeaderAnnouncementPath()))
                .build();
    }

    @Override
    public void start() {
        logger.info("Starting Mantis Master");
        mantisServices.start();

        try {
            blockUntilShutdown.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void enterActiveMode() {
    }

    @Override
    public void shutdown() {
        if (shutdownInitiated.compareAndSet(false, true)) {
            logger.info("Shutting down Mantis Master");
            mantisServices.shutdown();
            boolean shutdownCuratorEnabled = ConfigurationProvider.getConfig().getShutdownCuratorServiceEnabled();
            if (curatorService != null && shutdownCuratorEnabled) {
                logger.info("Shutting down Curator Service");
                curatorService.shutdown();
            } else {
                logger.info("not shutting down curator service {} shutdownEnabled? {}", curatorService, shutdownCuratorEnabled);
            }
            blockUntilShutdown.countDown();
        } else
            logger.info("Shutdown already initiated, not starting again");
    }

    public MasterConfiguration getConfig() {
        return config;
    }

    public String getDescriptionJson() {
        try {
            return DefaultObjectMapper.getInstance().writeValueAsString(leadershipManager.getDescription());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(String.format("Failed to convert the description %s to JSON: %s", leadershipManager.getDescription(), e.getMessage()), e);
        }
    }

    public AgentClusterOperationsImpl getAgentClusterOps() {
        return agentClusterOps;
    }

    public Consumer<String> getAgentVMEnabler() {
        return schedulingService::enableVM;
    }

    public Observable<MasterDescription> getMasterObservable() {
        return curatorService == null ?
                Observable.empty() :
                curatorService.getMasterMonitor().getMasterObservable();
    }

    public boolean isLeader() {
        return leadershipManager.isLeader();
    }
}