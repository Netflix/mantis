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

import static org.apache.flink.configuration.GlobalConfiguration.loadConfiguration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
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
import io.mantisrx.master.events.WorkerMetricsCollector;
import io.mantisrx.master.events.WorkerRegistryV2;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.resourcecluster.ResourceClustersAkkaImpl;
import io.mantisrx.master.resourcecluster.ResourceClustersHostManagerActor;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProviderAdapter;
import io.mantisrx.master.scheduler.AgentsErrorMonitorActor;
import io.mantisrx.master.scheduler.JobMessageRouterImpl;
import io.mantisrx.master.vm.AgentClusterOperationsImpl;
import io.mantisrx.master.zk.LeaderElector;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.metrics.MetricsPublisherService;
import io.mantisrx.server.core.metrics.MetricsServerService;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.master.config.ConfigurationFactory;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MantisExtensionFactory;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.server.master.mesos.MesosDriverSupplier;
import io.mantisrx.server.master.mesos.VirtualMachineMasterServiceMesosImpl;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactoryImpl;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.org.apache.curator.utils.ZKPaths;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
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
    private KeyValueBasedPersistenceProvider storageProvider;
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
            // log the configuration of the actor system
            system.logConfiguration();

            // log dead letter messages
            final ActorRef actor = system.actorOf(Props.create(DeadLetterActor.class), "MantisDeadLetter");
            system.eventStream().subscribe(actor, DeadLetter.class);

            ActorRef agentsErrorMonitorActor = system.actorOf(AgentsErrorMonitorActor.props(), "AgentsErrorMonitor");
            ActorRef statusEventBrokerActor = system.actorOf(StatusEventBrokerActor.props(agentsErrorMonitorActor), "StatusEventBroker");
            ActorRef auditEventBrokerActor = system.actorOf(AuditEventBrokerActor.props(auditEventSubscriber), "AuditEventBroker");
            final StatusEventSubscriber statusEventSubscriber = new StatusEventSubscriberAkkaImpl(statusEventBrokerActor);
            final AuditEventSubscriber auditEventSubscriberAkka = new AuditEventSubscriberAkkaImpl(auditEventBrokerActor);
            final WorkerEventSubscriber workerEventSubscriber = WorkerRegistryV2.INSTANCE;
            final WorkerMetricsCollector workerMetricsCollector = new WorkerMetricsCollector(
                Duration.ofMinutes(5), // cleanup jobs after 5 minutes
                Duration.ofMinutes(1), // check every 1 minute for jobs to be cleaned up
                Clock.systemDefaultZone());
            mantisServices.addService(BaseService.wrap(workerMetricsCollector));

            // TODO who watches actors created at this level?
            final LifecycleEventPublisher lifecycleEventPublisher =
                new LifecycleEventPublisherImpl(auditEventSubscriberAkka, statusEventSubscriber,
                    workerEventSubscriber.and(workerMetricsCollector));

            storageProvider = new KeyValueBasedPersistenceProvider(this.config.getStorageProvider(), lifecycleEventPublisher);
            final MantisJobStore mantisJobStore = new MantisJobStore(storageProvider);
            final CostsCalculator costsCalculator =
                MantisExtensionFactory.createObject(this.config.getCostsCalculatorClass(),
                    configFactory.getProperties());
            final ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(mantisJobStore, lifecycleEventPublisher, costsCalculator), "JobClustersManager");
            final JobMessageRouter jobMessageRouter = new JobMessageRouterImpl(jobClusterManagerActor);

            // Beginning of new stuff
            Configuration configuration = loadConfiguration();

            final ActorRef resourceClustersHostActor = system.actorOf(
                ResourceClustersHostManagerActor.props(
                    new ResourceClusterProviderAdapter(this.config.getResourceClusterProvider(), system),
                    storageProvider),
                "ResourceClusterHostActor");

            final RpcSystem rpcSystem =
                MantisAkkaRpcSystemLoader.getInstance();
            // the RPCService implementation will only be used for communicating with task executors but not for running a server itself.
            // Thus, there's no need for any valid external and bind addresses.
            final RpcService rpcService =
                RpcUtils.createRemoteRpcService(rpcSystem, configuration, null, "6123", null, Optional.empty());
            final ResourceClusters resourceClusters =
                ResourceClustersAkkaImpl.load(
                    getConfig(),
                    rpcService,
                    system,
                    mantisJobStore,
                    jobMessageRouter,
                    resourceClustersHostActor,
                    storageProvider);

            // end of new stuff
            final WorkerRegistry workerRegistry = WorkerRegistryV2.INSTANCE;

            if (config.getMesosEnabled()) {
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
                this.agentClusterOps = new AgentClusterOperationsImpl(storageProvider,
                    jobMessageRouter,
                    schedulingService,
                    lifecycleEventPublisher,
                    ConfigurationProvider.getConfig().getActiveSlaveAttributeName());

                // services
                mantisServices.addService(vmService);
                mantisServices.addService(schedulingService);
                mantisServices.addService(agentClusterOps);
            }

            final MantisSchedulerFactory mantisSchedulerFactory =
                new MantisSchedulerFactoryImpl(system, resourceClusters, new ExecuteStageRequestFactory(getConfig()), jobMessageRouter, schedulingService, getConfig(), MetricsRegistry.getInstance());

            final boolean loadJobsFromStoreOnInit = true;
            final JobClustersManagerService jobClustersManagerService = new JobClustersManagerService(jobClusterManagerActor, mantisSchedulerFactory, loadJobsFromStoreOnInit);

            // start serving metrics
            if (config.getMasterMetricsPort() > 0) {
                new MetricsServerService(config.getMasterMetricsPort(), 1, Collections.emptyMap()).start();
            }
            new MetricsPublisherService(config.getMetricsPublisher(), config.getMetricsPublisherFrequencyInSeconds(),
                    new HashMap<>()).start();

            // services
            mantisServices.addService(jobClustersManagerService);

            if (this.config.isLocalMode()) {
                mantisServices.addService(new MasterApiAkkaService(new LocalMasterMonitor(leadershipManager.getDescription()), leadershipManager.getDescription(), jobClusterManagerActor, statusEventBrokerActor,
                       resourceClusters, resourceClustersHostActor, config.getApiPort(), storageProvider, lifecycleEventPublisher, leadershipManager, agentClusterOps));
                leadershipManager.becomeLeader();
            } else {
                curatorService = new CuratorService(this.config);
                curatorService.start();
                mantisServices.addService(createLeaderElector(curatorService, leadershipManager));
                mantisServices.addService(new MasterApiAkkaService(curatorService.getMasterMonitor(), leadershipManager.getDescription(), jobClusterManagerActor, statusEventBrokerActor,
                       resourceClusters, resourceClustersHostActor, config.getApiPort(), storageProvider, lifecycleEventPublisher, leadershipManager, agentClusterOps));
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
            logger.info("mantis services shutdown complete");
            boolean shutdownCuratorEnabled = ConfigurationProvider.getConfig().getShutdownCuratorServiceEnabled();
            if (curatorService != null && shutdownCuratorEnabled) {
                logger.info("Shutting down Curator Service");
                curatorService.shutdown();
            } else {
                logger.info("not shutting down curator service {} shutdownEnabled? {}", curatorService, shutdownCuratorEnabled);
            }
            blockUntilShutdown.countDown();
            logger.info("Mantis Master shutdown done");
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
        if (schedulingService != null) {
            return schedulingService::enableVM;
        } else {
            return null;
        }
    }

    public Observable<MasterDescription> getMasterObservable() {
        return curatorService == null ?
                Observable.empty() :
                curatorService.getMasterMonitor().getMasterObservable();
    }

    public boolean isLeader() {
        return leadershipManager.isLeader();
    }

    public IMantisPersistenceProvider getStorageProvider() {
        return storageProvider;
    }
}
