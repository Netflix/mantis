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
import com.netflix.spectator.api.DefaultRegistry;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
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
import io.mantisrx.master.resourcecluster.ResourceClustersAkkaImpl;
import io.mantisrx.master.resourcecluster.ResourceClustersHostManagerActor;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterProviderAdapter;
import io.mantisrx.master.scheduler.JobMessageRouterImpl;
import io.mantisrx.master.zk.ZookeeperLeadershipFactory;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ILeaderElectorFactory;
import io.mantisrx.server.core.ILeaderMonitorFactory;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.master.LocalLeaderFactory;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.metrics.MetricsPublisherService;
import io.mantisrx.server.core.metrics.MetricsServerService;
import io.mantisrx.server.core.utils.ConfigUtils;
import io.mantisrx.server.master.config.ConfigurationFactory;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.persistence.KeyValueBasedPersistenceProvider;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactoryImpl;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class MasterMain implements Service {

    private static final Logger logger = LoggerFactory.getLogger(MasterMain.class);
    @Argument(alias = "p", description = "Specify a configuration file", required = false)
    private static String propFile = "master.properties";
    private final ServiceLifecycle mantisServices = new ServiceLifecycle();
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private KeyValueBasedPersistenceProvider storageProvider;
    private final CountDownLatch blockUntilShutdown = new CountDownLatch(1);
    private MasterConfiguration config;
    private ILeadershipManager leadershipManager;
    private MasterMonitor monitor;

    public MasterMain(
        ConfigurationFactory configFactory,
        MantisPropertiesLoader dynamicPropertiesLoader,
        AuditEventSubscriber auditEventSubscriber) {
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
            leadershipManager = new LeadershipManagerImpl(config, mantisServices);

            Thread t = new Thread(this::shutdown);
            t.setDaemon(true);
            // shutdown hook
            Runtime.getRuntime().addShutdownHook(t);

            final ActorSystem system = ActorSystem.create("MantisMaster");
            // log the configuration of the actor system
            system.logConfiguration();

            // log dead letter messages
            final ActorRef actor = system.actorOf(Props.create(DeadLetterActor.class), "MantisDeadLetter");
            system.eventStream().subscribe(actor, DeadLetter.class);

            ActorRef statusEventBrokerActor = system.actorOf(StatusEventBrokerActor.props(), "StatusEventBroker");
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

            final ActorRef jobClusterManagerActor = system.actorOf(JobClustersManagerActor.props(mantisJobStore, lifecycleEventPublisher, config.getJobCostsCalculator(), config.getSlaMaxHeadroomForAccepted()), "JobClustersManager");
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
                    configFactory,
                    rpcService,
                    system,
                    mantisJobStore,
                    jobMessageRouter,
                    resourceClustersHostActor,
                    storageProvider,
                    dynamicPropertiesLoader);

            // end of new stuff
            final MantisSchedulerFactory mantisSchedulerFactory =
                new MantisSchedulerFactoryImpl(system, resourceClusters, new ExecuteStageRequestFactory(getConfig()), jobMessageRouter, getConfig(), MetricsRegistry.getInstance());

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

            // set up leader election
            final ILeaderElectorFactory leaderFactory;
            final String fqcnLeaderFactory = config.getLeaderElectorFactory();
            if(!config.isLocalMode() && ConfigUtils.createInstance(fqcnLeaderFactory, ILeaderElectorFactory.class) instanceof LocalLeaderFactory) {
                logger.warn("local mode is {} and leader factory is {} this configuration is unsafe", config.isLocalMode(), config.getLeaderElectorFactory().getClass().getSimpleName());
                final ZookeeperLeadershipFactory zkLeadership = new ZookeeperLeadershipFactory();
                leaderFactory = zkLeadership;
                monitor = zkLeadership.createLeaderMonitor(config);
                logger.warn("using default non-local Zookeeper leader services you should set: "+
                    "mantis.leader.elector.factory=io.mantisrx.master.zk.ZookeeperLeadershipFactory");
            } else {
                leaderFactory = ConfigUtils.createInstance(fqcnLeaderFactory, ILeaderElectorFactory.class);
                monitor = ConfigUtils.createInstance(config.getLeaderMonitorFactoryName(), ILeaderMonitorFactory.class).createLeaderMonitor(config);
                logger.warn("using leader factory {}", config.isLocalMode());
            }
            monitor.start();
            mantisServices.addService(leaderFactory.createLeaderElector(config, leadershipManager));
            mantisServices.addService(new MasterApiAkkaService(monitor, leadershipManager.getDescription(), jobClusterManagerActor, statusEventBrokerActor,
                resourceClusters, resourceClustersHostActor, config.getApiPort(), storageProvider, lifecycleEventPublisher, leadershipManager));

            if (leaderFactory instanceof LocalLeaderFactory && !config.isLocalMode()) {
                logger.error("local mode is [ {} ] and leader factory is {} this configuration is unsafe", config.isLocalMode(), leaderFactory.getClass().getSimpleName());
                throw new RuntimeException("leader election is local but local mode is not enabled");
            }

            m.getCounter("masterInitSuccess").increment();
        } catch (Exception e) {
            logger.error("caught exception on Mantis Master initialization", e);
            m.getCounter("masterInitError").increment();
            shutdown();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        try {
            Args.parse(MasterMain.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(MasterMain.class);
            System.exit(1);
        }

        try {
            SpectatorRegistryFactory.setRegistry(new DefaultRegistry());
            Properties props = new Properties();
            props.putAll(System.getenv());
            props.putAll(System.getProperties());
            props.putAll(ConfigUtils.loadProperties(propFile));
            StaticPropertiesConfigurationFactory factory = new StaticPropertiesConfigurationFactory(props);
            final AuditEventSubscriber auditEventSubscriber = new AuditEventSubscriberLoggingImpl();
            final MantisPropertiesLoader propertiesLoader = new DefaultMantisPropertiesLoader(System.getProperties());
            MasterMain master = new MasterMain(factory, propertiesLoader, auditEventSubscriber);
            master.start(); // blocks until shutdown hook (ctrl-c)
        } catch (Exception e) {
            // unexpected to get a RuntimeException, will exit
            logger.error("Unexpected error: " + e.getMessage(), e);
            System.exit(2);
        }
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
            blockUntilShutdown.countDown();
            logger.info("Mantis Master shutdown done");
        } else
            logger.info("Shutdown already initiated, not starting again");
    }

    public MasterConfiguration getConfig() {
        return config;
    }

    public Observable<MasterDescription> getMasterObservable() {
        return monitor == null ?
            Observable.empty() :
            monitor.getMasterObservable();
    }

    public boolean isLeader() {
        return leadershipManager.isLeader();
    }

    public IMantisPersistenceProvider getStorageProvider() {
        return storageProvider;
    }
}
