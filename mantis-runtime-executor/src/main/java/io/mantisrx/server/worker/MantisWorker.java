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

package io.mantisrx.server.worker;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.common.metrics.netty.MantisNettyEventsListenerFactory;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.loader.ClassLoaderHandle;
import io.mantisrx.runtime.loader.SinkSubscriptionStateHandler;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.WrappedExecuteStageRequest;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.config.ConfigurationFactory;
import io.mantisrx.server.worker.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import mantis.io.reactivex.netty.RxNetty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;
import rx.subjects.PublishSubject;

/**
 * This class is the executable entry point for the worker. It constructs the related components (LeaderService),
 * and starts them.
 */
public class MantisWorker extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(MantisWorker.class);
    @Argument(alias = "p", description = "Specify a configuration file", required = false)
    private static String propFile = "worker.properties";
    private CountDownLatch blockUntilShutdown = new CountDownLatch(1);

    //    static {
    //    	RxNetty.useNativeTransportIfApplicable();
    //    }
    private List<Service> mantisServices = new LinkedList<Service>();

    public MantisWorker(ConfigurationFactory configFactory, io.mantisrx.server.master.client.config.ConfigurationFactory coreConfigFactory) {
        this(configFactory, Optional.empty());
    }

    public MantisWorker(ConfigurationFactory configFactory, Optional<Job> jobToRun) {
        // for rxjava
        System.setProperty("rx.ring-buffer.size", "1024");

        WorkerConfiguration config = configFactory.getConfig();
        final HighAvailabilityServices highAvailabilityServices =
            HighAvailabilityServicesUtil.createHAServices(config);
        mantisServices.add(new Service() {
            @Override
            public void start() {
                highAvailabilityServices.startAsync().awaitRunning();
            }

            @Override
            public void shutdown() {
                highAvailabilityServices.stopAsync().awaitTerminated();
            }

            @Override
            public void enterActiveMode() {

            }

            @Override
            public String toString() {
                return "HighAvailabilityServices Service";
            }
        });
        final MantisMasterGateway gateway =
            highAvailabilityServices.getMasterClientApi();
        // shutdown hook
        Thread t = new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        };
        t.setDaemon(true);
        Runtime.getRuntime().addShutdownHook(t);

        // services
        // metrics

        PublishSubject<WrappedExecuteStageRequest> executeStageSubject = PublishSubject.create();
        // TODO(sundaram): inline services are hard to read. Would be good to refactor this.
        mantisServices.add(new Service() {
            private RuntimeTaskImpl runtimeTaskImpl;
            private Subscription vmStatusSubscription;

            @Override
            public void start() {
                final ClassLoader classLoader;
                if (Thread.currentThread().getContextClassLoader() == null) {
                    classLoader = ClassLoader.getSystemClassLoader();
                    logger.info("Choosing system classloader {}", classLoader);
                } else {
                    classLoader = Thread.currentThread().getContextClassLoader();
                    logger.info("Choosing current thread classloader {}", classLoader);
                }

                executeStageSubject
                    .asObservable()
                    .first()
                    .subscribe(wrappedRequest -> {
                        try {
                            runtimeTaskImpl = new RuntimeTaskImpl();

                            // invoke internal runtimeTaskImpl initialize to inject the wrapped request.
                            runtimeTaskImpl.initialize(
                                wrappedRequest,
                                config,
                                gateway,
                                ClassLoaderHandle.fixed(classLoader).getOrResolveClassLoader(
                                    ImmutableList.of(), ImmutableList.of()),
                                SinkSubscriptionStateHandler
                                    .Factory
                                    .forEphemeralJobsThatNeedToBeKilledInAbsenceOfSubscriber(
                                        gateway,
                                        Clock.systemDefaultZone()));
                            runtimeTaskImpl.setJob(jobToRun);

                            runtimeTaskImpl.startAsync();
                        } catch (Exception ex) {
                            logger.error("Failed to start task, request: {}", wrappedRequest, ex);
                            throw new RuntimeException("Failed to start task", ex);
                        }
                    });
            }

            @Override
            public void shutdown() {
                if (runtimeTaskImpl != null) {
                    try {
                        runtimeTaskImpl.stopAsync().awaitTerminated();
                    } finally {
                        vmStatusSubscription.unsubscribe();
                    }
                }
            }

            @Override
            public void enterActiveMode() {

            }

            @Override
            public String toString() {
                return "TaskService";
            }
        });
        /* To run MantisWorker locally in IDE, use VirualMachineWorkerServiceLocalImpl instead
        WorkerTopologyInfo.Data workerData = new WorkerTopologyInfo.Data(data.getJobName(), data.getJobId(),
            data.getWorkerIndex(), data.getWorkerNumber(), data.getStageNumber(), data.getNumStages(), -1, -1, data.getMetricsPort());
        mantisServices.add(new VirtualMachineWorkerServiceLocalImpl(workerData, executeStageSubject, vmTaskStatusSubject));
        */
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
     * @return An {@link java.io.InputStream} instance that represents the found resource. Null otherwise.
     * @throws java.io.FileNotFoundException
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

    public static void main(String[] args) {
        try {
            Args.parse(MantisWorker.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(MantisWorker.class);
            System.exit(1);
        }

        try {
            StaticPropertiesConfigurationFactory workerConfigFactory = new StaticPropertiesConfigurationFactory(loadProperties(propFile));
            io.mantisrx.server.master.client.config.StaticPropertiesConfigurationFactory coreConfigFactory =
                new io.mantisrx.server.master.client.config.StaticPropertiesConfigurationFactory(loadProperties(propFile));

            MantisWorker worker = new MantisWorker(workerConfigFactory, coreConfigFactory);
            worker.start();
        } catch (Exception e) {
            // unexpected to get runtime exception, will exit
            logger.error("Unexpected error: " + e.getMessage(), e);
            System.exit(2);
        }
    }

    @Override
    public void start() {
        startUp();
        awaitTerminated();
    }

    /**
     * The difference between this method and MantisWorker::start is that this doesn't wait for the service to be shut down
     * while start gets blocked on the service to be shutdown.
     *
     * The reason it is this way is because of existing usages which depend upon this behavior. In the future, once
     * mantis migrates off the old architecture, this can be removed.
     */
    public void startUp() {
        logger.info("Starting Mantis Worker");
        RxNetty.useMetricListenersFactory(new MantisNettyEventsListenerFactory());
        for (Service service : mantisServices) {
            logger.info("Starting service: " + service);
            try {
                service.start();
            } catch (Throwable e) {
                logger.error(String.format("Failed to start service %s: %s", service, e.getMessage()), e);
                throw e;
            }
            logger.info("Started service: " + service);
        }

        logger.info("Started Mantis Worker successfully");
    }

    public void awaitTerminated() {
        try {
            blockUntilShutdown.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down Mantis Worker");
        for (Service service : mantisServices) {
            service.shutdown();
        }
        blockUntilShutdown.countDown();
    }

    @Override
    public void enterActiveMode() {
    }
}
