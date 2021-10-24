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
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.Service;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.worker.config.ConfigurationFactory;
import io.mantisrx.server.worker.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.server.worker.config.WorkerConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import mantis.io.reactivex.netty.RxNetty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the executable entry point for the worker. It constructs the related components (LeaderService),
 * and starts them.
 */
public class MantisWorker extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(MantisWorker.class);
    @Argument(alias = "p", description = "Specify a configuration file", required = false)
    private static String propFile = "worker.properties";

    private final ClassLoaderHandle classLoaderHandle;
    private CountDownLatch blockUntilShutdown = new CountDownLatch(1);

    //    static {
    //    	RxNetty.useNativeTransportIfApplicable();
    //    }
    private List<Service> mantisServices = new LinkedList<Service>();

    // todo(sundaram): Consolidate configurations into one class that can be used across all components.
    public MantisWorker(ConfigurationFactory configFactory) throws IOException {
        // for rxjava
        System.setProperty("rx.ring-buffer.size", "1024");

        WorkerConfiguration config = configFactory.getConfig();
        FileSystem.initialize();

        this.classLoaderHandle =
            new DefaultClassLoaderHandle(
                BlobStoreFactory.get(config.getClusterStorageDir(), config.getLocalStorageDir()),
                config.getAlwaysParentFirstLoaderPatterns());
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
//        Data data = WorkerTopologyInfo.Reader.getData();

//        mantisServices.add(new MetricsPublisherService(config.getMetricsPublisher(), config.getMetricsPublisherFrequencyInSeconds(),
//                commonTags));
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

            MantisWorker worker = new MantisWorker(workerConfigFactory);
            worker.start();
        } catch (Exception e) {
            // unexpected to get runtime exception, will exit
            logger.error("Unexpected error: " + e.getMessage(), e);
            System.exit(2);
        }
    }

    @Override
    public void start() {
        logger.info("Starting Mantis Worker");
        RxNetty.useMetricListenersFactory(new MantisNettyEventsListenerFactory());
        for (Service service : mantisServices) {
            logger.info("Starting service: " + service.getClass().getName());
            try {
                service.start();
            } catch (Throwable e) {
                logger.error(String.format("Failed to start service %s: %s", service, e.getMessage()), e);
                throw e;
            }
        }

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

    public MasterDescription getInitialMasterDescription() {
        String prop = System.getProperty("MASTER_DESCRIPTION");
        try {
            logger.info("The initial master description: " + prop);
            return DefaultObjectMapper.getInstance().readValue(prop, MasterDescription.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Can't convert master description %s to an object: %s", prop, e.getMessage()), e);
        }
    }

    public Optional<String> getJobProviderClass() {
        String jobProviderClass = System.getProperty("JOB_PROVIDER_CLASS");
        logger.info("JOB_PROVIDER_CLASS: " + jobProviderClass);
        if (jobProviderClass == null || jobProviderClass.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(jobProviderClass);
    }

    @Override
    public void enterActiveMode() {}
}
