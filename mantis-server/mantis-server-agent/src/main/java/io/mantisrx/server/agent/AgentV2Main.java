/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.agent;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.runtime.loader.RuntimeTask;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.server.agent.config.ConfigurationFactory;
import io.mantisrx.server.agent.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.server.core.Service;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentV2Main implements Service {
    private static final Logger logger = LoggerFactory.getLogger(AgentV2Main.class);
    @Argument(alias = "p", description = "Specify a configuration file", required = false)
    private static String propFile = "agent.properties";

    private final TaskExecutorStarter taskExecutorStarter;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public AgentV2Main(ConfigurationFactory configFactory, MantisPropertiesLoader propertiesLoader) throws Exception {
        WorkerConfiguration workerConfiguration = configFactory.getConfig();

        this.taskExecutorStarter =
            TaskExecutorStarter.builder(workerConfiguration)
                .taskFactory(new SingleTaskOnlyFactory())
                .propertiesLoader(propertiesLoader)
                .addListener(
                    new TaskExecutor.Listener() {
                        @Override
                        public void onTaskStarting(RuntimeTask task) {}

                        @Override
                        public void onTaskFailed(RuntimeTask task, Throwable throwable) {
                            // Something failed, at this point we could log it and perform cleanup actions.
                            // For now we will just exit.
                            logger.error("Task {} failed", task, throwable);
                            if (!isStopping()) {
                                System.exit(1);
                            }
                        }

                        @Override
                        public void onTaskCancelling(RuntimeTask task) {}

                        @Override
                        public void onTaskCancelled(RuntimeTask task, @Nullable Throwable throwable) {
                            // Task got cancelled, at this point we could log it and perform cleanup
                            // actions.
                            // For now we will just exit.
                            if (throwable != null) {
                                logger.error("Task {} cancellation failed", task, throwable);
                            }
                            if (!isStopping()) {
                                System.exit(1);
                            }
                        }
                    },
                    MoreExecutors.directExecutor())
                .build();
    }

    private boolean isStopping() {
        return stopping.get();
    }

    public static void main(String[] args) {
        try {
            Args.parse(AgentV2Main.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(AgentV2Main.class);
            System.exit(1);
        }

        try {
            Properties props = new Properties();
            props.putAll(System.getenv());
            props.putAll(System.getProperties());
            props.putAll(loadProperties(propFile));
            StaticPropertiesConfigurationFactory factory = new StaticPropertiesConfigurationFactory(props);
            DefaultMantisPropertiesLoader propertiesLoader = new DefaultMantisPropertiesLoader(props);
            AgentV2Main agent = new AgentV2Main(factory, propertiesLoader);
            agent.start(); // blocks until shutdown hook (ctrl-c)
        } catch (Exception e) {
            // unexpected to get a RuntimeException, will exit
            logger.error("Unexpected error: " + e.getMessage(), e);
            System.exit(2);
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

    @Override
    public void start() {
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(() -> shutdown()));

        try {
            taskExecutorStarter.startAsync().awaitRunning(2, TimeUnit.MINUTES);
            taskExecutorStarter.awaitTerminated();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        stopping.set(true);
        try {
            logger.info("Received signal to shutdown; shutting down task executor");
            taskExecutorStarter.stopAsync().awaitTerminated(2, TimeUnit.MINUTES);
        } catch (Throwable e) {
            logger.error("Failed to stop gracefully", e);
        }
    }

    @Override
    public void enterActiveMode() {

    }
}
