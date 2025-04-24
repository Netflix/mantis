/*
 * Copyright 2025 Netflix, Inc.
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

package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.server.core.MantisAkkaRpcSystemLoader;
import io.mantisrx.server.core.Service;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Path;

/**
 * A component loader for JobMasterServiceV2 that uses Flink's ComponentClassLoader
 * to load and instantiate JobMasterServiceV2 with proper class isolation.
 */
public class JobMasterComponentLoader {
    private static final Logger logger = LoggerFactory.getLogger(JobMasterComponentLoader.class);

    private final ComponentClassLoader componentLoader;

    public ComponentClassLoader getComponentLoader() {
        return componentLoader;
    }

    /**
     * Creates a new JobMasterComponentLoader with the provided ComponentClassLoader.
     *
     * @param componentLoader The ComponentClassLoader to use for loading JobMasterServiceV2
     */
    public JobMasterComponentLoader(ComponentClassLoader componentLoader) {
        this.componentLoader = componentLoader;
    }

    public static JobMasterComponentLoader fromAkkaRpc() {
        try {
            final ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
            Path createRpcAkkaJarFromResource =
                MantisAkkaRpcSystemLoader.createRpcAkkaJarFromResource(parentLoader);

            Path createMantisJMAkkaJarFromResource =
                MantisAkkaRpcSystemLoader.createTemporaryJarFromResource(
                    parentLoader, "mantis-jm-akka.jar", "mantis-jm-akka-jar");
            logger.info("using createMantisJMAkkaJarFromResource: {}", createMantisJMAkkaJarFromResource);

            final String parentFirstPkg = "org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback";
            final String jmPrefix = "io.mantisrx"; // "io.mantisrx.server.worker.jobmaster";
            ComponentClassLoader componentLoader = new ComponentClassLoader(
                new URL[]{createMantisJMAkkaJarFromResource.toUri().toURL(), createRpcAkkaJarFromResource.toUri().toURL()},
                parentLoader,
                CoreOptions.parseParentFirstLoaderPatterns(parentFirstPkg + ";io.mantisrx;rx;org;com", ""),
                new String[]{jmPrefix, "akka", "scala"});
            return new JobMasterComponentLoader(componentLoader);
        } catch (Exception e) {
            logger.error("Failed to create RpcAkkaJarFromResource", e);
            throw new RuntimeException("Failed to create RpcAkkaJarFromResource", e);
        }
    }

    /**
     * Creates a new instance of JobMasterServiceV2 using the ComponentClassLoader.
     *
     * @param context The JobScalerContext to pass to the JobMasterServiceV2 constructor
     * @return A new instance of JobMasterServiceV2 as a Service
     * @throws RuntimeException if there's an error loading or instantiating JobMasterServiceV2
     */
    public Service createJobMasterServiceV2(JobScalerContext context) {
//        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // Set the component class loader as the thread context class loader
            Thread.currentThread().setContextClassLoader(componentLoader);

            // Load the JobMasterServiceV2 class using the component class loader
            Class<?> jobMasterServiceClass = componentLoader.loadClass(
                "io.mantisrx.server.worker.jobmaster.akka.JobMasterServiceV2");
            logger.info("Successfully loaded JobMasterServiceV2 class using ComponentClassLoader");

            Constructor<?> constructor = jobMasterServiceClass.getConstructor(JobScalerContext.class);

            Object jobMasterServiceInstance = constructor.newInstance(context);

            logger.info("Successfully created JobMasterServiceV2 instance using ComponentClassLoader");

            ((Service) jobMasterServiceInstance).start();
            logger.info("JobMasterServiceV2 started");

            // Cast and return as Service
            return (Service) jobMasterServiceInstance;
        } catch (ClassNotFoundException e) {
            logger.error("Failed to load JobMasterServiceV2 class", e);
            throw new RuntimeException("Failed to load JobMasterServiceV2 class", e);
        } catch (NoSuchMethodException e) {
            logger.error("JobMasterServiceV2 constructor not found", e);
            throw new RuntimeException("JobMasterServiceV2 constructor not found", e);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            logger.error("Failed to instantiate JobMasterServiceV2", e);
            throw new RuntimeException("Failed to instantiate JobMasterServiceV2", e);
        } finally {
            // Restore the original context class loader
            //Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
}
