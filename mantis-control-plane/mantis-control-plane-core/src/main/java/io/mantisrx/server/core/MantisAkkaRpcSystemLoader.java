/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemLoader;
import org.apache.flink.util.IOUtils;

/**
 * RpcSystemLoader for mantis task executor and other services that need to expose an RPC API.
 * This particular implementation uses the akka RPC implementation under the hood.
 */
@Slf4j
public class MantisAkkaRpcSystemLoader implements RpcSystemLoader {

    private static final RpcSystem INSTANCE = createRpcSystem();

    public static RpcSystem getInstance() {
        return INSTANCE;
    }

    @Override
    public RpcSystem loadRpcSystem(Configuration config) {
        return INSTANCE;
    }

    /**
     * Creates a temporary JAR file from a resource in a thread-safe manner.
     * Each call creates a unique directory with a UUID to ensure isolation between threads.
     *
     * @param classLoader The ClassLoader to use for loading the resource
     * @param resourceName The name of the resource to load
     * @param baseDirectoryName The base name of the directory to create in the temp directory
     * @return The Path to the created JAR file
     * @throws IOException If an I/O error occurs
     */
    public static Path createTemporaryJarFromResource(
        ClassLoader classLoader,
        String resourceName,
        String baseDirectoryName) throws IOException {

        // Generate a UUID for this specific invocation
        String uniqueId = UUID.randomUUID().toString();

        // Create a unique directory path using the base name and the UUID
        final Path uniqueTmpDirectory = Paths.get(
            System.getProperty("java.io.tmpdir") + "/" + baseDirectoryName + "_" + uniqueId);

        // Create the directory - no need to synchronize as each thread uses its own directory
        Files.createDirectories(uniqueTmpDirectory);

        // Create the file with a simple name (no need for UUID in filename since directory is unique)
        final String simpleFileName = resourceName.replace(".jar", "") + ".jar";
        final Path tempFile = uniqueTmpDirectory.resolve(simpleFileName);

        // Load and copy the resource
        final InputStream resourceStream = classLoader.getResourceAsStream(resourceName);
        if (resourceStream == null) {
            throw new RuntimeException(
                "Resource " + resourceName + " could not be found. If this happened while running a test in the IDE,"
                    + "run the process-resources phase on the appropriate module via maven/gradle.");
        }

        // Create the file and copy the content
        Files.createFile(tempFile);
        IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));

        return tempFile;
    }

    public static Path createRpcAkkaJarFromResource(
        ClassLoader classLoader) throws IOException {
        return createTemporaryJarFromResource(classLoader, "flink-rpc-akka.jar", "flink-rpc-akka-jar");
    }

    private static RpcSystem createRpcSystem() {
        try {
            final ClassLoader flinkClassLoader = RpcSystem.class.getClassLoader();

            final Path tempFile = createRpcAkkaJarFromResource(
                flinkClassLoader);

            // The following classloader loads all classes from the submodule jar, except for explicitly white-listed packages.
            //
            //  <p>To ensure that classes from the submodule are always loaded through the submodule classloader
            //  (and thus from the submodule jar), even if the classes are also on the classpath (e.g., during
            //  tests), all classes from the "org.apache.flink" package are loaded child-first.
            //
            //  <p>Classes related to mantis-specific or logging are loaded parent-first.
            //
            //  <p>All other classes can only be loaded if they are either available in the submodule jar or the
            //  bootstrap/app classloader (i.e., provided by the JDK).
            final ComponentClassLoader componentClassLoader = new ComponentClassLoader(
                new URL[] {tempFile.toUri().toURL()},
                flinkClassLoader,
                // Dependencies which are needed by logic inside flink-rpc-akka (ie AkkaRpcService/System) which
                // are external to flink-rpc-akka itself (like ExecuteStageRequest in mantis-control-plane).
                CoreOptions.parseParentFirstLoaderPatterns(
                    "org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback;io.mantisrx.server.worker;io.mantisrx.server.core;io.mantisrx", ""),
                new String[] {"org.apache.flink"});

            return new CleanupOnCloseRpcSystem(
                ServiceLoader.load(RpcSystem.class, componentClassLoader).iterator().next(),
                componentClassLoader,
                tempFile);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize RPC system.", e);
        }
    }
}
