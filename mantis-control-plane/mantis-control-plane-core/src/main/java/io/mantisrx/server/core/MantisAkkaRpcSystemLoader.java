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

    private static RpcSystem createRpcSystem() {
        try {
            final ClassLoader flinkClassLoader = RpcSystem.class.getClassLoader();

            final Path tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir") + "/flink-rpc-akka-jar");
            Files.createDirectories(tmpDirectory);

            try {
                // Best-effort cleanup directory in case some other jobs has failed abruptly
                FileUtils.cleanDirectory(tmpDirectory.toFile());
            } catch (Exception e) {
                log.warn("Could not cleanup flink-rpc-akka jar directory {}.", tmpDirectory, e);
            }
            final Path tempFile =
                Files.createFile(
                    tmpDirectory.resolve("flink-rpc-akka_" + UUID.randomUUID() + ".jar"));

            final InputStream resourceStream =
                flinkClassLoader.getResourceAsStream("flink-rpc-akka.jar");
            if (resourceStream == null) {
                throw new RuntimeException(
                    "Akka RPC system could not be found. If this happened while running a test in the IDE,"
                        + "run the process-resources phase on flink-rpc/flink-rpc-akka-loader via maven.");
            }

            IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));

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
