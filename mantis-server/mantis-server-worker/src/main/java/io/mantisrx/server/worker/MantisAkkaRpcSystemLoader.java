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

package io.mantisrx.server.worker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemLoader;
import org.apache.flink.util.IOUtils;

public class MantisAkkaRpcSystemLoader implements RpcSystemLoader {
  @Override
  public RpcSystem loadRpcSystem(Configuration config) {
    try {
      final ClassLoader flinkClassLoader = RpcSystem.class.getClassLoader();

      final Path tmpDirectory = Paths.get(ConfigurationUtils.parseTempDirectories(config)[0]);
      Files.createDirectories(tmpDirectory);
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

      final SubmoduleClassLoader submoduleClassLoader =
          new SubmoduleClassLoader(
              new URL[] {tempFile.toUri().toURL()}, flinkClassLoader);

      return ServiceLoader.load(RpcSystem.class, submoduleClassLoader).iterator().next();
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize RPC system.", e);
    }
  }
}
