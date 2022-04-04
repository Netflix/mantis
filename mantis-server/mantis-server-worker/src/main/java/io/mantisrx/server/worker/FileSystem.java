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
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileSystem {

  private static final Map<String, FileSystemFactory> FS_FACTORIES = new HashMap<>();

  static {
      FileSystem.initialize();
  }

  /**
   * initialize needs to be called at the start of the JVM.
   */
  public static void initialize() {
    FS_FACTORIES.clear();

    // let's get all the registered implementations
    Iterator<FileSystemFactory> fileSystemFactoryIterator =
        ServiceLoader.load(FileSystemFactory.class).iterator();
    fileSystemFactoryIterator.forEachRemaining(fileSystemFactory -> {
      log.info("Initializing FileSystem Factory {}", fileSystemFactory);
      FS_FACTORIES.putIfAbsent(fileSystemFactory.getScheme(), fileSystemFactory);
    });
  }

  public static org.apache.hadoop.fs.FileSystem create(URI fsUri) throws IOException {
    FileSystemFactory factory = FS_FACTORIES.get(fsUri.getScheme());
    if (factory != null) {
      return factory.create(fsUri);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown schema", fsUri.getScheme().toString()));
    }
  }
}
