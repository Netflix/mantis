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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
@RequiredArgsConstructor
public class DefaultClassLoaderHandle implements ClassLoaderHandle {

  private final BlobStore blobStore;
  private final Collection<String> alwaysParentFirstPatterns;
  private final List<UserCodeClassLoader> openedHandles = new ArrayList<>();

  @Override
  public UserCodeClassLoader getOrResolveClassLoader(Collection<URI> requiredFiles,
      Collection<URL> requiredClasspaths) throws IOException {
    final List<URL> resolvedUrls = new ArrayList<>();
    for (URI requiredFile : requiredFiles) {
      // get a local version of the file that needs to be added to the classloader
      final File file = blobStore.get(requiredFile);
      if (file.isDirectory()) {
        // let's recursively add all jar files under this directory
        final Collection<File> childJarFiles =
            FileUtils.listFiles(file, new String[]{".jar"}, true);
        for (File jarFile : childJarFiles) {
          resolvedUrls.add(jarFile.toURI().toURL());
        }
      } else {
        // let's assume that this is actually a jar file
        resolvedUrls.add(file.toURI().toURL());
      }
    }

    resolvedUrls.addAll(requiredClasspaths);

    UserCodeClassLoader userCodeClassLoader = new UserCodeClassLoader() {
      private final ChildFirstClassLoader loader =
          new ChildFirstClassLoader(resolvedUrls, getClass().getClassLoader(),
              alwaysParentFirstPatterns);
      private volatile boolean closed = false;

      @Override
      public ClassLoader asClassLoader() {
        Preconditions.checkArgument(!closed, "cannot get class loader after its closed");
        return loader;
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          loader.close();
          closed = true;
        }
      }
    };

    openedHandles.add(userCodeClassLoader);
    return userCodeClassLoader;
  }

  @Override
  public void close() throws IOException {
    for (UserCodeClassLoader loader : openedHandles) {
      loader.close();
    }
  }
}
