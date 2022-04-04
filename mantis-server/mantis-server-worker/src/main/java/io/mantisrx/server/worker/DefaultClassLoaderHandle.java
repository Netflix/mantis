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

import static io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.collect.Iterables.getFirst;

import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

@Slf4j
@RequiredArgsConstructor
public class DefaultClassLoaderHandle implements ClassLoaderHandle {

  private final BlobStore userArtifactBlobStore;
  private final BlobStore runtimeArtifactBlobStore;
    private final List<UserCodeClassLoader> openedHandles = new ArrayList<>();

  private List<URL> getResolvedUrls(Collection<URI> requiredFiles, BlobStore blobStore)
      throws IOException, URISyntaxException {
    final List<URL> resolvedUrls = new ArrayList<>();

    for (URI requiredFile : requiredFiles) {
      // get a local version of the file that needs to be added to the classloader
      final File file = blobStore.get(requiredFile);
      log.info("Received file {} from blob store for creating class loader", file);
      if (file.isDirectory()) {
        // let's recursively add all jar files under this directory
        final Collection<File> childJarFiles =
            FileUtils.listFiles(file, new SuffixFileFilter("jar", IOCase.INSENSITIVE),
                TrueFileFilter.INSTANCE);
        log.info("Loading files {} into the class loader", childJarFiles);
        for (File jarFile : childJarFiles) {
          resolvedUrls.add(jarFile.toURI().toURL());
        }

        final Collection<File> userArtifactManifest =
            FileUtils.listFiles(file, new SuffixFileFilter("mantis_runtime_version.txt"), TrueFileFilter.INSTANCE);
        log.info("userArtifactManifest={}", userArtifactManifest);
        if (userArtifactManifest.size() == 1) {
          log.info("This is a thin artifact.. Now reading the mantis runtime version required");
          String runtimeVersion = Files.readAllLines(getFirst(userArtifactManifest, null).toPath()).get(0).trim();
          log.info("Mantis runtime version is {}", runtimeVersion);
          resolvedUrls.addAll(getResolvedUrls(ImmutableList.of(
                  new URI(String.format("s3://nfmantis-runtime-%s.zip", runtimeVersion))),
              runtimeArtifactBlobStore));
        }
      } else {
        // let's assume that this is actually a jar file
        resolvedUrls.add(file.toURI().toURL());
      }
    }

    return resolvedUrls;
  }

  @Override
  public UserCodeClassLoader getOrResolveClassLoader(Collection<URI> requiredFiles,
      Collection<URL> requiredClasspaths) throws IOException {
    final List<URL> resolvedUrls = new ArrayList<>();
    try {
      resolvedUrls.addAll(getResolvedUrls(requiredFiles, userArtifactBlobStore));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    resolvedUrls.addAll(requiredClasspaths);
    return SimpleUserCodeClassLoader.create(
        new ParentFirstClassLoader(resolvedUrls.toArray(new URL[0]), getClass().getClassLoader(),
            error -> log.error("Failed to load class", error)));
  }

  public static class ParentFirstClassLoader extends FlinkUserCodeClassLoader {

    ParentFirstClassLoader(
        URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
      super(urls, parent, classLoadingExceptionHandler);
    }

    static {
      ClassLoader.registerAsParallelCapable();
    }
  }

  @Override
  public void close() throws IOException {
//    for (UserCodeClassLoader loader : openedHandles) {
//      loader.close();
//    }
  }
}
