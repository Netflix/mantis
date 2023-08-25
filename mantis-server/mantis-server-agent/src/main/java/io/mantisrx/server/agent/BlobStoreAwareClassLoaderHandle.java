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
package io.mantisrx.server.agent;

import com.mantisrx.common.utils.Closeables;
import io.mantisrx.runtime.loader.ClassLoaderHandle;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.WeakHashMap;
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

/**
 * ClassLoader that gets created out of downloading blobs from the corresponding blob store.
 */
@Slf4j
@RequiredArgsConstructor
public class BlobStoreAwareClassLoaderHandle implements ClassLoaderHandle {

    private final BlobStore blobStore;
    private final WeakHashMap<URLClassLoader, Void> openedHandles = new WeakHashMap<>();

    protected List<URL> getResolvedUrls(Collection<URI> requiredFiles, BlobStore blobStore)
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
            resolvedUrls.addAll(getResolvedUrls(requiredFiles, blobStore));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }

        resolvedUrls.addAll(requiredClasspaths);
        final ParentFirstClassLoader classLoader = new ParentFirstClassLoader(resolvedUrls.toArray(new URL[0]), getClass().getClassLoader(),
            error -> log.error("Failed to load class", error));
        synchronized (openedHandles) {
            openedHandles.put(classLoader, null);
        }
        return SimpleUserCodeClassLoader.create(classLoader);
    }

    @Override
    public void cacheJobArtifacts(Collection<URI> artifacts) {
        try {
            getResolvedUrls(artifacts, blobStore);
        } catch (IOException | URISyntaxException e) {
            log.warn("Failed to download artifacts: {}", artifacts);
        }
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
        synchronized (openedHandles) {
            try {
                Closeables.combine(openedHandles.keySet()).close();
            } finally {
                openedHandles.clear();
                blobStore.close();
            }
        }
    }
}
