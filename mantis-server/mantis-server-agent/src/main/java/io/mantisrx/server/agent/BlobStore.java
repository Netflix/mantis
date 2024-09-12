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

import io.mantisrx.shaded.com.google.common.util.concurrent.Striped;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.locks.Lock;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 * Abstraction to deal with getting files stored in object stores such as s3.
 */
public interface BlobStore extends Closeable {
    File get(URI blobUrl) throws IOException;

    /**
     * blob store that adds a prefix to every requested URI.
     *
     * @param prefixUri prefix that needs to be prepended to every requested resource.
     * @return blob store with the prefix patterns baked in.
     */
    default BlobStore withPrefix(URI prefixUri) {
        return new PrefixedBlobStore(prefixUri, this);
    }

    default BlobStore withFallbackStore(BlobStore fallbackStore) {
        return new FallbackEnabledBlobStore(this, fallbackStore);
    }

    /**
     * blob store that when downloading zip files, also unpacks them and returns the unpacked file/directory to the caller.
     *
     * @return blob store that can effectively deal with zip files
     */
    default BlobStore withZipCapabilities() {
        return new ZipHandlingBlobStore(this);
    }

    default BlobStore withThreadSafeBlobStore() {
        return new ThreadSafeBlobStore(this);
    }

    static BlobStore forHadoopFileSystem(URI clusterStoragePath, File localStoreDir) throws Exception {
        final org.apache.hadoop.fs.FileSystem fileSystem =
            FileSystemInitializer.create(clusterStoragePath);

        return
            new HadoopFileSystemBlobStore(fileSystem, localStoreDir)
                .withPrefix(clusterStoragePath)
                .withZipCapabilities()
                .withThreadSafeBlobStore();
    }

    static BlobStore forHadoopFileSystem(URI clusterStoragePath, URI fallbackStoragePath, File localStoreDir)
        throws Exception {
        final org.apache.hadoop.fs.FileSystem fileSystem =
            FileSystemInitializer.create(clusterStoragePath);

        final org.apache.hadoop.fs.FileSystem fallbackFileSystem =
            FileSystemInitializer.create(fallbackStoragePath);

        return
            new HadoopFileSystemBlobStore(fileSystem, localStoreDir)
                .withPrefix(clusterStoragePath)
                .withFallbackStore(
                    new HadoopFileSystemBlobStore(fallbackFileSystem, localStoreDir).withPrefix(fallbackStoragePath))
                .withZipCapabilities()
                .withThreadSafeBlobStore();
    }

    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    class PrefixedBlobStore implements BlobStore {
        private final URI rootUri;
        private final BlobStore blobStore;

        @Override
        public File get(URI blobUrl) throws IOException {
            final String fileName = FilenameUtils.getName(blobUrl.toString());
            return blobStore.get(rootUri.resolve(fileName));
        }

        @Override
        public void close() throws IOException {
            blobStore.close();
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    @Slf4j
    class FallbackEnabledBlobStore implements BlobStore {
        private final BlobStore blobStore;
        private final BlobStore fallbackBlobStore;

        @Override
        public File get(URI blobUrl) throws IOException {
            try
            {
                return blobStore.get(blobUrl);
            }
            catch (Exception e) {
                log.error("Get blob error, fallback to next blobstore", e);
            }

            return fallbackBlobStore.get(blobUrl);
        }

        @Override
        public void close() throws IOException {
            blobStore.close();
            fallbackBlobStore.close();
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    class ZipHandlingBlobStore implements BlobStore {

        private final BlobStore blobStore;

        @Override
        public File get(URI blobUrl) throws IOException {
            final File localFile = blobStore.get(blobUrl);
            final ZipFile zipFile = getZipFile(localFile);
            if (zipFile == null) {
                return localFile;
            } else {
                File destDir = null;
                try (ZipFile z = zipFile) {
                    String destDirStr = getUnzippedDestDir(z);
                    destDir = new File(destDirStr);
                    if (destDir.exists()) {
                        return destDir;
                    }

                    z.extractAll(destDirStr);
                    return destDir;
                } catch(Exception e) {
                    // delete directory before re-throwing exception to avoid possible data corruptions
                    if (destDir != null) {
                        FileUtils.deleteDirectory(destDir);
                    }
                    throw e;
                }
            }
        }

        @Override
        public void close() throws IOException {
            blobStore.close();
        }

        private String getUnzippedDestDir(ZipFile zipFile) {
            return zipFile.getFile().getPath() + "-unzipped";
        }

        private ZipFile getZipFile(File file) {
            ZipFile file1 = new ZipFile(file);
            if (file1.isValidZipFile()) {
                return file1;
            } else {
                return null;
            }
        }
    }


    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    class ThreadSafeBlobStore implements BlobStore {

        private final BlobStore blobStore;

        final Striped<Lock> locks = Striped.lock(1024);

        @Override
        public File get(URI blobUrl) throws IOException {
            Lock lock = locks.get(blobUrl.getPath());
            lock.lock();
            try {
                return blobStore.get(blobUrl);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            blobStore.close();
        }
    }
}
