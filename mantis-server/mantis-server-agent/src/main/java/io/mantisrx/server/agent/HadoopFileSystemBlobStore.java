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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Blob store that uses the hadoop-filesystem base library to retrieve the requested resources.
 * Hadoop FileSystem is a good abstraction as it can deal with a variety of cloud-native object stores
 * such as s3, gfs, etc...
 */
@RequiredArgsConstructor
public class HadoopFileSystemBlobStore implements BlobStore {

    //  The file system in which blobs are stored. */
    private final FileSystem fileSystem;

    private final File localStoreDir;

    @Override
    public File get(URI blobUrl) throws IOException {
        final Path src = new Path(blobUrl);
        final Path dest = new Path(getStorageLocation(blobUrl));
        if (!fileSystem.exists(dest)) {
            fileSystem.copyToLocalFile(src, dest);
        }

        return new File(dest.toUri().getPath());
    }

    @Override
    public void close() throws IOException {
        FileUtils.deleteDirectory(localStoreDir);
        fileSystem.close();
    }

    private String getStorageLocation(URI blobUri) {
        return String.format("%s/%s", localStoreDir, FilenameUtils.getName(blobUri.getPath()));
    }
}
