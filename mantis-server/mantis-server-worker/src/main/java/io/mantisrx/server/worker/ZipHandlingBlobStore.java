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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import lombok.RequiredArgsConstructor;
import net.lingala.zip4j.ZipFile;

@RequiredArgsConstructor
public class ZipHandlingBlobStore implements BlobStore {

  private final BlobStore blobStore;

  @Override
  public File get(URI blobUrl) throws IOException {
    final File localFile = blobStore.get(blobUrl);
    final ZipFile zipFile = isZipFile(localFile);
    if (zipFile == null) {
      return localFile;
    } else {
      String destDirStr = getDestDir(zipFile);
      File destDir = new File(destDirStr);
      if (!destDir.exists()) {
        zipFile.extractAll(destDirStr);
      }
      return destDir;
    }
  }

  @Override
  public void close() throws IOException {
    blobStore.close();
  }

  private String getDestDir(ZipFile zipFile) {
    return zipFile.getFile().getPath() + "-unzipped";
  }

  private ZipFile isZipFile(File file) {
    ZipFile file1 = new ZipFile(file);
    if (file1.isValidZipFile()) {
      return file1;
    } else {
      return null;
    }
  }
}
