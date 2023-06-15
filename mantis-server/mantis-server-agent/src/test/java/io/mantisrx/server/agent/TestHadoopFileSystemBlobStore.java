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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

public class TestHadoopFileSystemBlobStore {

  @Test
  public void test() throws Exception {
    FileSystem fileSystem = mock(FileSystem.class);

    File localStoreDir = new File("/mnt/data/mantis-artifacts");
    HadoopFileSystemBlobStore blobStore =
        new HadoopFileSystemBlobStore(fileSystem, localStoreDir);
    URI src =
        new URI(
            "s3://netflix.s3.genpop.prod/mantis/jobs/sananthanarayanan-mantis-jobs-sine-function-thin-0.1.0.zip");
    URI dst =
        new URI(
            "/mnt/data/mantis-artifacts/sananthanarayanan-mantis-jobs-sine-function-thin-0.1.0.zip");
    blobStore.get(src);
    verify(fileSystem, times(1)).copyToLocalFile(Matchers.eq(new Path(src)),
        Matchers.eq(new Path(dst)));
  }
}
