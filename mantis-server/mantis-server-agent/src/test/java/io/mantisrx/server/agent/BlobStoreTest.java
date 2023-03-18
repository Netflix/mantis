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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

public class BlobStoreTest {
    @Test
    public void testPrefixedBlobStore() throws Exception {
        final BlobStore blobStore = mock(BlobStore.class);
        final File file = mock(File.class);
        when(blobStore.get(any())).thenReturn(file);

        final BlobStore prefixedBlobStpre =
            new BlobStore.PrefixedBlobStore(new URI("s3://mantisrx.s3.store/mantis/jobs/"), blobStore);
        prefixedBlobStpre.get(new URI("http://sananthanarayanan-mantis-jobs-sine-function-thin-0.1.0.zip"));

        final URI expectedUri =
            new URI("s3://mantisrx.s3.store/mantis/jobs/sananthanarayanan-mantis-jobs-sine-function-thin-0.1.0.zip");
        verify(blobStore, times(1)).get(Matchers.eq(expectedUri));

        prefixedBlobStpre.get(new URI(
            "https://mantisrx.region.prod.io.net/mantis-artifacts/sananthanarayanan-mantis-jobs-sine-function-thin-0.1.0.zip"));
        verify(blobStore, times(2)).get(Matchers.eq(expectedUri));
    }
}
