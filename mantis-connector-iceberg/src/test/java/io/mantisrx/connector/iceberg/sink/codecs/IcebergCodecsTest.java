/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import io.mantisrx.common.codec.Codec;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IcebergCodecsTest {

    private Codec<DataFile> codec;
    private DataFile dataFile;

    @BeforeEach
    void setUp() {
        codec = IcebergCodecs.dataFile();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        dataFile = DataFiles.builder(spec)
                .withPath("/path/filename.parquet")
                .withFileSizeInBytes(1)
                .withPartition(null)
                .withMetrics(mock(Metrics.class))
                .withSplitOffsets(Collections.singletonList(1L))
                .build();
    }

    @Test
    void shouldEncodeAndDecodeDataFile() {
        byte[] encoded = codec.encode(dataFile);
        DataFile actual = codec.decode(encoded);
        assertEquals(dataFile.path(), actual.path());
        assertEquals(dataFile.fileSizeInBytes(), actual.fileSizeInBytes());
        assertEquals(dataFile.partition(), actual.partition());
        assertEquals(dataFile.splitOffsets(), actual.splitOffsets());
    }
}