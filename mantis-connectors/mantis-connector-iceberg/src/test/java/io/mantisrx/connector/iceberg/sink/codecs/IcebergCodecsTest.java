/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.connector.iceberg.sink.codecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.mantisrx.common.codec.Codec;
import java.util.Collections;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IcebergCodecsTest {

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private Codec<Record> recordCodec;
    private Codec<DataFile> dataFileCodec;

    @BeforeEach
    void setUp() {
        this.recordCodec = IcebergCodecs.record(SCHEMA);
        this.dataFileCodec = IcebergCodecs.dataFile();
    }

    @Test
    void shouldEncodeAndDecodeRecord() {
        Record expected = GenericRecord.create(SCHEMA);
        expected.setField("id", 1);

        byte[] encoded = recordCodec.encode(expected);
        Record actual = recordCodec.decode(encoded);

        assertEquals(expected, actual);
    }

    @Test
    void shouldEncodeAndDecodeDataFile() {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        DataFile expected = DataFiles.builder(spec)
                .withPath("/path/filename.parquet")
                .withFileSizeInBytes(1)
                .withPartition(null)
                .withMetrics(mock(Metrics.class))
                .withSplitOffsets(Collections.singletonList(1L))
                .build();

        byte[] encoded = dataFileCodec.encode(expected);
        DataFile actual = dataFileCodec.decode(encoded);

        assertEquals(expected.path(), actual.path());
        assertEquals(expected.fileSizeInBytes(), actual.fileSizeInBytes());
        assertEquals(expected.partition(), actual.partition());
        assertEquals(expected.splitOffsets(), actual.splitOffsets());
    }
}