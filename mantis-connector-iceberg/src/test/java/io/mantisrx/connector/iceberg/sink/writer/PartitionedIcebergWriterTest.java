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

package io.mantisrx.connector.iceberg.sink.writer;

import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionedIcebergWriterTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "str", Types.StringType.get()),
            required(3, "bin", Types.BinaryType.get()),
            required(4, "ts", Types.LongType.get())
    );

    private static final Record record = GenericRecord.create(SCHEMA);

    @BeforeEach
    void setUp() {
        record.setField("id", 1);
        record.setField("str", "foo");
        record.setField("bin", new byte[]{});
        record.setField("ts", System.currentTimeMillis());
    }

    @Test
    void shouldPartition() {
    }
}