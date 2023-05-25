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

package io.mantisrx.connector.iceberg.sink.writer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.iceberg.sink.StageOverrideParameters;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.factory.IcebergWriterFactory;
import io.mantisrx.connector.iceberg.sink.writer.pool.FixedIcebergWriterPool;
import io.mantisrx.connector.iceberg.sink.writer.pool.IcebergWriterPool;
import io.mantisrx.runtime.parameter.Parameters;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FixedIcebergWriterPoolTest {

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private IcebergWriter writer;
    private IcebergWriterPool writerPool;

    private MantisRecord record;
    private StructLike partition;

    @BeforeEach
    void setUp() {
        Parameters parameters = StageOverrideParameters.newParameters();
        WriterConfig config = new WriterConfig(parameters, mock(Configuration.class));
        IcebergWriterFactory factory = mock(IcebergWriterFactory.class);
        this.writer = mock(IcebergWriter.class);
        when(this.writer.length()).thenReturn(Long.MAX_VALUE);
        when(factory.newIcebergWriter()).thenReturn(this.writer);

        this.writerPool = spy(new FixedIcebergWriterPool(
                factory,
                config.getWriterFlushFrequencyBytes(),
                config.getWriterMaximumPoolSize()));

        Record icebergRecord = GenericRecord.create(SCHEMA);
        icebergRecord.setField("id", 1);
        // Identity partitioning (without explicitly using a Partitioner).
        this.partition = icebergRecord.copy();

        record = new MantisRecord(icebergRecord, null);
    }

    @Test
    void shouldOpenNewWriter() {
        assertDoesNotThrow(() -> writerPool.open(record.getRecord()));
    }

    @Test
    void shouldFailToOpenNewWriterWhenMaximumPoolSizeExceeded() {
        writerPool = spy(new FixedIcebergWriterPool(mock(IcebergWriterFactory.class), 0, 0));
        assertThrows(IOException.class, () -> writerPool.open(any()));
    }

    @Test
    void shouldOpenWhenWriterExists() {
        assertDoesNotThrow(() -> writerPool.open(record.getRecord()));
        assertDoesNotThrow(() -> writerPool.open(record.getRecord()));
    }

    @Test
    void shouldFailToWriteWhenNoWriterExists() {
        assertThrows(RuntimeException.class, () -> writerPool.write(partition, record));
    }

    @Test
    void shouldWriteWhenWriterExists() throws IOException {
        writerPool.open(partition);
        assertDoesNotThrow(() -> writerPool.write(partition, record));
    }

    @Test
    void shouldFailToCloseWhenNoWriterExists() {
        assertThrows(RuntimeException.class, () -> writerPool.close(record.getRecord()));
    }

    @Test
    void shouldCloseWhenWriterExists() throws IOException {
        writerPool.open(partition);
        assertDoesNotThrow(() -> writerPool.close(partition));
    }

    @Test
    void shouldGetFlushableWriters() throws IOException {
        writerPool.open(partition);
        assertFalse(writerPool.getFlushableWriters().isEmpty());
        when(writer.length()).thenReturn(Long.MIN_VALUE);
        assertTrue(writerPool.getFlushableWriters().isEmpty());
    }
}
