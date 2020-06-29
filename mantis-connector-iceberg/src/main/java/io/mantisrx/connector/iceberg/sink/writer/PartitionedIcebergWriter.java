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

import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.WorkerInfo;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

/**
 * Writes partitioned {@link Record}s to Iceberg via a HDFS-compatible backend.
 *
 * Users have the flexibility to choose the semantics of opening, writing, and closing
 * this Writer, for example, closing the underlying appender after some number
 * of Bytes written and opening a new appender.
 */
public class PartitionedIcebergWriter extends BaseIcebergWriter {

    private final Schema schema;
    private final PartitionSpec spec;

    public PartitionedIcebergWriter(WriterConfig config, WorkerInfo workerInfo, Table table) {
        super(config, workerInfo, table);
        this.schema = table.schema();
        this.spec = table.spec();
    }

    /**
     * Writes Records by applying a partition transforms to fields of a record specified by a PartitionSpec.
     */
    @Override
    public void write(Record record) {
        // TODO: Partitioning.
        writeRecord(record);
    }

    /**
     * TODO: Need to upstream Accessors and Transforms compatible with Iceberg Schemas.
     */
    public Record partition(Record record) {
        Record partitioned = GenericRecord.create(spec.partitionType());
//        spec.fields().forEach(field -> {
//            int sourceId = field.sourceId();
//            String sourceName = writerSchema.findField(sourceId).name();
//            Class<?> sourceClass = writerSchema.findField(sourceId).type().typeId().javaClass();
//            Object o = sourceClass.cast(record.getField(sourceName));
//            partitioned.setField(field.name(), field.transform().apply(record.get(0, sourceClass));
//        });

        return partitioned;
    }
}
