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
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.runtime.WorkerInfo;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

/**
 */
public class PartitionedIcebergWriter extends BaseIcebergWriter {

    private final Schema writerSchema;
    private final PartitionSpec spec;

    public PartitionedIcebergWriter(
            WriterMetrics metrics,
            WriterConfig config,
            WorkerInfo workerInfo,
            Table table,
            Schema writerSchema,
            PartitionSpec spec) {
        super(metrics, config, workerInfo, table, spec);
        this.writerSchema = writerSchema;
        this.spec = spec;
    }

    /**
     *
     */
    @Override
    public void write(Record record) {
        // TODO: Partitioning.
        writeRecord(record);
    }

    /**
     *
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
