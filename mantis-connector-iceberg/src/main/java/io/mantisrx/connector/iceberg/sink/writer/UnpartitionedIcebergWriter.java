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
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * Writes unpartitioned {@link Record}s to Iceberg via a HDFS-compatible backend.
 *
 * Users have the flexibility to choose the semantics of opening, writing, and closing
 * this Writer, for example, closing the underlying appender after some number
 * of Bytes written and opening a new appender.
 */
public class UnpartitionedIcebergWriter extends BaseIcebergWriter {

    public UnpartitionedIcebergWriter(
            WriterMetrics metrics,
            WriterConfig config,
            WorkerInfo workerInfo,
            Table table,
            PartitionSpec spec) {
        super(metrics, config, workerInfo, table, spec);
    }

    /**
     * Writes Records without partitioning.
     */
    @Override
    public void write(Record record) {
        writeRecord(record);
    }
}
