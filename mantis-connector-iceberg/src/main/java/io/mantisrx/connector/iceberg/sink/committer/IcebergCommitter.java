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

package io.mantisrx.connector.iceberg.sink.committer;

import java.util.List;
import java.util.stream.Collectors;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.committer.metrics.CommitterMetrics;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Commits manifest files for Iceberg table metadata.
 */
public class IcebergCommitter {


    private static final Logger logger = LoggerFactory.getLogger(IcebergCommitter.class);

    private final CommitterMetrics metrics;
    private final CommitterConfig config;
    private final Table table;
    private final Schema committedDataFileSchema;

    public IcebergCommitter(
            CommitterMetrics metrics,
            CommitterConfig config,
            Table table) {
        this.metrics = metrics;
        this.config = config;
        this.table = table;
        this.committedDataFileSchema = new Schema(
                Types.NestedField.required(1, "ts_utc_msec", Types.LongType.get()),
                Types.NestedField.required(2, "committed_data_files", table.spec().schema().asStruct())
        );
    }

    /**
     * Uses Iceberg's Table API to append DataFiles and commit metadata to Iceberg.
     */
    public Record commit(List<DataFile> dataFiles) {
        AppendFiles tableAppender = table.newAppend();

        List<DataFile> dataFilesWithoutStats = dataFiles.stream().map(dataFile -> {
            tableAppender.appendFile(dataFile);
            return dataFile.copyWithoutStats();
        }).collect(Collectors.toList());

        Record committed = GenericRecord.create(committedDataFileSchema);
        committed.setField("ts_utc_msec", System.currentTimeMillis());
        committed.setField("committed_data_files", dataFilesWithoutStats);

        tableAppender.commit();
        logger.info("committed {}", committed.toString());
        return committed;
    }
}
