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

package io.mantisrx.connector.iceberg.sink.committer;

import static io.mantisrx.connector.iceberg.sink.writer.DefaultIcebergWriter.maxNullSafe;
import static io.mantisrx.connector.iceberg.sink.writer.DefaultIcebergWriter.minNullSafe;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.committer.watermarks.WatermarkExtractor;
import io.mantisrx.connector.iceberg.sink.writer.MantisDataFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;


/**
 * Commits {@link DataFile}s for Iceberg tables.
 *
 * This class uses Iceberg's Table API and only supports Table#append operations.
 */
@Slf4j
public class IcebergCommitter {

    private final Table table;
    private final CommitterConfig config;
    private final WatermarkExtractor watermarkExtractor;

    public IcebergCommitter(
        Table table,
        CommitterConfig committerConfig,
        WatermarkExtractor watermarkExtractor) {
        this.table = table;
        this.config = committerConfig;
        this.watermarkExtractor = watermarkExtractor;
    }

    /**
     * Uses Iceberg's Table API to append DataFiles and commit metadata to Iceberg.
     *
     * @return the current snapshot of the table.
     */
    public Map<String, Object> commit(List<MantisDataFile> dataFiles) {
        Transaction transaction = table.newTransaction();

        AppendFiles tableAppender = transaction.newAppend();
        dataFiles.stream().map(MantisDataFile::getDataFile).forEach(tableAppender::appendFile);
        tableAppender.commit();
        log.info(
            "Iceberg committer {}.{} appended {} data files to transaction",
            config.getDatabase(),
            config.getTable(),
            dataFiles.size());

        Long currentWatermark = watermarkExtractor.getWatermark(transaction);
        Long lowWatermark = null;
        for (MantisDataFile flinkDataFile : dataFiles) {
            lowWatermark = minNullSafe(lowWatermark, flinkDataFile.getLowWatermark());
        }
        final Long finalWatermark = maxNullSafe(currentWatermark, lowWatermark);

        if (finalWatermark != null) {
            watermarkExtractor.setWatermark(transaction, finalWatermark);
        }

        transaction.commitTransaction();
        return table.currentSnapshot() == null ? new HashMap<>() : new HashMap<>(table.currentSnapshot().summary());
    }
}
