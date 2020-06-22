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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.committer.metrics.CommitterMetrics;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
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

    public IcebergCommitter(
            CommitterMetrics metrics,
            CommitterConfig config,
            Table table) {
        this.metrics = metrics;
        this.config = config;
        this.table = table;
    }

    /**
     * Uses Iceberg's Table API to append DataFiles and commit metadata to Iceberg.
     */
    public Map<String, Object> commit(List<DataFile> dataFiles) {
        AppendFiles tableAppender = table.newAppend();
        dataFiles.forEach(tableAppender::appendFile);
        tableAppender.commit();
        Map<String, Object> summary =
                table.currentSnapshot() == null ? new HashMap<>() : new HashMap<>(table.currentSnapshot().summary());
        logger.info("committed {}", summary);
        return summary;
    }
}
