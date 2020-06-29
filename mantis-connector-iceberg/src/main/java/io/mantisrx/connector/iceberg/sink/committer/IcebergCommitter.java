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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;


/**
 * Commits {@link DataFile}s for Iceberg tables.
 *
 * This class uses Iceberg's Table API and only supports Table#append operations.
 */
public class IcebergCommitter {

    private final Table table;

    public IcebergCommitter(Table table) {
        this.table = table;
    }

    /**
     * Uses Iceberg's Table API to append DataFiles and commit metadata to Iceberg.
     *
     * @return the current snapshot of the table.
     */
    public Map<String, Object> commit(List<DataFile> dataFiles) {
        AppendFiles tableAppender = table.newAppend();
        dataFiles.forEach(tableAppender::appendFile);
        tableAppender.commit();
        return table.currentSnapshot() == null ? new HashMap<>() : new HashMap<>(table.currentSnapshot().summary());
    }
}
