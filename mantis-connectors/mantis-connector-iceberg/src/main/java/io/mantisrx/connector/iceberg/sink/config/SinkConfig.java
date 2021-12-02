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

package io.mantisrx.connector.iceberg.sink.config;


import static io.mantisrx.connector.iceberg.sink.config.SinkProperties.SINK_CATALOG;
import static io.mantisrx.connector.iceberg.sink.config.SinkProperties.SINK_DATABASE;
import static io.mantisrx.connector.iceberg.sink.config.SinkProperties.SINK_TABLE;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.parameter.Parameters;
import lombok.RequiredArgsConstructor;

/**
 * Convenient base config used by {@link WriterConfig} and {@link CommitterConfig}.
 */
@RequiredArgsConstructor
public class SinkConfig {

    private final String catalog;
    private final String database;
    private final String table;

    /**
     * Creates an instance from {@link Parameters} derived from the current Mantis Stage's {@code Context}.
     */
    public SinkConfig(Parameters parameters) {
        this.catalog = (String) parameters.get(SINK_CATALOG);
        this.database = (String) parameters.get(SINK_DATABASE);
        this.table = (String) parameters.get(SINK_TABLE);
    }

    /**
     * Returns a String for Iceberg Catalog name.
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Returns a String for the database name in a catalog.
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Returns a String for the table within a database.
     */
    public String getTable() {
        return table;
    }
}
