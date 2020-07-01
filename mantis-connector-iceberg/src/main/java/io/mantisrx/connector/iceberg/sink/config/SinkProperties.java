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

package io.mantisrx.connector.iceberg.sink.config;

/**
 * Property key names and default values for the base Iceberg Sink config.
 */
public class SinkProperties {

    private SinkProperties() {
    }

    /**
     * Name of Iceberg Catalog.
     */
    public static final String SINK_CATALOG = "sinkCatalog";
    public static final String SINK_CATALOG_DESCRIPTION = "Name of Iceberg Catalog";

    /**
     * Name of database within Iceberg Catalog.
     */
    public static final String SINK_DATABASE = "sinkDatabase";
    public static final String SINK_DATABASE_DESCRIPTION = "Name of database within Iceberg Catalog";

    /**
     * Name of table within database.
     */
    public static final String SINK_TABLE = "sinkTable";
    public static final String SINK_TABLE_DESCRIPTION = "Name of table within database";
}
