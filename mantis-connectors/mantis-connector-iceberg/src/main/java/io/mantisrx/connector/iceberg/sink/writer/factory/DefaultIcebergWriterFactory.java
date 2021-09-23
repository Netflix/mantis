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

package io.mantisrx.connector.iceberg.sink.writer.factory;

import io.mantisrx.connector.iceberg.sink.writer.DefaultIcebergWriter;
import io.mantisrx.connector.iceberg.sink.writer.IcebergWriter;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.WorkerInfo;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;

public class DefaultIcebergWriterFactory implements IcebergWriterFactory {

    private final WriterConfig config;
    private final WorkerInfo workerInfo;

    private final Table table;
    private final LocationProvider locationProvider;

    public DefaultIcebergWriterFactory(
            WriterConfig config,
            WorkerInfo workerInfo,
            Table table,
            LocationProvider locationProvider) {
        this.config = config;
        this.workerInfo = workerInfo;
        this.table = table;
        this.locationProvider = locationProvider;
    }

    @Override
    public IcebergWriter newIcebergWriter() {
        return new DefaultIcebergWriter(config, workerInfo, table, locationProvider);
    }
}
