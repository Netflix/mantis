/*
 * Copyright 2021 Netflix, Inc.
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

package io.mantisrx.connector.iceberg.sink.writer;

import com.google.common.io.Files;
import java.io.File;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.LocationProvider;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Junit Jupiter Extension to create Iceberg Tables for unit testing with a specified schema,
 * properties, etc.... and to also clean the files after unit tests.
 * <p>
 * The way to use the IcebergTableExtension is by adding the following code to your test class. This
 * creates the table before the test is executed.
 * <pre>
 *     @RegisterExtension
 *     static IcebergTableExtension tableExtension =
 *       IcebergTableExtension.builder()
 *           .schema(SCHEMA)
 *           .spec(SPEC)
 *           .build();
 * </pre>
 *
 * <p> The created table can be obtained by the {@link IcebergTableExtension#getTable()} method.
 */
@Slf4j
@Builder
public class IcebergTableExtension implements BeforeAllCallback, BeforeEachCallback,
        AfterEachCallback {

    private File rootDir;

    @Getter
    @Builder.Default
    private String catalog = "catalog";

    @Getter
    @Builder.Default
    private String database = "database";

    @Getter
    @Builder.Default
    private String tableName = "table";

    @Getter
    private Schema schema;
    private PartitionSpec spec;

    @Getter
    private Table table;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        log.info("Before All");
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        log.info("Before Each");
        if (rootDir == null) {
            rootDir = Files.createTempDir();
        }

        final File tableDir = new File(rootDir, getTableIdentifier().toString());
        final HadoopTables tables = new HadoopTables();
        table = tables.create(schema, spec, tableDir.getPath());
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        FileUtils.deleteDirectory(rootDir);
        rootDir = null;
    }

    public LocationProvider getLocationProvider() {
        return table.locationProvider();
    }

    public TableIdentifier getTableIdentifier() {
        return TableIdentifier.of(catalog, database, tableName);
    }
}
