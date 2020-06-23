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

package io.mantisrx.connector.iceberg.sink.writer.config;

import org.apache.iceberg.FileFormat;

/**
 * Property key names and default values for an Iceberg Committer.
 *
 * TODO: Add Iceberg table properties (the writer sub-props).
 * TODO: Use Iceberg table properties's row-group-size instead.
 * TODO: Add Hadoop Configuration.
 */
public class WriterProperties {

    private WriterProperties() {
    }

    /**
     * Maximum number of rows that should exist in a file.
     */
    public static final String WRITER_ROW_GROUP_SIZE = "writerRowGroupSize";
    public static final int WRITER_ROW_GROUP_SIZE_DEFAULT = 1000;
    public static final String WRITER_ROW_GROUP_SIZE_DESCRIPTION =
            String.format("Maximum number of rows that should exist in a file (default: %s)",
                    WRITER_ROW_GROUP_SIZE_DEFAULT);

    /**
     * Flush frequency by size (in Bytes).
     */
    public static final String WRITER_FLUSH_FREQUENCY_BYTES = "writerFlushFrequencyBytes";
    // TODO: Change to long.
    public static final String WRITER_FLUSH_FREQUENCY_BYTES_DEFAULT = "134217728";     // 128 MiB
    public static final String WRITER_FLUSH_FREQUENCY_BYTES_DESCRIPTION =
            String.format("Flush frequency by size in Bytes (default: %s)",
                    WRITER_FLUSH_FREQUENCY_BYTES_DEFAULT);

    /**
     * File format for writing data files to backing Iceberg store.
     */
    public static final String WRITER_FILE_FORMAT = "writerFileFormat";
    public static final String WRITER_FILE_FORMAT_DEFAULT = FileFormat.PARQUET.name();
    public static final String WRITER_FILE_FORMAT_DESCRIPTION =
            String.format("File format for writing data files to backing Iceberg store (default: %s)",
                    WRITER_FILE_FORMAT_DEFAULT);

    /**
     * Partition key for Iceberg partition path.
     */
    public static final String WRITER_PARTITION_KEY = "writerPartitionKey";
    // TODO: Change to long.
    public static final String WRITER_PARTITION_KEY_DEFAULT = "";
    public static final String WRITER_PARTITION_KEY_DESCRIPTION =
            "Partition key for Iceberg partition path (default: none)";

    /**
     * Type of Transform to apply using partition key field.
     *
     * From Iceberg: {@code identity, year, month, day, hour}.
     * TODO: Support bucket[N] and truncate[W].
     */
    public static final String WRITER_PARTITION_KEY_TRANSFORM = "writerPartitionKeyTransform";
    public static final Enum<PartitionTransforms> WRITER_PARTITION_KEY_TRANSFORM_DEFAULT = PartitionTransforms.NONE;
    public static final String WRITER_PARTITION_KEY_TRANSFORM_DESCRIPTION =
            "Type of Transform to apply using partition key field. Available transforms: " +
            "identity, year, month, day, hour (default: none)";

    public enum PartitionTransforms {
        IDENTITY,
        YEAR,
        MONTH,
        DAY,
        HOUR,
        NONE,
    }
}
