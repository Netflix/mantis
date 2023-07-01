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

package io.mantisrx.connector.iceberg.sink.writer.config;

import org.apache.iceberg.FileFormat;

/**
 * Property key names and default values for an Iceberg Committer.
 */
public class WriterProperties {

    private WriterProperties() {
    }

    /**
     * Maximum number of rows that should exist in a file.
     */
    public static final String WRITER_ROW_GROUP_SIZE = "writerRowGroupSize";
    public static final int WRITER_ROW_GROUP_SIZE_DEFAULT = 100;
    public static final String WRITER_ROW_GROUP_SIZE_DESCRIPTION =
            String.format("Number of rows to chunk before checking for file size (default: %s)",
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
     * Flush frequency by time (in milliseconds).
     */
    public static final String WRITER_FLUSH_FREQUENCY_MSEC = "writerFlushFrequencyMsec";
    // TODO: Change to long.
    public static final String WRITER_FLUSH_FREQUENCY_MSEC_DEFAULT = "60000";           // 1 min
    public static final String WRITER_FLUSH_FREQUENCY_MSEC_DESCRIPTION =
            String.format("Flush frequency by time in milliseconds (default: %s)",
                    WRITER_FLUSH_FREQUENCY_MSEC_DEFAULT);

    /**
     * File format for writing data files to backing Iceberg store.
     */
    public static final String WRITER_FILE_FORMAT = "writerFileFormat";
    public static final String WRITER_FILE_FORMAT_DEFAULT = FileFormat.PARQUET.name();
    public static final String WRITER_FILE_FORMAT_DESCRIPTION =
            String.format("File format for writing data files to backing Iceberg store (default: %s)",
                    WRITER_FILE_FORMAT_DEFAULT);

    /**
     * Maximum number of writers that should exist per worker.
     */
    public static final String WRITER_MAXIMUM_POOL_SIZE = "writerMaximumPoolSize";

    public static final String WATERMARK_ENABLED = "watermarkEnabled";
    public static final int WRITER_MAXIMUM_POOL_SIZE_DEFAULT = 5;
    public static final String WRITER_MAXIMUM_POOL_SIZE_DESCRIPTION =
            String.format("Maximum number of writers that should exist per worker (default: %s)",
                    WRITER_MAXIMUM_POOL_SIZE_DEFAULT);
}
