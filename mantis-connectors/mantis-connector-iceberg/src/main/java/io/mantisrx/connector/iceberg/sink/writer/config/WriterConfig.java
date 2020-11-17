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

import static io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties.*;

import io.mantisrx.connector.iceberg.sink.config.SinkConfig;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.hadoop.conf.Configuration;

/**
 * Config for controlling Iceberg Writer semantics.
 */
public class WriterConfig extends SinkConfig {

    private final int writerRowGroupSize;
    private final long writerFlushFrequencyBytes;
    private final long writerFlushFrequencyMsec;
    private final String writerFileFormat;
    private final int writerMaximumPoolSize;
    private final Configuration hadoopConfig;

    /**
     * Creates an instance from {@link Parameters} derived from the current Mantis Stage's {@code Context}.
     */
    public WriterConfig(Parameters parameters, Configuration hadoopConfig) {
        super(parameters);
        this.writerRowGroupSize = (int) parameters.get(
                WRITER_ROW_GROUP_SIZE, WRITER_ROW_GROUP_SIZE_DEFAULT);
        this.writerFlushFrequencyBytes = Long.parseLong((String) parameters.get(
                WRITER_FLUSH_FREQUENCY_BYTES, WRITER_FLUSH_FREQUENCY_BYTES_DEFAULT));
        this.writerFlushFrequencyMsec = Long.parseLong((String) parameters.get(
                WRITER_FLUSH_FREQUENCY_MSEC, WRITER_FLUSH_FREQUENCY_MSEC_DEFAULT));
        this.writerFileFormat = (String) parameters.get(
                WRITER_FILE_FORMAT, WRITER_FILE_FORMAT_DEFAULT);
        this.writerMaximumPoolSize = (int) parameters.get(
                WRITER_MAXIMUM_POOL_SIZE, WRITER_MAXIMUM_POOL_SIZE_DEFAULT);
        this.hadoopConfig = hadoopConfig;
    }

    /**
     * Returns an int representing maximum number of rows that should exist in a file.
     */
    public int getWriterRowGroupSize() {
        return writerRowGroupSize;
    }

    /**
     * Returns a long representing flush frequency by size in Bytes.
     */
    public long getWriterFlushFrequencyBytes() {
        return writerFlushFrequencyBytes;
    }

    /**
     * Returns a long representing flush frequency by size in milliseconds.
     */
    public long getWriterFlushFrequencyMsec() {
        return writerFlushFrequencyMsec;
    }

    /**
     * Returns the file format for Iceberg writers.
     */
    public String getWriterFileFormat() {
        return writerFileFormat;
    }

    /**
     * Returns an int representing the maximum number of writers that should exist per worker.
     */
    public int getWriterMaximumPoolSize() {
        return writerMaximumPoolSize;
    }

    /**
     * Returns a Hadoop configuration which has metadata for how and where to write files.
     */
    public Configuration getHadoopConfig() {
        return hadoopConfig;
    }
}
