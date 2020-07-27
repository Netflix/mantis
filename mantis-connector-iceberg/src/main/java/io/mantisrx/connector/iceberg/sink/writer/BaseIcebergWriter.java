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

package io.mantisrx.connector.iceberg.sink.writer;

import java.io.IOException;
import java.util.UUID;

import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.WorkerInfo;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for writing {@link Record}s to Iceberg via a HDFS-compatible backend.
 * For example, this class may be used with an S3 compatible filesystem library
 * which progressively uploads (multipart) to S3 on each write operation for
 * optimizing latencies.
 * <p>
 * Users have the flexibility to choose the semantics of opening, writing, and closing
 * this Writer, for example, closing the underlying appender after some number
 * of Bytes written and opening a new appender.
 */
public abstract class BaseIcebergWriter implements IcebergWriter {

    private static final Logger logger = LoggerFactory.getLogger(BaseIcebergWriter.class);

    private final WriterConfig config;
    private final WorkerInfo workerInfo;

    private final Table table;
    private final PartitionSpec spec;

    private final FileFormat format;

    private FileAppender<Record> appender;
    private OutputFile file;
    private StructLike key;

    public BaseIcebergWriter(WriterConfig config, WorkerInfo workerInfo, Table table) {
        this.config = config;

        this.table = table;
        this.spec = table.spec();
        this.workerInfo = workerInfo;
        this.format = FileFormat.valueOf(config.getWriterFileFormat());
    }

    /**
     * Opens a {@link FileAppender} for a specific {@link FileFormat}.
     * <p>
     * A filename is automatically generated for this appender.
     * <p>
     * Supports Parquet. Avro, Orc, and others unsupported.
     */
    @Override
    public void open() throws IOException {
        // TODO: Use key to generate part of the child path.
        String filename = generateFilename(workerInfo);
        String child = String.format("data/%s", filename);
        Path path = new Path(table.location(), child);
        logger.info("opening new {} file appender {}", format, path);
        file = HadoopOutputFile.fromPath(path, config.getHadoopConfig());

        switch (format) {
            case PARQUET:
                appender = Parquet.write(file)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .build();
                break;
            case AVRO:
            default:
                throw new UnsupportedOperationException("Cannot write using an unsupported file format " + format);
        }
    }

    @Override
    public abstract void write(Record record);

    /**
     * Closes the currently opened file appender and builds a DataFile.
     * <p>
     * Users are expected to {@link IcebergWriter#open()} a new file appender for this writer
     * if they want to continue writing. Users can check for status of the file appender
     * using {@link IcebergWriter#isClosed()}.
     *
     * @return a DataFile representing metadata about the records written.
     */
    @Override
    public DataFile close() throws IOException, RuntimeIOException {
        if (appender == null) {
            return null;
        }

        // Calls to FileAppender#close can fail if the backing file system fails to close.
        // For example, this can happen for an S3-backed file system where it might fail
        // to GET the status of the file.
        try {
            appender.close();

            return DataFiles.builder(spec)
                    .withPath(file.location())
                    .withInputFile(file.toInputFile())
                    .withFileSizeInBytes(appender.length())
                    .withPartition(spec.fields().size() == 0 ? null : key)
                    .withMetrics(appender.metrics())
                    .withSplitOffsets(appender.splitOffsets())
                    .build();
        } finally {
            appender = null;
            file = null;
        }
    }

    public boolean isClosed() {
        return appender == null;
    }

    /**
     * Returns the current file size (in Bytes) written using this writer's appender.
     * <p>
     * Users should be careful calling this method in a tight loop because it can
     * be expensive depending on the file format, for example in Parquet.
     *
     * @return current file size (in Bytes).
     */
    public long length() {
        return appender == null ? 0 : appender.length();
    }

    protected void writeRecord(Record record) {
        appender.add(record);
    }

    /**
     * Generate a Parquet filename with attributes which make it more friendly to determine
     * the source of the file. For example, if the caller exits unexpectedly and leaves
     * files in the system, it's possible to identify them through a recursive listing.
     */
    private String generateFilename(WorkerInfo workerInfo) {
        return format.addExtension(String.format("%s_%s_%s_%s_%s",
                workerInfo.getJobId(),
                workerInfo.getStageNumber(),
                workerInfo.getWorkerIndex(),
                workerInfo.getWorkerNumber(),
                UUID.randomUUID().toString()));
    }
}
