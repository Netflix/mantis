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

package io.mantisrx.connector.iceberg.sink.writer;

import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;

import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.WorkerInfo;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for writing {@link Record}s to Iceberg via a HDFS-compatible backend.
 * For example, this class may be used with an S3 compatible filesystem library
 * which progressively uploads (multipart) to S3 on each write operation for
 * optimizing latencies.
 * <p>
 * Users have the flexibility to choose the semantics of opening, writing, and closing
 * this Writer, for example, closing the underlying appender after some number
 * of Bytes written and opening a new appender.
 */
public class DefaultIcebergWriter implements IcebergWriter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultIcebergWriter.class);

    private final Map<String, String> tableProperties = new HashMap<>();

    private final WriterConfig config;
    private final WorkerInfo workerInfo;

    private final Table table;
    private final PartitionSpec spec;

    private final FileFormat format;

    private final LocationProvider locationProvider;

    private FileAppender<Record> appender;
    private OutputFile file;
    private StructLike partitionKey;
    @Nullable
    private Long lowWatermark;

    public DefaultIcebergWriter(
            WriterConfig config,
            WorkerInfo workerInfo,
            Table table,
            LocationProvider locationProvider) {

        this.config = config;

        this.workerInfo = workerInfo;
        this.table = table;
        this.spec = table.spec();
        this.format = FileFormat.valueOf(config.getWriterFileFormat());

        this.locationProvider = locationProvider;

        this.tableProperties.putAll(table.properties());
        if (!this.tableProperties.containsKey(PARQUET_COMPRESSION)) {
            // ZSTD is the recommended default compression
            tableProperties.put(PARQUET_COMPRESSION, CompressionCodecName.ZSTD.name());
        }
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
        open(null);
    }

    /**
     * Opens a {@link FileAppender} using a {@link StructLike} partition key
     * for a specific {@link FileFormat}.
     * <p>
     * A filename is automatically generated for this appender.
     * <p>
     * Supports Parquet. Avro, Orc, and others unsupported.
     */
    @Override
    public void open(StructLike newPartitionKey) throws IOException {
        partitionKey = newPartitionKey;
        Path path = new Path(table.location(), generateFilename());
        String location = locationProvider.newDataLocation(path.toString());
        logger.info("opening new {} file appender {}", format, location);
        file = table.io().newOutputFile(path.toString());

        switch (format) {
            case PARQUET:
                appender = Parquet.write(file)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .setAll(tableProperties)
                        .overwrite()
                        .build();
                lowWatermark = null;
                break;
            case AVRO:
            default:
                throw new UnsupportedOperationException("Cannot write using an unsupported file format " + format);
        }
    }

    @Override
    public void write(MantisRecord record) {
        appender.add(record.getRecord());
        lowWatermark = minNullSafe(lowWatermark, record.getTimestamp());
    }

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
    public MantisDataFile close() throws IOException, UncheckedIOException {
        if (isClosed()) {
            return null;
        }

        // Calls to FileAppender#close can fail if the backing file system fails to close.
        // For example, this can happen for an S3-backed file system where it might fail
        // to GET the status of the file. The file would have already been closed.
        // Callers should open a new appender.
        try {
            appender.close();

            final DataFile dataFile = DataFiles.builder(spec)
                    .withPath(file.location())
                    .withInputFile(file.toInputFile())
                    .withFileSizeInBytes(appender.length())
                    .withPartition(spec.fields().size() == 0 ? null : partitionKey)
                    .withMetrics(appender.metrics())
                    .withSplitOffsets(appender.splitOffsets())
                    .build();

            return new MantisDataFile(dataFile, lowWatermark);
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
    public long length() throws UncheckedIOException {
        return appender == null ? 0 : appender.length();
    }

    /**
     * Returns the partition key for which this record is partitioned in an Iceberg table.
     *
     * @return StructLike for partitioned tables; null for unpartitioned tables
     */
    public StructLike getPartitionKey() {
        return partitionKey;
    }

    /**
     * Generate a Parquet filename with attributes which make it more friendly to determine
     * the source of the file. For example, if the caller exits unexpectedly and leaves
     * files in the system, it's possible to identify them through a recursive listing.
     */
    private String generateFilename() {
        return generateDataPath(
                generatePartitionPath(
                        format.addExtension(String.format("%s_%s_%s_%s_%s",
                                workerInfo.getJobId(),
                                workerInfo.getStageNumber(),
                                workerInfo.getWorkerIndex(),
                                workerInfo.getWorkerNumber(),
                                UUID.randomUUID()))));
    }

    private String generateDataPath(String partitionPath) {
        return String.format("data/%s", partitionPath);
    }

    private String generatePartitionPath(String filename) {
        if (spec.isUnpartitioned()) {
            return filename;
        }

        return String.format("/%s/%s", spec.partitionToPath(partitionKey), filename);
    }

    public static Long minNullSafe(@Nullable Long v1, @Nullable Long v2) {
        return compareNullSafe(v1, v2, Math::min);
    }

    public static Long maxNullSafe(@Nullable Long v1, @Nullable Long v2) {
        return compareNullSafe(v1, v2, Math::max);
    }

    private static Long compareNullSafe(
        @Nullable Long v1, @Nullable Long v2, BiFunction<Long, Long, Long> comparator) {
        if (v1 != null && v2 != null) {
            return comparator.apply(v1, v2);
        } else if (v1 != null) {
            return v1;
        } else {
            return v2;
        }
    }
}
