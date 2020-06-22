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
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
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
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BaseIcebergWriter implements IcebergWriter {

    private static final Logger logger = LoggerFactory.getLogger(BaseIcebergWriter.class);

    private final WriterMetrics metrics;
    private final WriterConfig config;

    private final Table table;
    private final PartitionSpec spec;

    private final String filename;
    private final FileFormat format;

    private FileAppender<Record> appender;
    private OutputFile file;
    private StructLike key;

    public BaseIcebergWriter(
            WriterMetrics metrics,
            WriterConfig config,
            WorkerInfo workerInfo,
            Table table,
            PartitionSpec spec) {
        this.metrics = metrics;
        this.config = config;

        this.table = table;
        this.spec = spec;
        this.filename = generateFilename(workerInfo);
        this.format = FileFormat.valueOf(config.getWriterFileFormat());
    }

    /**
     *
     */
    @Override
    public void open() throws IOException {
        String child = String.format("data/%s/%s.parquet", key, filename);
        Path path = new Path(table.location(), child);
        file = HadoopOutputFile.fromPath(path, config.getHadoopConfig());

        switch(format) {
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
     *
     */
    @Override
    public DataFile close() throws IOException {
        appender.close();

        DataFile dataFile = DataFiles.builder(spec)
                .withPath(file.location())
                .withInputFile(file.toInputFile())
                .withFileSizeInBytes(appender.length())
                .withPartition(spec.fields().size() == 0 ? null : key)
                .withMetrics(appender.metrics())
                .withSplitOffsets(appender.splitOffsets())
                .build();

        appender = null;
        file = null;

        return dataFile;
    }

    public boolean isClosed() {
        return appender == null;
    }

    protected void writeRecord(Record record) {
        logger.info("writing record: {}", record);
        appender.add(record);
    }

    /**
     *
     */
    private String generateFilename(WorkerInfo workerInfo) {
        return String.format("%s_%s_%s_%s_%s.parquet",
                workerInfo.getJobId(),
                workerInfo.getStageNumber(),
                workerInfo.getWorkerIndex(),
                workerInfo.getWorkerNumber(),
                UUID.randomUUID().toString());
    }
}
