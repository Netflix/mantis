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

package io.mantisrx.connector.iceberg.sink.writer.pool;

import io.mantisrx.connector.iceberg.sink.writer.IcebergWriter;
import io.mantisrx.connector.iceberg.sink.writer.MantisDataFile;
import io.mantisrx.connector.iceberg.sink.writer.MantisRecord;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.factory.IcebergWriterFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;

/**
 * A service that delegates operations to {@link IcebergWriter}s.
 *
 * Writers can be added to the pool up to a maximum size, after which new writers will be rejected.
 */
public class FixedIcebergWriterPool implements IcebergWriterPool {

    private final IcebergWriterFactory factory;
    private final Map<StructLike, IcebergWriter> pool;
    private final long flushFrequencyBytes;
    private final int maximumPoolSize;

    public FixedIcebergWriterPool(IcebergWriterFactory factory, WriterConfig writerConfig) {
        this(factory, writerConfig.getWriterFlushFrequencyBytes(), writerConfig.getWriterMaximumPoolSize());
    }

    public FixedIcebergWriterPool(IcebergWriterFactory factory, long flushFrequencyBytes, int maximumPoolSize) {
        this.factory = factory;
        this.flushFrequencyBytes = flushFrequencyBytes;
        this.maximumPoolSize = maximumPoolSize;
        this.pool = new HashMap<>(this.maximumPoolSize);
    }

    @Override
    public void open(StructLike partition) throws IOException {
        if (pool.size() >= maximumPoolSize) {
            throw new IOException("problem opening writer; maximum writer pool size (" + maximumPoolSize + ") exceeded");
        }

        if (!isClosed(partition)) {
            return;
        }

        IcebergWriter writer = factory.newIcebergWriter();
        writer.open(partition);
        pool.put(partition, writer);
    }

    @Override
    public void write(StructLike partition, MantisRecord record) {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new RuntimeException("writer does not exist in writer pool");
        }
        writer.write(record);
    }

    @Override
    public MantisDataFile close(StructLike partition) throws IOException, UncheckedIOException {
        IcebergWriter writer = pool.get(partition);
        if (writer == null) {
            throw new RuntimeException("writer does not exist in writer pool");

        }
        try {
            return writer.close();
        } finally {
            pool.remove(partition);
        }
    }

    /**
     * Attempts to close all writers and produce {@link DataFile}s. If a writer is already closed, then it will
     * produce a {@code null} which will be excluded from the resulting list.
     */
    @Override
    public List<MantisDataFile> closeAll() throws IOException, UncheckedIOException {
        List<MantisDataFile> dataFiles = new ArrayList<>();
        for (StructLike partition : pool.keySet()) {
            MantisDataFile dataFile = close(partition);
            if (dataFile != null) {
                dataFiles.add(dataFile);
            }
        }

        return dataFiles;
    }

    /**
     * Returns a set of all writers in the pool.
     */
    @Override
    public Set<StructLike> getWriters() {
        return pool.keySet();
    }

    /**
     * Returns a set of writers whose lengths are greater than {@link WriterConfig#getWriterFlushFrequencyBytes()}.
     */
    @Override
    public Set<StructLike> getFlushableWriters() {
        return pool.entrySet().stream()
                .filter(entry -> entry.getValue().length() >= flushFrequencyBytes)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isClosed(StructLike partition) {
        return !pool.containsKey(partition);
    }
}
