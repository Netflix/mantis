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

package io.mantisrx.connector.iceberg.sink.writer.pool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;

public interface IcebergWriterPool {

    void addWriter(StructLike partition);

    void openWriter(StructLike partition) throws IOException;

    void write(StructLike partition, Record record);

    DataFile close(StructLike partition) throws IOException, UncheckedIOException;

    List<DataFile> closeAll() throws IOException, UncheckedIOException;

    List<StructLike> getFlushableWriters();

    boolean isClosed(StructLike partition);

    boolean hasWriter(StructLike partition);

    boolean isWriterFlushable(StructLike partition) throws UncheckedIOException;
}
