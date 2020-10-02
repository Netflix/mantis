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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.mantisrx.connector.iceberg.sink.StageOverrideParameters;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.Partitioner;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.PartitionerFactory;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.TestScheduler;

class IcebergWriterStageTest {

    private TestScheduler scheduler;
    private TestSubscriber<DataFile> subscriber;
    private IcebergWriterStage.Transformer transformer;
    private Catalog catalog;
    private Table table;
    private Context context;
    private IcebergWriter writer;
    private Partitioner partitioner;
    private Observable<DataFile> flow;

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private Record record;

    @BeforeEach
    void setUp() {
        record = GenericRecord.create(SCHEMA);
        record.setField("id", 1);
        this.scheduler = new TestScheduler();
        this.subscriber = new TestSubscriber<>();

        // Writer
        Parameters parameters = StageOverrideParameters.newParameters();
        WriterConfig config = new WriterConfig(parameters, mock(Configuration.class));
        WriterMetrics metrics = new WriterMetrics();
        this.writer = spy(FakeIcebergWriter.class);
        this.partitioner = mock(Partitioner.class);
        when(this.writer.length()).thenReturn(Long.MAX_VALUE);
        this.transformer = new IcebergWriterStage.Transformer(
                config,
                metrics,
                this.writer,
                this.partitioner,
                this.scheduler,
                this.scheduler);

        // Catalog
        ServiceLocator serviceLocator = mock(ServiceLocator.class);
        when(serviceLocator.service(Configuration.class)).thenReturn(mock(Configuration.class));
        this.catalog = mock(Catalog.class);
        this.table = mock(Table.class);
        when(this.table.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(this.catalog.loadTable(any())).thenReturn(this.table);
        when(serviceLocator.service(Catalog.class)).thenReturn(this.catalog);
        when(serviceLocator.service(PartitionerFactory.class)).thenReturn(mock(PartitionerFactory.class));

        // Mantis Context
        this.context = mock(Context.class);
        when(this.context.getParameters()).thenReturn(parameters);
        when(this.context.getServiceLocator()).thenReturn(serviceLocator);

        // Flow
        Observable<Record> source = Observable.interval(1, TimeUnit.MILLISECONDS, this.scheduler)
                .map(i -> record);
        this.flow = source.compose(this.transformer);
    }

    @Test
    void shouldCloseOnNewPartition() throws IOException {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        when(table.spec()).thenReturn(spec);
        when(catalog.loadTable(any())).thenReturn(table);
        Record recordWithNewPartition = GenericRecord.create(SCHEMA);
        recordWithNewPartition.setField("id", 2);
        // Identity partitioning.
        when(partitioner.partition(record)).thenReturn(record);
        when(partitioner.partition(recordWithNewPartition)).thenReturn(recordWithNewPartition);

        Observable<Record> source = Observable.just(record, record, recordWithNewPartition, record)
                .concatMap(r -> Observable.just(r).delay(1, TimeUnit.MILLISECONDS, scheduler));
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Same partition; no other thresholds (size, time) met.
        scheduler.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // New partition detected
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        // New partition detected
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(2);

        verify(writer, times(4)).write(any());
        // Two closes for [record, record] and [recordWithNewPartition]; a file is still open from the latest write.
        verify(writer, times(2)).close();
        verify(writer, times(3)).open(any());
    }

    @Test
    void shouldCloseOnSizeThreshold() throws IOException {
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Greater than size threshold, but not yet checked at row-group-size config.
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        scheduler.advanceTimeBy(99, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(2);

        subscriber.assertNoTerminalEvent();

        verify(writer, times(200)).write(any());
        verify(writer, times(2)).close();
    }

    @Test
    void shouldNotCloseWhenUnderSizeThreshold() throws IOException {
        when(writer.length()).thenReturn(1L);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Size is checked at row-group-size config, but under size-threshold, so no-op.
        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        subscriber.assertNoTerminalEvent();

        verify(writer, times(100)).write(any());
        verify(writer, times(0)).close();
    }

    @Test
    void shouldCloseWhenLowVolumeOnTimeThreshold() throws IOException {
        when(writer.length()).thenReturn(1L);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Size is checked at row-group-size config, but under size threshold, so no-op.
        scheduler.advanceTimeBy(999, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Hits time threshold; proceed to close.
        scheduler.advanceTimeBy(4000, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        subscriber.assertNoTerminalEvent();

        verify(writer, times(5000)).write(any());
        verify(writer, times(1)).close();
    }

    @Test
    void shouldCloseWhenHighVolumeOnTimeThreshold() throws IOException {
        Observable<Record> source = Observable.interval(500, TimeUnit.MILLISECONDS, scheduler)
                .map(i -> record);
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Over the size threshold, but not yet checked at row-group-size config.
        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Hits time threshold and there's data to write; proceed to close.
        scheduler.advanceTimeBy(4500, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        subscriber.assertNoTerminalEvent();

        verify(writer, times(10)).write(any());
        verify(writer, times(1)).close();
    }

    @Test
    void shouldNoOpWhenNoDataOnTimeThreshold() throws IOException {
        // Low volume stream.
        Observable<Record> source = Observable.interval(10_000, TimeUnit.MILLISECONDS, scheduler)
                .map(i -> record);
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(5000, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();
        subscriber.assertNoErrors();
        subscriber.assertNoTerminalEvent();

        verify(writer, times(0)).open(any());
        verify(writer, times(0)).write(any());
        verify(writer, times(1)).isClosed();
        verify(writer, times(0)).close();
    }

    @Test
    void shouldNoOpCloseWhenFailedToOpen() throws IOException {
        doThrow(new IOException()).when(writer).open(any());
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertError(RuntimeException.class);
        subscriber.assertTerminalEvent();

        verify(writer).open(any());
        verify(writer, times(1)).isClosed();
        verify(writer, times(1)).close();
    }

    @Test
    void shouldContinueOnWriteFailure() {
        doThrow(new RuntimeException()).when(writer).write(any());
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();

        verify(writer, times(2)).write(any());
    }

    @Test
    @Disabled("Will never terminate: Source terminates, but timer will continue to tick")
    void shouldCloseOnTerminate() throws IOException {
        Observable<Record> source = Observable.just(record);
        Observable<DataFile> flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.triggerActions();
        subscriber.assertNoErrors();

        verify(writer).open();
        verify(writer).write(any());
        verify(writer, times(2)).isClosed();
        verify(writer, times(1)).close();
    }

    @Test
    void shouldInitializeWithExistingTable() {
        IcebergWriterStage stage = new IcebergWriterStage();
        assertDoesNotThrow(() -> stage.init(context));
    }

    @Test
    void shouldFailToInitializeWithMissingTable() {
        when(catalog.loadTable(any())).thenThrow(new RuntimeException());
        IcebergWriterStage stage = new IcebergWriterStage();
        assertThrows(RuntimeException.class, () -> stage.init(context));
    }

    private static class FakeIcebergWriter implements IcebergWriter {

        private static final DataFile DATA_FILE = new DataFiles.Builder()
                .withPath("/datafile.parquet")
                .withFileSizeInBytes(1L)
                .withRecordCount(1L)
                .build();

        private final Object object;

        private Object fileAppender;
        private StructLike partitionKey;

        public FakeIcebergWriter() {
            this.object = new Object();
            this.fileAppender = null;
        }

        @Override
        public void open() throws IOException {
            open(null);
        }

        @Override
        public void open(StructLike newPartitionKey) throws IOException {
            fileAppender = object;
            partitionKey = newPartitionKey;
        }

        @Override
        public void write(Record record) {
        }

        @Override
        public DataFile close() throws IOException {
            fileAppender = null;
            return DATA_FILE;
        }

        @Override
        public boolean isClosed() {
            return fileAppender == null;
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public StructLike getPartitionKey() {
            return partitionKey;
        }
    }
}