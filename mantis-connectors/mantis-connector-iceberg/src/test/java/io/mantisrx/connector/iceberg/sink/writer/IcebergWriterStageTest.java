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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.iceberg.sink.StageOverrideParameters;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.factory.IcebergWriterFactory;
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.Partitioner;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.PartitionerFactory;
import io.mantisrx.connector.iceberg.sink.writer.pool.FixedIcebergWriterPool;
import io.mantisrx.connector.iceberg.sink.writer.pool.IcebergWriterPool;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.TestWorkerInfo;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameters;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
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

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private TestScheduler scheduler;
    private TestSubscriber<MantisDataFile> subscriber;
    private IcebergWriterStage.Transformer transformer;
    private Catalog catalog;
    private Context context;
    private IcebergWriterPool writerPool;
    private Partitioner partitioner;
    private Observable<MantisDataFile> flow;

    private MantisRecord record;

    @BeforeEach
    void setUp() {
        Record icebergRecord = GenericRecord.create(SCHEMA);
        icebergRecord.setField("id", 1);

        record = new MantisRecord(icebergRecord, null);
        this.scheduler = new TestScheduler();
        this.subscriber = new TestSubscriber<>();

        // Writer
        Parameters parameters = StageOverrideParameters.newParameters();
        WriterConfig config = new WriterConfig(parameters, mock(Configuration.class));
        WriterMetrics metrics = new WriterMetrics();
        IcebergWriterFactory factory = FakeIcebergWriter::new;

        this.writerPool = spy(new FixedIcebergWriterPool(
                factory,
                config.getWriterFlushFrequencyBytes(),
                config.getWriterMaximumPoolSize()));
        doReturn(Collections.singleton(record.getRecord())).when(writerPool).getFlushableWriters();

        this.partitioner = mock(Partitioner.class);
        when(partitioner.partition(icebergRecord)).thenReturn(icebergRecord);

        this.transformer = new IcebergWriterStage.Transformer(
                config,
                metrics,
                this.writerPool,
                this.partitioner,
                this.scheduler,
                this.scheduler);

        // Catalog
        ServiceLocator serviceLocator = mock(ServiceLocator.class);
        when(serviceLocator.service(Configuration.class)).thenReturn(mock(Configuration.class));

        this.catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        when(table.spec()).thenReturn(spec);
        when(this.catalog.loadTable(any())).thenReturn(table);
        when(serviceLocator.service(Catalog.class)).thenReturn(this.catalog);

        when(serviceLocator.service(PartitionerFactory.class)).thenReturn(mock(PartitionerFactory.class));

        // Mantis Context
        this.context = mock(Context.class);
        when(this.context.getParameters()).thenReturn(parameters);
        when(this.context.getServiceLocator()).thenReturn(serviceLocator);
        when(this.context.getWorkerInfo()).thenReturn(
            new TestWorkerInfo("testJobName", "jobId", 1, 1, 1, MantisJobDurationType.Perpetual,
                "host"));

        // Flow
        Observable<MantisRecord> source = Observable.interval(1, TimeUnit.MILLISECONDS, this.scheduler)
                .map(i -> record);
        this.flow = source.compose(this.transformer);
    }

    @Test
    void shouldAddWriterOnNewPartition() throws IOException {
        Record recordWithNewPartition = GenericRecord.create(SCHEMA);
        recordWithNewPartition.setField("id", 2);
        // Identity partitioning.
        when(partitioner.partition(recordWithNewPartition)).thenReturn(recordWithNewPartition);

        Observable<MantisRecord> source = Observable.just(record, record, new MantisRecord(recordWithNewPartition, null), record)
                .concatMap(r -> Observable.just(r).delay(1, TimeUnit.MILLISECONDS, scheduler));
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Same partition; no other thresholds (size, time) met.
        scheduler.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // New partition detected; no thresholds met yet.
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Existing partition detected; no thresholds met yet.
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        verify(writerPool, times(1)).open(record.getRecord());
        verify(writerPool, times(1)).open(recordWithNewPartition);
        verify(writerPool, times(3)).write(eq(record.getRecord()), any());
        verify(writerPool, times(1)).write(eq(recordWithNewPartition), any());
        verify(writerPool, times(0)).close(any());
    }

    @Test
    void shouldCloseOnSizeThreshold() throws IOException {
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        verify(writerPool, times(100)).write(any(), any());
        verify(writerPool, times(1)).close(record.getRecord());
    }

    @Test
    void shouldNotCloseWhenUnderSizeThreshold() throws IOException {
        doReturn(new HashSet<>()).when(writerPool).getFlushableWriters();
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Size is checked at row-group-size config, but under size-threshold, so no-op.
        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        subscriber.assertNoTerminalEvent();

        verify(writerPool, times(100)).write(eq(record.getRecord()), any());
        verify(writerPool, times(0)).close(any());
    }

    @Test
    void shouldCloseOnlyFlushableWritersOnSizeThreshold() throws IOException {
        Record recordWithNewPartition = GenericRecord.create(SCHEMA);
        when(partitioner.partition(recordWithNewPartition)).thenReturn(recordWithNewPartition);

        Observable<MantisRecord> source = Observable.just(record, new MantisRecord(recordWithNewPartition, null))
                .concatMap(r -> Observable.just(r).delay(1, TimeUnit.MILLISECONDS, scheduler))
                .repeat();
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        subscriber.assertNoTerminalEvent();

        verify(writerPool, times(101)).write(any(), any());
        verify(writerPool, times(1)).close(record.getRecord());
        verify(writerPool, times(0)).close(recordWithNewPartition);
    }

    @Test
    void shouldCloseAllWritersOnTimeThresholdWhenLowVolume() throws IOException {
        Record recordWithNewPartition = GenericRecord.create(SCHEMA);
        when(partitioner.partition(recordWithNewPartition)).thenReturn(recordWithNewPartition);
        doReturn(new HashSet<>()).when(writerPool).getFlushableWriters();
        // Low volume stream.
        Observable<MantisRecord> source = Observable.just(record, new MantisRecord(recordWithNewPartition, null))
                .concatMap(r -> Observable.just(r).delay(50, TimeUnit.MILLISECONDS, scheduler))
                .repeat();
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // Over the size threshold, but not yet checked at row-group-size config.
        scheduler.advanceTimeBy(50, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Hits time threshold and there's data to write; proceed to close.
        scheduler.advanceTimeBy(450, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(2);

        subscriber.assertNoTerminalEvent();

        verify(writerPool, times(10)).write(any(), any());
        verify(writerPool, times(2)).close(any());
    }

    @Test
    void shouldCloseAllWritersOnTimeThresholdWhenHighVolume() throws IOException {
        Record recordWithNewPartition = GenericRecord.create(SCHEMA);
        when(partitioner.partition(recordWithNewPartition)).thenReturn(recordWithNewPartition);
        doReturn(new HashSet<>()).when(writerPool).getFlushableWriters();
        Observable<MantisRecord> source = Observable.just(record, new MantisRecord(recordWithNewPartition, null))
                .concatMap(r -> Observable.just(r).delay(1, TimeUnit.MILLISECONDS, scheduler))
                .repeat();
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Size is checked at row-group-size config, but under size threshold, so no-op.
        scheduler.advanceTimeBy(99, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // Hits time threshold; proceed to close.
        scheduler.advanceTimeBy(400, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(2);

        subscriber.assertNoTerminalEvent();

        verify(writerPool, times(500)).write(any(), any());
        verify(writerPool, times(2)).close(any());
    }

    @Test
    void shouldNoOpOnTimeThresholdWhenNoData() throws IOException {
        doReturn(new HashSet<>()).when(writerPool).getFlushableWriters();
        // Low volume stream.
        Observable<MantisRecord> source = Observable.interval(900, TimeUnit.MILLISECONDS, scheduler)
                .map(i -> record);
        flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        // No event yet.
        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues();

        // 1 event, timer threshold met, size threshold not met: flush.
        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);

        // No event yet again, writer exists but is closed from previous flush, timer threshold met: noop.
        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        // Count should not increase.
        subscriber.assertValueCount(1);

        subscriber.assertNoErrors();
        subscriber.assertNoTerminalEvent();

        verify(writerPool, times(1)).open(any());
        verify(writerPool, times(1)).write(any(), any());
        // 2nd close is a noop.
        verify(writerPool, times(1)).close(any());
    }

    @Test
    void shouldNoOpWhenFailedToOpen() throws IOException {
        doThrow(new IOException()).when(writerPool).open(any());
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertError(RuntimeException.class);
        subscriber.assertTerminalEvent();

        verify(writerPool).open(any());
        subscriber.assertNoValues();
    }

    @Test
    void shouldContinueOnWriteFailure() {
        doThrow(new RuntimeException()).when(writerPool).write(any(), any());
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();

        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoTerminalEvent();

        verify(writerPool, times(2)).write(any(), any());
    }

    @Test
    @Disabled("Will never terminate: Source terminates, but timer will continue to tick")
    void shouldCloseOnTerminate() throws IOException {
        Observable<MantisRecord> source = Observable.just(record);
        Observable<MantisDataFile> flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.triggerActions();
        subscriber.assertNoErrors();

        verify(writerPool).open(record.getRecord());
        verify(writerPool).write(any(), any());
        verify(writerPool, times(2)).isClosed(record.getRecord());
        verify(writerPool, times(1)).close(record.getRecord());
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

        private static final DataFile DATA_FILE = new DataFiles.Builder(PartitionSpec.unpartitioned())
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
        public void write(MantisRecord record) {
        }

        @Override
        public MantisDataFile close() throws IOException {
            if (fileAppender != null) {
                fileAppender = null;
                return new MantisDataFile(DATA_FILE, null);
            }

            return null;
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
