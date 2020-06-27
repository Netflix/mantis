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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.TestScheduler;

class IcebergWriterStageTest {

    private TestScheduler scheduler;
    private TestSubscriber<DataFile> subscriber;
    private IcebergWriterStage.Transformer transformer;
    private Catalog catalog;
    private Context context;
    private IcebergWriter writer;
    private Observable<DataFile> flow;

    @BeforeEach
    void setUp() throws IOException {
        this.scheduler = new TestScheduler();
        this.subscriber = new TestSubscriber<>();

        WriterConfig config = new WriterConfig(new Parameters(), mock(Configuration.class));
        this.writer = mock(IcebergWriter.class);
        when(this.writer.close()).thenReturn(mock(DataFile.class));
        when(this.writer.length()).thenReturn(Long.MAX_VALUE);
        this.transformer = new IcebergWriterStage.Transformer(config, this.writer);

        ServiceLocator serviceLocator = mock(ServiceLocator.class);
        when(serviceLocator.service(Configuration.class)).thenReturn(mock(Configuration.class));
        this.catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(this.catalog.loadTable(any())).thenReturn(table);
        when(serviceLocator.service(Catalog.class)).thenReturn(this.catalog);
        this.context = mock(Context.class);
        when(this.context.getParameters()).thenReturn(new Parameters());
        when(this.context.getServiceLocator()).thenReturn(serviceLocator);

        Observable<Record> source = Observable.interval(1, TimeUnit.SECONDS, scheduler)
                .map(i -> mock(Record.class));
        flow = source.compose(transformer);
    }

    @Test
    void shouldCloseOnRowGroupSizeThreshold() throws IOException {
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        subscriber.assertNoValues();
        subscriber.assertNoTerminalEvent();

        scheduler.advanceTimeBy(999, TimeUnit.SECONDS);
        subscriber.assertValueCount(1);

        scheduler.advanceTimeBy(1000, TimeUnit.SECONDS);
        subscriber.assertValueCount(2);

        verify(writer, times(2000)).write(any());
        verify(writer, times(2)).close();
    }

    @Test
    void shouldOpenWriterOnSubscribe() throws IOException {
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.triggerActions();
        subscriber.assertNoTerminalEvent();

        verify(writer).open();
    }

    @Test
    void shouldNoOpCloseWhenFailedToOpen() throws IOException {
        when(writer.isClosed()).thenReturn(true);
        doThrow(new IOException()).when(writer).open();
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.triggerActions();
        subscriber.assertError(RuntimeException.class);
        subscriber.assertTerminalEvent();

        verify(writer).open();
        verify(writer).isClosed();
        verify(writer, times(0)).close();
    }

    @Test
    void shouldContinueOnWriteFailure() {
        doThrow(new RuntimeException()).when(writer).write(any());
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        subscriber.assertNoTerminalEvent();

        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        subscriber.assertNoTerminalEvent();

        verify(writer, times(2)).write(any());
    }

    @Test
    void shouldCloseOnTerminate() throws IOException {
        Observable<Record> source = Observable.just(mock(Record.class));
        Observable<DataFile> flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.triggerActions();
        subscriber.assertNoErrors();
        subscriber.assertCompleted();

        verify(writer).open();
        verify(writer).write(any());
        verify(writer, times(1)).close();
    }

    @Test
    void shouldInitializeWithExistingTable() {
        IcebergWriterStage stage = new IcebergWriterStage("catalog", "database", "table");
        assertDoesNotThrow(() -> stage.init(context));
    }

    @Test
    void shouldFailToInitializeWithMissingTable() {
        when(catalog.loadTable(any())).thenThrow(new RuntimeException());
        IcebergWriterStage stage = new IcebergWriterStage("catalog", "database", "missing");
        assertThrows(RuntimeException.class, () -> stage.init(context));
    }
}