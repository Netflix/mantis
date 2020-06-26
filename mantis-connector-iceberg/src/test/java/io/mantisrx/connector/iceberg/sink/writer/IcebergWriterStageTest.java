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
import java.time.Duration;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import rx.RxReactiveStreams;

class IcebergWriterStageTest {

    private IcebergWriterStage.Transformer transformer;
    private Catalog catalog;
    private Context context;
    private IcebergWriter writer;

    @BeforeEach
    void setUp() throws IOException {
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
    }

    @AfterEach
    void tearDown() {
        VirtualTimeScheduler.reset();
    }

    @Test
    void shouldCloseOnRowGroupSizeThreshold() throws IOException {
        StepVerifier
                .withVirtualTime(() -> {
                    Flux<Record> upstream = Flux.interval(Duration.ofSeconds(1)).map(i -> mock(Record.class));
                    return RxReactiveStreams.toPublisher(RxReactiveStreams.toObservable(upstream).compose(transformer));
                })
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))
                .thenAwait(Duration.ofSeconds(999))
                .expectNextCount(1)
                .thenAwait(Duration.ofSeconds(1000))
                .expectNextCount(1)
                .expectNoEvent(Duration.ofSeconds(1))
                .thenCancel()
                .verify();

        verify(writer, times(3)).open();
        verify(writer, times(2)).close();
    }

    @Test
    void shouldOpenWriterOnSubscribe() throws IOException {
        Flux<Record> upstream = Flux.just(mock(Record.class));
        Publisher<DataFile> flow =
                RxReactiveStreams.toPublisher(RxReactiveStreams.toObservable(upstream).compose(transformer));
        StepVerifier.create(flow)
                .expectSubscription()
                .verifyComplete();

        verify(writer).open();
    }

    @Test
    void shouldNoOpCloseWhenFailedToOpen() throws IOException {
        when(writer.isClosed()).thenReturn(true);
        doThrow(new IOException()).when(writer).open();
        Flux<Record> upstream = Flux.just(mock(Record.class));
        Publisher<DataFile> flow =
                RxReactiveStreams.toPublisher(RxReactiveStreams.toObservable(upstream).compose(transformer));
        StepVerifier.create(flow)
                .expectSubscription()
                .verifyError();

        verify(writer).open();
        verify(writer).isClosed();
        verify(writer, times(0)).close();
    }

    @Test
    void shouldCloseOnTerminate() throws IOException {
        doThrow(new RuntimeException()).when(writer).write(any());
        Flux<Record> upstream = Flux.just(mock(Record.class));
        Publisher<DataFile> flow =
                RxReactiveStreams.toPublisher(RxReactiveStreams.toObservable(upstream).compose(transformer));
        StepVerifier.create(flow)
                .expectSubscription()
                .verifyComplete();

        verify(writer).open();
        verify(writer).write(any());
        verify(writer).isClosed();
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