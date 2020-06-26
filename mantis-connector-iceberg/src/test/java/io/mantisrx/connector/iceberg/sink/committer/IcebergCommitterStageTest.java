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

package io.mantisrx.connector.iceberg.sink.committer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import rx.RxReactiveStreams;
import rx.schedulers.TestScheduler;

class IcebergCommitterStageTest {

    private CommitterConfig config;
    private Catalog catalog;
    private Context context;
    private IcebergCommitter committer;
    private TestScheduler scheduler;
    private IcebergCommitterStage.Transformer transformer;

    @BeforeEach
    void setUp() {
        this.config = new CommitterConfig(new Parameters());
        this.committer = mock(IcebergCommitter.class);
        this.scheduler = new TestScheduler();

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
    void shouldCommitPeriodically() {
        StepVerifier
                .withVirtualTime(() -> {
                    Flux<DataFile> upstream = Flux.interval(Duration.ofSeconds(1)).map(i -> mock(DataFile.class));
                    transformer = new IcebergCommitterStage.Transformer(config, committer, scheduler);
                    return RxReactiveStreams.toPublisher(RxReactiveStreams.toObservable(upstream).compose(transformer));
                })
                .expectSubscription()
                .then(() -> scheduler.advanceTimeBy(1, TimeUnit.MINUTES))
                .expectNoEvent(Duration.ofMinutes(1))
                .then(() -> scheduler.advanceTimeBy(4, TimeUnit.MINUTES))
                .thenAwait(Duration.ofMinutes(4))
                .expectNextCount(1)
                .then(() -> scheduler.advanceTimeBy(5, TimeUnit.MINUTES))
                .thenAwait(Duration.ofMinutes(5))
                .expectNextCount(1)
                .then(() -> scheduler.advanceTimeBy(4, TimeUnit.MINUTES))
                .expectNoEvent(Duration.ofMinutes(4))
                .thenCancel()
                .verify();

        verify(committer, times(2)).commit(any());
    }

    @Test
    void shouldInitializeWithExistingTable() {
        IcebergCommitterStage stage = new IcebergCommitterStage("catalog", "database", "table");
        assertDoesNotThrow(() -> stage.init(context));
    }

    @Test
    void shouldFailToInitializeWithMissingTable() {
        when(catalog.loadTable(any())).thenThrow(new RuntimeException());
        IcebergCommitterStage stage = new IcebergCommitterStage("catalog", "database", "missing");
        assertThrows(RuntimeException.class, () -> stage.init(context));
    }
}