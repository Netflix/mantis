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

package io.mantisrx.connector.iceberg.sink.committer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.iceberg.sink.StageOverrideParameters;
import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.connector.iceberg.sink.committer.metrics.CommitterMetrics;
import io.mantisrx.connector.iceberg.sink.writer.MantisDataFile;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.runtime.parameter.Parameters;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.TestScheduler;

class IcebergCommitterStageTest {

    private TestScheduler scheduler;
    private TestSubscriber<Map<String, Object>> subscriber;
    private Catalog catalog;
    private Context context;
    private IcebergCommitter committer;
    private IcebergCommitterStage.Transformer transformer;

    @BeforeEach
    void setUp() {
        this.scheduler = new TestScheduler();
        this.subscriber = new TestSubscriber<>();

        Parameters parameters = StageOverrideParameters.newParameters();
        CommitterConfig config = new CommitterConfig(parameters);
        CommitterMetrics metrics = new CommitterMetrics();
        this.committer = mock(IcebergCommitter.class);

        transformer = new IcebergCommitterStage.Transformer(config, metrics, committer, scheduler);

        ServiceLocator serviceLocator = mock(ServiceLocator.class);
        when(serviceLocator.service(Configuration.class)).thenReturn(mock(Configuration.class));
        this.catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(this.catalog.loadTable(any())).thenReturn(table);
        when(serviceLocator.service(Catalog.class)).thenReturn(this.catalog);
        this.context = mock(Context.class);
        when(this.context.getParameters()).thenReturn(parameters);
        when(this.context.getServiceLocator()).thenReturn(serviceLocator);
    }

    @Test
    void shouldCommitPeriodically() {
        Map<String, Object> summary = new HashMap<>();
        summary.put("test", "test");
        when(committer.commit(any())).thenReturn(summary);

        Observable<MantisDataFile> source = Observable.interval(1, TimeUnit.MINUTES, scheduler)
                .map(i -> new MantisDataFile(mock(DataFile.class), Long.MIN_VALUE));
        Observable<Map<String, Object>> flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        subscriber.assertNoValues();
        subscriber.assertNotCompleted();

        scheduler.advanceTimeBy(4, TimeUnit.MINUTES);
        subscriber.assertValueCount(1);

        scheduler.advanceTimeBy(5, TimeUnit.MINUTES);
        subscriber.assertValueCount(2);

        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        subscriber.assertValueCount(2);
        subscriber.assertNoErrors();

        verify(committer, times(2)).commit(any());
    }

    @Test
    void shouldContinueOnCommitFailure() {
        doThrow(new RuntimeException()).when(committer).commit(any());

        Observable<MantisDataFile> source = Observable.interval(1, TimeUnit.MINUTES, scheduler)
                .map(i -> new MantisDataFile(mock(DataFile.class), Long.MIN_VALUE));
        Observable<Map<String, Object>> flow = source.compose(transformer);
        flow.subscribeOn(scheduler).subscribe(subscriber);

        scheduler.advanceTimeBy(5, TimeUnit.MINUTES);
        subscriber.assertNoErrors();
        subscriber.assertNotCompleted();
        subscriber.assertValueCount(0);

        verify(committer).commit(any());
    }

    @Test
    void shouldInitializeWithExistingTable() {
        IcebergCommitterStage stage = new IcebergCommitterStage();
        assertDoesNotThrow(() -> stage.init(context));
    }

    @Test
    void shouldFailToInitializeWithMissingTable() {
        when(catalog.loadTable(any())).thenThrow(new RuntimeException());
        IcebergCommitterStage stage = new IcebergCommitterStage();
        assertThrows(RuntimeException.class, () -> stage.init(context));
    }
}
