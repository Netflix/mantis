/*
 * Copyright 2021 Netflix, Inc.
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

import static io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties.WRITER_FILE_FORMAT_DEFAULT;
import static io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties.WRITER_FLUSH_FREQUENCY_BYTES_DEFAULT;
import static io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties.WRITER_MAXIMUM_POOL_SIZE_DEFAULT;
import static io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties.WRITER_ROW_GROUP_SIZE_DEFAULT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.iceberg.sink.writer.IcebergWriterStage.Transformer;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.connector.iceberg.sink.writer.factory.DefaultIcebergWriterFactory;
import io.mantisrx.connector.iceberg.sink.writer.factory.IcebergWriterFactory;
import io.mantisrx.connector.iceberg.sink.writer.metrics.WriterMetrics;
import io.mantisrx.connector.iceberg.sink.writer.partitioner.Partitioner;
import io.mantisrx.connector.iceberg.sink.writer.pool.FixedIcebergWriterPool;
import io.mantisrx.connector.iceberg.sink.writer.pool.IcebergWriterPool;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.TestWorkerInfo;
import io.mantisrx.runtime.WorkerInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.RegisterExtension;
import rx.Observable;
import rx.Subscription;

@Slf4j
public class IcebergWriterEndToEndTest {

    // records of the following shape
    // {
    //    "partition": 1,
    //    "id": 2
    // }
    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.required(1, "partition", Types.IntegerType.get()),
                    Types.NestedField.required(2, "id", Types.IntegerType.get()));

    private static final PartitionSpec SPEC =
            PartitionSpec.builderFor(SCHEMA).identity("partition").build();

    @RegisterExtension
    static IcebergTableExtension tableExtension =
            IcebergTableExtension.builder()
                    .schema(SCHEMA)
                    .spec(SPEC)
                    .build();

    private static final WorkerInfo WORKER_INFO =
            new TestWorkerInfo("testJobName", "jobId", 1, 1, 1, MantisJobDurationType.Perpetual,
                    "host");

    private Partitioner partitioner = record -> {
        GenericRecord partitionRecord = GenericRecord.create(SPEC.schema());
        partitionRecord.setField("partition", 1);
        return partitionRecord;
    };

    private Context stageContext = mock(Context.class);

    @BeforeEach
    public void initStageContext() {
        when(stageContext.getWorkerInfo()).thenReturn(WORKER_INFO);
    }


    @Disabled("flaky test; probably needs a higher value in sleep!")
    public void testTransformerEndToEnd() throws Exception {
        final WriterConfig writerConfig = new WriterConfig(
                tableExtension.getCatalog(),
                tableExtension.getDatabase(),
                tableExtension.getTableName(),
                WRITER_ROW_GROUP_SIZE_DEFAULT,
                Long.parseLong(WRITER_FLUSH_FREQUENCY_BYTES_DEFAULT),
                100L,
                WRITER_FILE_FORMAT_DEFAULT,
                WRITER_MAXIMUM_POOL_SIZE_DEFAULT,
                new Configuration()
        );

        final IcebergWriterFactory writerFactory =
                new DefaultIcebergWriterFactory(writerConfig, WORKER_INFO, tableExtension.getTable(),
                        tableExtension.getLocationProvider());

        final IcebergWriterPool writerPool = new FixedIcebergWriterPool(writerFactory, writerConfig);
        Transformer transformer =
                IcebergWriterStage.newTransformer(writerConfig, new WriterMetrics(), writerPool, partitioner,
                        WORKER_INFO, null);

        final int size = 1000;
        // odd numbers observable
        Observable<GenericRecord> oddObservable =
                Observable
                        .interval(0, 1, TimeUnit.MILLISECONDS)
                        .filter(val -> (val % 2 == 1))
                        .takeUntil(val -> val > size)
                        .map(val -> {
                            GenericRecord record = GenericRecord.create(SCHEMA);
                            record.setField("partition", 1);
                            record.setField("id", val);
                            return record;
                        })
                        .publish()
                        .autoConnect();

        // even numbers observable
        Observable<GenericRecord> evenObservable =
                Observable
                        .interval(size / 2, 1, TimeUnit.MILLISECONDS)
                        .filter(val -> (val % 2 == 0))
                        .takeUntil(val -> val + (size / 2) > size)
                        .map(val -> {
                            GenericRecord record = GenericRecord.create(SCHEMA);
                            record.setField("partition", 1);
                            record.setField("id", val);
                            return record;
                        })
                        .publish()
                        .autoConnect();

        Observable<Record> recordObservable = Observable.merge(oddObservable, evenObservable);

        Observable<MantisDataFile> dataFileObservable = transformer.call(recordObservable.map(s -> new MantisRecord(s, null)));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<MantisDataFile> dataFileList = new ArrayList<>();
        Subscription subscription = dataFileObservable.subscribe(dataFileList::add, failure::set);
        Thread.sleep(TimeUnit.SECONDS.toMillis(15));
        if (failure.get() != null) {
            throw new Exception(failure.get());
        }
        log.info("Collected {} data files successfully", dataFileList.size());
    }
}
