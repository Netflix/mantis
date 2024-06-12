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

package io.mantisrx.connector.iceberg.sink.writer.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import java.util.concurrent.atomic.AtomicLong;

public class WriterMetrics {

    public static final String OPEN_SUCCESS_COUNT = "openSuccessCount";
    private final Counter openSuccessCount;

    public static final String OPEN_FAILURE_COUNT = "openFailureCount";
    private final Counter openFailureCount;

    public static final String WRITE_SUCCESS_COUNT = "writeSuccessCount";
    private final Counter writeSuccessCount;

    public static final String WRITE_FAILURE_COUNT = "writeFailureCount";
    private final Counter writeFailureCount;

    public static final String BATCH_SUCCESS_COUNT = "batchSuccessCount";
    private final Counter batchSuccessCount;

    public static final String BATCH_FAILURE_COUNT = "batchFailureCount";
    private final Counter batchFailureCount;

    public static final String BATCH_SIZE = "batchSize";
    private final Gauge batchSize;
    private AtomicLong batchSizeValue = new AtomicLong(0);

    public static final String BATCH_SIZE_BYTES = "batchSizeBytes";
    private final Gauge batchSizeBytes;
    private AtomicLong batchSizeBytesValue = new AtomicLong(0);
    private final MeterRegistry meterRegistry;

    public WriterMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        String groupName = WriterMetrics.class.getCanonicalName();
        openSuccessCount = Counter.builder(groupName + "_" + OPEN_SUCCESS_COUNT)
                .register(meterRegistry);
        openFailureCount = Counter.builder(groupName + "_" + OPEN_FAILURE_COUNT)
                .register(meterRegistry);
        writeSuccessCount = Counter.builder(groupName + "_" + WRITE_SUCCESS_COUNT)
                .register(meterRegistry);
        writeFailureCount = Counter.builder(groupName + "_" + WRITE_FAILURE_COUNT)
                .register(meterRegistry);
        batchSuccessCount = Counter.builder(groupName + "_" + BATCH_SUCCESS_COUNT)
                .register(meterRegistry);
        batchFailureCount = Counter.builder(groupName + "_" + BATCH_FAILURE_COUNT)
                .register(meterRegistry);
        batchSize = Gauge.builder(groupName + "_" + BATCH_SIZE, batchSizeValue::get)
                .register(meterRegistry);
        batchSizeBytes = Gauge.builder(groupName + "_" + BATCH_SIZE_BYTES, batchSizeBytesValue::get)
                .register(meterRegistry);
    }


    public void setGauge(final String metric, final long value) {
        switch (metric) {
            case BATCH_SIZE:
                batchSizeValue.set(value);
                break;
            case BATCH_SIZE_BYTES:
                batchSizeBytesValue.set(value);
                break;
            default:
                break;
        }
    }

    public void increment(final String metric) {
        switch (metric) {
            case OPEN_SUCCESS_COUNT:
                openSuccessCount.increment();
                break;
            case OPEN_FAILURE_COUNT:
                openFailureCount.increment();
                break;
            case WRITE_SUCCESS_COUNT:
                writeSuccessCount.increment();
                break;
            case WRITE_FAILURE_COUNT:
                writeFailureCount.increment();
                break;
            case BATCH_SUCCESS_COUNT:
                batchSuccessCount.increment();
                break;
            case BATCH_FAILURE_COUNT:
                batchFailureCount.increment();
                break;
            default:
                break;
        }
    }

    public void increment(final String metric, final long value) {
        switch (metric) {
            case OPEN_SUCCESS_COUNT:
                openSuccessCount.increment(value);
                break;
            case OPEN_FAILURE_COUNT:
                openFailureCount.increment(value);
                break;
            case WRITE_SUCCESS_COUNT:
                writeSuccessCount.increment(value);
                break;
            case WRITE_FAILURE_COUNT:
                writeFailureCount.increment(value);
                break;
            case BATCH_SUCCESS_COUNT:
                batchSuccessCount.increment(value);
                break;
            case BATCH_FAILURE_COUNT:
                batchFailureCount.increment(value);
                break;
            default:
                break;
        }
    }
}
