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

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;

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

    public static final String BATCH_SIZE_BYTES = "batchSizeBytes";
    private final Gauge batchSizeBytes;

    public WriterMetrics() {
        Metrics metrics = new Metrics.Builder()
                .name(WriterMetrics.class.getCanonicalName())
                .addCounter(OPEN_SUCCESS_COUNT)
                .addCounter(OPEN_FAILURE_COUNT)
                .addCounter(WRITE_SUCCESS_COUNT)
                .addCounter(WRITE_FAILURE_COUNT)
                .addCounter(BATCH_SUCCESS_COUNT)
                .addCounter(BATCH_FAILURE_COUNT)
                .addGauge(BATCH_SIZE)
                .addGauge(BATCH_SIZE_BYTES)
                .build();

        metrics = MetricsRegistry.getInstance().registerAndGet(metrics);

        openSuccessCount = metrics.getCounter(OPEN_SUCCESS_COUNT);
        openFailureCount = metrics.getCounter(OPEN_FAILURE_COUNT);
        writeSuccessCount = metrics.getCounter(WRITE_SUCCESS_COUNT);
        writeFailureCount = metrics.getCounter(WRITE_FAILURE_COUNT);
        batchSuccessCount = metrics.getCounter(BATCH_SUCCESS_COUNT);
        batchFailureCount = metrics.getCounter(BATCH_FAILURE_COUNT);
        batchSize = metrics.getGauge(BATCH_SIZE);
        batchSizeBytes = metrics.getGauge(BATCH_SIZE_BYTES);
    }

    public void setGauge(final String metric, final long value) {
        switch (metric) {
            case BATCH_SIZE:
                batchSize.set(value);
                break;
            case BATCH_SIZE_BYTES:
                batchSizeBytes.set(value);
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
