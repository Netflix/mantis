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

package io.mantisrx.connector.iceberg.sink.committer.metrics;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;

public class CommitterMetrics {

    public static final String INVOCATION_COUNT = "invocationCount";
    private final Counter invocationCount;

    public static final String COMMIT_SUCCESS_COUNT = "commitSuccessCount";
    private final Counter commitSuccessCount;

    public static final String COMMIT_FAILURE_COUNT = "commitFailureCount";
    private final Counter commitFailureCount;

    public static final String COMMIT_LATENCY_MSEC = "commitLatencyMsec";
    private final Gauge commitLatencyMsec;

    public static final String COMMIT_BATCH_SIZE = "commitBatchSize";
    private final Gauge commitBatchSize;

    public CommitterMetrics() {
        Metrics metrics = new Metrics.Builder()
                .name(CommitterMetrics.class.getCanonicalName())
                .addCounter(INVOCATION_COUNT)
                .addCounter(COMMIT_SUCCESS_COUNT)
                .addCounter(COMMIT_FAILURE_COUNT)
                .addCounter(COMMIT_LATENCY_MSEC)
                .addCounter(COMMIT_BATCH_SIZE)
                .build();

        metrics = MetricsRegistry.getInstance().registerAndGet(metrics);

        invocationCount = metrics.getCounter(INVOCATION_COUNT);
        commitSuccessCount = metrics.getCounter(COMMIT_SUCCESS_COUNT);
        commitFailureCount = metrics.getCounter(COMMIT_FAILURE_COUNT);
        commitLatencyMsec = metrics.getGauge(COMMIT_LATENCY_MSEC);
        commitBatchSize = metrics.getGauge(COMMIT_BATCH_SIZE);
    }

    public void setGauge(final String metric, final long value) {
        switch (metric) {
            case COMMIT_LATENCY_MSEC:
                commitLatencyMsec.set(value);
                break;
            case COMMIT_BATCH_SIZE:
                commitBatchSize.set(value);
                break;
            default:
                break;
        }
    }

    public void increment(final String metric) {
        switch (metric) {
            case INVOCATION_COUNT:
                invocationCount.increment();
                break;
            case COMMIT_SUCCESS_COUNT:
                commitSuccessCount.increment();
                break;
            case COMMIT_FAILURE_COUNT:
                commitFailureCount.increment();
                break;
            default:
                break;
        }
    }

    public void increment(final String metric, final long value) {
        switch (metric) {
            case INVOCATION_COUNT:
                invocationCount.increment(value);
                break;
            case COMMIT_SUCCESS_COUNT:
                commitSuccessCount.increment(value);
                break;
            case COMMIT_FAILURE_COUNT:
                commitFailureCount.increment(value);
                break;
            case COMMIT_BATCH_SIZE:
                commitBatchSize.increment(value);
                break;
            default:
                break;
        }
    }
}
