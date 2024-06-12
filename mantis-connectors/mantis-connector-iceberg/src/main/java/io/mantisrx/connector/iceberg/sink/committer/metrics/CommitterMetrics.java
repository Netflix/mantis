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

package io.mantisrx.connector.iceberg.sink.committer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CommitterMetrics {

    public static final String INVOCATION_COUNT = "invocationCount";
    private final Counter invocationCount;

    public static final String COMMIT_SUCCESS_COUNT = "commitSuccessCount";
    private final Counter commitSuccessCount;

    public static final String COMMIT_FAILURE_COUNT = "commitFailureCount";
    private final Counter commitFailureCount;

    public static final String COMMIT_LATENCY_MSEC = "commitLatencyMsec";
    private final Timer commitLatencyMsec;

    public static final String COMMIT_BATCH_SIZE = "commitBatchSize";
    private final Gauge commitBatchSize;
    private AtomicLong commitBatchSizeValue = new AtomicLong(0);
    private final MeterRegistry meterRegistry;

    public CommitterMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        String groupName = CommitterMetrics.class.getCanonicalName();
        invocationCount = Counter.builder(groupName + "_" + INVOCATION_COUNT)
                .register(meterRegistry);
        commitSuccessCount = Counter.builder(groupName + "_" + COMMIT_SUCCESS_COUNT)
                .register(meterRegistry);
        commitFailureCount = Counter.builder(groupName + "_" + COMMIT_FAILURE_COUNT)
                .register(meterRegistry);
        commitLatencyMsec = Timer.builder(groupName + "_" + COMMIT_LATENCY_MSEC)
                .register(meterRegistry);
        commitBatchSize = Gauge.builder(groupName + "_" + COMMIT_BATCH_SIZE, commitBatchSizeValue::get)
                .register(meterRegistry);

    }

    public void setGauge(final String metric, final long value) {
        switch (metric) {
            case COMMIT_BATCH_SIZE:
                commitBatchSizeValue.set(value);
                break;
            default:
                break;
        }
    }

    public void record(final String metric, final long amount, TimeUnit unit) {
        switch (metric) {
            case COMMIT_LATENCY_MSEC:
                commitLatencyMsec.record(amount, unit);
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
                commitBatchSizeValue.addAndGet(value);
                break;
            default:
                break;
        }
    }
}
