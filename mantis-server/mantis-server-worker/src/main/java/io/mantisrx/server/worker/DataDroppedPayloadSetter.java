/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.server.worker;

import static com.mantisrx.common.utils.MantisMetricStringConstants.DROP_OPERATOR_INCOMING_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.DATA_DROP_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.DROP_COUNT;
import static io.mantisrx.server.core.stats.MetricStringConstants.ON_NEXT_COUNT;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.mantisrx.server.core.StatusPayloads;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivx.mantis.operators.DropOperator;
import io.reactivx.mantis.operators.DropOperator.Counters;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataDroppedPayloadSetter implements Closeable {

    private static final String metricNamePrefix = DROP_OPERATOR_INCOMING_METRIC_GROUP;
    private static final Logger logger = LoggerFactory.getLogger(DataDroppedPayloadSetter.class);
    private final Heartbeat heartbeat;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledThreadPoolExecutor executor;
    private ScheduledFuture<?> future;

    private final Gauge dropCountGauge;
    private final AtomicLong dropCountValue = new AtomicLong(0);
    private final Gauge onNextCountGauge;
    private final AtomicLong onNextCountValue = new AtomicLong(0);
    private final MeterRegistry meterRegistry;


    DataDroppedPayloadSetter(Heartbeat heartbeat, MeterRegistry meterRegistry) {
        this.heartbeat = heartbeat;
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        executor = new ScheduledThreadPoolExecutor(1);
        this.meterRegistry = meterRegistry;
        dropCountGauge = Gauge.builder(DATA_DROP_METRIC_GROUP + "_" + DROP_COUNT, dropCountValue::get)
            .register(meterRegistry);
        onNextCountGauge = Gauge.builder(DATA_DROP_METRIC_GROUP + "_" + ON_NEXT_COUNT, dropCountValue::get)
            .register(meterRegistry);
    }

    protected void setPayload(final long intervalSecs) {
        long totalDropped = 0L;
        long totalOnNext = 0L;
        try {
            if (meterRegistry.getMeters().contains(dropCountGauge) && meterRegistry.getMeters().contains(onNextCountGauge)){
                //logger.info("Got " + metrics.size() + " metrics for DropOperator");
                final Counter dropped = meterRegistry.find("DropOperator_" + "" + DropOperator.Counters.dropped).counter();
                final Counter onNext = meterRegistry.find("DropOperator_" + "" + DropOperator.Counters.onNext).counter();
                if (dropped != null)
                    totalDropped += dropped.count();
                else
                    logger.warn("Unexpected to get null dropped counter for metric DropOperator DropOperator_dropped.");
                if (onNext != null)
                    totalOnNext += onNext.count();
                else
                    logger.warn("Unexpected to get null onNext counter for metric DropOperator_onNext.");

                final StatusPayloads.DataDropCounts dataDrop = new StatusPayloads.DataDropCounts(totalOnNext, totalDropped);
                try {
                    heartbeat.addSingleUsePayload("" + StatusPayloads.Type.IncomingDataDrop, objectMapper.writeValueAsString(dataDrop));
                } catch (JsonProcessingException e) {
                    logger.warn("Error writing json for dataDrop payload: " + e.getMessage());
                }
                dropCountValue.set(dataDrop.getDroppedCount());
                onNextCountValue.set(dataDrop.getOnNextCount());
            } else
                logger.debug("Got no metrics from DropOperator");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    void start(final long intervalSecs) {
        future =
            executor.scheduleAtFixedRate(
                () -> setPayload(intervalSecs),
                intervalSecs,
                intervalSecs,
                TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
        if (future != null) {
            future.cancel(false);
        }
        executor.shutdownNow();
    }
}
